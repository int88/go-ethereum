// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package miner

import (
	"errors"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	mapset "github.com/deckarep/golang-set"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/trie"
)

const (
	// resultQueueSize is the size of channel listening to sealing result.
	resultQueueSize = 10

	// txChanSize is the size of channel listening to NewTxsEvent.
	// txChanSize是用于监听NewTxsEvent的channel的大小
	// The number is referenced from the size of tx pool.
	// 这个数字由tx pool的大小引用
	txChanSize = 4096

	// chainHeadChanSize is the size of channel listening to ChainHeadEvent.
	chainHeadChanSize = 10

	// chainSideChanSize is the size of channel listening to ChainSideEvent.
	// chainSideChanSize是用于监听ChainSideEvent的channel的大小
	chainSideChanSize = 10

	// resubmitAdjustChanSize is the size of resubmitting interval adjustment channel.
	resubmitAdjustChanSize = 10

	// sealingLogAtDepth is the number of confirmations before logging successful sealing.
	sealingLogAtDepth = 7

	// minRecommitInterval is the minimal time interval to recreate the sealing block with
	// any newly arrived transactions.
	// minRecommitInterval是最小的时间间隔用来重新创建sealing block，随着任何新到达的transactions
	minRecommitInterval = 1 * time.Second

	// maxRecommitInterval is the maximum time interval to recreate the sealing block with
	// any newly arrived transactions.
	// maxRecommitInterval是对于新达到的transactions重建sealing block的最大时间
	maxRecommitInterval = 15 * time.Second

	// intervalAdjustRatio is the impact a single interval adjustment has on sealing work
	// resubmitting interval.
	intervalAdjustRatio = 0.1

	// intervalAdjustBias is applied during the new resubmit interval calculation in favor of
	// increasing upper limit or decreasing lower limit so that the limit can be reachable.
	intervalAdjustBias = 200 * 1000.0 * 1000.0

	// staleThreshold is the maximum depth of the acceptable stale block.
	// staleThreshold是可以接受的最大深度的stale block
	staleThreshold = 7
)

var (
	// 在构建block的时候有新的head到达
	errBlockInterruptedByNewHead = errors.New("new head arrived while building block")
	// 在构建block的时候有recommit interrupt
	errBlockInterruptedByRecommit = errors.New("recommit interrupt while building block")
)

// environment is the worker's current environment and holds all
// information of the sealing block generation.
// environment是worker当前的执行环境并且拥有所有的信息关于当前正在封装的block
type environment struct {
	signer types.Signer

	state *state.StateDB // apply state changes here //在这里应用state changes
	// ancestor set，用于检查uncle parent的正确性
	ancestors mapset.Set // ancestor set (used for checking uncle parent validity)
	// family set，用于检查uncle invalidity
	family   mapset.Set    // family set (used for checking uncle invalidity)
	tcount   int           // tx count in cycle
	gasPool  *core.GasPool // available gas used to pack transactions	// 可用的gas，用于追踪transactions
	coinbase common.Address

	header   *types.Header
	txs      []*types.Transaction
	receipts []*types.Receipt
	uncles   map[common.Hash]*types.Header
}

// copy creates a deep copy of environment.
func (env *environment) copy() *environment {
	cpy := &environment{
		signer:    env.signer,
		state:     env.state.Copy(),
		ancestors: env.ancestors.Clone(),
		family:    env.family.Clone(),
		tcount:    env.tcount,
		coinbase:  env.coinbase,
		header:    types.CopyHeader(env.header),
		receipts:  copyReceipts(env.receipts),
	}
	if env.gasPool != nil {
		gasPool := *env.gasPool
		cpy.gasPool = &gasPool
	}
	// The content of txs and uncles are immutable, unnecessary
	// to do the expensive deep copy for them.
	cpy.txs = make([]*types.Transaction, len(env.txs))
	copy(cpy.txs, env.txs)
	cpy.uncles = make(map[common.Hash]*types.Header)
	for hash, uncle := range env.uncles {
		cpy.uncles[hash] = uncle
	}
	return cpy
}

// unclelist returns the contained uncles as the list format.
func (env *environment) unclelist() []*types.Header {
	var uncles []*types.Header
	for _, uncle := range env.uncles {
		uncles = append(uncles, uncle)
	}
	return uncles
}

// discard terminates the background prefetcher go-routine. It should
// always be called for all created environment instances otherwise
// the go-routine leak can happen.
// discard中止后台的prefetcher go-routine，它应该总是被所有创建的environment
// 实例调用，否则go-routine的泄露就可能发生
func (env *environment) discard() {
	if env.state == nil {
		return
	}
	env.state.StopPrefetcher()
}

// task contains all information for consensus engine sealing and result submitting.
// task包含所有的信息用于共识引擎sealing以及result submitting
type task struct {
	receipts  []*types.Receipt
	state     *state.StateDB
	block     *types.Block
	createdAt time.Time
}

const (
	commitInterruptNone int32 = iota
	commitInterruptNewHead
	commitInterruptResubmit
)

// newWorkReq represents a request for new sealing work submitting with relative interrupt notifier.
// newWorkReq代表一个request用于新的sealing work的提交，伴随着相关的interrupt notifier
type newWorkReq struct {
	interrupt *int32
	noempty   bool
	timestamp int64
}

// getWorkReq represents a request for getting a new sealing work with provided parameters.
// getWorkReq代表一个请求用于获取一个新的sealing work，用提供的参数
type getWorkReq struct {
	params *generateParams
	result chan *types.Block // non-blocking channel
	err    chan error
}

// intervalAdjust represents a resubmitting interval adjustment.
type intervalAdjust struct {
	ratio float64
	inc   bool
}

// worker is the main object which takes care of submitting new work to consensus engine
// and gathering the sealing result.
// worker是主要的对象，负责提交新的work到共识引擎并且收集sealing result
type worker struct {
	config      *Config
	chainConfig *params.ChainConfig
	engine      consensus.Engine
	eth         Backend
	chain       *core.BlockChain

	// Feeds
	pendingLogsFeed event.Feed

	// Subscriptions
	// 对于一系列对象的订阅
	mux          *event.TypeMux
	txsCh        chan core.NewTxsEvent
	txsSub       event.Subscription
	chainHeadCh  chan core.ChainHeadEvent
	chainHeadSub event.Subscription
	chainSideCh  chan core.ChainSideEvent
	chainSideSub event.Subscription

	// Channels
	newWorkCh          chan *newWorkReq
	getWorkCh          chan *getWorkReq
	taskCh             chan *task
	resultCh           chan *types.Block
	startCh            chan struct{}
	exitCh             chan struct{}
	resubmitIntervalCh chan time.Duration
	resubmitAdjustCh   chan *intervalAdjust

	wg sync.WaitGroup

	current *environment // An environment for current running cycle. // 当前运行周期的执行环境
	// 一系列本地产生的side blocks，作为可能的uncle blocks
	localUncles map[common.Hash]*types.Block // A set of side blocks generated locally as the possible uncle blocks.
	// 一系列的side blocks，可能作为uncle blocks
	remoteUncles map[common.Hash]*types.Block // A set of side blocks as the possible uncle blocks.
	unconfirmed  *unconfirmedBlocks           // A set of locally mined blocks pending canonicalness confirmations.

	mu       sync.RWMutex // The lock used to protect the coinbase and extra fields
	coinbase common.Address
	extra    []byte

	pendingMu    sync.RWMutex
	pendingTasks map[common.Hash]*task

	snapshotMu       sync.RWMutex // The lock used to protect the snapshots below
	snapshotBlock    *types.Block
	snapshotReceipts types.Receipts
	snapshotState    *state.StateDB

	// atomic status counters
	// 表明consensus engine是否在运行
	running int32 // The indicator whether the consensus engine is running or not.
	// 新来的transaction的数目，从上次的sealing work提交之后
	newTxs int32 // New arrival transaction count since last sealing work submitting.

	// noempty is the flag used to control whether the feature of pre-seal empty
	// block is enabled. The default value is false(pre-seal is enabled by default).
	// But in some special scenario the consensus engine will seal blocks instantaneously,
	// in this case this feature will add all empty blocks into canonical chain
	// non-stop and no real transaction will be included.
	// noempty是一个flag用于控制是否pre-seal一个空的block是否使能，默认的值为false（pre-seal默认打开）
	// 但是在一些特殊的场景下，共识引擎会瞬间的封装blocks，这种情况下，这个特性会添加所有空的blocks
	// 到canonical chain，而没有真正的transaction被包含
	noempty uint32

	// External functions
	isLocalBlock func(header *types.Header) bool // Function used to determine whether the specified block is mined by local miner.

	// Test hooks
	newTaskHook func(*task) // Method to call upon receiving a new sealing task.	// 当接收到一个新的sealing task时候的方法
	// 方法用于决定是否跳过sealing
	skipSealHook func(*task) bool // Method to decide whether skipping the sealing.
	// 方法用于调用在推送full sealing task之前
	fullTaskHook func() // Method to call before pushing the full sealing task.
	// 当更新resubmitting间隔的时候被调用
	resubmitHook func(time.Duration, time.Duration) // Method to call upon updating resubmitting interval.
}

func newWorker(config *Config, chainConfig *params.ChainConfig, engine consensus.Engine, eth Backend, mux *event.TypeMux, isLocalBlock func(header *types.Header) bool, init bool) *worker {
	worker := &worker{
		config:             config,
		chainConfig:        chainConfig,
		engine:             engine,
		eth:                eth,
		mux:                mux,
		chain:              eth.BlockChain(),
		isLocalBlock:       isLocalBlock,
		localUncles:        make(map[common.Hash]*types.Block),
		remoteUncles:       make(map[common.Hash]*types.Block),
		unconfirmed:        newUnconfirmedBlocks(eth.BlockChain(), sealingLogAtDepth),
		pendingTasks:       make(map[common.Hash]*task),
		txsCh:              make(chan core.NewTxsEvent, txChanSize),
		chainHeadCh:        make(chan core.ChainHeadEvent, chainHeadChanSize),
		chainSideCh:        make(chan core.ChainSideEvent, chainSideChanSize),
		newWorkCh:          make(chan *newWorkReq),
		getWorkCh:          make(chan *getWorkReq),
		taskCh:             make(chan *task),
		resultCh:           make(chan *types.Block, resultQueueSize),
		exitCh:             make(chan struct{}),
		startCh:            make(chan struct{}, 1),
		resubmitIntervalCh: make(chan time.Duration),
		resubmitAdjustCh:   make(chan *intervalAdjust, resubmitAdjustChanSize),
	}
	// Subscribe NewTxsEvent for tx pool
	// 从tx pool订阅NewTxsEvent
	worker.txsSub = eth.TxPool().SubscribeNewTxsEvent(worker.txsCh)
	// Subscribe events for blockchain
	// 订阅blockchain的events
	worker.chainHeadSub = eth.BlockChain().SubscribeChainHeadEvent(worker.chainHeadCh)
	worker.chainSideSub = eth.BlockChain().SubscribeChainSideEvent(worker.chainSideCh)

	// Sanitize recommit interval if the user-specified one is too short.
	// 检查recommit interval，如果用户指定的太短了
	recommit := worker.config.Recommit
	if recommit < minRecommitInterval {
		// 当有新的tx来的时候进行recommit
		log.Warn("Sanitizing miner recommit interval", "provided", recommit, "updated", minRecommitInterval)
		recommit = minRecommitInterval
	}

	worker.wg.Add(4)
	// 创建多个goroutine运行
	go worker.mainLoop()
	go worker.newWorkLoop(recommit)
	go worker.resultLoop()
	go worker.taskLoop()

	// Submit first work to initialize pending state.
	// 提交first work来初始化pending state
	if init {
		worker.startCh <- struct{}{}
	}
	return worker
}

// setEtherbase sets the etherbase used to initialize the block coinbase field.
func (w *worker) setEtherbase(addr common.Address) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.coinbase = addr
}

func (w *worker) setGasCeil(ceil uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.config.GasCeil = ceil
}

// setExtra sets the content used to initialize the block extra field.
func (w *worker) setExtra(extra []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.extra = extra
}

// setRecommitInterval updates the interval for miner sealing work recommitting.
func (w *worker) setRecommitInterval(interval time.Duration) {
	select {
	case w.resubmitIntervalCh <- interval:
	case <-w.exitCh:
	}
}

// disablePreseal disables pre-sealing feature
func (w *worker) disablePreseal() {
	atomic.StoreUint32(&w.noempty, 1)
}

// enablePreseal enables pre-sealing feature
func (w *worker) enablePreseal() {
	atomic.StoreUint32(&w.noempty, 0)
}

// pending returns the pending state and corresponding block.
func (w *worker) pending() (*types.Block, *state.StateDB) {
	// return a snapshot to avoid contention on currentMu mutex
	w.snapshotMu.RLock()
	defer w.snapshotMu.RUnlock()
	if w.snapshotState == nil {
		return nil, nil
	}
	return w.snapshotBlock, w.snapshotState.Copy()
}

// pendingBlock returns pending block.
func (w *worker) pendingBlock() *types.Block {
	// return a snapshot to avoid contention on currentMu mutex
	w.snapshotMu.RLock()
	defer w.snapshotMu.RUnlock()
	return w.snapshotBlock
}

// pendingBlockAndReceipts returns pending block and corresponding receipts.
func (w *worker) pendingBlockAndReceipts() (*types.Block, types.Receipts) {
	// return a snapshot to avoid contention on currentMu mutex
	w.snapshotMu.RLock()
	defer w.snapshotMu.RUnlock()
	return w.snapshotBlock, w.snapshotReceipts
}

// start sets the running status as 1 and triggers new work submitting.
// start设置running status为1并且触发新的work提交
func (w *worker) start() {
	atomic.StoreInt32(&w.running, 1)
	w.startCh <- struct{}{}
}

// stop sets the running status as 0.
func (w *worker) stop() {
	atomic.StoreInt32(&w.running, 0)
}

// isRunning returns an indicator whether worker is running or not.
func (w *worker) isRunning() bool {
	return atomic.LoadInt32(&w.running) == 1
}

// close terminates all background threads maintained by the worker.
// Note the worker does not support being closed multiple times.
func (w *worker) close() {
	atomic.StoreInt32(&w.running, 0)
	close(w.exitCh)
	w.wg.Wait()
}

// recalcRecommit recalculates the resubmitting interval upon feedback.
func recalcRecommit(minRecommit, prev time.Duration, target float64, inc bool) time.Duration {
	var (
		prevF = float64(prev.Nanoseconds())
		next  float64
	)
	if inc {
		next = prevF*(1-intervalAdjustRatio) + intervalAdjustRatio*(target+intervalAdjustBias)
		max := float64(maxRecommitInterval.Nanoseconds())
		if next > max {
			next = max
		}
	} else {
		next = prevF*(1-intervalAdjustRatio) + intervalAdjustRatio*(target-intervalAdjustBias)
		min := float64(minRecommit.Nanoseconds())
		if next < min {
			next = min
		}
	}
	return time.Duration(int64(next))
}

// newWorkLoop is a standalone goroutine to submit new sealing work upon received events.
// newWorkLoop是一个独立的goroutine，用于在接收到events之后提交新的sealing work
func (w *worker) newWorkLoop(recommit time.Duration) {
	defer w.wg.Done()
	var (
		interrupt   *int32
		minRecommit = recommit // minimal resubmit interval specified by user.
		timestamp   int64      // timestamp for each round of sealing.
	)

	timer := time.NewTimer(0)
	defer timer.Stop()
	<-timer.C // discard the initial tick

	// commit aborts in-flight transaction execution with given signal and resubmits a new one.
	// commit终止in-flight transaction的执行，用给定的signal并且提交一个新的
	commit := func(noempty bool, s int32) {
		if interrupt != nil {
			atomic.StoreInt32(interrupt, s)
		}
		interrupt = new(int32)
		select {
		// 构建新的worker request，真是开始封装block，进行mining
		case w.newWorkCh <- &newWorkReq{interrupt: interrupt, noempty: noempty, timestamp: timestamp}:
		case <-w.exitCh:
			return
		}
		timer.Reset(recommit)
		atomic.StoreInt32(&w.newTxs, 0)
	}
	// clearPending cleans the stale pending tasks.
	// clearPending清理过去的pending tasks
	clearPending := func(number uint64) {
		w.pendingMu.Lock()
		for h, t := range w.pendingTasks {
			if t.block.NumberU64()+staleThreshold <= number {
				delete(w.pendingTasks, h)
			}
		}
		w.pendingMu.Unlock()
	}

	for {
		select {
		case <-w.startCh:
			// worker开始运行
			log.Info("newWorkLoop worker start")
			clearPending(w.chain.CurrentBlock().NumberU64())
			timestamp = time.Now().Unix()
			// 直接开始commit?可以为empty
			commit(false, commitInterruptNewHead)

		case head := <-w.chainHeadCh:
			// 发生了chain head事件
			clearPending(head.Block.NumberU64())
			timestamp = time.Now().Unix()
			// 当chain head事件发生的时候，也进行commit
			commit(false, commitInterruptNewHead)

		case <-timer.C:
			log.Info("newWorkLoop timer is triggered")
			// If sealing is running resubmit a new work cycle periodically to pull in
			// higher priced transactions. Disable this overhead for pending blocks.
			// 如果sealing正在运行resubmit，一个新的work cycle周期性的运行来拉取更高加个的transaction
			// 对于pending blocks禁止这个损耗
			if w.isRunning() && (w.chainConfig.Clique == nil || w.chainConfig.Clique.Period > 0) {
				// Short circuit if no new transaction arrives.
				// 如果没有新的transaction到来，则短路
				if atomic.LoadInt32(&w.newTxs) == 0 {
					// 重置recommit的时间
					log.Info("newWorkLoop reset recommit timer")
					timer.Reset(recommit)
					continue
				}
				commit(true, commitInterruptResubmit)
			}

		case interval := <-w.resubmitIntervalCh:
			// Adjust resubmit interval explicitly by user.
			// 用户显式地调整resubmit interval
			if interval < minRecommitInterval {
				log.Warn("Sanitizing miner recommit interval", "provided", interval, "updated", minRecommitInterval)
				interval = minRecommitInterval
			}
			log.Info("Miner recommit interval update", "from", minRecommit, "to", interval)
			minRecommit, recommit = interval, interval

			if w.resubmitHook != nil {
				w.resubmitHook(minRecommit, recommit)
			}

		case adjust := <-w.resubmitAdjustCh:
			// Adjust resubmit interval by feedback.
			// 通过feedback调整resubmit interval
			if adjust.inc {
				before := recommit
				target := float64(recommit.Nanoseconds()) / adjust.ratio
				recommit = recalcRecommit(minRecommit, recommit, target, true)
				log.Trace("Increase miner recommit interval", "from", before, "to", recommit)
			} else {
				before := recommit
				recommit = recalcRecommit(minRecommit, recommit, float64(minRecommit.Nanoseconds()), false)
				log.Trace("Decrease miner recommit interval", "from", before, "to", recommit)
			}

			if w.resubmitHook != nil {
				w.resubmitHook(minRecommit, recommit)
			}

		case <-w.exitCh:
			return
		}
	}
}

// mainLoop is responsible for generating and submitting sealing work based on
// the received event. It can support two modes: automatically generate task and
// submit it or return task according to given parameters for various proposes.
// mainLoop负责生成并且提交sealing work基于接收到的事件，它可以支持两种模式：自动生成task
// 并且提交它，或者返回task更具给定的参数用于各种提议
func (w *worker) mainLoop() {
	defer w.wg.Done()
	defer w.txsSub.Unsubscribe()
	defer w.chainHeadSub.Unsubscribe()
	defer w.chainSideSub.Unsubscribe()
	defer func() {
		if w.current != nil {
			w.current.discard()
		}
	}()

	cleanTicker := time.NewTicker(time.Second * 10)
	defer cleanTicker.Stop()

	for {
		select {
		case req := <-w.newWorkCh:
			w.commitWork(req.interrupt, req.noempty, req.timestamp)

		case req := <-w.getWorkCh:
			// 真正创建block
			block, err := w.generateWork(req.params)
			if err != nil {
				req.err <- err
				req.result <- nil
			} else {
				req.err <- nil
				req.result <- block
			}
		case ev := <-w.chainSideCh:
			// Short circuit for duplicate side blocks
			// 对于重复的side blocks，直接短路
			if _, exist := w.localUncles[ev.Block.Hash()]; exist {
				continue
			}
			if _, exist := w.remoteUncles[ev.Block.Hash()]; exist {
				continue
			}
			// Add side block to possible uncle block set depending on the author.
			// 添加side block到可能的uncle block set，基于author
			if w.isLocalBlock != nil && w.isLocalBlock(ev.Block.Header()) {
				w.localUncles[ev.Block.Hash()] = ev.Block
			} else {
				w.remoteUncles[ev.Block.Hash()] = ev.Block
			}
			// If our sealing block contains less than 2 uncle blocks,
			// add the new uncle block if valid and regenerate a new
			// sealing block for higher profit.
			// 如果我们的sealing block包含少于2个uncle blocks，添加新的uncle block
			// 如果合法的话并且重新生成一个新的sealing block，为了更高的profit
			if w.isRunning() && w.current != nil && len(w.current.uncles) < 2 {
				start := time.Now()
				// 对uncle进行commit
				if err := w.commitUncle(w.current, ev.Block.Header()); err == nil {
					w.commit(w.current.copy(), nil, true, start)
				}
			}

		case <-cleanTicker.C:
			// 每十秒清理一次
			chainHead := w.chain.CurrentBlock()
			// 不在stale threshold之内的，都删除
			for hash, uncle := range w.localUncles {
				if uncle.NumberU64()+staleThreshold <= chainHead.NumberU64() {
					delete(w.localUncles, hash)
				}
			}
			for hash, uncle := range w.remoteUncles {
				if uncle.NumberU64()+staleThreshold <= chainHead.NumberU64() {
					delete(w.remoteUncles, hash)
				}
			}

		case ev := <-w.txsCh:
			// 订阅来自tx pool的tx
			// Apply transactions to the pending state if we're not sealing
			// 添加transactions到pending state，如果我们不在sealing
			//
			// Note all transactions received may not be continuous with transactions
			// already included in the current sealing block. These transactions will
			// be automatically eliminated.
			// 注意所有收到的transactions可能和已经包含在当前sealing block的transactions是不连续的
			// 这些transactions会被自动淘汰
			if !w.isRunning() && w.current != nil {
				// If block is already full, abort
				// 如果block已经满了，退出
				if gp := w.current.gasPool; gp != nil && gp.Gas() < params.TxGas {
					// gas已经满了
					continue
				}
				txs := make(map[common.Address]types.Transactions)
				for _, tx := range ev.Txs {
					acc, _ := types.Sender(w.current.signer, tx)
					txs[acc] = append(txs[acc], tx)
				}
				txset := types.NewTransactionsByPriceAndNonce(w.current.signer, txs, w.current.header.BaseFee)
				tcount := w.current.tcount
				// 提交transactions
				w.commitTransactions(w.current, txset, nil)

				// Only update the snapshot if any new transactions were added
				// to the pending block
				// 只有任何新的transactions被加入到pending block的时候才更新snapshot
				if tcount != w.current.tcount {
					w.updateSnapshot(w.current)
				}
			} else {
				// Special case, if the consensus engine is 0 period clique(dev mode),
				// submit sealing work here since all empty submission will be rejected
				// by clique. Of course the advance sealing(empty submission) is disabled.
				if w.chainConfig.Clique != nil && w.chainConfig.Clique.Period == 0 {
					w.commitWork(nil, true, time.Now().Unix())
				}
			}
			atomic.AddInt32(&w.newTxs, int32(len(ev.Txs)))

		// System stopped
		// 系统停止了
		case <-w.exitCh:
			return
		case <-w.txsSub.Err():
			return
		case <-w.chainHeadSub.Err():
			return
		case <-w.chainSideSub.Err():
			return
		}
	}
}

// taskLoop is a standalone goroutine to fetch sealing task from the generator and
// push them to consensus engine.
// taskLoop是一个独立的goroutine，用于从generator获取sealing task并且把它们推向consensus engine
func (w *worker) taskLoop() {
	defer w.wg.Done()
	var (
		stopCh chan struct{}
		prev   common.Hash
	)

	// interrupt aborts the in-flight sealing task.
	interrupt := func() {
		if stopCh != nil {
			close(stopCh)
			stopCh = nil
		}
	}
	for {
		select {
		case task := <-w.taskCh:
			if w.newTaskHook != nil {
				// 运行task hook
				w.newTaskHook(task)
			}
			// Reject duplicate sealing work due to resubmitting.
			// 拒绝因为重复提交的产生的重复的sealing work，根据block的header
			sealHash := w.engine.SealHash(task.block.Header())
			if sealHash == prev {
				// 如果和之前的sealHash一样，则continue
				continue
			}
			// Interrupt previous sealing operation
			// 中断之前的sealing操作
			interrupt()
			stopCh, prev = make(chan struct{}), sealHash

			// 是否跳过seal
			if w.skipSealHook != nil && w.skipSealHook(task) {
				continue
			}
			w.pendingMu.Lock()
			// 放在pending task中
			w.pendingTasks[sealHash] = task
			w.pendingMu.Unlock()

			// 调用执行引擎，最后会通过result ch传输结果
			if err := w.engine.Seal(w.chain, task.block, w.resultCh, stopCh); err != nil {
				log.Warn("Block sealing failed", "err", err)
				w.pendingMu.Lock()
				delete(w.pendingTasks, sealHash)
				w.pendingMu.Unlock()
			}
		case <-w.exitCh:
			interrupt()
			return
		}
	}
}

// resultLoop is a standalone goroutine to handle sealing result submitting
// and flush relative data to the database.
// resultLoop是一个独立的goroutine用于处理sealing result的提交并且flush相关的数据
// 到数据库中
func (w *worker) resultLoop() {
	defer w.wg.Done()
	for {
		select {
		case block := <-w.resultCh:
			// Short circuit when receiving empty result.
			// 当收到空的结果，则直接跳过
			if block == nil {
				continue
			}
			log.Info("resultLoop get block from w.resultCh")
			// Short circuit when receiving duplicate result caused by resubmitting.
			// 由重复提交导致收到了重复的结果
			if w.chain.HasBlock(block.Hash(), block.NumberU64()) {
				continue
			}
			var (
				sealhash = w.engine.SealHash(block.Header())
				hash     = block.Hash()
			)
			w.pendingMu.RLock()
			// 根据sealHash获取task
			task, exist := w.pendingTasks[sealhash]
			w.pendingMu.RUnlock()
			if !exist {
				// 找到了block但是没有相关的pending task
				log.Error("Block found but no relative pending task", "number", block.Number(), "sealhash", sealhash, "hash", hash)
				continue
			}
			// Different block could share same sealhash, deep copy here to prevent write-write conflict.
			// 不同的block可以共享同样的sealhash，深拷贝来避免write-write conflict
			var (
				receipts = make([]*types.Receipt, len(task.receipts))
				logs     []*types.Log
			)
			// 遍历receipts
			for i, taskReceipt := range task.receipts {
				receipt := new(types.Receipt)
				receipts[i] = receipt
				*receipt = *taskReceipt

				// add block location fields
				// 添加block的location字段
				receipt.BlockHash = hash
				receipt.BlockNumber = block.Number()
				receipt.TransactionIndex = uint(i)

				// Update the block hash in all logs since it is now available and not when the
				// receipt/log of individual transactions were created.
				// 在所有的logs中更新block hash，因为它现在可用，不是在单个的transactions创建时候的receipts/log
				receipt.Logs = make([]*types.Log, len(taskReceipt.Logs))
				for i, taskLog := range taskReceipt.Logs {
					log := new(types.Log)
					receipt.Logs[i] = log
					*log = *taskLog
					log.BlockHash = hash
				}
				logs = append(logs, receipt.Logs...)
			}
			// Commit block and state to database.
			// 提交block以及state到database
			_, err := w.chain.WriteBlockAndSetHead(block, receipts, logs, task.state, true)
			if err != nil {
				log.Error("Failed writing block to chain", "err", err)
				continue
			}
			// 成功封装一个新的block，
			log.Info("Successfully sealed new block", "number", block.Number(), "sealhash", sealhash, "hash", hash,
				"elapsed", common.PrettyDuration(time.Since(task.createdAt)))

			// Broadcast the block and announce chain insertion event
			// 广播block并且通知chain insertion event
			w.mux.Post(core.NewMinedBlockEvent{Block: block})

			// Insert the block into the set of pending ones to resultLoop for confirmations
			// 将block插入pending集合，在resultLoop中用于确认
			w.unconfirmed.Insert(block.NumberU64(), block.Hash())

		case <-w.exitCh:
			return
		}
	}
}

// makeEnv creates a new environment for the sealing block.
// makeEnv对于sealing block创建一个新的environment
func (w *worker) makeEnv(parent *types.Block, header *types.Header, coinbase common.Address) (*environment, error) {
	// Retrieve the parent state to execute on top and start a prefetcher for
	// the miner to speed block sealing up a bit.
	// 获取parent state在它之上执行并且启动一个prefetcher，用于miner来加速block sealing
	state, err := w.chain.StateAt(parent.Root())
	if err != nil {
		// Note since the sealing block can be created upon the arbitrary parent
		// block, but the state of parent block may already be pruned, so the necessary
		// state recovery is needed here in the future.
		//
		// The maximum acceptable reorg depth can be limited by the finalised block
		// somehow. TODO(rjl493456442) fix the hard-coded number here later.
		state, err = w.eth.StateAtBlock(parent, 1024, nil, false, false)
		log.Warn("Recovered mining state", "root", parent.Root(), "err", err)
	}
	if err != nil {
		return nil, err
	}
	state.StartPrefetcher("miner")

	// Note the passed coinbase may be different with header.Coinbase.
	// 注意传入的coinbase可能和header.Coinbase不一样
	env := &environment{
		signer:    types.MakeSigner(w.chainConfig, header.Number),
		state:     state,
		coinbase:  coinbase,
		ancestors: mapset.NewSet(),
		family:    mapset.NewSet(),
		header:    header,
		uncles:    make(map[common.Hash]*types.Header),
	}
	// when 08 is processed ancestors contain 07 (quick block)
	for _, ancestor := range w.chain.GetBlocksFromHash(parent.Hash(), 7) {
		for _, uncle := range ancestor.Uncles() {
			env.family.Add(uncle.Hash())
		}
		env.family.Add(ancestor.Hash())
		env.ancestors.Add(ancestor.Hash())
	}
	// Keep track of transactions which return errors so they can be removed
	// 追踪transactions，它可以返回errors，这样他们可以被移除
	env.tcount = 0
	return env, nil
}

// commitUncle adds the given block to uncle block set, returns error if failed to add.
// commitUncle添加给定的block到uncle block set，返回error，如果添加失败的话
func (w *worker) commitUncle(env *environment, uncle *types.Header) error {
	if w.isTTDReached(env.header) {
		return errors.New("ignore uncle for beacon block")
	}
	hash := uncle.Hash()
	if _, exist := env.uncles[hash]; exist {
		return errors.New("uncle not unique")
	}
	if env.header.ParentHash == uncle.ParentHash {
		// uncle是兄弟
		return errors.New("uncle is sibling")
	}
	if !env.ancestors.Contains(uncle.ParentHash) {
		// 需要在ancestors中包含uncle的parent hash
		return errors.New("uncle's parent unknown")
	}
	// family中已经包含了uncle
	if env.family.Contains(hash) {
		return errors.New("uncle already included")
	}
	env.uncles[hash] = uncle
	return nil
}

// updateSnapshot updates pending snapshot block, receipts and state.
// updateSnapshot更新pending装的snapshot block, receipts以及state
func (w *worker) updateSnapshot(env *environment) {
	w.snapshotMu.Lock()
	defer w.snapshotMu.Unlock()

	// 构建snapshotBlock, snapshotReceipts和snapshotState
	w.snapshotBlock = types.NewBlock(
		env.header,
		env.txs,
		env.unclelist(),
		env.receipts,
		trie.NewStackTrie(nil),
	)
	w.snapshotReceipts = copyReceipts(env.receipts)
	w.snapshotState = env.state.Copy()
}

func (w *worker) commitTransaction(env *environment, tx *types.Transaction) ([]*types.Log, error) {
	// 每次commitTransaction执行前都要记录当前StateDB的snapshot，一旦transaction执行失败，基于这个snapshot进行回滚
	snap := env.state.Snapshot()
	log.Info("commitTransaction really commit transaction")

	// 应用transaction
	receipt, err := core.ApplyTransaction(w.chainConfig, w.chain, &env.coinbase, env.gasPool, env.state, env.header, tx, &env.header.GasUsed, *w.chain.GetVMConfig())
	if err != nil {
		env.state.RevertToSnapshot(snap)
		return nil, err
	}
	// 扩展env中的txs和receipts
	env.txs = append(env.txs, tx)
	env.receipts = append(env.receipts, receipt)

	return receipt.Logs, nil
}

func (w *worker) commitTransactions(env *environment, txs *types.TransactionsByPriceAndNonce, interrupt *int32) error {
	gasLimit := env.header.GasLimit
	if env.gasPool == nil {
		env.gasPool = new(core.GasPool).AddGas(gasLimit)
	}
	var coalescedLogs []*types.Log

	for {
		// In the following three cases, we will interrupt the execution of the transaction.
		// 在以下三个场景，我们会中断transaction的执行
		// (1) new head block event arrival, the interrupt signal is 1
		// (1) 新的block event到来，interrupt signal为1
		// (2) worker start or restart, the interrupt signal is 1
		// (2) worker启动或者重启，inerrupt signal为1
		// (3) worker recreate the sealing block with any newly arrived transactions, the interrupt signal is 2.
		// (3) worker重新创建了sealing block，用新到来的transactions，interrupt signal为2
		// For the first two cases, the semi-finished work will be discarded.
		// 对于前两种情况，semi-finished work会被丢弃
		// For the third case, the semi-finished work will be submitted to the consensus engine.
		// 对于第三种情况，semi-finished work会被提交到共识引擎
		if interrupt != nil && atomic.LoadInt32(interrupt) != commitInterruptNone {
			// Notify resubmit loop to increase resubmitting interval due to too frequent commits.
			// 通知resubmit loop来降低resubmitting的延时，因为太频繁的commits
			if atomic.LoadInt32(interrupt) == commitInterruptResubmit {
				ratio := float64(gasLimit-env.gasPool.Gas()) / float64(gasLimit)
				if ratio < 0.1 {
					ratio = 0.1
				}
				w.resubmitAdjustCh <- &intervalAdjust{
					ratio: ratio,
					inc:   true,
				}
				return errBlockInterruptedByRecommit
			}
			return errBlockInterruptedByNewHead
		}
		// If we don't have enough gas for any further transactions then we're done
		// 如果我们没有足够的gas用于后续的transactions，那么我们就结束了
		if env.gasPool.Gas() < params.TxGas {
			log.Trace("Not enough gas for further transactions", "have", env.gasPool, "want", params.TxGas)
			break
		}
		// Retrieve the next transaction and abort if all done
		// 获取下一个transaction并且退出如果所有都结束的话
		tx := txs.Peek()
		if tx == nil {
			break
		}
		// Error may be ignored here. The error has already been checked
		// during transaction acceptance is the transaction pool.
		//
		// We use the eip155 signer regardless of the current hf.
		from, _ := types.Sender(env.signer, tx)
		// Check whether the tx is replay protected. If we're not in the EIP155 hf
		// phase, start ignoring the sender until we do.
		if tx.Protected() && !w.chainConfig.IsEIP155(env.header.Number) {
			log.Trace("Ignoring reply protected transaction", "hash", tx.Hash(), "eip155", w.chainConfig.EIP155Block)

			txs.Pop()
			continue
		}
		// Start executing the transaction
		// 开始执行transaction
		env.state.Prepare(tx.Hash(), env.tcount)

		// 提交transaction，真正执行transaction
		logs, err := w.commitTransaction(env, tx)
		switch {
		case errors.Is(err, core.ErrGasLimitReached):
			// Pop the current out-of-gas transaction without shifting in the next from the account
			log.Trace("Gas limit exceeded for current block", "sender", from)
			txs.Pop()

		case errors.Is(err, core.ErrNonceTooLow):
			// New head notification data race between the transaction pool and miner, shift
			log.Trace("Skipping transaction with low nonce", "sender", from, "nonce", tx.Nonce())
			txs.Shift()

		case errors.Is(err, core.ErrNonceTooHigh):
			// Reorg notification data race between the transaction pool and miner, skip account =
			log.Trace("Skipping account with hight nonce", "sender", from, "nonce", tx.Nonce())
			txs.Pop()

		case errors.Is(err, nil):
			// Everything ok, collect the logs and shift in the next transaction from the same account
			// 都ok，收集日志并且从同一账户转移下一笔交易
			coalescedLogs = append(coalescedLogs, logs...)
			env.tcount++
			txs.Shift()

		case errors.Is(err, core.ErrTxTypeNotSupported):
			// Pop the unsupported transaction without shifting in the next from the account
			log.Trace("Skipping unsupported transaction type", "sender", from, "type", tx.Type())
			txs.Pop()

		default:
			// Strange error, discard the transaction and get the next in line (note, the
			// nonce-too-high clause will prevent us from executing in vain).
			log.Debug("Transaction failed, account skipped", "hash", tx.Hash(), "err", err)
			txs.Shift()
		}
	}

	if !w.isRunning() && len(coalescedLogs) > 0 {
		// We don't push the pendingLogsEvent while we are sealing. The reason is that
		// when we are sealing, the worker will regenerate a sealing block every 3 seconds.
		// In order to avoid pushing the repeated pendingLog, we disable the pending log pushing.

		// make a copy, the state caches the logs and these logs get "upgraded" from pending to mined
		// logs by filling in the block hash when the block was mined by the local miner. This can
		// cause a race condition if a log was "upgraded" before the PendingLogsEvent is processed.
		cpy := make([]*types.Log, len(coalescedLogs))
		for i, l := range coalescedLogs {
			cpy[i] = new(types.Log)
			*cpy[i] = *l
		}
		w.pendingLogsFeed.Send(cpy)
	}
	// Notify resubmit loop to decrease resubmitting interval if current interval is larger
	// than the user-specified one.
	// 通知resubmit loop，来降低resubmitting interval，如果当前的interval大于用户指定的
	if interrupt != nil {
		w.resubmitAdjustCh <- &intervalAdjust{inc: false}
	}
	return nil
}

// generateParams wraps various of settings for generating sealing task.
// generateParams封装了 一系列的设置用于生成sealing task
type generateParams struct {
	timestamp  uint64         // The timstamp for sealing task
	forceTime  bool           // Flag whether the given timestamp is immutable or not
	parentHash common.Hash    // Parent block hash, empty means the latest chain head
	coinbase   common.Address // The fee recipient address for including transaction
	random     common.Hash    // The randomness generated by beacon chain, empty before the merge
	noUncle    bool           // Flag whether the uncle block inclusion is allowed	// 包含block是否是允许的
	noExtra    bool           // Flag whether the extra field assignment is allowed
	// Flag表示是否期望一个没有transaction的，空的block
	noTxs bool // Flag whether an empty block without any transaction is expected
}

// prepareWork constructs the sealing task according to the given parameters,
// prepareWork构建sealing task，基于给定的参数，要么基于last chain head，要么基于指定的parent
// either based on the last chain head or specified parent. In this function
// the pending transactions are not filled yet, only the empty task returned.
// 在这个函数中，pending transactions还没有被填充，只返回empty task
func (w *worker) prepareWork(genParams *generateParams) (*environment, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// Find the parent block for sealing task
	// 找到parent block用于sealing task
	parent := w.chain.CurrentBlock()
	if genParams.parentHash != (common.Hash{}) {
		parent = w.chain.GetBlockByHash(genParams.parentHash)
	}
	if parent == nil {
		return nil, fmt.Errorf("missing parent")
	}
	// Sanity check the timestamp correctness, recap the timestamp
	// to parent+1 if the mutation is allowed.
	// Sanity检查timestamp的正确性，重新设置时间戳到parent+1，如果允许修改的话
	timestamp := genParams.timestamp
	if parent.Time() >= timestamp {
		if genParams.forceTime {
			return nil, fmt.Errorf("invalid timestamp, parent %d given %d", parent.Time(), timestamp)
		}
		timestamp = parent.Time() + 1
	}
	// Construct the sealing block header, set the extra field if it's allowed
	// 构建sealing block header，设置extra field，如果允许的话
	num := parent.Number()
	// 构建header
	header := &types.Header{
		ParentHash: parent.Hash(),
		Number:     num.Add(num, common.Big1),
		// 计算gas limit并且设置
		GasLimit: core.CalcGasLimit(parent.GasLimit(), w.config.GasCeil),
		Time:     timestamp,
		Coinbase: genParams.coinbase,
	}
	if !genParams.noExtra && len(w.extra) != 0 {
		header.Extra = w.extra
	}
	// Set the randomness field from the beacon chain if it's available.
	// 设置来自beacon的随机字段，如果可用的话
	if genParams.random != (common.Hash{}) {
		header.MixDigest = genParams.random
	}
	// Set baseFee and GasLimit if we are on an EIP-1559 chain
	// 设置baseFee和GasLimit，如果我们在EIP-1559 chain
	if w.chainConfig.IsLondon(header.Number) {
		header.BaseFee = misc.CalcBaseFee(w.chainConfig, parent.Header())
		if !w.chainConfig.IsLondon(parent.Number()) {
			parentGasLimit := parent.GasLimit() * params.ElasticityMultiplier
			header.GasLimit = core.CalcGasLimit(parentGasLimit, w.config.GasCeil)
		}
	}
	// Run the consensus preparation with the default or customized consensus engine.
	// 运行consensus preparation，用默认的或者自定义的共识引擎
	if err := w.engine.Prepare(w.chain, header); err != nil {
		log.Error("Failed to prepare header for sealing", "err", err)
		return nil, err
	}
	// Could potentially happen if starting to mine in an odd state.
	// Note genParams.coinbase can be different with header.Coinbase
	// since clique algorithm can modify the coinbase field in header.
	// 可能发生，如果在一种奇怪的状态进行mine，注意genParams.coinbase可能和
	// header.Coinbase不同，因为clique算法可以修改header中的coinbase字段
	env, err := w.makeEnv(parent, header, genParams.coinbase)
	if err != nil {
		log.Error("Failed to create sealing context", "err", err)
		return nil, err
	}
	// Accumulate the uncles for the sealing work only if it's allowed.
	// 累计uncles，对于sealing work，只有它们被允许的情况下
	if !genParams.noUncle {
		commitUncles := func(blocks map[common.Hash]*types.Block) {
			for hash, uncle := range blocks {
				if len(env.uncles) == 2 {
					break
				}
				// 对uncle进行提交
				if err := w.commitUncle(env, uncle.Header()); err != nil {
					log.Trace("Possible uncle rejected", "hash", hash, "reason", err)
				} else {
					log.Debug("Committing new uncle to block", "hash", hash)
				}
			}
		}
		// Prefer to locally generated uncle
		// 更倾向于本地产生的uncle
		commitUncles(w.localUncles)
		commitUncles(w.remoteUncles)
	}
	return env, nil
}

// fillTransactions retrieves the pending transactions from the txpool and fills them
// into the given sealing block. The transaction selection and ordering strategy can
// be customized with the plugin in the future.
// fillTransactions从txpool中获取pending transactions并且将它们填充进给定的sealing block
// transaction的选择以及排序策略可用插件自定义，在未来
func (w *worker) fillTransactions(interrupt *int32, env *environment) error {
	// Split the pending transactions into locals and remotes
	// Fill the block with all available pending transactions.
	// 将pending transactions分为locals和remotes，用所有的pending transactions
	// 填充block
	log.Info("fillTransactions being called")
	pending := w.eth.TxPool().Pending(true)
	localTxs, remoteTxs := make(map[common.Address]types.Transactions), pending
	for _, account := range w.eth.TxPool().Locals() {
		// 如果本地的tx存在于remoteTxs中，则先将其删除
		if txs := remoteTxs[account]; len(txs) > 0 {
			delete(remoteTxs, account)
			localTxs[account] = txs
		}
	}
	// 如果本地transactions存在
	if len(localTxs) > 0 {
		txs := types.NewTransactionsByPriceAndNonce(env.signer, localTxs, env.header.BaseFee)
		// 提交本地transactions
		if err := w.commitTransactions(env, txs, interrupt); err != nil {
			return err
		}
	}
	// 如果远程transactions存在
	if len(remoteTxs) > 0 {
		txs := types.NewTransactionsByPriceAndNonce(env.signer, remoteTxs, env.header.BaseFee)
		// 提交远程transactions，最终提交成功的话会将tx放在env中
		if err := w.commitTransactions(env, txs, interrupt); err != nil {
			return err
		}
	}
	return nil
}

// generateWork generates a sealing block based on the given parameters.
// generateWork基于给定的参数生成一个sealing block
func (w *worker) generateWork(params *generateParams) (*types.Block, error) {
	work, err := w.prepareWork(params)
	if err != nil {
		return nil, err
	}
	defer work.discard()

	if !params.noTxs {
		// 填充transactions
		w.fillTransactions(nil, work)
	}
	// 最终调用共识引擎进行Finalize以及Assemble
	return w.engine.FinalizeAndAssemble(w.chain, work.header, work.state, work.txs, work.unclelist(), work.receipts)
}

// commitWork generates several new sealing tasks based on the parent block
// and submit them to the sealer.
// commitWork产生几个新的sealing tasks，基于parent block，并且将它们提交到sealer
func (w *worker) commitWork(interrupt *int32, noempty bool, timestamp int64) {
	start := time.Now()

	// Set the coinbase if the worker is running or it's required
	// 设置coinbase，如果worker正在运行，或者它是必须的
	var coinbase common.Address
	if w.isRunning() {
		if w.coinbase == (common.Address{}) {
			// 没有设置coinbase就禁止运行
			log.Error("Refusing to mine without etherbase")
			return
		}
		// 使用提前设置的地址作为fee recipient
		coinbase = w.coinbase // Use the preset address as the fee recipient
	}
	// 构建environment
	work, err := w.prepareWork(&generateParams{
		timestamp: uint64(timestamp),
		coinbase:  coinbase,
	})
	if err != nil {
		return
	}
	// Create an empty block based on temporary copied state for
	// sealing in advance without waiting block execution finished.
	// 创建一个空的block，基于临时的copied state，用于提前进行sealing，而不用等待
	// block执行结束
	if !noempty && atomic.LoadUint32(&w.noempty) == 0 {
		log.Info("commit empty block")
		w.commit(work.copy(), nil, false, start)
	}

	// Fill pending transactions from the txpool
	// 从txpool中填充pending transactions
	err = w.fillTransactions(interrupt, work)
	if errors.Is(err, errBlockInterruptedByNewHead) {
		work.discard()
		return
	}
	// 提交commit
	log.Info("commit non-empty block")
	w.commit(work.copy(), w.fullTaskHook, true, start)

	// Swap out the old work with the new one, terminating any leftover
	// prefetcher processes in the mean time and starting a new one.
	// 用new one替换old work，同时终结任何残留的prefetcher processes，并且启动一个新的
	if w.current != nil {
		w.current.discard()
	}
	w.current = work
}

// commit runs any post-transaction state modifications, assembles the final block
// and commits new work if consensus engine is running.
// commit运行任何post-transaction的状态修改，组装最后的block并且提交新的work，如果共识引擎在运行的话
// Note the assumption is held that the mutation is allowed to the passed env, do
// the deep copy first.
// 注意mutation是允许的，对于传入的env
func (w *worker) commit(env *environment, interval func(), update bool, start time.Time) error {
	if w.isRunning() {
		if interval != nil {
			interval()
		}
		// Create a local environment copy, avoid the data race with snapshot state.
		// https://github.com/ethereum/go-ethereum/issues/24299
		env := env.copy()
		// 基于env中的header, state以及txs，封装block
		block, err := w.engine.FinalizeAndAssemble(w.chain, env.header, env.state, env.txs, env.unclelist(), env.receipts)
		if err != nil {
			return err
		}
		// If we're post merge, just ignore
		// 如果我们是post merge，则直接忽略
		if !w.isTTDReached(block.Header()) {
			select {
			// 发送给taskCh
			case w.taskCh <- &task{receipts: env.receipts, state: env.state, block: block, createdAt: time.Now()}:
				w.unconfirmed.Shift(block.NumberU64() - 1)
				// 提交了新的sealing work
				log.Info("", "number", block.Number(), "sealhash", w.engine.SealHash(block.Header()),
					"uncles", len(env.uncles), "txs", env.tcount,
					"gas", block.GasUsed(), "fees", totalFees(block, env.receipts),
					"elapsed", common.PrettyDuration(time.Since(start)))

			case <-w.exitCh:
				log.Info("Worker has exited")
			}
		}
	}
	if update {
		// 更新snapshot
		w.updateSnapshot(env)
	}
	return nil
}

// getSealingBlock generates the sealing block based on the given parameters.
// The generation result will be passed back via the given channel no matter
// the generation itself succeeds or not.
// getSealingBlock基于给定的参数生成sealing block，generation result应该通过给定的channel
// 返回，不管generation自己成功与否
func (w *worker) getSealingBlock(parent common.Hash, timestamp uint64, coinbase common.Address, random common.Hash, noTxs bool) (chan *types.Block, chan error, error) {
	var (
		resCh = make(chan *types.Block, 1)
		errCh = make(chan error, 1)
	)
	req := &getWorkReq{
		params: &generateParams{
			timestamp:  timestamp,
			forceTime:  true,
			parentHash: parent,
			coinbase:   coinbase,
			random:     random,
			noUncle:    true,
			noExtra:    true,
			noTxs:      noTxs,
		},
		result: resCh,
		err:    errCh,
	}
	select {
	case w.getWorkCh <- req:
		return resCh, errCh, nil
	case <-w.exitCh:
		return nil, nil, errors.New("miner closed")
	}
}

// isTTDReached returns the indicator if the given block has reached the total
// terminal difficulty for The Merge transition.
// isTTDReached返回indicator表明给定的block已经到达了total terminal difficulty，对于The Merge transition
func (w *worker) isTTDReached(header *types.Header) bool {
	td, ttd := w.chain.GetTd(header.ParentHash, header.Number.Uint64()-1), w.chain.Config().TerminalTotalDifficulty
	return td != nil && ttd != nil && td.Cmp(ttd) >= 0
}

// copyReceipts makes a deep copy of the given receipts.
func copyReceipts(receipts []*types.Receipt) []*types.Receipt {
	result := make([]*types.Receipt, len(receipts))
	for i, l := range receipts {
		cpy := *l
		result[i] = &cpy
	}
	return result
}

// postSideBlock fires a side chain event, only use it for testing.
// postSideBlock触发一个side chain event
func (w *worker) postSideBlock(event core.ChainSideEvent) {
	select {
	case w.chainSideCh <- event:
	case <-w.exitCh:
	}
}

// totalFees computes total consumed miner fees in ETH. Block transactions and receipts have to have the same order.
func totalFees(block *types.Block, receipts []*types.Receipt) *big.Float {
	feesWei := new(big.Int)
	for i, tx := range block.Transactions() {
		minerFee, _ := tx.EffectiveGasTip(block.BaseFee())
		feesWei.Add(feesWei, new(big.Int).Mul(new(big.Int).SetUint64(receipts[i].GasUsed), minerFee))
	}
	return new(big.Float).Quo(new(big.Float).SetInt(feesWei), new(big.Float).SetInt(big.NewInt(params.Ether)))
}
