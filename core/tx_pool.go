// Copyright 2014 The go-ethereum Authors
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

package core

import (
	"errors"
	"math"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/prque"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/params"
)

const (
	// chainHeadChanSize is the size of channel listening to ChainHeadEvent.
	chainHeadChanSize = 10

	// txSlotSize is used to calculate how many data slots a single transaction
	// takes up based on its size. The slots are used as DoS protection, ensuring
	// that validating a new transaction remains a constant operation (in reality
	// O(maxslots), where max slots are 4 currently).
	// txSlotSize用于计算基于它的大小，单个的transaction可以占据多少个data slots
	// slots用于作为DoS protection，确保校验一个新的transaction依然是一个常量操作
	txSlotSize = 32 * 1024

	// txMaxSize is the maximum size a single transaction can have. This field has
	// non-trivial consequences: larger transactions are significantly harder and
	// more expensive to propagate; larger transactions also take more resources
	// to validate whether they fit into the pool or not.
	txMaxSize = 4 * txSlotSize // 128KB
)

var (
	// ErrAlreadyKnown is returned if the transactions is already contained
	// within the pool.
	ErrAlreadyKnown = errors.New("already known")

	// ErrInvalidSender is returned if the transaction contains an invalid signature.
	ErrInvalidSender = errors.New("invalid sender")

	// ErrUnderpriced is returned if a transaction's gas price is below the minimum
	// configured for the transaction pool.
	// ErrUnderpriced被返回，如果一个transaction的gas price小于transaction pool配置的最小d
	// gas price
	ErrUnderpriced = errors.New("transaction underpriced")

	// ErrTxPoolOverflow is returned if the transaction pool is full and can't accpet
	// another remote transaction.
	// ErrTxPoolOverflow被返回如果transaction pool已经满了并且不能接受另一个remote transaction
	ErrTxPoolOverflow = errors.New("txpool is full")

	// ErrReplaceUnderpriced is returned if a transaction is attempted to be replaced
	// with a different one without the required price bump.
	ErrReplaceUnderpriced = errors.New("replacement transaction underpriced")

	// ErrGasLimit is returned if a transaction's requested gas limit exceeds the
	// maximum allowance of the current block.
	ErrGasLimit = errors.New("exceeds block gas limit")

	// ErrNegativeValue is a sanity error to ensure no one is able to specify a
	// transaction with a negative value.
	// ErrNegativeValue是一个sanity error来确保没有人能指定一个负值的transaction
	ErrNegativeValue = errors.New("negative value")

	// ErrOversizedData is returned if the input data of a transaction is greater
	// than some meaningful limit a user might use. This is not a consensus error
	// making the transaction invalid, rather a DOS protection.
	ErrOversizedData = errors.New("oversized data")
)

var (
	evictionInterval    = time.Minute     // Time interval to check for evictable transactions
	statsReportInterval = 8 * time.Second // Time interval to report transaction pool stats
)

var (
	// Metrics for the pending pool
	pendingDiscardMeter   = metrics.NewRegisteredMeter("txpool/pending/discard", nil)
	pendingReplaceMeter   = metrics.NewRegisteredMeter("txpool/pending/replace", nil)
	pendingRateLimitMeter = metrics.NewRegisteredMeter("txpool/pending/ratelimit", nil) // Dropped due to rate limiting
	pendingNofundsMeter   = metrics.NewRegisteredMeter("txpool/pending/nofunds", nil)   // Dropped due to out-of-funds

	// Metrics for the queued pool
	queuedDiscardMeter   = metrics.NewRegisteredMeter("txpool/queued/discard", nil)
	queuedReplaceMeter   = metrics.NewRegisteredMeter("txpool/queued/replace", nil)
	queuedRateLimitMeter = metrics.NewRegisteredMeter("txpool/queued/ratelimit", nil) // Dropped due to rate limiting
	queuedNofundsMeter   = metrics.NewRegisteredMeter("txpool/queued/nofunds", nil)   // Dropped due to out-of-funds
	queuedEvictionMeter  = metrics.NewRegisteredMeter("txpool/queued/eviction", nil)  // Dropped due to lifetime

	// General tx metrics
	knownTxMeter       = metrics.NewRegisteredMeter("txpool/known", nil)
	validTxMeter       = metrics.NewRegisteredMeter("txpool/valid", nil)
	invalidTxMeter     = metrics.NewRegisteredMeter("txpool/invalid", nil)
	underpricedTxMeter = metrics.NewRegisteredMeter("txpool/underpriced", nil)
	overflowedTxMeter  = metrics.NewRegisteredMeter("txpool/overflowed", nil)
	// throttleTxMeter counts how many transactions are rejected due to too-many-changes between
	// txpool reorgs.
	throttleTxMeter = metrics.NewRegisteredMeter("txpool/throttle", nil)
	// reorgDurationTimer measures how long time a txpool reorg takes.
	reorgDurationTimer = metrics.NewRegisteredTimer("txpool/reorgtime", nil)
	// dropBetweenReorgHistogram counts how many drops we experience between two reorg runs. It is expected
	// that this number is pretty low, since txpool reorgs happen very frequently.
	dropBetweenReorgHistogram = metrics.NewRegisteredHistogram("txpool/dropbetweenreorg", nil, metrics.NewExpDecaySample(1028, 0.015))

	pendingGauge = metrics.NewRegisteredGauge("txpool/pending", nil)
	queuedGauge  = metrics.NewRegisteredGauge("txpool/queued", nil)
	localGauge   = metrics.NewRegisteredGauge("txpool/local", nil)
	slotsGauge   = metrics.NewRegisteredGauge("txpool/slots", nil)

	reheapTimer = metrics.NewRegisteredTimer("txpool/reheap", nil)
)

// TxStatus is the current status of a transaction as seen by the pool.
type TxStatus uint

const (
	TxStatusUnknown TxStatus = iota
	TxStatusQueued
	TxStatusPending
	TxStatusIncluded
)

// blockChain provides the state of blockchain and current gas limit to do
// some pre checks in tx pool and event subscribers.
// blockChain提供了blockchain的状态以及当前的gas limit，在tx pool中做一些pre check以及事件订阅
type blockChain interface {
	CurrentBlock() *types.Block
	GetBlock(hash common.Hash, number uint64) *types.Block
	StateAt(root common.Hash) (*state.StateDB, error)

	SubscribeChainHeadEvent(ch chan<- ChainHeadEvent) event.Subscription
}

// TxPoolConfig are the configuration parameters of the transaction pool.
// TxPoolConfig是transaction pool的配置参数
type TxPoolConfig struct {
	Locals   []common.Address // Addresses that should be treated by default as local // 那些应该被作为local的地址
	NoLocals bool             // Whether local transaction handling should be disabled // 是否应该禁止local transactrion handling
	// 本地transaction的journal从而在node重启的时候能存活
	Journal   string        // Journal of local transactions to survive node restarts
	Rejournal time.Duration // Time interval to regenerate the local transaction journal

	// 最小的gas price来执行接收入pool
	PriceLimit uint64 // Minimum gas price to enforce for acceptance into the pool
	// 最小的price bump percentage来替换一个已经存在的transaction
	PriceBump uint64 // Minimum price bump percentage to replace an already existing transaction (nonce)

	// 买个account保证的executable transaction slots的数目
	AccountSlots uint64 // Number of executable transaction slots guaranteed per account
	// 对于所有的accounts，最大的executable transaction slots的数目
	GlobalSlots uint64 // Maximum number of executable transaction slots for all accounts
	// 每个account允许有的最大的non-executable transaction slots的数目
	AccountQueue uint64 // Maximum number of non-executable transaction slots permitted per account
	// 对于所有account的最大的non-executable transaction slots的数目
	GlobalQueue uint64 // Maximum number of non-executable transaction slots for all accounts

	// non-executalb transaction排队的最长时间
	Lifetime time.Duration // Maximum amount of time non-executable transaction are queued
}

// DefaultTxPoolConfig contains the default configurations for the transaction
// pool.
// DefaultTxPoolConfig包含了默认配置，用于transaction pool
var DefaultTxPoolConfig = TxPoolConfig{
	Journal:   "transactions.rlp",
	Rejournal: time.Hour,

	PriceLimit: 1,
	PriceBump:  10,

	AccountSlots: 16,
	GlobalSlots:  4096 + 1024, // urgent + floating queue capacity with 4:1 ratio
	AccountQueue: 64,
	GlobalQueue:  1024,

	Lifetime: 3 * time.Hour,
}

// sanitize checks the provided user configurations and changes anything that's
// unreasonable or unworkable.
func (config *TxPoolConfig) sanitize() TxPoolConfig {
	conf := *config
	if conf.Rejournal < time.Second {
		log.Warn("Sanitizing invalid txpool journal time", "provided", conf.Rejournal, "updated", time.Second)
		conf.Rejournal = time.Second
	}
	if conf.PriceLimit < 1 {
		log.Warn("Sanitizing invalid txpool price limit", "provided", conf.PriceLimit, "updated", DefaultTxPoolConfig.PriceLimit)
		conf.PriceLimit = DefaultTxPoolConfig.PriceLimit
	}
	if conf.PriceBump < 1 {
		log.Warn("Sanitizing invalid txpool price bump", "provided", conf.PriceBump, "updated", DefaultTxPoolConfig.PriceBump)
		conf.PriceBump = DefaultTxPoolConfig.PriceBump
	}
	if conf.AccountSlots < 1 {
		log.Warn("Sanitizing invalid txpool account slots", "provided", conf.AccountSlots, "updated", DefaultTxPoolConfig.AccountSlots)
		conf.AccountSlots = DefaultTxPoolConfig.AccountSlots
	}
	if conf.GlobalSlots < 1 {
		log.Warn("Sanitizing invalid txpool global slots", "provided", conf.GlobalSlots, "updated", DefaultTxPoolConfig.GlobalSlots)
		conf.GlobalSlots = DefaultTxPoolConfig.GlobalSlots
	}
	if conf.AccountQueue < 1 {
		log.Warn("Sanitizing invalid txpool account queue", "provided", conf.AccountQueue, "updated", DefaultTxPoolConfig.AccountQueue)
		conf.AccountQueue = DefaultTxPoolConfig.AccountQueue
	}
	if conf.GlobalQueue < 1 {
		log.Warn("Sanitizing invalid txpool global queue", "provided", conf.GlobalQueue, "updated", DefaultTxPoolConfig.GlobalQueue)
		conf.GlobalQueue = DefaultTxPoolConfig.GlobalQueue
	}
	if conf.Lifetime < 1 {
		log.Warn("Sanitizing invalid txpool lifetime", "provided", conf.Lifetime, "updated", DefaultTxPoolConfig.Lifetime)
		conf.Lifetime = DefaultTxPoolConfig.Lifetime
	}
	return conf
}

// TxPool contains all currently known transactions. Transactions
// enter the pool when they are received from the network or submitted
// locally. They exit the pool when they are included in the blockchain.
// TxPool包含了所有已经知道的transactions，transactions当它们从网络接收到或者由本地提交时
// 进入pool，当它们被包含在blockchain之后离开pool
//
// The pool separates processable transactions (which can be applied to the
// current state) and future transactions. Transactions move between those
// two states over time as they are received and processed.
// pool将transactions区分为processable transactions（它们可以应用到current state）
// 以及future transactions，随着transaction被接收以及处理，Transactions在这两个状态之间移动
type TxPool struct {
	config      TxPoolConfig
	chainconfig *params.ChainConfig
	chain       blockChain
	gasPrice    *big.Int
	txFeed      event.Feed // 对于tx事件的订阅
	scope       event.SubscriptionScope
	signer      types.Signer
	mu          sync.RWMutex

	istanbul bool // Fork indicator whether we are in the istanbul stage.
	eip2718  bool // Fork indicator whether we are using EIP-2718 type transactions.
	eip1559  bool // Fork indicator whether we are using EIP-1559 type transactions.

	currentState  *state.StateDB // Current state in the blockchain head	// blockchain head当前的状态
	pendingNonces *txNoncer      // Pending state tracking virtual nonces	// Pending sttate追踪virtual nonces
	currentMaxGas uint64         // Current gas limit for transaction caps	// 对于transaction caps当前的gas limit

	locals  *accountSet // Set of local transaction to exempt from eviction rules
	journal *txJournal  // Journal of local transaction to back up to disk	// 关于local transaction的日志，备份到磁盘中

	// 当前所有可以处理的transactions
	pending map[common.Address]*txList // All currently processable transactions
	// 队列中但是不能处理的transaction
	queue map[common.Address]*txList // Queued but non-processable transactions
	// 每个已知的account的最后的心跳
	beats map[common.Address]time.Time // Last heartbeat from each known account
	// 所有允许查找的transactions
	all    *txLookup     // All transactions to allow lookups
	priced *txPricedList // All transactions sorted by price	// 根据price排序的所有transactions

	chainHeadCh    chan ChainHeadEvent
	chainHeadSub   event.Subscription
	reqResetCh     chan *txpoolResetRequest
	reqPromoteCh   chan *accountSet
	queueTxEventCh chan *types.Transaction
	reorgDoneCh    chan chan struct{}
	// 请求关闭scheduleReorgLoop
	reorgShutdownCh chan struct{}  // requests shutdown of scheduleReorgLoop
	wg              sync.WaitGroup // tracks loop, scheduleReorgLoop
	initDoneCh      chan struct{}  // is closed once the pool is initialized (for tests)

	changesSinceReorg int // A counter for how many drops we've performed in-between reorg.
}

type txpoolResetRequest struct {
	oldHead, newHead *types.Header
}

// NewTxPool creates a new transaction pool to gather, sort and filter inbound
// transactions from the network.
// NewTxPool创建一个新的transaction pool用来收集，排序以及过滤来自network的inbound transaction
func NewTxPool(config TxPoolConfig, chainconfig *params.ChainConfig, chain blockChain) *TxPool {
	// Sanitize the input to ensure no vulnerable gas prices are set
	// 确保没有设置易受攻击的gas prices
	config = (&config).sanitize()

	// Create the transaction pool with its initial settings
	// 用初始配置创建transaction pool
	pool := &TxPool{
		config:      config,
		chainconfig: chainconfig,
		// 包含chain的信息
		chain:           chain,
		signer:          types.LatestSigner(chainconfig),
		pending:         make(map[common.Address]*txList),
		queue:           make(map[common.Address]*txList),
		beats:           make(map[common.Address]time.Time),
		all:             newTxLookup(),
		chainHeadCh:     make(chan ChainHeadEvent, chainHeadChanSize),
		reqResetCh:      make(chan *txpoolResetRequest),
		reqPromoteCh:    make(chan *accountSet),
		queueTxEventCh:  make(chan *types.Transaction),
		reorgDoneCh:     make(chan chan struct{}),
		reorgShutdownCh: make(chan struct{}),
		initDoneCh:      make(chan struct{}),
		gasPrice:        new(big.Int).SetUint64(config.PriceLimit),
	}
	// 设置本地的accout
	pool.locals = newAccountSet(pool.signer)
	for _, addr := range config.Locals {
		log.Info("Setting new local account", "address", addr)
		pool.locals.add(addr)
	}
	pool.priced = newTxPricedList(pool.all)
	pool.reset(nil, chain.CurrentBlock().Header())

	// Start the reorg loop early so it can handle requests generated during journal loading.
	// 尽早启动reorg，这样它可以处理在journal loading的时候处理requests
	pool.wg.Add(1)
	// 运行reorg loop
	go pool.scheduleReorgLoop()

	// If local transactions and journaling is enabled, load from disk
	// 如果使能了local transactions和journaling，从磁盘中加载
	if !config.NoLocals && config.Journal != "" {
		// 构建journal
		pool.journal = newTxJournal(config.Journal)

		if err := pool.journal.load(pool.AddLocals); err != nil {
			log.Warn("Failed to load transaction journal", "err", err)
		}
		if err := pool.journal.rotate(pool.local()); err != nil {
			log.Warn("Failed to rotate transaction journal", "err", err)
		}
	}

	// Subscribe events from blockchain and start the main event loop.
	// 订阅来自blockchain的events并且启动main event loop
	pool.chainHeadSub = pool.chain.SubscribeChainHeadEvent(pool.chainHeadCh)
	pool.wg.Add(1)
	go pool.loop()

	return pool
}

// loop is the transaction pool's main event loop, waiting for and reacting to
// outside blockchain events as well as for various reporting and transaction
// eviction events.
// loop是transaction pool的主事件循环，等待并且对outside blockchain event做出反应
// 以及各种reporting以及transaction eviction events
func (pool *TxPool) loop() {
	defer pool.wg.Done()

	var (
		prevPending, prevQueued, prevStales int
		// Start the stats reporting and transaction eviction tickers
		// 启动stats reporting以及transaction eviction的计时器
		report  = time.NewTicker(statsReportInterval)
		evict   = time.NewTicker(evictionInterval)
		journal = time.NewTicker(pool.config.Rejournal)
		// Track the previous head headers for transaction reorgs
		// 追踪之前的head headers用于transaction reorgs
		head = pool.chain.CurrentBlock()
	)
	defer report.Stop()
	defer evict.Stop()
	defer journal.Stop()

	// Notify tests that the init phase is done
	close(pool.initDoneCh)
	for {
		select {
		// Handle ChainHeadEvent
		// 处理ChainHeadEvent
		case ev := <-pool.chainHeadCh:
			if ev.Block != nil {
				// 当有chain head事件发生的时候，调用requestReset，引发tx的promotion
				pool.requestReset(head.Header(), ev.Block.Header())
				head = ev.Block
			}

		// System shutdown.
		case <-pool.chainHeadSub.Err():
			close(pool.reorgShutdownCh)
			return

		// Handle stats reporting ticks
		// 处理stats reporting的上报
		case <-report.C:
			pool.mu.RLock()
			pending, queued := pool.stats()
			pool.mu.RUnlock()
			stales := int(atomic.LoadInt64(&pool.priced.stales))

			if pending != prevPending || queued != prevQueued || stales != prevStales {
				log.Debug("Transaction pool status report", "executable", pending, "queued", queued, "stales", stales)
				prevPending, prevQueued, prevStales = pending, queued, stales
			}

		// Handle inactive account transaction eviction
		// 处理inactive account transaction的驱逐
		case <-evict.C:
			pool.mu.Lock()
			for addr := range pool.queue {
				// Skip local transactions from the eviction mechanism
				// 让local transactions跳过驱逐机制
				if pool.locals.contains(addr) {
					continue
				}
				// Any non-locals old enough should be removed
				if time.Since(pool.beats[addr]) > pool.config.Lifetime {
					list := pool.queue[addr].Flatten()
					for _, tx := range list {
						pool.removeTx(tx.Hash(), true)
					}
					queuedEvictionMeter.Mark(int64(len(list)))
				}
			}
			pool.mu.Unlock()

		// Handle local transaction journal rotation
		// 处理本地transaction的journal rotation
		case <-journal.C:
			if pool.journal != nil {
				pool.mu.Lock()
				if err := pool.journal.rotate(pool.local()); err != nil {
					log.Warn("Failed to rotate local tx journal", "err", err)
				}
				pool.mu.Unlock()
			}
		}
	}
}

// Stop terminates the transaction pool.
func (pool *TxPool) Stop() {
	// Unsubscribe all subscriptions registered from txpool
	pool.scope.Close()

	// Unsubscribe subscriptions registered from blockchain
	pool.chainHeadSub.Unsubscribe()
	pool.wg.Wait()

	if pool.journal != nil {
		pool.journal.close()
	}
	log.Info("Transaction pool stopped")
}

// SubscribeNewTxsEvent registers a subscription of NewTxsEvent and
// starts sending event to the given channel.
// SubscribeNewTxsEvent注册对于NewTxsEvent的订阅并且开始发送事件到给定的channel
// miner worker会调用这个方法
func (pool *TxPool) SubscribeNewTxsEvent(ch chan<- NewTxsEvent) event.Subscription {
	return pool.scope.Track(pool.txFeed.Subscribe(ch))
}

// GasPrice returns the current gas price enforced by the transaction pool.
func (pool *TxPool) GasPrice() *big.Int {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	return new(big.Int).Set(pool.gasPrice)
}

// SetGasPrice updates the minimum price required by the transaction pool for a
// new transaction, and drops all transactions below this threshold.
// SetGasPrice更新transaction pool所需的最小的price，对于一个新的transaction，并且丢弃所有
// 小于这个threshold的transactions
func (pool *TxPool) SetGasPrice(price *big.Int) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	old := pool.gasPrice
	// 设置gasPrice
	pool.gasPrice = price
	// if the min miner fee increased, remove transactions below the new threshold
	if price.Cmp(old) > 0 {
		// pool.priced is sorted by GasFeeCap, so we have to iterate through pool.all instead
		drop := pool.all.RemotesBelowTip(price)
		for _, tx := range drop {
			pool.removeTx(tx.Hash(), false)
		}
		pool.priced.Removed(len(drop))
	}

	log.Info("Transaction pool price threshold updated", "price", price)
}

// Nonce returns the next nonce of an account, with all transactions executable
// by the pool already applied on top.
// Nonce返回一个account的下一个nonce，pool中所有可执行的transactions已经在top了
func (pool *TxPool) Nonce(addr common.Address) uint64 {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	return pool.pendingNonces.get(addr)
}

// Stats retrieves the current pool stats, namely the number of pending and the
// number of queued (non-executable) transactions.
func (pool *TxPool) Stats() (int, int) {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	return pool.stats()
}

// stats retrieves the current pool stats, namely the number of pending and the
// number of queued (non-executable) transactions.
func (pool *TxPool) stats() (int, int) {
	pending := 0
	for _, list := range pool.pending {
		pending += list.Len()
	}
	queued := 0
	for _, list := range pool.queue {
		queued += list.Len()
	}
	return pending, queued
}

// Content retrieves the data content of the transaction pool, returning all the
// pending as well as queued transactions, grouped by account and sorted by nonce.
// Content获取transaction pool的data content，返回所有的pending以及queued transactions
// 根据account分组并且按照nonce排序
func (pool *TxPool) Content() (map[common.Address]types.Transactions, map[common.Address]types.Transactions) {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	pending := make(map[common.Address]types.Transactions)
	for addr, list := range pool.pending {
		pending[addr] = list.Flatten()
	}
	queued := make(map[common.Address]types.Transactions)
	for addr, list := range pool.queue {
		queued[addr] = list.Flatten()
	}
	return pending, queued
}

// ContentFrom retrieves the data content of the transaction pool, returning the
// pending as well as queued transactions of this address, grouped by nonce.
func (pool *TxPool) ContentFrom(addr common.Address) (types.Transactions, types.Transactions) {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	var pending types.Transactions
	if list, ok := pool.pending[addr]; ok {
		pending = list.Flatten()
	}
	var queued types.Transactions
	if list, ok := pool.queue[addr]; ok {
		queued = list.Flatten()
	}
	return pending, queued
}

// Pending retrieves all currently processable transactions, grouped by origin
// account and sorted by nonce. The returned transaction set is a copy and can be
// freely modified by calling code.
// Pending获取所有当前可以处理的transactions，由origin account聚合并且由nonce排序
// 返回的transaction set是一个拷贝并且可以由调用代码释放
//
// The enforceTips parameter can be used to do an extra filtering on the pending
// transactions and only return those whose **effective** tip is large enough in
// the next pending execution environment.
func (pool *TxPool) Pending(enforceTips bool) map[common.Address]types.Transactions {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	pending := make(map[common.Address]types.Transactions)
	for addr, list := range pool.pending {
		txs := list.Flatten()

		// If the miner requests tip enforcement, cap the lists now
		if enforceTips && !pool.locals.contains(addr) {
			for i, tx := range txs {
				if tx.EffectiveGasTipIntCmp(pool.gasPrice, pool.priced.urgent.baseFee) < 0 {
					txs = txs[:i]
					break
				}
			}
		}
		if len(txs) > 0 {
			pending[addr] = txs
		}
	}
	return pending
}

// Locals retrieves the accounts currently considered local by the pool.
// Locals返回当前pool认为是local的账号
func (pool *TxPool) Locals() []common.Address {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	return pool.locals.flatten()
}

// local retrieves all currently known local transactions, grouped by origin
// account and sorted by nonce. The returned transaction set is a copy and can be
// freely modified by calling code.
func (pool *TxPool) local() map[common.Address]types.Transactions {
	txs := make(map[common.Address]types.Transactions)
	for addr := range pool.locals.accounts {
		if pending := pool.pending[addr]; pending != nil {
			txs[addr] = append(txs[addr], pending.Flatten()...)
		}
		if queued := pool.queue[addr]; queued != nil {
			txs[addr] = append(txs[addr], queued.Flatten()...)
		}
	}
	return txs
}

// validateTx checks whether a transaction is valid according to the consensus
// rules and adheres to some heuristic limits of the local node (price and size).
func (pool *TxPool) validateTx(tx *types.Transaction, local bool) error {
	// Accept only legacy transactions until EIP-2718/2930 activates.
	if !pool.eip2718 && tx.Type() != types.LegacyTxType {
		return ErrTxTypeNotSupported
	}
	// Reject dynamic fee transactions until EIP-1559 activates.
	if !pool.eip1559 && tx.Type() == types.DynamicFeeTxType {
		return ErrTxTypeNotSupported
	}
	// Reject transactions over defined size to prevent DOS attacks
	if uint64(tx.Size()) > txMaxSize {
		return ErrOversizedData
	}
	// Transactions can't be negative. This may never happen using RLP decoded
	// transactions but may occur if you create a transaction using the RPC.
	if tx.Value().Sign() < 0 {
		return ErrNegativeValue
	}
	// Ensure the transaction doesn't exceed the current block limit gas.
	if pool.currentMaxGas < tx.Gas() {
		return ErrGasLimit
	}
	// Sanity check for extremely large numbers
	if tx.GasFeeCap().BitLen() > 256 {
		return ErrFeeCapVeryHigh
	}
	if tx.GasTipCap().BitLen() > 256 {
		return ErrTipVeryHigh
	}
	// Ensure gasFeeCap is greater than or equal to gasTipCap.
	if tx.GasFeeCapIntCmp(tx.GasTipCap()) < 0 {
		return ErrTipAboveFeeCap
	}
	// Make sure the transaction is signed properly.
	from, err := types.Sender(pool.signer, tx)
	if err != nil {
		return ErrInvalidSender
	}
	// Drop non-local transactions under our own minimal accepted gas price or tip
	if !local && tx.GasTipCapIntCmp(pool.gasPrice) < 0 {
		return ErrUnderpriced
	}
	// Ensure the transaction adheres to nonce ordering
	if pool.currentState.GetNonce(from) > tx.Nonce() {
		return ErrNonceTooLow
	}
	// Transactor should have enough funds to cover the costs
	// cost == V + GP * GL
	if pool.currentState.GetBalance(from).Cmp(tx.Cost()) < 0 {
		return ErrInsufficientFunds
	}
	// Ensure the transaction has more gas than the basic tx fee.
	intrGas, err := IntrinsicGas(tx.Data(), tx.AccessList(), tx.To() == nil, true, pool.istanbul)
	if err != nil {
		return err
	}
	if tx.Gas() < intrGas {
		return ErrIntrinsicGas
	}
	return nil
}

// add validates a transaction and inserts it into the non-executable queue for later
// pending promotion and execution. If the transaction is a replacement for an already
// pending or queued one, it overwrites the previous transaction if its price is higher.
// add验证一个transaction并且插入到non-executable queue用于后续的pending promotion以及执行
// 如果transaction是对于一个已经pending或者入队的trnasaction的替代，它会覆盖之前的transaction
// 如果price更高
//
// If a newly added transaction is marked as local, its sending account will be
// be added to the allowlist, preventing any associated transaction from being dropped
// out of the pool due to pricing constraints.
// 如果一个新加入的transaction被标记为local，它的sending account会被添加到allowlist，防止任何相关的transaction
// 从Pool中丢弃，因为pricing的限制
func (pool *TxPool) add(tx *types.Transaction, local bool) (replaced bool, err error) {
	// If the transaction is already known, discard it
	hash := tx.Hash()
	if pool.all.Get(hash) != nil {
		log.Trace("Discarding already known transaction", "hash", hash)
		knownTxMeter.Mark(1)
		return false, ErrAlreadyKnown
	}
	// Make the local flag. If it's from local source or it's from the network but
	// the sender is marked as local previously, treat it as the local transaction.
	// 设置local标记，如果它来自local source或者来自network但是sender之前被标记为local
	// 将它作为local transaction对待
	isLocal := local || pool.locals.containsTx(tx)

	// If the transaction fails basic validation, discard it
	if err := pool.validateTx(tx, isLocal); err != nil {
		log.Trace("Discarding invalid transaction", "hash", hash, "err", err)
		invalidTxMeter.Mark(1)
		return false, err
	}
	// If the transaction pool is full, discard underpriced transactions
	// 如果transaction pool已经满了，丢弃underpriced transactions
	if uint64(pool.all.Slots()+numSlots(tx)) > pool.config.GlobalSlots+pool.config.GlobalQueue {
		// If the new transaction is underpriced, don't accept it
		// 如果新的transaction是underpriced，不要接受它
		if !isLocal && pool.priced.Underpriced(tx) {
			log.Trace("Discarding underpriced transaction", "hash", hash, "gasTipCap", tx.GasTipCap(), "gasFeeCap", tx.GasFeeCap())
			underpricedTxMeter.Mark(1)
			return false, ErrUnderpriced
		}
		// We're about to replace a transaction. The reorg does a more thorough
		// analysis of what to remove and how, but it runs async. We don't want to
		// do too many replacements between reorg-runs, so we cap the number of
		// replacements to 25% of the slots
		// 我们准备替换一个transaction，reorg会做更完整的分析，关于移除什么以及怎么移除
		// 但是它是异步运行的，我们不希望在reorg-runs之间做太多的replacements，因此
		// 我们将replacements的数目限制到slots的25%
		if pool.changesSinceReorg > int(pool.config.GlobalSlots/4) {
			throttleTxMeter.Mark(1)
			return false, ErrTxPoolOverflow
		}

		// New transaction is better than our worse ones, make room for it.
		// 新的transaction比我们最差的要好，为它make room
		// If it's a local transaction, forcibly discard all available transactions.
		// Otherwise if we can't make enough room for new one, abort the operation.
		// 如果这是一个local transaction，强制丢弃所有可用的transactions
		// 否则，如果我们不能为新的transaction制造空间，则中止操作
		drop, success := pool.priced.Discard(pool.all.Slots()-int(pool.config.GlobalSlots+pool.config.GlobalQueue)+numSlots(tx), isLocal)

		// Special case, we still can't make the room for the new remote one.
		// 特殊情况，我们还是不能容纳新的remote one
		if !isLocal && !success {
			log.Trace("Discarding overflown transaction", "hash", hash)
			overflowedTxMeter.Mark(1)
			return false, ErrTxPoolOverflow
		}
		// Bump the counter of rejections-since-reorg
		pool.changesSinceReorg += len(drop)
		// Kick out the underpriced remote transactions.
		// 移除underpriced remote transactions
		for _, tx := range drop {
			log.Trace("Discarding freshly underpriced transaction", "hash", tx.Hash(), "gasTipCap", tx.GasTipCap(), "gasFeeCap", tx.GasFeeCap())
			underpricedTxMeter.Mark(1)
			// 将tx从pool中移除
			pool.removeTx(tx.Hash(), false)
		}
	}
	// Try to replace an existing transaction in the pending pool
	// 试着替换pending pool中一个已经存在的transaction
	from, _ := types.Sender(pool.signer, tx) // already validated
	if list := pool.pending[from]; list != nil && list.Overlaps(tx) {
		// Nonce already pending, check if required price bump is met
		// Nonce已经处于pending，检查是否需要price bump
		inserted, old := list.Add(tx, pool.config.PriceBump)
		if !inserted {
			pendingDiscardMeter.Mark(1)
			return false, ErrReplaceUnderpriced
		}
		// New transaction is better, replace old one
		// 新的transaction更好，替换旧的
		if old != nil {
			pool.all.Remove(old.Hash())
			pool.priced.Removed(1)
			pendingReplaceMeter.Mark(1)
		}
		pool.all.Add(tx, isLocal)
		pool.priced.Put(tx, isLocal)
		pool.journalTx(from, tx)
		pool.queueTxEvent(tx)
		log.Trace("Pooled new executable transaction", "hash", hash, "from", from, "to", tx.To())

		// Successful promotion, bump the heartbeat
		// 成功promotion，撞击心跳
		pool.beats[from] = time.Now()
		return old != nil, nil
	}
	// New transaction isn't replacing a pending one, push into queue
	// 新的transaction没有替换pending one，将它加入队列
	replaced, err = pool.enqueueTx(hash, tx, isLocal, true)
	if err != nil {
		return false, err
	}
	// Mark local addresses and journal local transactions
	// 标记local addresses以及journal local transactions
	if local && !pool.locals.contains(from) {
		log.Info("Setting new local account", "address", from)
		pool.locals.add(from)
		pool.priced.Removed(pool.all.RemoteToLocals(pool.locals)) // Migrate the remotes if it's marked as local first time.
	}
	if isLocal {
		localGauge.Inc(1)
	}
	pool.journalTx(from, tx)

	// 将新的future transaction加入到pool
	log.Trace("Pooled new future transaction", "hash", hash, "from", from, "to", tx.To())
	return replaced, nil
}

// enqueueTx inserts a new transaction into the non-executable transaction queue.
// enqueueTx插入一个新的transaction到non-executable transaction queue
//
// Note, this method assumes the pool lock is held!
func (pool *TxPool) enqueueTx(hash common.Hash, tx *types.Transaction, local bool, addAll bool) (bool, error) {
	// Try to insert the transaction into the future queue
	// 试着插入transaction到future queue中
	from, _ := types.Sender(pool.signer, tx) // already validated
	if pool.queue[from] == nil {
		pool.queue[from] = newTxList(false)
	}
	// 将tx加入队列中
	inserted, old := pool.queue[from].Add(tx, pool.config.PriceBump)
	if !inserted {
		// An older transaction was better, discard this
		// 如果一个older transaction更好，丢弃它
		queuedDiscardMeter.Mark(1)
		return false, ErrReplaceUnderpriced
	}
	// Discard any previous transaction and mark this
	// 丢弃任何之前的transaction
	if old != nil {
		pool.all.Remove(old.Hash())
		pool.priced.Removed(1)
		queuedReplaceMeter.Mark(1)
	} else {
		// Nothing was replaced, bump the queued counter
		// 没有东西被替换，queued counter加一
		queuedGauge.Inc(1)
	}
	// If the transaction isn't in lookup set but it's expected to be there,
	// show the error log.
	// 如果transaction不再lookup set，但是又期望在那里，展示错误日志
	if pool.all.Get(hash) == nil && !addAll {
		log.Error("Missing transaction in lookup set, please report the issue", "hash", hash)
	}
	if addAll {
		// 将transaction加入
		pool.all.Add(tx, local)
		pool.priced.Put(tx, local)
	}
	// If we never record the heartbeat, do it right now.
	// 如果我们从不记录heartbeat，现在就做
	if _, exist := pool.beats[from]; !exist {
		pool.beats[from] = time.Now()
	}
	return old != nil, nil
}

// journalTx adds the specified transaction to the local disk journal if it is
// deemed to have been sent from a local account.
// journalTx添加特定的transaction到本地的disk journal，如果它被认为是本地发送的
func (pool *TxPool) journalTx(from common.Address, tx *types.Transaction) {
	// Only journal if it's enabled and the transaction is local
	// 只有在enabled并且transaction是local的情况下，才journal
	if pool.journal == nil || !pool.locals.contains(from) {
		return
	}
	if err := pool.journal.insert(tx); err != nil {
		log.Warn("Failed to journal local transaction", "err", err)
	}
}

// promoteTx adds a transaction to the pending (processable) list of transactions
// and returns whether it was inserted or an older was better.
// promoteTx插入一个transaction到pending transactions（processable）并且返回是否它被插入
// 或者一个older更好
//
// Note, this method assumes the pool lock is held!
func (pool *TxPool) promoteTx(addr common.Address, hash common.Hash, tx *types.Transaction) bool {
	// Try to insert the transaction into the pending queue
	// 试着插入transaction到pending queue
	if pool.pending[addr] == nil {
		pool.pending[addr] = newTxList(true)
	}
	list := pool.pending[addr]

	// 加入list中
	inserted, old := list.Add(tx, pool.config.PriceBump)
	if !inserted {
		// An older transaction was better, discard this
		// 有个老的transaction更好，丢弃当前的
		pool.all.Remove(hash)
		pool.priced.Removed(1)
		pendingDiscardMeter.Mark(1)
		return false
	}
	// Otherwise discard any previous transaction and mark this
	// 否则移除任何潜在的transaction并且标记它
	if old != nil {
		pool.all.Remove(old.Hash())
		pool.priced.Removed(1)
		pendingReplaceMeter.Mark(1)
	} else {
		// Nothing was replaced, bump the pending counter
		pendingGauge.Inc(1)
	}
	// Set the potentially new pending nonce and notify any subsystems of the new tx
	// 设置潜在的新的pending nonce并且通知所有的子系统，关于新的tx
	pool.pendingNonces.set(addr, tx.Nonce()+1)

	// Successful promotion, bump the heartbeat
	// 成功promotion，heartbeat + 1
	pool.beats[addr] = time.Now()
	return true
}

// AddLocals enqueues a batch of transactions into the pool if they are valid, marking the
// senders as a local ones, ensuring they go around the local pricing constraints.
//
// This method is used to add transactions from the RPC API and performs synchronous pool
// reorganization and event propagation.
// 这个方法用于从RPC API添加transactions并且执行同步pool的reorganization以及event propagation
func (pool *TxPool) AddLocals(txs []*types.Transaction) []error {
	return pool.addTxs(txs, !pool.config.NoLocals, true)
}

// AddLocal enqueues a single local transaction into the pool if it is valid. This is
// a convenience wrapper aroundd AddLocals.
// AddLocal将单个的local transaction加入到pool，如果它是合法的话，这是AddLocals的一个方便的
// wrapper
func (pool *TxPool) AddLocal(tx *types.Transaction) error {
	errs := pool.AddLocals([]*types.Transaction{tx})
	return errs[0]
}

// AddRemotes enqueues a batch of transactions into the pool if they are valid. If the
// senders are not among the locally tracked ones, full pricing constraints will apply.
// AddRemotes将一批transactions加入到pool，如果它们是合法的话，如果sender不是locally tracked ones
// 会应用full priciing constraints
//
// This method is used to add transactions from the p2p network and does not wait for pool
// reorganization and internal event propagation.
// 这个方法用于从p2p network加入transactions，不等待pool reorganization以及内部的event propagation
func (pool *TxPool) AddRemotes(txs []*types.Transaction) []error {
	// 第三个参数为false，表示异步添加
	return pool.addTxs(txs, false, false)
}

// This is like AddRemotes, but waits for pool reorganization. Tests use this method.
// 类似于AddRemotes，但是等待pool reorganization
func (pool *TxPool) AddRemotesSync(txs []*types.Transaction) []error {
	return pool.addTxs(txs, false, true)
}

// This is like AddRemotes with a single transaction, but waits for pool reorganization. Tests use this method.
func (pool *TxPool) addRemoteSync(tx *types.Transaction) error {
	errs := pool.AddRemotesSync([]*types.Transaction{tx})
	return errs[0]
}

// AddRemote enqueues a single transaction into the pool if it is valid. This is a convenience
// wrapper around AddRemotes.
// AddRemote将单个的transaction加入到pool，如果它是合法的，这是对于AddRemotes方便的封装
//
// Deprecated: use AddRemotes
func (pool *TxPool) AddRemote(tx *types.Transaction) error {
	errs := pool.AddRemotes([]*types.Transaction{tx})
	return errs[0]
}

// addTxs attempts to queue a batch of transactions if they are valid.
// addTxs试着将一批transactions加入到队列，如果他们是合法的话
func (pool *TxPool) addTxs(txs []*types.Transaction, local, sync bool) []error {
	// Filter out known ones without obtaining the pool lock or recovering signatures
	var (
		errs = make([]error, len(txs))
		news = make([]*types.Transaction, 0, len(txs))
	)
	for i, tx := range txs {
		// If the transaction is known, pre-set the error slot
		// 如果transaction是已知的，提前设置error slot
		if pool.all.Get(tx.Hash()) != nil {
			errs[i] = ErrAlreadyKnown
			knownTxMeter.Mark(1)
			continue
		}
		// Exclude transactions with invalid signatures as soon as
		// possible and cache senders in transactions before
		// obtaining lock
		_, err := types.Sender(pool.signer, tx)
		if err != nil {
			errs[i] = ErrInvalidSender
			invalidTxMeter.Mark(1)
			continue
		}
		// Accumulate all unknown transactions for deeper processing
		// 累计所有未知的transactions用于进一步处理
		news = append(news, tx)
	}
	if len(news) == 0 {
		return errs
	}

	// Process all the new transaction and merge any errors into the original slice
	// 处理所有新的transaction并且合并任何的errors到original slice
	pool.mu.Lock()
	newErrs, dirtyAddrs := pool.addTxsLocked(news, local)
	pool.mu.Unlock()

	var nilSlot = 0
	for _, err := range newErrs {
		for errs[nilSlot] != nil {
			nilSlot++
		}
		errs[nilSlot] = err
		nilSlot++
	}
	// Reorg the pool internals if needed and return
	// Reorg pool的内部，如果需要的话并且返回
	done := pool.requestPromoteExecutables(dirtyAddrs)
	if sync {
		<-done
	}
	return errs
}

// addTxsLocked attempts to queue a batch of transactions if they are valid.
// The transaction pool lock must be held.
// addTxsLocked试着将一批的transactions入队，如果他们是合法的话
func (pool *TxPool) addTxsLocked(txs []*types.Transaction, local bool) ([]error, *accountSet) {
	dirty := newAccountSet(pool.signer)
	errs := make([]error, len(txs))
	for i, tx := range txs {
		// 将transaction加入到pool中
		replaced, err := pool.add(tx, local)
		errs[i] = err
		if err == nil && !replaced {
			dirty.addTx(tx)
		}
	}
	validTxMeter.Mark(int64(len(dirty.accounts)))
	return errs, dirty
}

// Status returns the status (unknown/pending/queued) of a batch of transactions
// identified by their hashes.
// Status返回一系列transactions的状态（unknown/pending/queued），通过它们的哈希值进行标识
func (pool *TxPool) Status(hashes []common.Hash) []TxStatus {
	status := make([]TxStatus, len(hashes))
	log.Info("Status", "length", len(status))
	for i, hash := range hashes {
		tx := pool.Get(hash)
		if tx == nil {
			continue
		}
		from, _ := types.Sender(pool.signer, tx) // already validated
		pool.mu.RLock()
		if txList := pool.pending[from]; txList != nil && txList.txs.items[tx.Nonce()] != nil {
			status[i] = TxStatusPending
		} else if txList := pool.queue[from]; txList != nil && txList.txs.items[tx.Nonce()] != nil {
			status[i] = TxStatusQueued
		}
		// implicit else: the tx may have been included into a block between
		// checking pool.Get and obtaining the lock. In that case, TxStatusUnknown is correct
		pool.mu.RUnlock()
	}
	return status
}

// Get returns a transaction if it is contained in the pool and nil otherwise.
func (pool *TxPool) Get(hash common.Hash) *types.Transaction {
	return pool.all.Get(hash)
}

// Has returns an indicator whether txpool has a transaction cached with the
// given hash.
func (pool *TxPool) Has(hash common.Hash) bool {
	return pool.all.Get(hash) != nil
}

// removeTx removes a single transaction from the queue, moving all subsequent
// transactions back to the future queue.
// removeTx从队列中移除单个transaction，将所有后续的transactions移入future queue
func (pool *TxPool) removeTx(hash common.Hash, outofbound bool) {
	// Fetch the transaction we wish to delete
	// 找到我们希望移除的transaction
	tx := pool.all.Get(hash)
	if tx == nil {
		return
	}
	addr, _ := types.Sender(pool.signer, tx) // already validated during insertion

	// Remove it from the list of known transactions
	// 从已知的transactions list中移除
	pool.all.Remove(hash)
	if outofbound {
		pool.priced.Removed(1)
	}
	if pool.locals.contains(addr) {
		localGauge.Dec(1)
	}
	// Remove the transaction from the pending lists and reset the account nonce
	// 从pending lists中移除transaction并且重置account nonce
	if pending := pool.pending[addr]; pending != nil {
		if removed, invalids := pending.Remove(tx); removed {
			// If no more pending transactions are left, remove the list
			if pending.Empty() {
				delete(pool.pending, addr)
			}
			// Postpone any invalidated transactions
			// 推迟任何非法的transactions
			for _, tx := range invalids {
				// Internal shuffle shouldn't touch the lookup set.
				// 内部的shuffle不应该触碰lookup set
				pool.enqueueTx(tx.Hash(), tx, false, false)
			}
			// Update the account nonce if needed
			// 更新account nonce，如果需要的话
			pool.pendingNonces.setIfLower(addr, tx.Nonce())
			// Reduce the pending counter
			pendingGauge.Dec(int64(1 + len(invalids)))
			return
		}
	}
	// Transaction is in the future queue
	// Transaction处于future queue中
	if future := pool.queue[addr]; future != nil {
		if removed, _ := future.Remove(tx); removed {
			// Reduce the queued counter
			queuedGauge.Dec(1)
		}
		if future.Empty() {
			delete(pool.queue, addr)
			delete(pool.beats, addr)
		}
	}
}

// requestReset requests a pool reset to the new head block.
// requestReset请求一个pool reset到新的head block
// The returned channel is closed when the reset has occurred.
// 返回的channel被关闭，当reset发生的时候
func (pool *TxPool) requestReset(oldHead *types.Header, newHead *types.Header) chan struct{} {
	select {
	case pool.reqResetCh <- &txpoolResetRequest{oldHead, newHead}:
		return <-pool.reorgDoneCh
	case <-pool.reorgShutdownCh:
		return pool.reorgShutdownCh
	}
}

// requestPromoteExecutables requests transaction promotion checks for the given addresses.
// requestPromoteExecutables请求对于一个给定的addresses的transaction promotion checks
// The returned channel is closed when the promotion checks have occurred.
func (pool *TxPool) requestPromoteExecutables(set *accountSet) chan struct{} {
	select {
	case pool.reqPromoteCh <- set:
		// 等待reorg完成
		return <-pool.reorgDoneCh
	case <-pool.reorgShutdownCh:
		return pool.reorgShutdownCh
	}
}

// queueTxEvent enqueues a transaction event to be sent in the next reorg run.
// queueTxEvent将一个transaction event入队，在下一个reorg运行的时候发送
func (pool *TxPool) queueTxEvent(tx *types.Transaction) {
	select {
	case pool.queueTxEventCh <- tx:
	case <-pool.reorgShutdownCh:
	}
}

// scheduleReorgLoop schedules runs of reset and promoteExecutables. Code above should not
// call those methods directly, but request them being run using requestReset and
// requestPromoteExecutables instead.
// scheduleReorgLoop调度reset以及promoteExecutables的运行，上面的代码不应直接调用这些方法
// 用requestReset以及requestPromoteExecutables请求它们运行
func (pool *TxPool) scheduleReorgLoop() {
	defer pool.wg.Done()

	var (
		// 当runReorg为active时为non-nil
		curDone       chan struct{} // non-nil while runReorg is active
		nextDone      = make(chan struct{})
		launchNextRun bool
		reset         *txpoolResetRequest
		dirtyAccounts *accountSet
		queuedEvents  = make(map[common.Address]*txSortedMap)
	)
	for {
		// Launch next background reorg if needed
		// 运行下一个background reorg，如果需要的话
		if curDone == nil && launchNextRun {
			// Run the background reorg and announcements
			// 运行后台的reorg和announcements
			log.Info("run reorg")
			go pool.runReorg(nextDone, reset, dirtyAccounts, queuedEvents)

			// Prepare everything for the next round of reorg
			// 准备好所有，用于下一轮的reorg
			curDone, nextDone = nextDone, make(chan struct{})
			// 重置launchNextRun
			launchNextRun = false

			reset, dirtyAccounts = nil, nil
			queuedEvents = make(map[common.Address]*txSortedMap)
		}

		select {
		case req := <-pool.reqResetCh:
			// Reset request: update head if request is already pending.
			// 重置request：更新head，如果request已经处于pending
			if reset == nil {
				reset = req
			} else {
				reset.newHead = req.newHead
			}
			// 下一次运行reorg
			launchNextRun = true
			// 返回一个nextDone的channel
			pool.reorgDoneCh <- nextDone

		case req := <-pool.reqPromoteCh:
			// Promote request: update address set if request is already pending.
			// 提升request：更新地址集，如果请求已经处于pending
			if dirtyAccounts == nil {
				dirtyAccounts = req
			} else {
				dirtyAccounts.merge(req)
			}
			launchNextRun = true
			// 给reorgDoneCh发送信息
			pool.reorgDoneCh <- nextDone

		case tx := <-pool.queueTxEventCh:
			// Queue up the event, but don't schedule a reorg. It's up to the caller to
			// request one later if they want the events sent.
			// 将一个event进行排队，但是不要调度一个reorg，取决于调用者之后请求，如果他们想要
			// 发送events
			addr, _ := types.Sender(pool.signer, tx)
			if _, ok := queuedEvents[addr]; !ok {
				queuedEvents[addr] = newTxSortedMap()
			}
			queuedEvents[addr].Put(tx)

		case <-curDone:
			curDone = nil

		case <-pool.reorgShutdownCh:
			// Wait for current run to finish.
			// 等待当前的运行结束
			if curDone != nil {
				<-curDone
			}
			// 关闭nextDone
			close(nextDone)
			return
		}
	}
}

// runReorg runs reset and promoteExecutables on behalf of scheduleReorgLoop.
// runReorg运行reset并且代表scheduleReorgLoop进行promoteExecutable
func (pool *TxPool) runReorg(done chan struct{}, reset *txpoolResetRequest, dirtyAccounts *accountSet, events map[common.Address]*txSortedMap) {
	defer func(t0 time.Time) {
		reorgDurationTimer.Update(time.Since(t0))
	}(time.Now())
	// 当reorg运行结束的时候，关闭done
	defer close(done)

	var promoteAddrs []common.Address
	if dirtyAccounts != nil && reset == nil {
		// Only dirty accounts need to be promoted, unless we're resetting.
		// For resets, all addresses in the tx queue will be promoted and
		// the flatten operation can be avoided.
		// 只有dirty accounts需要被promoted，除非我们正在重置
		// 对于重置，tx queue中的所有地址都会被promoted并且flatten操作会被避免
		promoteAddrs = dirtyAccounts.flatten()
	}
	pool.mu.Lock()
	if reset != nil {
		// Reset from the old head to the new, rescheduling any reorged transactions
		// 重置old head到新的，重新调度任何reorged transactions
		pool.reset(reset.oldHead, reset.newHead)

		// Nonces were reset, discard any events that became stale
		// Nonces被重置，丢弃任何变为stale的events
		for addr := range events {
			events[addr].Forward(pool.pendingNonces.get(addr))
			if events[addr].Len() == 0 {
				delete(events, addr)
			}
		}
		// Reset needs promote for all addresses
		// Reset需要重置所有地址
		promoteAddrs = make([]common.Address, 0, len(pool.queue))
		for addr := range pool.queue {
			promoteAddrs = append(promoteAddrs, addr)
		}
	}
	// Check for pending transactions for every account that sent new ones
	// 检查每一个account的pending transactions
	promoted := pool.promoteExecutables(promoteAddrs)

	// If a new block appeared, validate the pool of pending transactions. This will
	// remove any transaction that has been included in the block or was invalidated
	// because of another transaction (e.g. higher gas price).
	// 如果出现了一个新的block，检查pending transactions的pool，它会移除包含着在block中的所有
	// transactions，以及任何transaction，如果gas price更高
	if reset != nil {
		// 移除所有非法的unexecutable transactions
		pool.demoteUnexecutables()
		if reset.newHead != nil && pool.chainconfig.IsLondon(new(big.Int).Add(reset.newHead.Number, big.NewInt(1))) {
			pendingBaseFee := misc.CalcBaseFee(pool.chainconfig, reset.newHead)
			pool.priced.SetBaseFee(pendingBaseFee)
		}
		// Update all accounts to the latest known pending nonce
		// 更新所有的accounts到最新已知的pending nonce
		nonces := make(map[common.Address]uint64, len(pool.pending))
		for addr, list := range pool.pending {
			highestPending := list.LastElement()
			nonces[addr] = highestPending.Nonce() + 1
		}
		pool.pendingNonces.setAll(nonces)
	}
	// Ensure pool.queue and pool.pending sizes stay within the configured limits.
	// 确保pool.queue和pool.pending的大小依然在配置的limits内
	pool.truncatePending()
	pool.truncateQueue()

	dropBetweenReorgHistogram.Update(int64(pool.changesSinceReorg))
	pool.changesSinceReorg = 0 // Reset change counter
	pool.mu.Unlock()

	// Notify subsystems for newly added transactions
	// 通知子系统，关于新添加的transactions
	for _, tx := range promoted {
		addr, _ := types.Sender(pool.signer, tx)
		if _, ok := events[addr]; !ok {
			events[addr] = newTxSortedMap()
		}
		events[addr].Put(tx)
	}
	if len(events) > 0 {
		var txs []*types.Transaction
		for _, set := range events {
			txs = append(txs, set.Flatten()...)
		}
		// 发送transactions到txFeed, miner worker会对它进行订阅
		pool.txFeed.Send(NewTxsEvent{txs})
	}
}

// reset retrieves the current state of the blockchain and ensures the content
// of the transaction pool is valid with regard to the chain state.
// reset获取blockchain的当前状态并且确保transaction pool的内容是合法的，对于chian st
func (pool *TxPool) reset(oldHead, newHead *types.Header) {
	// If we're reorging an old state, reinject all dropped transactions
	// 如果我们要reorg一个old state，重新注入所有丢弃的transactions
	var reinject types.Transactions

	if oldHead != nil && oldHead.Hash() != newHead.ParentHash {
		// If the reorg is too deep, avoid doing it (will happen during fast sync)
		// 如果reorg太深，避免进行操作
		oldNum := oldHead.Number.Uint64()
		newNum := newHead.Number.Uint64()

		if depth := uint64(math.Abs(float64(oldNum) - float64(newNum))); depth > 64 {
			log.Debug("Skipping deep transaction reorg", "depth", depth)
		} else {
			// Reorg seems shallow enough to pull in all transactions into memory
			// Reorg看起来足够浅，能够将所有transactions加入到内存中
			var discarded, included types.Transactions
			var (
				rem = pool.chain.GetBlock(oldHead.Hash(), oldHead.Number.Uint64())
				add = pool.chain.GetBlock(newHead.Hash(), newHead.Number.Uint64())
			)
			if rem == nil {
				// This can happen if a setHead is performed, where we simply discard the old
				// head from the chain.
				// 如果执行了一个setHead的时候会发生，我们简单的从chain中丢弃old head
				// If that is the case, we don't have the lost transactions any more, and
				// there's nothing to add
				// 如果是这样的话，我们不再拥有lost transactions，并且没有东西需要添加
				if newNum >= oldNum {
					// If we reorged to a same or higher number, then it's not a case of setHead
					log.Warn("Transaction pool reset with missing oldhead",
						"old", oldHead.Hash(), "oldnum", oldNum, "new", newHead.Hash(), "newnum", newNum)
					return
				}
				// If the reorg ended up on a lower number, it's indicative of setHead being the cause
				log.Debug("Skipping transaction reset caused by setHead",
					"old", oldHead.Hash(), "oldnum", oldNum, "new", newHead.Hash(), "newnum", newNum)
				// We still need to update the current state s.th. the lost transactions can be readded by the user
			} else {
				for rem.NumberU64() > add.NumberU64() {
					discarded = append(discarded, rem.Transactions()...)
					if rem = pool.chain.GetBlock(rem.ParentHash(), rem.NumberU64()-1); rem == nil {
						log.Error("Unrooted old chain seen by tx pool", "block", oldHead.Number, "hash", oldHead.Hash())
						return
					}
				}
				for add.NumberU64() > rem.NumberU64() {
					included = append(included, add.Transactions()...)
					if add = pool.chain.GetBlock(add.ParentHash(), add.NumberU64()-1); add == nil {
						log.Error("Unrooted new chain seen by tx pool", "block", newHead.Number, "hash", newHead.Hash())
						return
					}
				}
				for rem.Hash() != add.Hash() {
					discarded = append(discarded, rem.Transactions()...)
					if rem = pool.chain.GetBlock(rem.ParentHash(), rem.NumberU64()-1); rem == nil {
						log.Error("Unrooted old chain seen by tx pool", "block", oldHead.Number, "hash", oldHead.Hash())
						return
					}
					included = append(included, add.Transactions()...)
					if add = pool.chain.GetBlock(add.ParentHash(), add.NumberU64()-1); add == nil {
						log.Error("Unrooted new chain seen by tx pool", "block", newHead.Number, "hash", newHead.Hash())
						return
					}
				}
				reinject = types.TxDifference(discarded, included)
			}
		}
	}
	// Initialize the internal state to the current head
	// 将internal state初始化到当前的head
	if newHead == nil {
		newHead = pool.chain.CurrentBlock().Header() // Special case during testing
	}
	statedb, err := pool.chain.StateAt(newHead.Root)
	if err != nil {
		log.Error("Failed to reset txpool state", "err", err)
		return
	}
	pool.currentState = statedb
	pool.pendingNonces = newTxNoncer(statedb)
	pool.currentMaxGas = newHead.GasLimit

	// Inject any transactions discarded due to reorgs
	// 注入任何在reorgs期间丢弃的transactions
	log.Debug("Reinjecting stale transactions", "count", len(reinject))
	senderCacher.recover(pool.signer, reinject)
	pool.addTxsLocked(reinject, false)

	// Update all fork indicator by next pending block number.
	// 更新所有的fork indicator，通过下一个pending block number
	next := new(big.Int).Add(newHead.Number, big.NewInt(1))
	pool.istanbul = pool.chainconfig.IsIstanbul(next)
	pool.eip2718 = pool.chainconfig.IsBerlin(next)
	pool.eip1559 = pool.chainconfig.IsLondon(next)
}

// promoteExecutables moves transactions that have become processable from the
// future queue to the set of pending transactions. During this process, all
// invalidated transactions (low nonce, low balance) are deleted.
// promoteExecutables移动processable transactions，从future queue到一系列的pending transactions
// 在这个过程中，所有非法的transactions（low nonce, low balance）都会被删除
func (pool *TxPool) promoteExecutables(accounts []common.Address) []*types.Transaction {
	// Track the promoted transactions to broadcast them at once
	// 追踪promoted transactions来立刻广播它们
	var promoted []*types.Transaction

	// Iterate over all accounts and promote any executable transactions
	// 遍历所有的accounts并且promote任何可执行的transactions
	for _, addr := range accounts {
		list := pool.queue[addr]
		if list == nil {
			continue // Just in case someone calls with a non existing account
		}
		// Drop all transactions that are deemed too old (low nonce)
		// 丢弃所有被认为太老的transactions
		forwards := list.Forward(pool.currentState.GetNonce(addr))
		for _, tx := range forwards {
			hash := tx.Hash()
			pool.all.Remove(hash)
		}
		log.Trace("Removed old queued transactions", "count", len(forwards))
		// Drop all transactions that are too costly (low balance or out of gas)
		// 丢弃所有costly transactions（low balance或者out of gas）
		drops, _ := list.Filter(pool.currentState.GetBalance(addr), pool.currentMaxGas)
		for _, tx := range drops {
			hash := tx.Hash()
			pool.all.Remove(hash)
		}
		log.Trace("Removed unpayable queued transactions", "count", len(drops))
		queuedNofundsMeter.Mark(int64(len(drops)))

		// Gather all executable transactions and promote them
		// 收集所有可执行的transactions并且promote它们
		readies := list.Ready(pool.pendingNonces.get(addr))
		for _, tx := range readies {
			hash := tx.Hash()
			if pool.promoteTx(addr, hash, tx) {
				// 获取promoted tx
				promoted = append(promoted, tx)
			}
		}
		log.Trace("Promoted queued transactions", "count", len(promoted))
		queuedGauge.Dec(int64(len(readies)))

		// Drop all transactions over the allowed limit
		// 丢弃所有的transactions，超过允许的limit
		var caps types.Transactions
		if !pool.locals.contains(addr) {
			caps = list.Cap(int(pool.config.AccountQueue))
			for _, tx := range caps {
				hash := tx.Hash()
				pool.all.Remove(hash)
				log.Trace("Removed cap-exceeding queued transaction", "hash", hash)
			}
			queuedRateLimitMeter.Mark(int64(len(caps)))
		}
		// Mark all the items dropped as removed
		// 将所有丢弃的items标记为removed
		pool.priced.Removed(len(forwards) + len(drops) + len(caps))
		queuedGauge.Dec(int64(len(forwards) + len(drops) + len(caps)))
		if pool.locals.contains(addr) {
			localGauge.Dec(int64(len(forwards) + len(drops) + len(caps)))
		}
		// Delete the entire queue entry if it became empty.
		// 移除整个queue entry，如果它变为空的话
		if list.Empty() {
			// 如果移除完了，则从pool中删除
			delete(pool.queue, addr)
			delete(pool.beats, addr)
		}
	}
	return promoted
}

// truncatePending removes transactions from the pending queue if the pool is above the
// pending limit. The algorithm tries to reduce transaction counts by an approximately
// equal number for all for accounts with many pending transactions.
// truncatePending从pending queue中移除transactions，如果pool超过了pending limit
func (pool *TxPool) truncatePending() {
	pending := uint64(0)
	for _, list := range pool.pending {
		pending += uint64(list.Len())
	}
	if pending <= pool.config.GlobalSlots {
		return
	}

	pendingBeforeCap := pending
	// Assemble a spam order to penalize large transactors first
	spammers := prque.New(nil)
	for addr, list := range pool.pending {
		// Only evict transactions from high rollers
		if !pool.locals.contains(addr) && uint64(list.Len()) > pool.config.AccountSlots {
			spammers.Push(addr, int64(list.Len()))
		}
	}
	// Gradually drop transactions from offenders
	offenders := []common.Address{}
	for pending > pool.config.GlobalSlots && !spammers.Empty() {
		// Retrieve the next offender if not local address
		offender, _ := spammers.Pop()
		offenders = append(offenders, offender.(common.Address))

		// Equalize balances until all the same or below threshold
		if len(offenders) > 1 {
			// Calculate the equalization threshold for all current offenders
			threshold := pool.pending[offender.(common.Address)].Len()

			// Iteratively reduce all offenders until below limit or threshold reached
			for pending > pool.config.GlobalSlots && pool.pending[offenders[len(offenders)-2]].Len() > threshold {
				for i := 0; i < len(offenders)-1; i++ {
					list := pool.pending[offenders[i]]

					caps := list.Cap(list.Len() - 1)
					for _, tx := range caps {
						// Drop the transaction from the global pools too
						hash := tx.Hash()
						pool.all.Remove(hash)

						// Update the account nonce to the dropped transaction
						pool.pendingNonces.setIfLower(offenders[i], tx.Nonce())
						log.Trace("Removed fairness-exceeding pending transaction", "hash", hash)
					}
					pool.priced.Removed(len(caps))
					pendingGauge.Dec(int64(len(caps)))
					if pool.locals.contains(offenders[i]) {
						localGauge.Dec(int64(len(caps)))
					}
					pending--
				}
			}
		}
	}

	// If still above threshold, reduce to limit or min allowance
	if pending > pool.config.GlobalSlots && len(offenders) > 0 {
		for pending > pool.config.GlobalSlots && uint64(pool.pending[offenders[len(offenders)-1]].Len()) > pool.config.AccountSlots {
			for _, addr := range offenders {
				list := pool.pending[addr]

				caps := list.Cap(list.Len() - 1)
				for _, tx := range caps {
					// Drop the transaction from the global pools too
					hash := tx.Hash()
					pool.all.Remove(hash)

					// Update the account nonce to the dropped transaction
					pool.pendingNonces.setIfLower(addr, tx.Nonce())
					log.Trace("Removed fairness-exceeding pending transaction", "hash", hash)
				}
				pool.priced.Removed(len(caps))
				pendingGauge.Dec(int64(len(caps)))
				if pool.locals.contains(addr) {
					localGauge.Dec(int64(len(caps)))
				}
				pending--
			}
		}
	}
	pendingRateLimitMeter.Mark(int64(pendingBeforeCap - pending))
}

// truncateQueue drops the oldes transactions in the queue if the pool is above the global queue limit.
// truncateQueue移除更老的transactions，从队列中，如果pool超过了global queue limit
func (pool *TxPool) truncateQueue() {
	queued := uint64(0)
	for _, list := range pool.queue {
		queued += uint64(list.Len())
	}
	if queued <= pool.config.GlobalQueue {
		return
	}

	// Sort all accounts with queued transactions by heartbeat
	// 对所有有着queued transactions的accounts按照心跳进行排序
	addresses := make(addressesByHeartbeat, 0, len(pool.queue))
	for addr := range pool.queue {
		if !pool.locals.contains(addr) { // don't drop locals
			addresses = append(addresses, addressByHeartbeat{addr, pool.beats[addr]})
		}
	}
	sort.Sort(sort.Reverse(addresses))

	// Drop transactions until the total is below the limit or only locals remain
	for drop := queued - pool.config.GlobalQueue; drop > 0 && len(addresses) > 0; {
		addr := addresses[len(addresses)-1]
		list := pool.queue[addr.address]

		addresses = addresses[:len(addresses)-1]

		// Drop all transactions if they are less than the overflow
		if size := uint64(list.Len()); size <= drop {
			for _, tx := range list.Flatten() {
				pool.removeTx(tx.Hash(), true)
			}
			drop -= size
			queuedRateLimitMeter.Mark(int64(size))
			continue
		}
		// Otherwise drop only last few transactions
		txs := list.Flatten()
		for i := len(txs) - 1; i >= 0 && drop > 0; i-- {
			pool.removeTx(txs[i].Hash(), true)
			drop--
			queuedRateLimitMeter.Mark(1)
		}
	}
}

// demoteUnexecutables removes invalid and processed transactions from the pools
// executable/pending queue and any subsequent transactions that become unexecutable
// are moved back into the future queue.
// demoteUnexecutables移除非法的和处理过的transactions，从pools的executable/pending对垒
// 并且后续的transactions变为unexecutable会被移动到future queue
//
// Note: transactions are not marked as removed in the priced list because re-heaping
// is always explicitly triggered by SetBaseFee and it would be unnecessary and wasteful
// to trigger a re-heap is this function
func (pool *TxPool) demoteUnexecutables() {
	// Iterate over all accounts and demote any non-executable transactions
	// 遍历所有的accounts并且demote任何non-executable transactions
	for addr, list := range pool.pending {
		nonce := pool.currentState.GetNonce(addr)

		// Drop all transactions that are deemed too old (low nonce)
		// 丢弃所有太老的transactions
		olds := list.Forward(nonce)
		for _, tx := range olds {
			hash := tx.Hash()
			pool.all.Remove(hash)
			log.Trace("Removed old pending transaction", "hash", hash)
		}
		// Drop all transactions that are too costly (low balance or out of gas), and queue any invalids back for later
		// 丢弃所有costly transactions（low balance或者out of gas）并且排队任何非法的
		drops, invalids := list.Filter(pool.currentState.GetBalance(addr), pool.currentMaxGas)
		for _, tx := range drops {
			hash := tx.Hash()
			log.Trace("Removed unpayable pending transaction", "hash", hash)
			pool.all.Remove(hash)
		}
		pendingNofundsMeter.Mark(int64(len(drops)))

		for _, tx := range invalids {
			hash := tx.Hash()
			log.Trace("Demoting pending transaction", "hash", hash)

			// Internal shuffle shouldn't touch the lookup set.
			pool.enqueueTx(hash, tx, false, false)
		}
		pendingGauge.Dec(int64(len(olds) + len(drops) + len(invalids)))
		if pool.locals.contains(addr) {
			localGauge.Dec(int64(len(olds) + len(drops) + len(invalids)))
		}
		// If there's a gap in front, alert (should never happen) and postpone all transactions
		// 如果在front有一个gap，告警（不应该发生）并且延迟所有的transactions
		if list.Len() > 0 && list.txs.Get(nonce) == nil {
			gapped := list.Cap(0)
			for _, tx := range gapped {
				hash := tx.Hash()
				log.Error("Demoting invalidated transaction", "hash", hash)

				// Internal shuffle shouldn't touch the lookup set.
				pool.enqueueTx(hash, tx, false, false)
			}
			pendingGauge.Dec(int64(len(gapped)))
			// This might happen in a reorg, so log it to the metering
			blockReorgInvalidatedTx.Mark(int64(len(gapped)))
		}
		// Delete the entire pending entry if it became empty.
		if list.Empty() {
			delete(pool.pending, addr)
		}
	}
}

// addressByHeartbeat is an account address tagged with its last activity timestamp.
type addressByHeartbeat struct {
	address   common.Address
	heartbeat time.Time
}

type addressesByHeartbeat []addressByHeartbeat

func (a addressesByHeartbeat) Len() int           { return len(a) }
func (a addressesByHeartbeat) Less(i, j int) bool { return a[i].heartbeat.Before(a[j].heartbeat) }
func (a addressesByHeartbeat) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// accountSet is simply a set of addresses to check for existence, and a signer
// capable of deriving addresses from transactions.
type accountSet struct {
	accounts map[common.Address]struct{}
	signer   types.Signer
	cache    *[]common.Address
}

// newAccountSet creates a new address set with an associated signer for sender
// derivations.
func newAccountSet(signer types.Signer, addrs ...common.Address) *accountSet {
	as := &accountSet{
		accounts: make(map[common.Address]struct{}),
		signer:   signer,
	}
	for _, addr := range addrs {
		as.add(addr)
	}
	return as
}

// contains checks if a given address is contained within the set.
func (as *accountSet) contains(addr common.Address) bool {
	_, exist := as.accounts[addr]
	return exist
}

func (as *accountSet) empty() bool {
	return len(as.accounts) == 0
}

// containsTx checks if the sender of a given tx is within the set. If the sender
// cannot be derived, this method returns false.
func (as *accountSet) containsTx(tx *types.Transaction) bool {
	if addr, err := types.Sender(as.signer, tx); err == nil {
		return as.contains(addr)
	}
	return false
}

// add inserts a new address into the set to track.
// add插入一个新的地址到集合中用于追踪
func (as *accountSet) add(addr common.Address) {
	as.accounts[addr] = struct{}{}
	as.cache = nil
}

// addTx adds the sender of tx into the set.
func (as *accountSet) addTx(tx *types.Transaction) {
	if addr, err := types.Sender(as.signer, tx); err == nil {
		as.add(addr)
	}
}

// flatten returns the list of addresses within this set, also caching it for later
// reuse. The returned slice should not be changed!
func (as *accountSet) flatten() []common.Address {
	if as.cache == nil {
		accounts := make([]common.Address, 0, len(as.accounts))
		for account := range as.accounts {
			accounts = append(accounts, account)
		}
		as.cache = &accounts
	}
	return *as.cache
}

// merge adds all addresses from the 'other' set into 'as'.
func (as *accountSet) merge(other *accountSet) {
	for addr := range other.accounts {
		as.accounts[addr] = struct{}{}
	}
	as.cache = nil
}

// txLookup is used internally by TxPool to track transactions while allowing
// lookup without mutex contention.
// txLookup由TxPool在内部使用，来追踪transactions，同时允许在没有锁竞争的情况下进行查询
//
// Note, although this type is properly protected against concurrent access, it
// is **not** a type that should ever be mutated or even exposed outside of the
// transaction pool, since its internal state is tightly coupled with the pools
// internal mechanisms. The sole purpose of the type is to permit out-of-bound
// peeking into the pool in TxPool.Get without having to acquire the widely scoped
// TxPool.mu mutex.
//
// This lookup set combines the notion of "local transactions", which is useful
// to build upper-level structure.
type txLookup struct {
	slots   int
	lock    sync.RWMutex
	locals  map[common.Hash]*types.Transaction
	remotes map[common.Hash]*types.Transaction
}

// newTxLookup returns a new txLookup structure.
func newTxLookup() *txLookup {
	return &txLookup{
		locals:  make(map[common.Hash]*types.Transaction),
		remotes: make(map[common.Hash]*types.Transaction),
	}
}

// Range calls f on each key and value present in the map. The callback passed
// should return the indicator whether the iteration needs to be continued.
// Callers need to specify which set (or both) to be iterated.
func (t *txLookup) Range(f func(hash common.Hash, tx *types.Transaction, local bool) bool, local bool, remote bool) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	if local {
		for key, value := range t.locals {
			if !f(key, value, true) {
				return
			}
		}
	}
	if remote {
		for key, value := range t.remotes {
			if !f(key, value, false) {
				return
			}
		}
	}
}

// Get returns a transaction if it exists in the lookup, or nil if not found.
func (t *txLookup) Get(hash common.Hash) *types.Transaction {
	t.lock.RLock()
	defer t.lock.RUnlock()

	if tx := t.locals[hash]; tx != nil {
		return tx
	}
	return t.remotes[hash]
}

// GetLocal returns a transaction if it exists in the lookup, or nil if not found.
func (t *txLookup) GetLocal(hash common.Hash) *types.Transaction {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return t.locals[hash]
}

// GetRemote returns a transaction if it exists in the lookup, or nil if not found.
func (t *txLookup) GetRemote(hash common.Hash) *types.Transaction {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return t.remotes[hash]
}

// Count returns the current number of transactions in the lookup.
func (t *txLookup) Count() int {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return len(t.locals) + len(t.remotes)
}

// LocalCount returns the current number of local transactions in the lookup.
func (t *txLookup) LocalCount() int {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return len(t.locals)
}

// RemoteCount returns the current number of remote transactions in the lookup.
func (t *txLookup) RemoteCount() int {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return len(t.remotes)
}

// Slots returns the current number of slots used in the lookup.
func (t *txLookup) Slots() int {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return t.slots
}

// Add adds a transaction to the lookup.
// 将一个transaction加入到lookup
func (t *txLookup) Add(tx *types.Transaction, local bool) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.slots += numSlots(tx)
	slotsGauge.Update(int64(t.slots))

	// 加入到locals或者remotes列表中
	if local {
		t.locals[tx.Hash()] = tx
	} else {
		t.remotes[tx.Hash()] = tx
	}
}

// Remove removes a transaction from the lookup.
func (t *txLookup) Remove(hash common.Hash) {
	t.lock.Lock()
	defer t.lock.Unlock()

	tx, ok := t.locals[hash]
	if !ok {
		tx, ok = t.remotes[hash]
	}
	if !ok {
		log.Error("No transaction found to be deleted", "hash", hash)
		return
	}
	t.slots -= numSlots(tx)
	slotsGauge.Update(int64(t.slots))

	delete(t.locals, hash)
	delete(t.remotes, hash)
}

// RemoteToLocals migrates the transactions belongs to the given locals to locals
// set. The assumption is held the locals set is thread-safe to be used.
func (t *txLookup) RemoteToLocals(locals *accountSet) int {
	t.lock.Lock()
	defer t.lock.Unlock()

	var migrated int
	for hash, tx := range t.remotes {
		if locals.containsTx(tx) {
			t.locals[hash] = tx
			delete(t.remotes, hash)
			migrated += 1
		}
	}
	return migrated
}

// RemotesBelowTip finds all remote transactions below the given tip threshold.
func (t *txLookup) RemotesBelowTip(threshold *big.Int) types.Transactions {
	found := make(types.Transactions, 0, 128)
	t.Range(func(hash common.Hash, tx *types.Transaction, local bool) bool {
		if tx.GasTipCapIntCmp(threshold) < 0 {
			found = append(found, tx)
		}
		return true
	}, false, true) // Only iterate remotes
	return found
}

// numSlots calculates the number of slots needed for a single transaction.
// numSlots计算单个transaction所需的slots的数目
func numSlots(tx *types.Transaction) int {
	return int((tx.Size() + txSlotSize - 1) / txSlotSize)
}
