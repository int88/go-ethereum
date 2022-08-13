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

// Package fetcher contains the announcement based header, blocks or transaction synchronisation.
package fetcher

import (
	"errors"
	"math/rand"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/prque"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/protocols/eth"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/trie"
)

const (
	lightTimeout  = time.Millisecond       // Time allowance before an announced header is explicitly requested
	arriveTimeout = 500 * time.Millisecond // Time allowance before an announced block/transaction is explicitly requested
	// 整理fetches的快要过期的announces的时间间隔
	gatherSlack  = 100 * time.Millisecond // Interval used to collate almost-expired announces with fetches
	fetchTimeout = 5 * time.Second        // Maximum allotted time to return an explicitly requested block/transaction
)

const (
	maxUncleDist = 7 // Maximum allowed backward distance from the chain head
	// 从chain head开始能排队的最大的distance
	maxQueueDist = 32 // Maximum allowed distance from the chain head to queue
	// 一个peer能announce的最多的unique blocks或者headers的数目
	hashLimit  = 256 // Maximum number of unique blocks or headers a peer may have announced
	blockLimit = 64  // Maximum number of unique blocks a peer may have delivered
)

var (
	blockAnnounceInMeter   = metrics.NewRegisteredMeter("eth/fetcher/block/announces/in", nil)
	blockAnnounceOutTimer  = metrics.NewRegisteredTimer("eth/fetcher/block/announces/out", nil)
	blockAnnounceDropMeter = metrics.NewRegisteredMeter("eth/fetcher/block/announces/drop", nil)
	blockAnnounceDOSMeter  = metrics.NewRegisteredMeter("eth/fetcher/block/announces/dos", nil)

	blockBroadcastInMeter   = metrics.NewRegisteredMeter("eth/fetcher/block/broadcasts/in", nil)
	blockBroadcastOutTimer  = metrics.NewRegisteredTimer("eth/fetcher/block/broadcasts/out", nil)
	blockBroadcastDropMeter = metrics.NewRegisteredMeter("eth/fetcher/block/broadcasts/drop", nil)
	blockBroadcastDOSMeter  = metrics.NewRegisteredMeter("eth/fetcher/block/broadcasts/dos", nil)

	headerFetchMeter = metrics.NewRegisteredMeter("eth/fetcher/block/headers", nil)
	bodyFetchMeter   = metrics.NewRegisteredMeter("eth/fetcher/block/bodies", nil)

	headerFilterInMeter  = metrics.NewRegisteredMeter("eth/fetcher/block/filter/headers/in", nil)
	headerFilterOutMeter = metrics.NewRegisteredMeter("eth/fetcher/block/filter/headers/out", nil)
	bodyFilterInMeter    = metrics.NewRegisteredMeter("eth/fetcher/block/filter/bodies/in", nil)
	bodyFilterOutMeter   = metrics.NewRegisteredMeter("eth/fetcher/block/filter/bodies/out", nil)
)

var errTerminated = errors.New("terminated")

// HeaderRetrievalFn is a callback type for retrieving a header from the local chain.
// HeaderRetrievalFn是一个回调函数类型，用于从local chain获取一个header
type HeaderRetrievalFn func(common.Hash) *types.Header

// blockRetrievalFn is a callback type for retrieving a block from the local chain.
type blockRetrievalFn func(common.Hash) *types.Block

// headerRequesterFn is a callback type for sending a header retrieval request.
// headerRequesterFn是一个callback类型用于发送一个header retrieval request
type headerRequesterFn func(common.Hash, chan *eth.Response) (*eth.Request, error)

// bodyRequesterFn is a callback type for sending a body retrieval request.
// bodyRequesterFn是一个callback类型用于发送一个body retrieval request
type bodyRequesterFn func([]common.Hash, chan *eth.Response) (*eth.Request, error)

// headerVerifierFn is a callback type to verify a block's header for fast propagation.
type headerVerifierFn func(header *types.Header) error

// blockBroadcasterFn is a callback type for broadcasting a block to connected peers.
// blockBroadcasterFn是一个回调函数类型用于广播一个block到连接的peers
type blockBroadcasterFn func(block *types.Block, propagate bool)

// chainHeightFn is a callback type to retrieve the current chain height.
type chainHeightFn func() uint64

// headersInsertFn is a callback type to insert a batch of headers into the local chain.
type headersInsertFn func(headers []*types.Header) (int, error)

// chainInsertFn is a callback type to insert a batch of blocks into the local chain.
type chainInsertFn func(types.Blocks) (int, error)

// peerDropFn is a callback type for dropping a peer detected as malicious.
type peerDropFn func(id string)

// blockAnnounce is the hash notification of the availability of a new block in the
// network.
// blockAnnounce是hash notification，通知在网络中一个新的block可用
type blockAnnounce struct {
	// 被announce的block的哈希值
	hash common.Hash // Hash of the block being announced
	// 被announced的block的number
	number uint64 // Number of the block being announced (0 = unknown | old protocol)
	// 部分重新组装起来的Header of block
	header *types.Header // Header of the block partially reassembled (new protocol)
	time   time.Time     // Timestamp of the announcement

	// 产生这个notification的peer的identifier
	origin string // Identifier of the peer originating the notification

	// Fetcher函数用于获取一个announced block的header
	fetchHeader headerRequesterFn // Fetcher function to retrieve the header of an announced block
	// Fetcher函数用于获取一个announced block的body
	fetchBodies bodyRequesterFn // Fetcher function to retrieve the body of an announced block
}

// headerFilterTask represents a batch of headers needing fetcher filtering.
// headerFilterTask代表一批headers，需要fetcher进行filtering
type headerFilterTask struct {
	peer    string          // The source peer of block headers
	headers []*types.Header // Collection of headers to filter
	time    time.Time       // Arrival time of the headers
}

// bodyFilterTask represents a batch of block bodies (transactions and uncles)
// needing fetcher filtering.
// bodyFilterTask代表一批的block bodies（transactions以及uncles）需要fetcher filtering
type bodyFilterTask struct {
	peer         string                 // The source peer of block bodies
	transactions [][]*types.Transaction // Collection of transactions per block bodies
	uncles       [][]*types.Header      // Collection of uncles per block bodies
	time         time.Time              // Arrival time of the blocks' contents
}

// blockOrHeaderInject represents a schedules import operation.
// blockOrHeaderInject代表一个schedules import操作
type blockOrHeaderInject struct {
	origin string

	// 用于light mode fetcher，它只关心header
	header *types.Header // Used for light mode fetcher which only cares about header.
	// 用于normal mode fetcher，导入full block
	block *types.Block // Used for normal mode fetcher which imports full block.
}

// number returns the block number of the injected object.
func (inject *blockOrHeaderInject) number() uint64 {
	if inject.header != nil {
		return inject.header.Number.Uint64()
	}
	return inject.block.NumberU64()
}

// number returns the block hash of the injected object.
func (inject *blockOrHeaderInject) hash() common.Hash {
	if inject.header != nil {
		return inject.header.Hash()
	}
	return inject.block.Hash()
}

// BlockFetcher is responsible for accumulating block announcements from various peers
// and scheduling them for retrieval.
// BlockFetcher负责从各个peers收集block announcements并且调度它们用于获取
type BlockFetcher struct {
	light bool // The indicator whether it's a light fetcher or normal one.

	// Various event channels
	// 各种事件的channels
	notify chan *blockAnnounce
	inject chan *blockOrHeaderInject

	// channel的channel
	headerFilter chan chan *headerFilterTask
	bodyFilter   chan chan *bodyFilterTask

	done chan common.Hash
	quit chan struct{}

	// Announce states
	announces map[string]int // Per peer blockAnnounce counts to prevent memory exhaustion
	// 已经announced blocks，调度用于fetching
	announced map[common.Hash][]*blockAnnounce // Announced blocks, scheduled for fetching
	// 当前正在fetcing的blocks
	fetching map[common.Hash]*blockAnnounce // Announced blocks, currently fetching
	// header已经fetched的blocks，等待用于body retreival
	fetched map[common.Hash][]*blockAnnounce // Blocks with headers fetched, scheduled for body retrieval
	// 有着headers的Blocks，当前正在body-completing
	completing map[common.Hash]*blockAnnounce // Blocks with headers, currently body-completing

	// Block cache
	// Block的缓存
	// Queue包含了导入操作（按照block number进行排序）
	queue  *prque.Prque                         // Queue containing the import operations (block number sorted)
	queues map[string]int                       // Per peer block counts to prevent memory exhaustion
	queued map[common.Hash]*blockOrHeaderInject // Set of already queued blocks (to dedup imports)

	// Callbacks
	// 回调函数
	getHeader      HeaderRetrievalFn  // Retrieves a header from the local chain	// 从local chain获取一个header
	getBlock       blockRetrievalFn   // Retrieves a block from the local chain		// 从local chain获取一个block
	verifyHeader   headerVerifierFn   // Checks if a block's headers have a valid proof of work	// 检查一个block的headers是否有正确的pow
	broadcastBlock blockBroadcasterFn // Broadcasts a block to connected peers	// 广播一个block到连接的peers
	chainHeight    chainHeightFn      // Retrieves the current chain's height	// 获取当前chain的height
	insertHeaders  headersInsertFn    // Injects a batch of headers into the chain	// 注入一系列的headers到chain
	insertChain    chainInsertFn      // Injects a batch of blocks into the chain	// 注入一系列的blocks到chain
	dropPeer       peerDropFn         // Drops a peer for misbehaving	// 丢弃misbehaving的peer

	// Testing hooks
	// 当从blockAnnounce添加或者移除一个hash的时候被调用
	announceChangeHook func(common.Hash, bool) // Method to call upon adding or deleting a hash from the blockAnnounce list
	// 当从import queue中添加或者移除一个block的时候被调用
	queueChangeHook func(common.Hash, bool) // Method to call upon adding or deleting a block from the import queue
	// 当开始一个block或者header fetch的时候被调用
	fetchingHook func([]common.Hash) // Method to call upon starting a block (eth/61) or header (eth/62) fetch
	// 当开始一个block fetching的时候被调用
	completingHook func([]common.Hash) // Method to call upon starting a block body fetch (eth/62)
	// 当成功导入一个header或者block的时候被调用
	importedHook func(*types.Header, *types.Block) // Method to call upon successful header or block import (both eth/61 and eth/62)
}

// NewBlockFetcher creates a block fetcher to retrieve blocks based on hash announcements.
// NewBlockFetcher创建一个block fetcher用于获取blocks，基于hash announcements
// 其中一些hook函数都通过命令行参数指定
func NewBlockFetcher(light bool, getHeader HeaderRetrievalFn, getBlock blockRetrievalFn, verifyHeader headerVerifierFn, broadcastBlock blockBroadcasterFn, chainHeight chainHeightFn, insertHeaders headersInsertFn, insertChain chainInsertFn, dropPeer peerDropFn) *BlockFetcher {
	return &BlockFetcher{
		light:          light,
		notify:         make(chan *blockAnnounce),
		inject:         make(chan *blockOrHeaderInject),
		headerFilter:   make(chan chan *headerFilterTask),
		bodyFilter:     make(chan chan *bodyFilterTask),
		done:           make(chan common.Hash),
		quit:           make(chan struct{}),
		announces:      make(map[string]int),
		announced:      make(map[common.Hash][]*blockAnnounce),
		fetching:       make(map[common.Hash]*blockAnnounce),
		fetched:        make(map[common.Hash][]*blockAnnounce),
		completing:     make(map[common.Hash]*blockAnnounce),
		queue:          prque.New(nil),
		queues:         make(map[string]int),
		queued:         make(map[common.Hash]*blockOrHeaderInject),
		getHeader:      getHeader,
		getBlock:       getBlock,
		verifyHeader:   verifyHeader,
		broadcastBlock: broadcastBlock,
		chainHeight:    chainHeight,
		insertHeaders:  insertHeaders,
		insertChain:    insertChain,
		dropPeer:       dropPeer,
	}
}

// Start boots up the announcement based synchroniser, accepting and processing
// hash notifications and block fetches until termination requested.
// Start启动基于announcement的synchroniser，接收并且处理hash notifications并且进行block
// fetches，直到请求termination
func (f *BlockFetcher) Start() {
	go f.loop()
}

// Stop terminates the announcement based synchroniser, canceling all pending
// operations.
func (f *BlockFetcher) Stop() {
	close(f.quit)
}

// Notify announces the fetcher of the potential availability of a new block in
// the network.
// Notify通知fetcher在network中一个新的block潜在可用
func (f *BlockFetcher) Notify(peer string, hash common.Hash, number uint64, time time.Time,
	headerFetcher headerRequesterFn, bodyFetcher bodyRequesterFn) error {
	block := &blockAnnounce{
		hash:   hash,
		number: number,
		time:   time,
		origin: peer,
		// 构建block announce
		fetchHeader: headerFetcher,
		fetchBodies: bodyFetcher,
	}
	select {
	// 通知block到f.notify
	case f.notify <- block:
		return nil
	case <-f.quit:
		return errTerminated
	}
}

// Enqueue tries to fill gaps the fetcher's future import queue.
// Enqueue试着填充fetcher的future import队列的gaps
func (f *BlockFetcher) Enqueue(peer string, block *types.Block) error {
	op := &blockOrHeaderInject{
		origin: peer,
		block:  block,
	}
	select {
	case f.inject <- op:
		return nil
	case <-f.quit:
		return errTerminated
	}
}

// FilterHeaders extracts all the headers that were explicitly requested by the fetcher,
// returning those that should be handled differently.
// FilterHeaders抽取出所有fetcher显式请求的headers，返回那些需要被不同处理的
func (f *BlockFetcher) FilterHeaders(peer string, headers []*types.Header, time time.Time) []*types.Header {
	log.Trace("Filtering headers", "peer", peer, "headers", len(headers))

	// Send the filter channel to the fetcher
	filter := make(chan *headerFilterTask)

	select {
	case f.headerFilter <- filter:
	case <-f.quit:
		return nil
	}
	// Request the filtering of the header list
	// 请求对于headers的filtering
	select {
	case filter <- &headerFilterTask{peer: peer, headers: headers, time: time}:
	case <-f.quit:
		return nil
	}
	// Retrieve the headers remaining after filtering
	// 返回filtering之后剩余的headers
	select {
	case task := <-filter:
		return task.headers
	case <-f.quit:
		return nil
	}
}

// FilterBodies extracts all the block bodies that were explicitly requested by
// the fetcher, returning those that should be handled differently.
func (f *BlockFetcher) FilterBodies(peer string, transactions [][]*types.Transaction, uncles [][]*types.Header, time time.Time) ([][]*types.Transaction, [][]*types.Header) {
	log.Trace("Filtering bodies", "peer", peer, "txs", len(transactions), "uncles", len(uncles))

	// Send the filter channel to the fetcher
	filter := make(chan *bodyFilterTask)

	select {
	case f.bodyFilter <- filter:
	case <-f.quit:
		return nil, nil
	}
	// Request the filtering of the body list
	select {
	case filter <- &bodyFilterTask{peer: peer, transactions: transactions, uncles: uncles, time: time}:
	case <-f.quit:
		return nil, nil
	}
	// Retrieve the bodies remaining after filtering
	select {
	case task := <-filter:
		return task.transactions, task.uncles
	case <-f.quit:
		return nil, nil
	}
}

// Loop is the main fetcher loop, checking and processing various notification
// events.
// Loop是主要的fetcher loop，检查并且处理各种notification events
func (f *BlockFetcher) loop() {
	// Iterate the block fetching until a quit is requested
	// 遍历block fetching，直到请求一个quit
	var (
		fetchTimer    = time.NewTimer(0)
		completeTimer = time.NewTimer(0)
	)
	<-fetchTimer.C // clear out the channel
	<-completeTimer.C
	defer fetchTimer.Stop()
	defer completeTimer.Stop()

	for {
		// Clean up any expired block fetches
		for hash, announce := range f.fetching {
			if time.Since(announce.time) > fetchTimeout {
				f.forgetHash(hash)
			}
		}
		// Import any queued blocks that could potentially fit
		// 导入任何排队的blocks，可能合适
		height := f.chainHeight()
		for !f.queue.Empty() {
			// 从队列中取出Items
			op := f.queue.PopItem().(*blockOrHeaderInject)
			hash := op.hash()
			if f.queueChangeHook != nil {
				f.queueChangeHook(hash, false)
			}
			// If too high up the chain or phase, continue later
			number := op.number()
			if number > height+1 {
				f.queue.Push(op, -int64(number))
				if f.queueChangeHook != nil {
					f.queueChangeHook(hash, true)
				}
				break
			}
			// Otherwise if fresh and still unknown, try and import
			if (number+maxUncleDist < height) || (f.light && f.getHeader(hash) != nil) || (!f.light && f.getBlock(hash) != nil) {
				f.forgetBlock(hash)
				continue
			}
			if f.light {
				f.importHeaders(op.origin, op.header)
			} else {
				f.importBlocks(op.origin, op.block)
			}
		}
		// Wait for an outside event to occur
		select {
		case <-f.quit:
			// BlockFetcher terminating, abort all operations
			return

		case notification := <-f.notify:
			// A block was announced, make sure the peer isn't DOSing us
			// 一个block已经被announced，确保peer不是在DOSing我们
			blockAnnounceInMeter.Mark(1)

			count := f.announces[notification.origin] + 1
			if count > hashLimit {
				log.Debug("Peer exceeded outstanding announces", "peer", notification.origin, "limit", hashLimit)
				blockAnnounceDOSMeter.Mark(1)
				break
			}
			if notification.number == 0 {
				break
			}
			// If we have a valid block number, check that it's potentially useful
			// 如果我们有一个合法的block number，检查它是否潜在有用
			if dist := int64(notification.number) - int64(f.chainHeight()); dist < -maxUncleDist || dist > maxQueueDist {
				// 丢弃peer announcement
				log.Debug("Peer discarded announcement", "peer", notification.origin, "number", notification.number, "hash", notification.hash, "distance", dist)
				blockAnnounceDropMeter.Mark(1)
				break
			}
			// All is well, schedule the announce if block's not yet downloading
			// 都准备好了，调度announce，如果block还没有下载
			if _, ok := f.fetching[notification.hash]; ok {
				break
			}
			if _, ok := f.completing[notification.hash]; ok {
				break
			}
			// 同时加入announces和announced中
			f.announces[notification.origin] = count
			// 扩展announced
			f.announced[notification.hash] = append(f.announced[notification.hash], notification)
			if f.announceChangeHook != nil && len(f.announced[notification.hash]) == 1 {
				f.announceChangeHook(notification.hash, true)
			}
			if len(f.announced) == 1 {
				// 重新调度fetch
				f.rescheduleFetch(fetchTimer)
			}

		case op := <-f.inject:
			// A direct block insertion was requested, try and fill any pending gaps
			// 一个直接的block insertion已经被请求了，试着填充任何的pending gaps
			blockBroadcastInMeter.Mark(1)

			// Now only direct block injection is allowed, drop the header injection
			// here silently if we receive.
			if f.light {
				continue
			}
			// 将block插入队列
			f.enqueue(op.origin, nil, op.block)

		case hash := <-f.done:
			// A pending import finished, remove all traces of the notification
			// 一个pending import已经结束了，移除所有的traces of the notification
			f.forgetHash(hash)
			f.forgetBlock(hash)

		case <-fetchTimer.C:
			// At least one block's timer ran out, check for needing retrieval
			// 至少一个block的timer用尽，检查需要的retrieval
			request := make(map[string][]common.Hash)

			for hash, announces := range f.announced {
				// In current LES protocol(les2/les3), only header announce is
				// available, no need to wait too much time for header broadcast.
				// 在当前的LES协议中，只有header announce可用，不需要等待太长时间用于header broadcast
				timeout := arriveTimeout - gatherSlack
				if f.light {
					timeout = 0
				}
				if time.Since(announces[0].time) > timeout {
					// Pick a random peer to retrieve from, reset all others
					// 随机获取一个random peer用于获取，重置其他所有的
					announce := announces[rand.Intn(len(announces))]
					f.forgetHash(hash)

					// If the block still didn't arrive, queue for fetching
					// 如果block还没有到，等待用于获取
					if (f.light && f.getHeader(hash) == nil) || (!f.light && f.getBlock(hash) == nil) {
						request[announce.origin] = append(request[announce.origin], hash)
						f.fetching[hash] = announce
					}
				}
			}
			// Send out all block header requests
			// 发出所有的block header requests
			for peer, hashes := range request {
				// 抓取被调度的headers
				log.Trace("Fetching scheduled headers", "peer", peer, "list", hashes)

				// Create a closure of the fetch and schedule in on a new thread
				// 创建一个fetch的closure，在另一个新的thread调度
				fetchHeader, hashes := f.fetching[hashes[0]].fetchHeader, hashes
				go func(peer string) {
					if f.fetchingHook != nil {
						f.fetchingHook(hashes)
					}
					for _, hash := range hashes {
						headerFetchMeter.Mark(1)
						// 对每个hashes都构建goroutine来抓取header
						go func(hash common.Hash) {
							resCh := make(chan *eth.Response)

							// 抓取Header
							req, err := fetchHeader(hash, resCh)
							if err != nil {
								return // Legacy code, yolo
							}
							defer req.Close()

							timeout := time.NewTimer(2 * fetchTimeout) // 2x leeway before dropping the peer
							defer timeout.Stop()

							select {
							case res := <-resCh:
								res.Done <- nil
								// 对于返回的headers，过滤之后的headers，不做处理
								f.FilterHeaders(peer, *res.Res.(*eth.BlockHeadersPacket), time.Now().Add(res.Time))

							case <-timeout.C:
								// The peer didn't respond in time. The request
								// was already rescheduled at this point, we were
								// waiting for a catchup. With an unresponsive
								// peer however, it's a protocol violation.
								// peer没有及时回复，request此时已经被重新调度了
								f.dropPeer(peer)
							}
						}(hash)
					}
				}(peer)
			}
			// Schedule the next fetch if blocks are still pending
			// 调度下一次的fetch，如果blocks依然处于pending
			f.rescheduleFetch(fetchTimer)

		case <-completeTimer.C:
			// At least one header's timer ran out, retrieve everything
			// 至少一个header的timer用尽，获取所有
			request := make(map[string][]common.Hash)

			for hash, announces := range f.fetched {
				// Pick a random peer to retrieve from, reset all others
				// 选取一个随机的peer来获取，重置其他所有的
				announce := announces[rand.Intn(len(announces))]
				f.forgetHash(hash)

				// If the block still didn't arrive, queue for completion
				// 如果block还没有到达，排队等待完成
				if f.getBlock(hash) == nil {
					// 构建来自同一个announce的request
					request[announce.origin] = append(request[announce.origin], hash)
					f.completing[hash] = announce
				}
			}
			// Send out all block body requests
			// 发出所有的block body requests
			for peer, hashes := range request {
				log.Trace("Fetching scheduled bodies", "peer", peer, "list", hashes)

				// Create a closure of the fetch and schedule in on a new thread
				// 创建一个fetch的closure并且在一个新的线程调度
				if f.completingHook != nil {
					f.completingHook(hashes)
				}
				fetchBodies := f.completing[hashes[0]].fetchBodies
				bodyFetchMeter.Mark(int64(len(hashes)))

				go func(peer string, hashes []common.Hash) {
					resCh := make(chan *eth.Response)

					// 对bodies进行获取
					req, err := fetchBodies(hashes, resCh)
					if err != nil {
						return // Legacy code, yolo
					}
					defer req.Close()

					timeout := time.NewTimer(2 * fetchTimeout) // 2x leeway before dropping the peer
					defer timeout.Stop()

					select {
					case res := <-resCh:
						res.Done <- nil

						txs, uncles := res.Res.(*eth.BlockBodiesPacket).Unpack()
						// 对bodies进行过滤
						f.FilterBodies(peer, txs, uncles, time.Now())

					case <-timeout.C:
						// The peer didn't respond in time. The request
						// was already rescheduled at this point, we were
						// waiting for a catchup. With an unresponsive
						// peer however, it's a protocol violation.
						f.dropPeer(peer)
					}
				}(peer, hashes)
			}
			// Schedule the next fetch if blocks are still pending
			// 调度下一个fetch，如果blocks依然处于pending
			f.rescheduleComplete(completeTimer)

		case filter := <-f.headerFilter:
			// Headers arrived from a remote peer. Extract those that were explicitly
			// requested by the fetcher, and return everything else so it's delivered
			// to other parts of the system.
			// Headers从一个remote peer到达，抽取出fetcher显式请求的部分，并且返回其他的
			// 这样它被传递到系统的其他部分
			var task *headerFilterTask
			select {
			case task = <-filter:
			case <-f.quit:
				return
			}
			headerFilterInMeter.Mark(int64(len(task.headers)))

			// Split the batch of headers into unknown ones (to return to the caller),
			// known incomplete ones (requiring body retrievals) and completed blocks.
			// 划分batch of headers到unknown ones（返回给调用者），known但是不完整的（需要body retrieval）
			// 以及完整的blocks
			unknown, incomplete, complete, lightHeaders := []*types.Header{}, []*blockAnnounce{}, []*types.Block{}, []*blockAnnounce{}
			for _, header := range task.headers {
				hash := header.Hash()

				// Filter fetcher-requested headers from other synchronisation algorithms
				if announce := f.fetching[hash]; announce != nil && announce.origin == task.peer && f.fetched[hash] == nil && f.completing[hash] == nil && f.queued[hash] == nil {
					// If the delivered header does not match the promised number, drop the announcer
					if header.Number.Uint64() != announce.number {
						log.Trace("Invalid block number fetched", "peer", announce.origin, "hash", header.Hash(), "announced", announce.number, "provided", header.Number)
						f.dropPeer(announce.origin)
						f.forgetHash(hash)
						continue
					}
					// Collect all headers only if we are running in light
					// mode and the headers are not imported by other means.
					if f.light {
						if f.getHeader(hash) == nil {
							announce.header = header
							lightHeaders = append(lightHeaders, announce)
						}
						f.forgetHash(hash)
						continue
					}
					// Only keep if not imported by other means
					if f.getBlock(hash) == nil {
						announce.header = header
						announce.time = task.time

						// If the block is empty (header only), short circuit into the final import queue
						if header.TxHash == types.EmptyRootHash && header.UncleHash == types.EmptyUncleHash {
							log.Trace("Block empty, skipping body retrieval", "peer", announce.origin, "number", header.Number, "hash", header.Hash())

							block := types.NewBlockWithHeader(header)
							block.ReceivedAt = task.time

							complete = append(complete, block)
							f.completing[hash] = announce
							continue
						}
						// Otherwise add to the list of blocks needing completion
						incomplete = append(incomplete, announce)
					} else {
						log.Trace("Block already imported, discarding header", "peer", announce.origin, "number", header.Number, "hash", header.Hash())
						f.forgetHash(hash)
					}
				} else {
					// BlockFetcher doesn't know about it, add to the return list
					// BlockFetcher不知道它们，将它们加入到return list
					unknown = append(unknown, header)
				}
			}
			headerFilterOutMeter.Mark(int64(len(unknown)))
			select {
			case filter <- &headerFilterTask{headers: unknown, time: task.time}:
			case <-f.quit:
				return
			}
			// Schedule the retrieved headers for body completion
			// 调度获取到的headers用于body completion
			for _, announce := range incomplete {
				hash := announce.header.Hash()
				if _, ok := f.completing[hash]; ok {
					continue
				}
				// 设置fetched
				f.fetched[hash] = append(f.fetched[hash], announce)
				if len(f.fetched) == 1 {
					f.rescheduleComplete(completeTimer)
				}
			}
			// Schedule the header for light fetcher import
			// 调度header用于light fetcher import
			for _, announce := range lightHeaders {
				f.enqueue(announce.origin, announce.header, nil)
			}
			// Schedule the header-only blocks for import
			// 调度header-only blocks用于导入
			for _, block := range complete {
				if announce := f.completing[block.Hash()]; announce != nil {
					f.enqueue(announce.origin, nil, block)
				}
			}

		case filter := <-f.bodyFilter:
			// Block bodies arrived, extract any explicitly requested blocks, return the rest
			// Block bodies到了，抽取任何显式请求的blocks，返回reset
			var task *bodyFilterTask
			select {
			case task = <-filter:
			case <-f.quit:
				return
			}
			bodyFilterInMeter.Mark(int64(len(task.transactions)))
			blocks := []*types.Block{}
			// abort early if there's nothing explicitly requested
			// 尽早中止，如果没有东西是显式请求的
			if len(f.completing) > 0 {
				for i := 0; i < len(task.transactions) && i < len(task.uncles); i++ {
					// Match up a body to any possible completion request
					var (
						matched   = false
						uncleHash common.Hash // calculated lazily and reused
						txnHash   common.Hash // calculated lazily and reused
					)
					for hash, announce := range f.completing {
						if f.queued[hash] != nil || announce.origin != task.peer {
							continue
						}
						if uncleHash == (common.Hash{}) {
							uncleHash = types.CalcUncleHash(task.uncles[i])
						}
						if uncleHash != announce.header.UncleHash {
							continue
						}
						if txnHash == (common.Hash{}) {
							txnHash = types.DeriveSha(types.Transactions(task.transactions[i]), trie.NewStackTrie(nil))
						}
						if txnHash != announce.header.TxHash {
							continue
						}
						// Mark the body matched, reassemble if still unknown
						matched = true
						if f.getBlock(hash) == nil {
							block := types.NewBlockWithHeader(announce.header).WithBody(task.transactions[i], task.uncles[i])
							block.ReceivedAt = task.time
							blocks = append(blocks, block)
						} else {
							f.forgetHash(hash)
						}

					}
					if matched {
						task.transactions = append(task.transactions[:i], task.transactions[i+1:]...)
						task.uncles = append(task.uncles[:i], task.uncles[i+1:]...)
						i--
						continue
					}
				}
			}
			bodyFilterOutMeter.Mark(int64(len(task.transactions)))
			select {
			// 把过滤好的tasks返回
			case filter <- task:
			case <-f.quit:
				return
			}
			// Schedule the retrieved blocks for ordered import
			// 调度获取到的blocks用于有序的导入
			for _, block := range blocks {
				if announce := f.completing[block.Hash()]; announce != nil {
					f.enqueue(announce.origin, nil, block)
				}
			}
		}
	}
}

// rescheduleFetch resets the specified fetch timer to the next blockAnnounce timeout.
// rescheduleFetch重置指定的fetch timer到下一个blockAnnounce超时
func (f *BlockFetcher) rescheduleFetch(fetch *time.Timer) {
	// Short circuit if no blocks are announced
	// 已经没有blocks处于announced，则直接返回
	if len(f.announced) == 0 {
		return
	}
	// Schedule announcement retrieval quickly for light mode
	// since server won't send any headers to client.
	if f.light {
		fetch.Reset(lightTimeout)
		return
	}
	// Otherwise find the earliest expiring announcement
	// 否则找到最早的expiring announcement
	earliest := time.Now()
	for _, announces := range f.announced {
		if earliest.After(announces[0].time) {
			earliest = announces[0].time
		}
	}
	fetch.Reset(arriveTimeout - time.Since(earliest))
}

// rescheduleComplete resets the specified completion timer to the next fetch timeout.
// rescheduleComplete重置指定的completion timer到下一个fetch timeout
func (f *BlockFetcher) rescheduleComplete(complete *time.Timer) {
	// Short circuit if no headers are fetched
	if len(f.fetched) == 0 {
		return
	}
	// Otherwise find the earliest expiring announcement
	// 否则找到最早过期的announcement
	earliest := time.Now()
	for _, announces := range f.fetched {
		if earliest.After(announces[0].time) {
			earliest = announces[0].time
		}
	}
	// 重置complete time
	complete.Reset(gatherSlack - time.Since(earliest))
}

// enqueue schedules a new header or block import operation, if the component
// to be imported has not yet been seen.
// enqueue调度一个新的header或者block的import操作，如果被导入的component还没看到
func (f *BlockFetcher) enqueue(peer string, header *types.Header, block *types.Block) {
	var (
		hash   common.Hash
		number uint64
	)
	if header != nil {
		hash, number = header.Hash(), header.Number.Uint64()
	} else {
		hash, number = block.Hash(), block.NumberU64()
	}
	// Ensure the peer isn't DOSing us
	// 确保peer没有在DOSing我们
	count := f.queues[peer] + 1
	if count > blockLimit {
		// 未处理的block最多64个
		log.Debug("Discarded delivered header or block, exceeded allowance", "peer", peer, "number", number, "hash", hash, "limit", blockLimit)
		blockBroadcastDOSMeter.Mark(1)
		f.forgetHash(hash)
		return
	}
	// Discard any past or too distant blocks
	// 丢弃任何过去或者太遥远的blocks
	if dist := int64(number) - int64(f.chainHeight()); dist < -maxUncleDist || dist > maxQueueDist {
		log.Debug("Discarded delivered header or block, too far away", "peer", peer, "number", number, "hash", hash, "distance", dist)
		blockBroadcastDropMeter.Mark(1)
		f.forgetHash(hash)
		return
	}
	// Schedule the block for future importing
	// 调度block用于后续的importing
	if _, ok := f.queued[hash]; !ok {
		op := &blockOrHeaderInject{origin: peer}
		if header != nil {
			op.header = header
		} else {
			op.block = block
		}
		f.queues[peer] = count
		f.queued[hash] = op
		f.queue.Push(op, -int64(number))
		if f.queueChangeHook != nil {
			f.queueChangeHook(hash, true)
		}
		// 排队传递header或者block
		log.Debug("Queued delivered header or block", "peer", peer, "number", number, "hash", hash, "queued", f.queue.Size())
	}
}

// importHeaders spawns a new goroutine to run a header insertion into the chain.
// importHeaders生成一个新的goroutine来在chain上运行一个header insertion
// If the header's number is at the same height as the current import phase, it
// updates the phase states accordingly.
// 如果header的number和当前的import phase的height相同，它相应地更新phase states
func (f *BlockFetcher) importHeaders(peer string, header *types.Header) {
	hash := header.Hash()
	log.Debug("Importing propagated header", "peer", peer, "number", header.Number, "hash", hash)

	go func() {
		defer func() { f.done <- hash }()
		// If the parent's unknown, abort insertion
		// 如果parent未知，中止插入
		parent := f.getHeader(header.ParentHash)
		if parent == nil {
			log.Debug("Unknown parent of propagated header", "peer", peer, "number", header.Number, "hash", hash, "parent", header.ParentHash)
			return
		}
		// Validate the header and if something went wrong, drop the peer
		// 校验header，如果出了问题，则丢弃peer
		if err := f.verifyHeader(header); err != nil && err != consensus.ErrFutureBlock {
			log.Debug("Propagated header verification failed", "peer", peer, "number", header.Number, "hash", hash, "err", err)
			f.dropPeer(peer)
			return
		}
		// Run the actual import and log any issues
		// 运行真正的import并且打出任何的issues
		if _, err := f.insertHeaders([]*types.Header{header}); err != nil {
			log.Debug("Propagated header import failed", "peer", peer, "number", header.Number, "hash", hash, "err", err)
			return
		}
		// Invoke the testing hook if needed
		if f.importedHook != nil {
			f.importedHook(header, nil)
		}
	}()
}

// importBlocks spawns a new goroutine to run a block insertion into the chain. If the
// block's number is at the same height as the current import phase, it updates
// the phase states accordingly.
// importBlocks生成一个新的goroutine用于运行插入一个block到chain，如果block的number和当前的import phase
// 处于同样的height，它相应地更新phase state
func (f *BlockFetcher) importBlocks(peer string, block *types.Block) {
	hash := block.Hash()

	// Run the import on a new thread
	// 在一个新的thread运行import
	log.Debug("Importing propagated block", "peer", peer, "number", block.Number(), "hash", hash)
	go func() {
		defer func() { f.done <- hash }()

		// If the parent's unknown, abort insertion
		// 如果parent是unknown，则停止插入
		parent := f.getBlock(block.ParentHash())
		if parent == nil {
			log.Debug("Unknown parent of propagated block", "peer", peer, "number", block.Number(), "hash", hash, "parent", block.ParentHash())
			return
		}
		// Quickly validate the header and propagate the block if it passes
		// 快速校验header并且传播block，如果通过的话
		switch err := f.verifyHeader(block.Header()); err {
		case nil:
			// All ok, quickly propagate to our peers
			// 所有校验都ok，快速传播到我们的peers
			blockBroadcastOutTimer.UpdateSince(block.ReceivedAt)
			go f.broadcastBlock(block, true)

		case consensus.ErrFutureBlock:
			// Weird future block, don't fail, but neither propagate
			// 奇怪的future block，不失败，但是也不传播

		default:
			// Something went very wrong, drop the peer
			// 出现了一些问题，丢弃peer
			log.Debug("Propagated block verification failed", "peer", peer, "number", block.Number(), "hash", hash, "err", err)
			f.dropPeer(peer)
			return
		}
		// Run the actual import and log any issues
		// 运行真正的import并且log任何的问题
		if _, err := f.insertChain(types.Blocks{block}); err != nil {
			log.Debug("Propagated block import failed", "peer", peer, "number", block.Number(), "hash", hash, "err", err)
			return
		}
		// If import succeeded, broadcast the block
		// 如果导入成功，广播block
		blockAnnounceOutTimer.UpdateSince(block.ReceivedAt)
		go f.broadcastBlock(block, false)

		// Invoke the testing hook if needed
		// 调用testing hook，如果需要的话
		if f.importedHook != nil {
			f.importedHook(nil, block)
		}
	}()
}

// forgetHash removes all traces of a block announcement from the fetcher's
// internal state.
func (f *BlockFetcher) forgetHash(hash common.Hash) {
	// Remove all pending announces and decrement DOS counters
	if announceMap, ok := f.announced[hash]; ok {
		for _, announce := range announceMap {
			f.announces[announce.origin]--
			if f.announces[announce.origin] <= 0 {
				delete(f.announces, announce.origin)
			}
		}
		delete(f.announced, hash)
		if f.announceChangeHook != nil {
			f.announceChangeHook(hash, false)
		}
	}
	// Remove any pending fetches and decrement the DOS counters
	if announce := f.fetching[hash]; announce != nil {
		f.announces[announce.origin]--
		if f.announces[announce.origin] <= 0 {
			delete(f.announces, announce.origin)
		}
		delete(f.fetching, hash)
	}

	// Remove any pending completion requests and decrement the DOS counters
	for _, announce := range f.fetched[hash] {
		f.announces[announce.origin]--
		if f.announces[announce.origin] <= 0 {
			delete(f.announces, announce.origin)
		}
	}
	delete(f.fetched, hash)

	// Remove any pending completions and decrement the DOS counters
	if announce := f.completing[hash]; announce != nil {
		f.announces[announce.origin]--
		if f.announces[announce.origin] <= 0 {
			delete(f.announces, announce.origin)
		}
		delete(f.completing, hash)
	}
}

// forgetBlock removes all traces of a queued block from the fetcher's internal
// state.
func (f *BlockFetcher) forgetBlock(hash common.Hash) {
	if insert := f.queued[hash]; insert != nil {
		f.queues[insert.origin]--
		if f.queues[insert.origin] == 0 {
			delete(f.queues, insert.origin)
		}
		delete(f.queued, hash)
	}
}
