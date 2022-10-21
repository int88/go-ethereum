// Copyright 2022 The go-ethereum Authors
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

package downloader

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/protocols/eth"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

// scratchHeaders is the number of headers to store in a scratch space to allow
// concurrent downloads. A header is about 0.5KB in size, so there is no worry
// about using too much memory. The only catch is that we can only validate gaps
// afer they're linked to the head, so the bigger the scratch space, the larger
// potential for invalid headers.
// scratchHeaders是在scratch space中存储的headers的数目，从而允许并发的下载，一个header大概
// 0.5KB，因此不用担心消耗太多内存，唯一的陷阱是我们只有在连接到head之后才能检测gaps
// 因此scratch space越大，invalid headers存在的可能性越大
//
// The current scratch space of 131072 headers is expected to use 64MB RAM.
// 当前的scratch space为131072，期望使用64MB的RAM
const scratchHeaders = 131072

// requestHeaders is the number of header to request from a remote peer in a single
// network packet. Although the skeleton downloader takes into consideration peer
// capacities when picking idlers, the packet size was decided to remain constant
// since headers are relatively small and it's easier to work with fixed batches
// vs. dynamic interval fillings.
// requestHeaders是在单个network packet中从一个remote peer中请求的headers的数目，尽管
// 在选择idlers的时候，skeleton downloader会考虑peer capacities，packet size依然保持不变
// 因此Headers相对较小并且fixed batches和dynamic interval fillings相比更简单
const requestHeaders = 512

// errSyncLinked is an internal helper error to signal that the current sync
// cycle linked up to the genesis block, this the skeleton syncer should ping
// the backfiller to resume. Since we already have that logic on sync start,
// piggie-back on that instead of 2 entrypoints.
var errSyncLinked = errors.New("sync linked")

// errSyncMerged is an internal helper error to signal that the current sync
// cycle merged with a previously aborted subchain, thus the skeleton syncer
// should abort and restart with the new state.
var errSyncMerged = errors.New("sync merged")

// errSyncReorged is an internal helper error to signal that the head chain of
// the current sync cycle was (partially) reorged, thus the skeleton syncer
// should abort and restart with the new state.
var errSyncReorged = errors.New("sync reorged")

// errTerminated is returned if the sync mechanism was terminated for this run of
// the process. This is usually the case when Geth is shutting down and some events
// might still be propagating.
var errTerminated = errors.New("terminated")

// errReorgDenied is returned if an attempt is made to extend the beacon chain
// with a new header, but it does not link up to the existing sync.
// errReorgDenied被返回，如果尝试扩展beacon chain，用一个新的header，但是它没有链接到
// 已经存在的sync
var errReorgDenied = errors.New("non-forced head reorg denied")

func init() {
	// Tuning parameters is nice, but the scratch space must be assignable in
	// full to peers. It's a useless cornercase to support a dangling half-group.
	if scratchHeaders%requestHeaders != 0 {
		panic("Please make scratchHeaders divisible by requestHeaders")
	}
}

// subchain is a contiguous header chain segment that is backed by the database,
// but may not be linked to the live chain. The skeleton downloader may produce
// a new one of these every time it is restarted until the subchain grows large
// enough to connect with a previous subchain.
// subchain是连续的header chain segment，背后由数据库支持，但是可能没有连接到live chain
// skeleton downloader可能在每次重启之后生成一个新的，直到subchain长得足够大来连接之前的subchain
//
// The subchains use the exact same database namespace and are not disjoint from
// each other. As such, extending one to overlap the other entails reducing the
// second one first. This combined buffer model is used to avoid having to move
// data on disk when two subchains are joined together.
// subchains使用同样的database namespace并且不是和彼此不连接的，延伸一个和另一个重叠
// 会首先减小第一个，这种重合的buffer model会用于避免在磁盘中移动data，当两个subchain连接在一起的时候
type subchain struct {
	// 在subchain中最新的header
	Head uint64 // Block number of the newest header in the subchain
	// 在subchain中最老的header
	Tail uint64      // Block number of the oldest header in the subchain
	Next common.Hash // Block hash of the next oldest header in the subchain
}

// skeletonProgress is a database entry to allow suspending and resuming a chain
// sync. As the skeleton header chain is downloaded backwards, restarts can and
// will produce temporarily disjoint subchains. There is no way to restart a
// suspended skeleton sync without prior knowledge of all prior suspension points.
// skeletonProgress是一个database entry，用于允许暂停以及继续chain sync，因为skeleton header
// chain是向后下载的，重启可以并且会导致临时的disjoint subchains，没有办法重启一个一个skeleton
// sync，如果没有所有之前的prior suspension points的知识的话
type skeletonProgress struct {
	// 到现在为止下载的subchains
	Subchains []*subchain // Disjoint subchains downloaded until now
}

// headUpdate is a notification that the beacon sync should switch to a new target.
// headUpdate是一个通知，表明beacon sync应该转换到一个新的target
// The update might request whether to forcefully change the target, or only try to
// extend it and fail if it's not possible.
// 这个update可能请求是否强制改变target，或者只是试着扩展它，并且失败，如果不可能的话
type headUpdate struct {
	// 用于更新sync update的Header
	header *types.Header // Header to update the sync target to
	// 是否强制更新或者只是扩展，如果可能的话
	force bool // Whether to force the update or only extend if possible
	// 用于表明新的head是否被接受的channel
	errc chan error // Channel to signal acceptance of the new head
}

// headerRequest tracks a pending header request to ensure responses are to
// actual requests and to validate any security constraints.
// headerRequest追踪一个pending header request来确保responses是响应真正的requests
// 并且来校验security constraints
//
// Concurrency note: header requests and responses are handled concurrently from
// the main runloop to allow Keccak256 hash verifications on the peer's thread and
// to drop on invalid response. The request struct must contain all the data to
// construct the response without accessing runloop internals (i.e. subchains).
// That is only included to allow the runloop to match a response to the task being
// synced without having yet another set of maps.
// header requests和responses从main runloop被并发处理，来运行Keccak256的hash verification
// 在peer的thread并且对invalid response进行丢弃，request结构包含了所有的data用于构建response
// 而不需要访问runloop内部（例如，subchains）
type headerRequest struct {
	// 这个request被请求的Peer
	peer string // Peer to which this request is assigned
	// 这个请求的Request ID
	id uint64 // Request ID of this request

	// 用于传递成功的response的channel
	deliver chan *headerResponse // Channel to deliver successful response on
	// 用于传递失败的request的channel
	revert chan *headerRequest // Channel to deliver request failure on
	cancel chan struct{}       // Channel to track sync cancellation
	// 用来表明请求已经被丢弃的channel
	stale chan struct{} // Channel to signal the request was dropped

	// 批量请求的headers的数目
	head uint64 // Head number of the requested batch of headers
}

// headerResponse is an already verified remote response to a header request.
type headerResponse struct {
	peer    *peerConnection // Peer from which this response originates
	reqid   uint64          // Request ID that this response fulfils
	headers []*types.Header // Chain of headers
}

// backfiller is a callback interface through which the skeleton sync can tell
// the downloader that it should suspend or resume backfilling on specific head
// events (e.g. suspend on forks or gaps, resume on successful linkups).
// backfiller是一个回调接口，通过它skeleton sync可以告诉downloader，它应该停止还是继续回填
// 特定的head events（例如，停止forks或者gaps，在成功的连接之后继续）
type backfiller interface {
	// suspend requests the backfiller to abort any running full or snap sync
	// based on the skeleton chain as it might be invalid. The backfiller should
	// gracefully handle multiple consecutive suspends without a resume, even
	// on initial sartup.
	// suspend请求bakcfiller退出任何正在运行的full或者snap sync，基于skeleton chain
	// 因为它可能是非法的，backfiller应该能优雅地处理多个连续的suspends，而没有resume
	// 即使在initial startup
	//
	// The method should return the last block header that has been successfully
	// backfilled, or nil if the backfiller was not resumed.
	// 这个方法应该返回最后一个已经成功回填的block header，或者为nil，如果backfiller
	// 没有恢复
	suspend() *types.Header

	// resume requests the backfiller to start running fill or snap sync based on
	// the skeleton chain as it has successfully been linked. Appending new heads
	// to the end of the chain will not result in suspend/resume cycles.
	// leaking too much sync logic out to the filler.
	// resume请求bakfiller开始运行full或者snap sync，基于skeleton chain它已经
	// 成功连接的，扩展新的headers到end of the chain不会导致suspend/resume cycles
	// 泄露太多的sync logic在filler之外
	resume()
}

// skeleton represents a header chain synchronized after the merge where blocks
// aren't validated any more via PoW in a forward fashion, rather are dictated
// and extended at the head via the beacon chain and backfilled on the original
// Ethereum block sync protocol.
// skeleton代表一个header chain，在merge之后，blocks不再通过PoW校验，而是通过beacon chain
// 在head进行扩展，并且回填到原来的Ethereum block sync protocol
//
// Since the skeleton is grown backwards from head to genesis, it is handled as
// a separate entity, not mixed in with the logical sequential transition of the
// blocks. Once the skeleton is connected to an existing, validated chain, the
// headers will be moved into the main downloader for filling and execution.
// 因为skeleton从head反向长回genesis，它由一个单独的实例进行处理，不和blocks的逻辑线性转换关联
// 一旦skeleton和一个已经存在的，合法的chain连接，headers会被移动到main downloader用于填充和执行
//
// Opposed to the original Ethereum block synchronization which is trustless (and
// uses a master peer to minimize the attack surface), post-merge block sync starts
// from a trusted head. As such, there is no need for a master peer any more and
// headers can be requested fully concurrently (though some batches might be
// discarded if they don't link up correctly).
// 和原先的Ethereum block同步相反，它是trustless（使用一个master peer来最小化攻击界面），post-merge
// block sync从一个trusted head开始，这样的话，就不再需要一个master peer并且headers可以完全并发
// 地请求（尽管有的batches会被抛弃，因为他们没有正确地被连接）
//
// Although a skeleton is part of a sync cycle, it is not recreated, rather stays
// alive throughout the lifetime of the downloader. This allows it to be extended
// concurrently with the sync cycle, since extensions arrive from an API surface,
// not from within (vs. legacy Ethereum sync).
// 尽管skeleton是sync cycle的一部分，它不会被重新创建，而是在downloader的整个生命周期存在
// 这允许它在sync cycle并发扩展，因为extension从一个API接口到来，而不是来自内部（和传统的Ethereum sync对比）
//
// Since the skeleton tracks the entire header chain until it is consumed by the
// forward block filling, it needs 0.5KB/block storage. At current mainnet sizes
// this is only possible with a disk backend. Since the skeleton is separate from
// the node's header chain, storing the headers ephemerally until sync finishes
// is wasted disk IO, but it's a price we're going to pay to keep things simple
// for now.
// 因为skeleton追踪整个的header chain，直到它被forward block filling消费，它需要每个block
// 0.5KB的存储，当前的mainnet的大小，它需要一个disk backend，因为skeleton和node的header chain
// 分开存储，临时存储headers直到sync结束会浪费磁盘IO，但是这是我们让事情保持简单需要付出的代价
type skeleton struct {
	// skeleton背后的Database
	db ethdb.Database // Database backing the skeleton
	// 由head events暂停/启动的Chain syncer
	filler backfiller // Chain syncer suspended/resumed by head events

	peers *peerSet // Set of peers we can sync from
	// 当前的sync cyle中一系列的idle peers
	idles map[string]*peerConnection // Set of idle peers in the current sync cycle
	// 对于misbehaving的peer进行drop
	drop peerDropFn // Drops a peer for misbehaving

	// Sync progress tracker用于恢复以及监控
	progress *skeletonProgress // Sync progress tracker for resumption and metrics
	started  time.Time         // Timestamp when the skeleton syncer was created
	logged   time.Time         // Timestamp when progress was last logged to the user
	pulled   uint64            // Number of headers downloaded in this run

	// 用来累计headers的Scratch space
	scratchSpace []*types.Header // Scratch space to accumulate headers in (first = recent)
	// Peer IDs拥有批量的scratch space（pend或者delivered）
	scratchOwners []string // Peer IDs owning chunks of the scratch space (pend or delivered)
	// 在scratch space中的第一个item的block number
	scratchHead uint64 // Block number of the first item in the scratch space

	// 当前正在运行请求的Header
	requests map[uint64]*headerRequest // Header requests currently running

	// 对于新的heads的Notification channel
	headEvents chan *headUpdate // Notification channel for new heads
	// 用于中止sync的Termination channel
	terminate chan chan error // Termination channel to abort sync
	// 用于表明syncer已经dead的Channel
	terminated chan struct{} // Channel to signal that the syner is dead

	// Callback hooks used during testing
	// 用于在测试中使用的callback hooks，在一个sync cycle已经初始化但是没有启动的时候被调用
	syncStarting func() // callback triggered after a sync cycle is inited but before started
}

// newSkeleton creates a new sync skeleton that tracks a potentially dangling
// header chain until it's linked into an existing set of blocks.
// newSkeleton创建一个新的sync skeleton，追踪一系列的dangling header chain，直到它连接到
// 一个已经存在的一系列blocks
func newSkeleton(db ethdb.Database, peers *peerSet, drop peerDropFn, filler backfiller) *skeleton {
	sk := &skeleton{
		db:         db,
		filler:     filler,
		peers:      peers,
		drop:       drop,
		requests:   make(map[uint64]*headerRequest),
		headEvents: make(chan *headUpdate),
		terminate:  make(chan chan error),
		terminated: make(chan struct{}),
	}
	go sk.startup()
	return sk
}

// startup is an initial background loop which waits for an event to start or
// tear the syncer down. This is required to make the skeleton sync loop once
// per process but at the same time not start before the beacon chain announces
// a new (existing) head.
// startup是一个初始的background loop，等待一个event来启动或者关闭syncer，这是必须的，让
// skeleton sync loop一次，对于每个process，但是同时不在beacon chain宣布一个新的head之前启动
func (s *skeleton) startup() {
	// Close a notification channel so anyone sending us events will know if the
	// sync loop was torn down for good.
	// 关闭一个notification channel，这样任何发送给我们event的人都会知道是否sync loop已经关闭了
	defer close(s.terminated)

	// Wait for startup or teardown. This wait might loop a few times if a beacon
	// client requests sync head extensions, but not forced reorgs (i.e. they are
	// giving us new payloads without setting a starting head initially).
	// 等待启动或者teardown，这个wait可能循环多次，如果一个beacon client请求sync head extensions
	// 但是不强制reorgs（例如它给我们新的payloads，而不初始设置一个starting head）
	for {
		select {
		case errc := <-s.terminate:
			// No head was announced but Geth is shutting down
			// 没有head被announced，但是Geth被关闭了
			errc <- nil
			return

		case event := <-s.headEvents:
			// New head announced, start syncing to it, looping every time a current
			// cycle is terminated due to a chain event (head reorg, old chain merge).
			// 有新的head announced，启动同步它，循环，每次一个当前的cycle被终止，由于一个chain event
			if !event.force {
				event.errc <- errors.New("forced head needed for startup")
				continue
			}
			event.errc <- nil // forced head accepted for startup
			head := event.header
			s.started = time.Now()

			for {
				// If the sync cycle terminated or was terminated, propagate up when
				// higher layers request termination. There's no fancy explicit error
				// signalling as the sync loop should never terminate (TM).
				newhead, err := s.sync(head)
				switch {
				case err == errSyncLinked:
					// Sync cycle linked up to the genesis block. Tear down the loop
					// and restart it so, it can properly notify the backfiller. Don't
					// account a new head.
					head = nil

				case err == errSyncMerged:
					// Subchains were merged, we just need to reinit the internal
					// start to continue on the tail of the merged chain. Don't
					// announce a new head,
					head = nil

				case err == errSyncReorged:
					// The subchain being synced got modified at the head in a
					// way that requires resyncing it. Restart sync with the new
					// head to force a cleanup.
					head = newhead

				case err == errTerminated:
					// Sync was requested to be terminated from within, stop and
					// return (no need to pass a message, was already done internally)
					return

				default:
					// Sync either successfully terminated or failed with an unhandled
					// error. Abort and wait until Geth requests a termination.
					// Sync要么成功终止或者失败，有着一个未处理的error，中止并且等待，直到
					// Geth请求一个termination
					errc := <-s.terminate
					errc <- err
					return
				}
			}
		}
	}
}

// Terminate tears down the syncer indefinitely.
// Terminate不定期地中止syncer
func (s *skeleton) Terminate() error {
	// Request termination and fetch any errors
	// 请求termination并且获取任何的errors
	errc := make(chan error)
	s.terminate <- errc
	err := <-errc

	// Wait for full shutdown (not necessary, but cleaner)
	// 等待完整的shutdown（不必要，但是更干净）
	<-s.terminated
	return err
}

// Sync starts or resumes a previous sync cycle to download and maintain a reverse
// header chain starting at the head and leading towards genesis to an available
// ancestor.
// Sync启动或者重启一个之前的sync cycle，来下载并且维护一个反向的header chain，从header启动并且
// 指向gesis，到一个可用的ancestor
//
// This method does not block, rather it just waits until the syncer receives the
// fed header. What the syncer does with it is the syncer's problem.
// 这个方法不会阻塞，它只是等待，直到syncer收到fed header，syncer怎么做是syncer的问题
func (s *skeleton) Sync(head *types.Header, force bool) error {
	log.Trace("New skeleton head announced", "number", head.Number, "hash", head.Hash(), "force", force)
	errc := make(chan error)

	select {
	case s.headEvents <- &headUpdate{header: head, force: force, errc: errc}:
		// 直到新的header被接受
		return <-errc
	case <-s.terminated:
		return errTerminated
	}
}

// sync is the internal version of Sync that executes a single sync cycle, either
// until some termination condition is reached, or until the current cycle merges
// with a previously aborted run.
// sync是Sync的内部版本，执行一个sync cycle，要么达到一些termination condition，或者直到
// 当前的cycle和一个之前终止的合并
func (s *skeleton) sync(head *types.Header) (*types.Header, error) {
	// If we're continuing a previous merge interrupt, just access the existing
	// old state without initing from disk.
	// 如果我们正在继续一个之前的merge interrupt，只是访问已经存在的old state，而不是从磁盘初始化
	if head == nil {
		head = rawdb.ReadSkeletonHeader(s.db, s.progress.Subchains[0].Head)
	} else {
		// Otherwise, initialize the sync, trimming and previous leftovers until
		// we're consistent with the newly requested chain head
		// 否则，初始化sync，截取并且之前的leftovers直到我们和新请求的chain head一致
		s.initSync(head)
	}
	// Create the scratch space to fill with concurrently downloaded headers
	// 创建scratch space来填充当前下载的headers
	s.scratchSpace = make([]*types.Header, scratchHeaders)
	// 在同步之后，不要维护引用
	defer func() { s.scratchSpace = nil }() // don't hold on to references after sync

	s.scratchOwners = make([]string, scratchHeaders/requestHeaders)
	defer func() { s.scratchOwners = nil }() // don't hold on to references after sync

	s.scratchHead = s.progress.Subchains[0].Tail - 1 // tail must not be 0!

	// If the sync is already done, resume the backfiller. When the loop stops,
	// terminate the backfiller too.
	// 如果sync已经结束了，继续backfiller，当loop结束的时候，也停止backfiller
	linked := len(s.progress.Subchains) == 1 &&
		rawdb.HasBody(s.db, s.progress.Subchains[0].Next, s.scratchHead) &&
		rawdb.HasReceipts(s.db, s.progress.Subchains[0].Next, s.scratchHead)
	if linked {
		s.filler.resume()
	}
	defer func() {
		if filled := s.filler.suspend(); filled != nil {
			// If something was filled, try to delete stale sync helpers. If
			// unsuccessful, warn the user, but not much else we can do (it's
			// a programming error, just let users report an issue and don't
			// choke in the meantime).
			if err := s.cleanStales(filled); err != nil {
				log.Error("Failed to clean stale beacon headers", "err", err)
			}
		}
	}()
	// Create a set of unique channels for this sync cycle. We need these to be
	// ephemeral so a data race doesn't accidentally deliver something stale on
	// a persistent channel across syncs (yup, this happened)
	// 创建一系列的unique channels，对于这个sync cycle，我们需要它们是临时的，这样一个data race
	// 不会意外地传输一些过时的东西，在一个持久的，跨越多个syncs的channel
	var (
		requestFails = make(chan *headerRequest)
		responses    = make(chan *headerResponse)
	)
	cancel := make(chan struct{})
	defer close(cancel)

	// 开始方向的header sync cycle
	log.Debug("Starting reverse header sync cycle", "head", head.Number, "hash", head.Hash(), "cont", s.scratchHead)

	// Whether sync completed or not, disregard any future packets
	// 不管sync是否完成 ，无视任何的future packets
	defer func() {
		log.Debug("Terminating reverse header sync cycle", "head", head.Number, "hash", head.Hash(), "cont", s.scratchHead)
		s.requests = make(map[uint64]*headerRequest)
	}()

	// Start tracking idle peers for task assignments
	// 开始追踪idle peers用于task assignments
	peering := make(chan *peeringEvent, 64) // arbitrary buffer, just some burst protection

	peeringSub := s.peers.SubscribeEvents(peering)
	defer peeringSub.Unsubscribe()

	s.idles = make(map[string]*peerConnection)
	for _, peer := range s.peers.AllPeers() {
		s.idles[peer.id] = peer
	}
	// Nofity any tester listening for startup events
	// 通知任何tester，在监听startup events
	if s.syncStarting != nil {
		s.syncStarting()
	}
	for {
		// Something happened, try to assign new tasks to any idle peers
		// 发生一些事情，试着将新的tasks赋予任何的idle peers
		if !linked {
			s.assignTasks(responses, requestFails, cancel)
		}
		// Wait for something to happen
		// 等待一些事情发生
		select {
		case event := <-peering:
			// A peer joined or left, the tasks queue and allocations need to be
			// checked for potential assignment or reassignment
			// 一个peer加入或者离开，task queue以及allocations需要对潜在的assignment或者
			// reassignment进行检查
			peerid := event.peer.id
			if event.join {
				log.Debug("Joining skeleton peer", "id", peerid)
				s.idles[peerid] = event.peer
			} else {
				log.Debug("Leaving skeleton peer", "id", peerid)
				s.revertRequests(peerid)
				delete(s.idles, peerid)
			}

		case errc := <-s.terminate:
			errc <- nil
			// 已经被终结了
			return nil, errTerminated

		case event := <-s.headEvents:
			// New head was announced, try to integrate it. If successful, nothing
			// needs to be done as the head simply extended the last range. For now
			// we don't seamlessly integrate reorgs to keep things simple. If the
			// network starts doing many mini reorgs, it might be worthwhile handling
			// a limited depth without an error.
			// 宣布了新的head，试着集成它，如果成功的话，什么都不需要做，因为head只是简单地扩展
			// last range，现在我们没有无缝集成reorgs，为了让事情保持简单，如果network启动许多
			// mini reorgs，可能值得处理一个limited depth，而没有错误
			if reorged := s.processNewHead(event.header, event.force); reorged {
				// If a reorg is needed, and we're forcing the new head, signal
				// the syncer to tear down and start over. Otherwise, drop the
				// non-force reorg.
				// 如果需要一个reorg，并且我们正在逼近新的head，通知syncer结束并且重启
				// 否则丢弃non-force reorg
				if event.force {
					event.errc <- nil // forced head reorg accepted
					return event.header, errSyncReorged
				}
				event.errc <- errReorgDenied
				continue
			}
			event.errc <- nil // head extension accepted

			// New head was integrated into the skeleton chain. If the backfiller
			// is still running, it will pick it up. If it already terminated,
			// a new cycle needs to be spun up.
			// 新的head已经集成到了skeleton，如果backfiller仍然在运行，它会拿起它
			// 如果它已经终止了，一个新的cycle需要被启动
			if linked {
				s.filler.resume()
			}

		case req := <-requestFails:
			// 请求失败了
			s.revertRequest(req)

		case res := <-responses:
			// Process the batch of headers. If though processing we managed to
			// link the current subchain to a previously downloaded one, abort the
			// sync and restart with the merged subchains.
			//
			// If we managed to link to the existing local chain or genesis block,
			// abort sync altogether.
			// 处理批量的headers，如果管理连接到当前的subchain到一个之前下载的，中止sync
			// 并且重新启动merged subchains
			// 如果我们管理连接到一个已经存在的local chain或者genesis block，则全部停止sync
			linked, merged := s.processResponse(res)
			if linked {
				// Beacon sync连接到local chain
				log.Debug("Beacon sync linked to local chain")
				return nil, errSyncLinked
			}
			if merged {
				// beacon sync合并了subchain
				log.Debug("Beacon sync merged subchains")
				return nil, errSyncMerged
			}
			// We still have work to do, loop and repeat
			// 我们依然有工作要做，loop并且重复
		}
	}
}

// initSync attempts to get the skeleton sync into a consistent state wrt any
// past state on disk and the newly requested head to sync to. If the new head
// is nil, the method will return and continue from the previous head.
// initSync试着将skeleton同步到一个一致的状态，写入磁盘上任何过去的状态，以及新请求的head
// 用于同步，如果新的head是nil，这个方法会返回并且继续之前的head
func (s *skeleton) initSync(head *types.Header) {
	// Extract the head number, we'll need it all over
	// 抽取出head number，我们需要它
	number := head.Number.Uint64()

	// Retrieve the previously saved sync progress
	if status := rawdb.ReadSkeletonSyncStatus(s.db); len(status) > 0 {
		s.progress = new(skeletonProgress)
		if err := json.Unmarshal(status, s.progress); err != nil {
			log.Error("Failed to decode skeleton sync status", "err", err)
		} else {
			// Previous sync was available, print some continuation logs
			for _, subchain := range s.progress.Subchains {
				log.Debug("Restarting skeleton subchain", "head", subchain.Head, "tail", subchain.Tail)
			}
			// Create a new subchain for the head (unless the last can be extended),
			// trimming anything it would overwrite
			headchain := &subchain{
				Head: number,
				Tail: number,
				Next: head.ParentHash,
			}
			for len(s.progress.Subchains) > 0 {
				// If the last chain is above the new head, delete altogether
				lastchain := s.progress.Subchains[0]
				if lastchain.Tail >= headchain.Tail {
					log.Debug("Dropping skeleton subchain", "head", lastchain.Head, "tail", lastchain.Tail)
					s.progress.Subchains = s.progress.Subchains[1:]
					continue
				}
				// Otherwise truncate the last chain if needed and abort trimming
				if lastchain.Head >= headchain.Tail {
					log.Debug("Trimming skeleton subchain", "oldhead", lastchain.Head, "newhead", headchain.Tail-1, "tail", lastchain.Tail)
					lastchain.Head = headchain.Tail - 1
				}
				break
			}
			// If the last subchain can be extended, we're lucky. Otherwise create
			// a new subchain sync task.
			var extended bool
			if n := len(s.progress.Subchains); n > 0 {
				lastchain := s.progress.Subchains[0]
				if lastchain.Head == headchain.Tail-1 {
					lasthead := rawdb.ReadSkeletonHeader(s.db, lastchain.Head)
					if lasthead.Hash() == head.ParentHash {
						log.Debug("Extended skeleton subchain with new head", "head", headchain.Tail, "tail", lastchain.Tail)
						lastchain.Head = headchain.Tail
						extended = true
					}
				}
			}
			if !extended {
				log.Debug("Created new skeleton subchain", "head", number, "tail", number)
				s.progress.Subchains = append([]*subchain{headchain}, s.progress.Subchains...)
			}
			// Update the database with the new sync stats and insert the new
			// head header. We won't delete any trimmed skeleton headers since
			// those will be outside the index space of the many subchains and
			// the database space will be reclaimed eventually when processing
			// blocks above the current head (TODO(karalabe): don't forget).
			batch := s.db.NewBatch()

			rawdb.WriteSkeletonHeader(batch, head)
			s.saveSyncStatus(batch)

			if err := batch.Write(); err != nil {
				log.Crit("Failed to write skeleton sync status", "err", err)
			}
			return
		}
	}
	// Either we've failed to decode the previus state, or there was none. Start
	// a fresh sync with a single subchain represented by the currently sent
	// chain head.
	// 要么解码之前的状态失败，或者没有之前的状态，启动一个fresh sync，用当前发送的chain head
	// 作为single subchain
	s.progress = &skeletonProgress{
		Subchains: []*subchain{
			{
				Head: number,
				Tail: number,
				Next: head.ParentHash,
			},
		},
	}
	batch := s.db.NewBatch()

	rawdb.WriteSkeletonHeader(batch, head)
	s.saveSyncStatus(batch)

	if err := batch.Write(); err != nil {
		log.Crit("Failed to write initial skeleton sync status", "err", err)
	}
	log.Debug("Created initial skeleton subchain", "head", number, "tail", number)
}

// saveSyncStatus marshals the remaining sync tasks into leveldb.
// saveSyncStatus序列化剩余的sync tasks到leveldb
func (s *skeleton) saveSyncStatus(db ethdb.KeyValueWriter) {
	status, err := json.Marshal(s.progress)
	if err != nil {
		panic(err) // This can only fail during implementation
	}
	rawdb.WriteSkeletonSyncStatus(db, status)
}

// processNewHead does the internal shuffling for a new head marker and either
// accepts and integrates it into the skeleton or requests a reorg. Upon reorg,
// the syncer will tear itself down and restart with a fresh head. It is simpler
// to reconstruct the sync state than to mutate it and hope for the best.
// processNewHead做internal shuffling，对于一个新的head marker，要么接收并且集成它到
// skeleton中，要么请求一个reorg，对于reorg，syncer会停止自己并且用一个新的head重启
// 重新构建sync state比修改它来得更简单
func (s *skeleton) processNewHead(head *types.Header, force bool) bool {
	// If the header cannot be inserted without interruption, return an error for
	// the outer loop to tear down the skeleton sync and restart it
	number := head.Number.Uint64()

	lastchain := s.progress.Subchains[0]
	if lastchain.Tail >= number {
		// If the chain is down to a single beacon header, and it is re-announced
		// once more, ignore it instead of tearing down sync for a noop.
		if lastchain.Head == lastchain.Tail {
			if current := rawdb.ReadSkeletonHeader(s.db, number); current.Hash() == head.Hash() {
				return false
			}
		}
		// Not a noop / double head announce, abort with a reorg
		if force {
			log.Warn("Beacon chain reorged", "tail", lastchain.Tail, "head", lastchain.Head, "newHead", number)
		}
		return true
	}
	if lastchain.Head+1 < number {
		if force {
			log.Warn("Beacon chain gapped", "head", lastchain.Head, "newHead", number)
		}
		return true
	}
	if parent := rawdb.ReadSkeletonHeader(s.db, number-1); parent.Hash() != head.ParentHash {
		if force {
			log.Warn("Beacon chain forked", "ancestor", parent.Number, "hash", parent.Hash(), "want", head.ParentHash)
		}
		return true
	}
	// New header seems to be in the last subchain range. Unwind any extra headers
	// from the chain tip and insert the new head. We won't delete any trimmed
	// skeleton headers since those will be outside the index space of the many
	// subchains and the database space will be reclaimed eventually when processing
	// blocks above the current head (TODO(karalabe): don't forget).
	batch := s.db.NewBatch()

	rawdb.WriteSkeletonHeader(batch, head)
	lastchain.Head = number
	s.saveSyncStatus(batch)

	if err := batch.Write(); err != nil {
		log.Crit("Failed to write skeleton sync status", "err", err)
	}
	return false
}

// assignTasks attempts to match idle peers to pending header retrievals.
// assignTasks试着匹配idle peers和pending header retrievals
func (s *skeleton) assignTasks(success chan *headerResponse, fail chan *headerRequest, cancel chan struct{}) {
	// Sort the peers by download capacity to use faster ones if many available
	// 对peers的下载能力进行排序，使用更快的peers，如果有许多peer可用的话
	idlers := &peerCapacitySort{
		peers: make([]*peerConnection, 0, len(s.idles)),
		caps:  make([]int, 0, len(s.idles)),
	}
	targetTTL := s.peers.rates.TargetTimeout()
	for _, peer := range s.idles {
		idlers.peers = append(idlers.peers, peer)
		idlers.caps = append(idlers.caps, s.peers.rates.Capacity(peer.id, eth.BlockHeadersMsg, targetTTL))
	}
	if len(idlers.peers) == 0 {
		// 没有peers，直接返回
		return
	}
	sort.Sort(idlers)

	// Find header regions not yet downloading and fill them
	// 找到还没有下载的header regions并且填充它们
	for task, owner := range s.scratchOwners {
		// If we're out of idle peers, stop assigning tasks
		// 如果我们用完了idle peers，停止分配tasks
		if len(idlers.peers) == 0 {
			return
		}
		// Skip any tasks already filling
		// 跳过任何已经在填充的tasks
		if owner != "" {
			continue
		}
		// If we've reached the genesis, stop assigning tasks
		// 如果我们到达了genesis，停止分配tasks
		if uint64(task*requestHeaders) >= s.scratchHead {
			return
		}
		// Found a task and have peers available, assign it
		// 找到一个task以及有peers可用，则进行分配
		idle := idlers.peers[0]

		idlers.peers = idlers.peers[1:]
		idlers.caps = idlers.caps[1:]

		// Matched a pending task to an idle peer, allocate a unique request id
		// 匹配一个pending task到一个idle peer，分配一个唯一的request id
		var reqid uint64
		for {
			reqid = uint64(rand.Int63())
			if reqid == 0 {
				continue
			}
			if _, ok := s.requests[reqid]; ok {
				continue
			}
			break
		}
		// Generate the network query and send it to the peer
		// 生成一个network query并且发送到peer
		req := &headerRequest{
			peer:    idle.id,
			id:      reqid,
			deliver: success,
			revert:  fail,
			cancel:  cancel,
			stale:   make(chan struct{}),
			// 计算出请求的task的head
			head: s.scratchHead - uint64(task*requestHeaders),
		}
		s.requests[reqid] = req
		delete(s.idles, idle.id)

		// Generate the network query and send it to the peer
		// 执行network query并且发送到peer
		go s.executeTask(idle, req)

		// Inject the request into the task to block further assignments
		// 注入request到task来阻塞后面的分配
		s.scratchOwners[task] = idle.id
	}
}

// executeTask executes a single fetch request, blocking until either a result
// arrives or a timeouts / cancellation is triggered. The method should be run
// on its own goroutine and will deliver on the requested channels.
// executeTask执行单个的fetch request，阻塞直到一个result到达或者触发了一个timeouts/cancellation
// 这个方法应该在它自己的goroutine运行并且会在请求的channels进行传输
func (s *skeleton) executeTask(peer *peerConnection, req *headerRequest) {
	start := time.Now()
	resCh := make(chan *eth.Response)

	// Figure out how many headers to fetch. Usually this will be a full batch,
	// but for the very tail of the chain, trim the request to the number left.
	// Since nodes may or may not return the genesis header for a batch request,
	// don't even request it. The parent hash of block #1 is enough to link.
	// 搞清楚要抓取的headers，通常会是一个full batch，但是对于very tail of the chain
	// 修剪request到剩下的number，因为nodes可能会或者不会返回一个batch request的genesis header
	// 甚至不要请求它，block #1的parent hash足够进行link
	requestCount := requestHeaders
	if req.head < requestHeaders {
		requestCount = int(req.head)
	}
	peer.log.Trace("Fetching skeleton headers", "from", req.head, "count", requestCount)
	netreq, err := peer.peer.RequestHeadersByNumber(req.head, requestCount, 0, true, resCh)
	if err != nil {
		peer.log.Trace("Failed to request headers", "err", err)
		// 重新调度revert request
		s.scheduleRevertRequest(req)
		return
	}
	defer netreq.Close()

	// Wait until the response arrives, the request is cancelled or times out
	// 等待直到response到达，request被取消或者超时
	ttl := s.peers.rates.TargetTimeout()

	timeoutTimer := time.NewTimer(ttl)
	defer timeoutTimer.Stop()

	select {
	case <-req.cancel:
		peer.log.Debug("Header request cancelled")
		s.scheduleRevertRequest(req)

	case <-timeoutTimer.C:
		// Header retrieval timed out, update the metrics
		// Header获取超时，更新metrics
		peer.log.Warn("Header request timed out, dropping peer", "elapsed", ttl)
		headerTimeoutMeter.Mark(1)
		s.peers.rates.Update(peer.id, eth.BlockHeadersMsg, 0, 0)
		s.scheduleRevertRequest(req)

		// At this point we either need to drop the offending peer, or we need a
		// mechanism to allow waiting for the response and not cancel it. For now
		// lets go with dropping since the header sizes are deterministic and the
		// beacon sync runs exclusive (downloader is idle) so there should be no
		// other load to make timeouts probable. If we notice that timeouts happen
		// more often than we'd like, we can introduce a tracker for the requests
		// gone stale and monitor them. However, in that case too, we need a way
		// to protect against malicious peers never responding, so it would need
		// a second, hard-timeout mechanism.
		// 对peer进行丢弃
		s.drop(peer.id)

	case res := <-resCh:
		// Headers successfully retrieved, update the metrics
		// Headers成功被获取，更新metrics
		headers := *res.Res.(*eth.BlockHeadersPacket)

		headerReqTimer.Update(time.Since(start))
		s.peers.rates.Update(peer.id, eth.BlockHeadersMsg, res.Time, len(headers))

		// Cross validate the headers with the requests
		// 用requests交叉验证headers
		switch {
		case len(headers) == 0:
			// No headers were delivered, reject the response and reschedule
			// 没有headers被传输，拒绝response并且重新调度
			peer.log.Debug("No headers delivered")
			res.Done <- errors.New("no headers delivered")
			s.scheduleRevertRequest(req)

		case headers[0].Number.Uint64() != req.head:
			// Header batch anchored at non-requested number
			peer.log.Debug("Invalid header response head", "have", headers[0].Number, "want", req.head)
			res.Done <- errors.New("invalid header batch anchor")
			s.scheduleRevertRequest(req)

		case req.head >= requestHeaders && len(headers) != requestHeaders:
			// Invalid number of non-genesis headers delivered, reject the response and reschedule
			peer.log.Debug("Invalid non-genesis header count", "have", len(headers), "want", requestHeaders)
			res.Done <- errors.New("not enough non-genesis headers delivered")
			s.scheduleRevertRequest(req)

		case req.head < requestHeaders && uint64(len(headers)) != req.head:
			// Invalid number of genesis headers delivered, reject the response and reschedule
			// 非法的genesis header传输，拒绝response并且重新调度
			peer.log.Debug("Invalid genesis header count", "have", len(headers), "want", headers[0].Number.Uint64())
			res.Done <- errors.New("not enough genesis headers delivered")
			s.scheduleRevertRequest(req)

		default:
			// Packet seems structurally valid, check hash progression and if it
			// is correct too, deliver for storage
			// Packet的结构看起来是正确的，检查hash progression并且如果它是正确的 ，
			// 传输用于存储
			for i := 0; i < len(headers)-1; i++ {
				if headers[i].ParentHash != headers[i+1].Hash() {
					peer.log.Debug("Invalid hash progression", "index", i, "wantparenthash", headers[i].ParentHash, "haveparenthash", headers[i+1].Hash())
					res.Done <- errors.New("invalid hash progression")
					s.scheduleRevertRequest(req)
					return
				}
			}
			// Hash chain is valid. The delivery might still be junk as we're
			// downloading batches concurrently (so no way to link the headers
			// until gaps are filled); in that case, we'll nuke the peer when
			// we detect the fault.
			// Hash chain是合法的，delivery仍然可能是垃圾，因为我们并发地下载batches
			// 因此没有办法对headers进行连接，直到gaps被填充，在这种情况下，我们会nuke the peer
			// 当我们检测到失败的时候
			res.Done <- nil

			select {
			// 将response发送给req.deliver
			case req.deliver <- &headerResponse{
				peer:    peer,
				reqid:   req.id,
				headers: headers,
			}:
			case <-req.cancel:
			}
		}
	}
}

// revertRequests locates all the currently pending reuqests from a particular
// peer and reverts them, rescheduling for others to fulfill.
// revertRequests定位所有当前的pending requests，从一个特定的peer，并且恢复它们
// 重新调度它们到其他peer来填充
func (s *skeleton) revertRequests(peer string) {
	// Gather the requests first, revertals need the lock too
	// 首先收集requests，恢复也需要锁
	var requests []*headerRequest
	for _, req := range s.requests {
		if req.peer == peer {
			requests = append(requests, req)
		}
	}
	// Revert all the requests matching the peer
	// Revert所有匹配peer的请求
	for _, req := range requests {
		s.revertRequest(req)
	}
}

// scheduleRevertRequest asks the event loop to clean up a request and return
// all failed retrieval tasks to the scheduler for reassignment.
// scheduleRevertRequest请求event loop来清理一个request并且返回所有失败的retrieval tasks
// 来调度用于reassignment
func (s *skeleton) scheduleRevertRequest(req *headerRequest) {
	select {
	case req.revert <- req:
		// Sync event loop notified
		// Sync event loop被通知
	case <-req.cancel:
		// Sync cycle got cancelled
		// Sync cycle被取消
	case <-req.stale:
		// Request already reverted
		// 请求已经被取消
	}
}

// revertRequest cleans up a request and returns all failed retrieval tasks to
// the scheduler for reassignment.
// revertRequest清理一个request并且返回所有失败的retrieval tasks到scheduler来进行reassignment
//
// Note, this needs to run on the event runloop thread to reschedule to idle peers.
// On peer threads, use scheduleRevertRequest.
// 注意这需要运行在event runloop thread来重新调度到idle peers，在peer threads，使用scheduleRevertRequest
func (s *skeleton) revertRequest(req *headerRequest) {
	log.Trace("Reverting header request", "peer", req.peer, "reqid", req.id)
	select {
	case <-req.stale:
		log.Trace("Header request already reverted", "peer", req.peer, "reqid", req.id)
		return
	default:
	}
	// 关闭req.stale
	close(req.stale)

	// Remove the request from the tracked set
	// 从tracked set中移除request
	delete(s.requests, req.id)

	// Remove the request from the tracked set and mark the task as not-pending,
	// ready for resheduling
	// 将request从tracked set中移除并且标记task为not-pending，准备好进行重新调度
	s.scratchOwners[(s.scratchHead-req.head)/requestHeaders] = ""
}

func (s *skeleton) processResponse(res *headerResponse) (linked bool, merged bool) {
	res.peer.log.Trace("Processing header response", "head", res.headers[0].Number, "hash", res.headers[0].Hash(), "count", len(res.headers))

	// Whether the response is valid, we can mark the peer as idle and notify
	// the scheduler to assign a new task. If the response is invalid, we'll
	// drop the peer in a bit.
	// response是否是合法的，我们可以标记peer为idle并且通知scheduler赋予一个新的task
	// 如果response是非法的，我们会丢弃peer
	s.idles[res.peer.id] = res.peer

	// Ensure the response is for a valid request
	// 确保response是用于一个合法的request
	if _, ok := s.requests[res.reqid]; !ok {
		// Some internal accounting is broken. A request either times out or it
		// gets fulfilled successfully. It should not be possible to deliver a
		// response to a non-existing request.
		// 内部的记录出了问题，一个request要么超时或者已经成功被填充了，应该不可能
		// 传输一个response到一个不存在的request
		res.peer.log.Error("Unexpected header packet")
		return false, false
	}
	delete(s.requests, res.reqid)

	// Insert the delivered headers into the scratch space independent of the
	// content or continuation; those will be validated in a moment
	// 插入delivered headers到scatch space，独立于内容或者连续性；它们会在之后被校验
	head := res.headers[0].Number.Uint64()
	copy(s.scratchSpace[s.scratchHead-head:], res.headers)

	// If there's still a gap in the head of the scratch space, abort
	// 如果head of the scratch space还是有gap，中止
	if s.scratchSpace[0] == nil {
		return false, false
	}
	// Try to consume any head headers, validating the boundary conditions
	// 试着消费任何的head headers，校验边界条件
	batch := s.db.NewBatch()
	for s.scratchSpace[0] != nil {
		// Next batch of headers available, cross-reference with the subchain
		// we are extending and either accept or discard
		// 下一批headers可用，用subchain交叉引用，我们正在扩展，要么接收要么丢弃
		if s.progress.Subchains[0].Next != s.scratchSpace[0].Hash() {
			// Print a log messages to track what's going on
			tail := s.progress.Subchains[0].Tail
			want := s.progress.Subchains[0].Next
			have := s.scratchSpace[0].Hash()

			log.Warn("Invalid skeleton headers", "peer", s.scratchOwners[0], "number", tail-1, "want", want, "have", have)

			// The peer delivered junk, or at least not the subchain we are
			// syncing to. Free up the scratch space and assignment, reassign
			// and drop the original peer.
			for i := 0; i < requestHeaders; i++ {
				s.scratchSpace[i] = nil
			}
			s.drop(s.scratchOwners[0])
			s.scratchOwners[0] = ""
			break
		}
		// Scratch delivery matches required subchain, deliver the batch of
		// headers and push the subchain forward
		// Scratch delivery匹配请求的subchain，传输批量的headers并且推进subchain
		var consumed int
		for _, header := range s.scratchSpace[:requestHeaders] {
			if header != nil { // nil when the genesis is reached
				consumed++

				rawdb.WriteSkeletonHeader(batch, header)
				s.pulled++

				s.progress.Subchains[0].Tail--
				s.progress.Subchains[0].Next = header.ParentHash

				// If we've reached an existing block in the chain, stop retrieving
				// headers. Note, if we want to support light clients with the same
				// code we'd need to switch here based on the downloader mode. That
				// said, there's no such functionality for now, so don't complicate.
				//
				// In the case of full sync it would be enough to check for the body,
				// but even a full syncing node will generate a receipt once block
				// processing is done, so it's just one more "needless" check.
				var (
					hasBody    = rawdb.HasBody(s.db, header.ParentHash, header.Number.Uint64()-1)
					hasReceipt = rawdb.HasReceipts(s.db, header.ParentHash, header.Number.Uint64()-1)
				)
				if hasBody && hasReceipt {
					linked = true
					break
				}
			}
		}
		head := s.progress.Subchains[0].Head
		tail := s.progress.Subchains[0].Tail
		next := s.progress.Subchains[0].Next

		log.Trace("Primary subchain extended", "head", head, "tail", tail, "next", next)

		// If the beacon chain was linked to the local chain, completely swap out
		// all internal progress and abort header synchronization.
		// 如果beacon chain被连接到local chain，完全交换所有的internal progress并且中止header synchronization
		if linked {
			// Linking into the local chain should also mean that there are no
			// leftover subchains, but in the case of importing the blocks via
			// the engine API, we will not push the subchains forward. This will
			// lead to a gap between an old sync cycle and a future one.
			if subchains := len(s.progress.Subchains); subchains > 1 {
				switch {
				// If there are only 2 subchains - the current one and an older
				// one - and the old one consists of a single block, then it's
				// the expected new sync cycle after some propagated blocks. Log
				// it for debugging purposes, explicitly clean and don't escalate.
				case subchains == 2 && s.progress.Subchains[1].Head == s.progress.Subchains[1].Tail:
					log.Debug("Cleaning previous beacon sync state", "head", s.progress.Subchains[1].Head)
					rawdb.DeleteSkeletonHeader(batch, s.progress.Subchains[1].Head)
					s.progress.Subchains = s.progress.Subchains[:1]

				// If we have more than one header or more than one leftover chain,
				// the syncer's internal state is corrupted. Do try to fix it, but
				// be very vocal about the fault.
				default:
					var context []interface{}

					for i := range s.progress.Subchains[1:] {
						context = append(context, fmt.Sprintf("stale_head_%d", i+1))
						context = append(context, s.progress.Subchains[i+1].Head)
						context = append(context, fmt.Sprintf("stale_tail_%d", i+1))
						context = append(context, s.progress.Subchains[i+1].Tail)
						context = append(context, fmt.Sprintf("stale_next_%d", i+1))
						context = append(context, s.progress.Subchains[i+1].Next)
					}
					log.Error("Cleaning spurious beacon sync leftovers", context...)
					s.progress.Subchains = s.progress.Subchains[:1]

					// Note, here we didn't actually delete the headers at all,
					// just the metadata. We could implement a cleanup mechanism,
					// but further modifying corrupted state is kind of asking
					// for it. Unless there's a good enough reason to risk it,
					// better to live with the small database junk.
				}
			}
			break
		}
		// Batch of headers consumed, shift the download window forward
		// 消费了Batch of headers，往前提升download window
		copy(s.scratchSpace, s.scratchSpace[requestHeaders:])
		for i := 0; i < requestHeaders; i++ {
			s.scratchSpace[scratchHeaders-i-1] = nil
		}
		copy(s.scratchOwners, s.scratchOwners[1:])
		s.scratchOwners[scratchHeaders/requestHeaders-1] = ""

		s.scratchHead -= uint64(consumed)

		// If the subchain extended into the next subchain, we need to handle
		// the overlap. Since there could be many overlaps (come on), do this
		// in a loop.
		// 如果subchain被扩展到下一个subchain，我们需要处理overlap，因为会有很多overlaps
		// 在一个loop里完成
		for len(s.progress.Subchains) > 1 && s.progress.Subchains[1].Head >= s.progress.Subchains[0].Tail {
			// Extract some stats from the second subchain
			head := s.progress.Subchains[1].Head
			tail := s.progress.Subchains[1].Tail
			next := s.progress.Subchains[1].Next

			// Since we just overwrote part of the next subchain, we need to trim
			// its head independent of matching or mismatching content
			if s.progress.Subchains[1].Tail >= s.progress.Subchains[0].Tail {
				// Fully overwritten, get rid of the subchain as a whole
				log.Debug("Previous subchain fully overwritten", "head", head, "tail", tail, "next", next)
				s.progress.Subchains = append(s.progress.Subchains[:1], s.progress.Subchains[2:]...)
				continue
			} else {
				// Partially overwritten, trim the head to the overwritten size
				log.Debug("Previous subchain partially overwritten", "head", head, "tail", tail, "next", next)
				s.progress.Subchains[1].Head = s.progress.Subchains[0].Tail - 1
			}
			// If the old subchain is an extension of the new one, merge the two
			// and let the skeleton syncer restart (to clean internal state)
			if rawdb.ReadSkeletonHeader(s.db, s.progress.Subchains[1].Head).Hash() == s.progress.Subchains[0].Next {
				log.Debug("Previous subchain merged", "head", head, "tail", tail, "next", next)
				s.progress.Subchains[0].Tail = s.progress.Subchains[1].Tail
				s.progress.Subchains[0].Next = s.progress.Subchains[1].Next

				s.progress.Subchains = append(s.progress.Subchains[:1], s.progress.Subchains[2:]...)
				merged = true
			}
		}
		// If subchains were merged, all further available headers in the scratch
		// space are invalid since we skipped ahead. Stop processing the scratch
		// space to avoid dropping peers thinking they delivered invalid data.
		if merged {
			break
		}
	}
	s.saveSyncStatus(batch)
	if err := batch.Write(); err != nil {
		log.Crit("Failed to write skeleton headers and progress", "err", err)
	}
	// Print a progress report making the UX a bit nicer
	// 输出一个progress report，让UX更好
	left := s.progress.Subchains[0].Tail - 1
	if linked {
		left = 0
	}
	if time.Since(s.logged) > 8*time.Second || left == 0 {
		s.logged = time.Now()

		if s.pulled == 0 {
			log.Info("Beacon sync starting", "left", left)
		} else {
			eta := float64(time.Since(s.started)) / float64(s.pulled) * float64(left)
			log.Info("Syncing beacon headers", "downloaded", s.pulled, "left", left, "eta", common.PrettyDuration(eta))
		}
	}
	return linked, merged
}

// cleanStales removes previously synced beacon headers that have become stale
// due to the downloader backfilling past the tracked tail.
// cleanStales移除之前已经同步的beacon headers，它们已经变为stale，因为downloader往回
// 填充超过了tracked tail
func (s *skeleton) cleanStales(filled *types.Header) error {
	number := filled.Number.Uint64()
	log.Trace("Cleaning stale beacon headers", "filled", number, "hash", filled.Hash())

	// If the filled header is below the linked subchain, something's
	// corrupted internally. Report and error and refuse to do anything.
	// 如果filled header在linked subchain以下了，则内部出了些问题，报告一个error
	// 并且拒绝做任何事
	if number < s.progress.Subchains[0].Tail {
		return fmt.Errorf("filled header below beacon header tail: %d < %d", number, s.progress.Subchains[0].Tail)
	}
	// Subchain seems trimmable, push the tail forward up to the last
	// filled header and delete everything before it - if available. In
	// case we filled past the head, recreate the subchain with a new
	// head to keep it consistent with the data on disk.
	var (
		start = s.progress.Subchains[0].Tail // start deleting from the first known header
		end   = number                       // delete until the requested threshold
	)
	s.progress.Subchains[0].Tail = number
	s.progress.Subchains[0].Next = filled.ParentHash

	if s.progress.Subchains[0].Head < number {
		// If more headers were filled than available, push the entire
		// subchain forward to keep tracking the node's block imports
		end = s.progress.Subchains[0].Head + 1 // delete the entire original range, including the head
		s.progress.Subchains[0].Head = number  // assign a new head (tail is already assigned to this)
	}
	// Execute the trimming and the potential rewiring of the progress
	batch := s.db.NewBatch()

	if end != number {
		// The entire original skeleton chain was deleted and a new one
		// defined. Make sure the new single-header chain gets pushed to
		// disk to keep internal state consistent.
		rawdb.WriteSkeletonHeader(batch, filled)
	}
	s.saveSyncStatus(batch)
	for n := start; n < end; n++ {
		// If the batch grew too big, flush it and continue with a new batch.
		// The catch is that the sync metadata needs to reflect the actually
		// flushed state, so temporarily change the subchain progress and
		// revert after the flush.
		// 如果batch增长地太大，flush it并且用一个新的batch继续，问题是sync metadata
		// 需要反映真的flushed state，这样临时的变更subchain progress并且回退，在flush之后
		if batch.ValueSize() >= ethdb.IdealBatchSize {
			tmpTail := s.progress.Subchains[0].Tail
			tmpNext := s.progress.Subchains[0].Next

			s.progress.Subchains[0].Tail = n
			s.progress.Subchains[0].Next = rawdb.ReadSkeletonHeader(s.db, n).ParentHash
			s.saveSyncStatus(batch)

			if err := batch.Write(); err != nil {
				log.Crit("Failed to write beacon trim data", "err", err)
			}
			batch.Reset()

			s.progress.Subchains[0].Tail = tmpTail
			s.progress.Subchains[0].Next = tmpNext
			s.saveSyncStatus(batch)
		}
		rawdb.DeleteSkeletonHeader(batch, n)
	}
	if err := batch.Write(); err != nil {
		// 写入beacon trim data失败
		log.Crit("Failed to write beacon trim data", "err", err)
	}
	return nil
}

// Bounds retrieves the current head and tail tracked by the skeleton syncer.
// This method is used by the backfiller, whose life cycle is controlled by the
// skeleton syncer.
// Bounds获取skeleton syncer追踪的当前的head以及tail，这个方法由backfiller使用，它的lifecycle
// 由skeleton syncer控制
//
// Note, the method will not use the internal state of the skeleton, but will
// rather blindly pull stuff from the database. This is fine, because the back-
// filler will only run when the skeleton chain is fully downloaded and stable.
// There might be new heads appended, but those are atomic from the perspective
// of this method. Any head reorg will first tear down the backfiller and only
// then make the modification.
// 注意，这个方法不会使用skeleton的内部状态，但是会盲目地从数据库中进行拉取，这没问题，因为
// backfiller只会在skeleton chain完全下载并且稳定时才运行，可能有新的heads扩展，但是这些都是
// 原子的，从这个方法的角度，任何head reorg会首先关闭backfiller并且之后才会修改
func (s *skeleton) Bounds() (head *types.Header, tail *types.Header, err error) {
	// Read the current sync progress from disk and figure out the current head.
	// Although there's a lot of error handling here, these are mostly as sanity
	// checks to avoid crashing if a programming error happens. These should not
	// happen in live code.
	status := rawdb.ReadSkeletonSyncStatus(s.db)
	if len(status) == 0 {
		return nil, nil, errors.New("beacon sync not yet started")
	}
	progress := new(skeletonProgress)
	if err := json.Unmarshal(status, progress); err != nil {
		return nil, nil, err
	}
	head = rawdb.ReadSkeletonHeader(s.db, progress.Subchains[0].Head)
	tail = rawdb.ReadSkeletonHeader(s.db, progress.Subchains[0].Tail)

	return head, tail, nil
}

// Header retrieves a specific header tracked by the skeleton syncer. This method
// is meant to be used by the backfiller, whose life cycle is controlled by the
// skeleton syncer.
// Header获取一个特定的headr，由skeleton syncer追踪，这个方法被backfiller使用，它们的生命周期
// 由skeleton syncer掌控
//
// Note, outside the permitted runtimes, this method might return nil results and
// subsequent calls might return headers from different chains.
func (s *skeleton) Header(number uint64) *types.Header {
	return rawdb.ReadSkeletonHeader(s.db, number)
}
