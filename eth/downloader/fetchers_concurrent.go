// Copyright 2021 The go-ethereum Authors
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
	"errors"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/prque"
	"github.com/ethereum/go-ethereum/eth/protocols/eth"
	"github.com/ethereum/go-ethereum/log"
)

// timeoutGracePeriod is the amount of time to allow for a peer to deliver a
// response to a locally already timed out request. Timeouts are not penalized
// as a peer might be temporarily overloaded, however, they still must reply
// to each request. Failing to do so is considered a protocol violation.
var timeoutGracePeriod = 2 * time.Minute

// typedQueue is an interface defining the adaptor needed to translate the type
// specific downloader/queue schedulers into the type-agnostic general concurrent
// fetcher algorithm calls.
// typedQueue是一个接口，定义了需要的adaptor用于转换特定类型的downloader/queue schedulers
// 到类型无关的通用concurrent fetcher algorightm calls
type typedQueue interface {
	// waker returns a notification channel that gets pinged in case more fetches
	// have been queued up, so the fetcher might assign it to idle peers.
	// waker返回一个notification channel，当有更多的fetches已经入队的时候会被ping
	// 这样fetcher可能将它赋值给idle peers
	waker() chan bool

	// pending returns the number of wrapped items that are currently queued for
	// fetching by the concurrent downloader.
	// pending返回concurrent downloader当前排队等待获取的wrapped items的数目
	pending() int

	// capacity is responsible for calculating how many items of the abstracted
	// type a particular peer is estimated to be able to retrieve within the
	// alloted round trip time.
	// capacity负责计算一个特定的peer在round trip time中获取items的时间
	capacity(peer *peerConnection, rtt time.Duration) int

	// updateCapacity is responsible for updating how many items of the abstracted
	// type a particular peer is estimated to be able to retrieve in a unit time.
	updateCapacity(peer *peerConnection, items int, elapsed time.Duration)

	// reserve is responsible for allocating a requested number of pending items
	// from the download queue to the specified peer.
	// reserve负责分配download queue中请求的pending items的数目到特定的peer
	reserve(peer *peerConnection, items int) (*fetchRequest, bool, bool)

	// unreserve is resposible for removing the current retrieval allocation
	// assigned to a specific peer and placing it back into the pool to allow
	// reassigning to some other peer.
	// unreserve负责将分配给一个特定的peer的retrival移除并且将它加回到pool中允许
	// 重新分配给一些其他的peer
	unreserve(peer string) int

	// request is responsible for converting a generic fetch request into a typed
	// one and sending it to the remote peer for fulfillment.
	// request负责将一个通用的fetch request转换为类型特定的，并且将他们发送到remote peer用于填充
	request(peer *peerConnection, req *fetchRequest, resCh chan *eth.Response) (*eth.Request, error)

	// deliver is responsible for taking a generic response packet from the
	// concurrent fetcher, unpacking the type specific data and delivering
	// it to the downloader's queue.
	// deliver负责从concurrent fetcher获取一个generic response packet
	// unpacking类型特定的数据并且传输到downloader的队列中
	deliver(peer *peerConnection, packet *eth.Response) (int, error)
}

// concurrentFetch iteratively downloads scheduled block parts, taking available
// peers, reserving a chunk of fetch requests for each and waiting for delivery
// or timeouts.
// concurrentFetch迭代的下载调度的block parts，按照可用的peers，为每个保留一系列的fetch requests
// 等待被传送或者超时，这个函数不仅下载blocks，还下载receipts等等
func (d *Downloader) concurrentFetch(queue typedQueue, beaconMode bool) error {
	// Create a delivery channel to accept responses from all peers
	// 创建一个delivery channel，用来从所有peers接收responses
	responses := make(chan *eth.Response)

	// Track the currently active requests and their timeout order
	// 追踪当前的active requests以及它们的超时顺序
	pending := make(map[string]*eth.Request)
	defer func() {
		// Abort all requests on sync cycle cancellation. The requests may still
		// be fulfilled by the remote side, but the dispatcher will not wait to
		// deliver them since nobody's going to be listening.
		for _, req := range pending {
			req.Close()
		}
	}()
	ordering := make(map[*eth.Request]int)
	timeouts := prque.New(func(data interface{}, index int) {
		ordering[data.(*eth.Request)] = index
	})

	timeout := time.NewTimer(0)
	if !timeout.Stop() {
		<-timeout.C
	}
	defer timeout.Stop()

	// Track the timed-out but not-yet-answered requests separately. We want to
	// keep tracking which peers are busy (potentially overloaded), so removing
	// all trace of a timed out request is not good. We also can't just cancel
	// the pending request altogether as that would prevent a late response from
	// being delivered, thus never unblocking the peer.
	stales := make(map[string]*eth.Request)
	defer func() {
		// Abort all requests on sync cycle cancellation. The requests may still
		// be fulfilled by the remote side, but the dispatcher will not wait to
		// deliver them since nobody's going to be listening.
		for _, req := range stales {
			req.Close()
		}
	}()
	// Subscribe to peer lifecycle events to schedule tasks to new joiners and
	// reschedule tasks upon disconnections. We don't care which event happened
	// for simplicity, so just use a single channel.
	// 订阅peer的lifecycle events，从而调度任务到new joiners并且在断开连接的时候重新调度
	// tasks
	peering := make(chan *peeringEvent, 64) // arbitrary buffer, just some burst protection

	peeringSub := d.peers.SubscribeEvents(peering)
	defer peeringSub.Unsubscribe()

	// Prepare the queue and fetch block parts until the block header fetcher's done
	// 准备queue并且抓取block parts，直到block header fetcher已经完成
	finished := false
	for {
		// Short circuit if we lost all our peers
		// 丢失了所有的peers
		if d.peers.Len() == 0 && !beaconMode {
			return errNoPeers
		}
		// If there's nothing more to fetch, wait or terminate
		// 如果没有更多东西需要获取了，等待或者终结
		if queue.pending() == 0 {
			if len(pending) == 0 && finished {
				// 结束了则返回
				return nil
			}
		} else {
			// Send a download request to all idle peers, until throttled
			// 发送一个download request到所有idle peers，直到限流
			var (
				idles []*peerConnection
				caps  []int
			)
			for _, peer := range d.peers.AllPeers() {
				// 遍历所有peers
				pending, stale := pending[peer.id], stales[peer.id]
				if pending == nil && stale == nil {
					idles = append(idles, peer)
					caps = append(caps, queue.capacity(peer, time.Second))
				} else if stale != nil {
					if waited := time.Since(stale.Sent); waited > timeoutGracePeriod {
						// Request has been in flight longer than the grace period
						// permitted it, consider the peer malicious attempting to
						// stall the sync.
						peer.log.Warn("Peer stalling, dropping", "waited", common.PrettyDuration(waited))
						d.dropPeer(peer.id)
					}
				}
			}
			sort.Sort(&peerCapacitySort{idles, caps})

			var (
				progressed bool
				throttled  bool
				queued     = queue.pending()
			)
			for _, peer := range idles {
				// Short circuit if throttling activated or there are no more
				// queued tasks to be retrieved
				// 快速路径，如果触发了限流或者没有更多的queued tasks需要被获取
				if throttled {
					break
				}
				if queued = queue.pending(); queued == 0 {
					break
				}
				// Reserve a chunk of fetches for a peer. A nil can mean either that
				// no more headers are available, or that the peer is known not to
				// have them.
				// 为一个Peer保留chunk of fetches，一个nil意味着没有更多的headers可用
				// 或者那个peer已知没有这些header
				request, progress, throttle := queue.reserve(peer, queue.capacity(peer, d.peers.rates.TargetRoundTrip()))
				if progress {
					progressed = true
				}
				if throttle {
					throttled = true
					throttleCounter.Inc(1)
				}
				if request == nil {
					continue
				}
				// Fetch the chunk and make sure any errors return the hashes to the queue
				// 抓取chunk并且确保任何错误返回hashes到队列
				req, err := queue.request(peer, request, responses)
				if err != nil {
					// Sending the request failed, which generally means the peer
					// was diconnected in between assignment and network send.
					// Although all peer removal operations return allocated tasks
					// to the queue, that is async, and we can do better here by
					// immediately pushing the unfulfilled requests.
					queue.unreserve(peer.id) // TODO(karalabe): This needs a non-expiration method
					continue
				}
				pending[peer.id] = req

				ttl := d.peers.rates.TargetTimeout()
				ordering[req] = timeouts.Size()

				timeouts.Push(req, -time.Now().Add(ttl).UnixNano())
				if timeouts.Size() == 1 {
					timeout.Reset(ttl)
				}
			}
			// Make sure that we have peers available for fetching. If all peers have been tried
			// and all failed throw an error
			// 确保我们有peers可用用于fetching，如果所有peers都已经尝试了并且失败了，发送一个error
			if !progressed && !throttled && len(pending) == 0 && len(idles) == d.peers.Len() && queued > 0 && !beaconMode {
				return errPeersUnavailable
			}
		}
		// Wait for something to happen
		// 等待一些事情发生
		select {
		case <-d.cancelCh:
			// If sync was cancelled, tear down the parallel retriever. Pending
			// requests will be cancelled locally, and the remote responses will
			// be dropped when they arrive
			// 如果同步为取消了，摧毁parallel retrieval，Pending requests会在本地取消
			// 远程的responses会被丢弃，当它们到达的时候
			return errCanceled

		case event := <-peering:
			// A peer joined or left, the tasks queue and allocations need to be
			// checked for potential assignment or reassignment
			// 一个peer加入或者离开，tasks queue以及allocations需要被检查，对于潜在
			// 的assignment或者reassignment
			peerid := event.peer.id

			if event.join {
				// Sanity check the internal state; this can be dropped later
				if _, ok := pending[peerid]; ok {
					event.peer.log.Error("Pending request exists for joining peer")
				}
				if _, ok := stales[peerid]; ok {
					event.peer.log.Error("Stale request exists for joining peer")
				}
				// Loop back to the entry point for task assignment
				// 回到entry point用于task assignment
				continue
			}
			// A peer left, any existing requests need to be untracked, pending
			// tasks returned and possible reassignment checked
			// 一个peer离开了，任何已经存在的requests需要被untracked，返回pending tasks
			// 并且检查可能的reassignment
			if req, ok := pending[peerid]; ok {
				queue.unreserve(peerid) // TODO(karalabe): This needs a non-expiration method
				// 从pending中移除
				delete(pending, peerid)
				req.Close()

				if index, live := ordering[req]; live {
					timeouts.Remove(index)
					if index == 0 {
						if !timeout.Stop() {
							<-timeout.C
						}
						if timeouts.Size() > 0 {
							_, exp := timeouts.Peek()
							timeout.Reset(time.Until(time.Unix(0, -exp)))
						}
					}
					// 从ordering中移除
					delete(ordering, req)
				}
			}
			if req, ok := stales[peerid]; ok {
				delete(stales, peerid)
				req.Close()
			}

		case <-timeout.C:
			// Retrieve the next request which should have timed out. The check
			// below is purely for to catch programming errors, given the correct
			// code, there's no possible order of events that should result in a
			// timeout firing for a non-existent event.
			item, exp := timeouts.Peek()
			if now, at := time.Now(), time.Unix(0, -exp); now.Before(at) {
				log.Error("Timeout triggered but not reached", "left", at.Sub(now))
				timeout.Reset(at.Sub(now))
				continue
			}
			req := item.(*eth.Request)

			// Stop tracking the timed out request from a timing perspective,
			// cancel it, so it's not considered in-flight anymore, but keep
			// the peer marked busy to prevent assigning a second request and
			// overloading it further.
			delete(pending, req.Peer)
			stales[req.Peer] = req
			delete(ordering, req)

			timeouts.Pop()
			if timeouts.Size() > 0 {
				_, exp := timeouts.Peek()
				timeout.Reset(time.Until(time.Unix(0, -exp)))
			}
			// New timeout potentially set if there are more requests pending,
			// reschedule the failed one to a free peer
			fails := queue.unreserve(req.Peer)

			// Finally, update the peer's retrieval capacity, or if it's already
			// below the minimum allowance, drop the peer. If a lot of retrieval
			// elements expired, we might have overestimated the remote peer or
			// perhaps ourselves. Only reset to minimal throughput but don't drop
			// just yet.
			// 最后，更新peer的retrieval capacity，或者如果它已经在minimum allowance之下
			// 丢弃peer，如果很多retrieval elements超时，我们可能高估了remote peer或者我们自己
			// 只是重置minimal throughput，但是还不丢弃
			//
			// The reason the minimum threshold is 2 is that the downloader tries
			// to estimate the bandwidth and latency of a peer separately, which
			// requires pushing the measured capacity a bit and seeing how response
			// times reacts, to it always requests one more than the minimum (i.e.
			// min 2).
			peer := d.peers.Peer(req.Peer)
			if peer == nil {
				// If the peer got disconnected in between, we should really have
				// short-circuited it already. Just in case there's some strange
				// codepath, leave this check in not to crash.
				log.Error("Delivery timeout from unknown peer", "peer", req.Peer)
				continue
			}
			if fails > 2 {
				queue.updateCapacity(peer, 0, 0)
			} else {
				d.dropPeer(peer.id)

				// If this peer was the master peer, abort sync immediately
				d.cancelLock.RLock()
				master := peer.id == d.cancelPeer
				d.cancelLock.RUnlock()

				if master {
					d.cancel()
					return errTimeout
				}
			}

		case res := <-responses:
			// Response arrived, it may be for an existing or an already timed
			// out request. If the former, update the timeout heap and perhaps
			// reschedule the timeout timer.
			// Response到底，它可能是对于一个已经存在或者一个已经超时的request
			index, live := ordering[res.Req]
			if live {
				timeouts.Remove(index)
				if index == 0 {
					if !timeout.Stop() {
						<-timeout.C
					}
					if timeouts.Size() > 0 {
						_, exp := timeouts.Peek()
						timeout.Reset(time.Until(time.Unix(0, -exp)))
					}
				}
				delete(ordering, res.Req)
			}
			// Delete the pending request (if it still exists) and mark the peer idle
			// 删除pending request（如果它依然存在的话）并且将peer标记为idle
			delete(pending, res.Req.Peer)
			delete(stales, res.Req.Peer)

			// Signal the dispatcher that the round trip is done. We'll drop the
			// peer if the data turns out to be junk.
			// 提醒dispatcher，round trip已经结束了，我们会丢弃peer，如果返回的data是垃圾
			res.Done <- nil
			res.Req.Close()

			// If the peer was previously banned and failed to deliver its pack
			// in a reasonable time frame, ignore its message.
			// 如果peer之前已经被屏蔽了并且不能在合理的时间内传递它的pack，忽略它的message
			if peer := d.peers.Peer(res.Req.Peer); peer != nil {
				// Deliver the received chunk of data and check chain validity
				// 传送接收到的chunk of data并且检查chain validity
				// 用queue进行deliver
				accepted, err := queue.deliver(peer, res)
				if errors.Is(err, errInvalidChain) {
					return err
				}
				// Unless a peer delivered something completely else than requested (usually
				// caused by a timed out request which came through in the end), set it to
				// idle. If the delivery's stale, the peer should have already been idled.
				if !errors.Is(err, errStaleDelivery) {
					queue.updateCapacity(peer, accepted, res.Time)
				}
			}

		case cont := <-queue.waker():
			// The header fetcher sent a continuation flag, check if it's done
			// header fetcher发送了一个continuation flag，检查它是否完成
			if !cont {
				finished = true
			}
		}
	}
}
