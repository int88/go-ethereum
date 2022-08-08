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
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/eth/protocols/eth"
	"github.com/ethereum/go-ethereum/log"
)

// headerQueue implements typedQueue and is a type adapter between the generic
// concurrent fetcher and the downloader.
// headerQueue实现了typedQueue并且是一个type adapter，在通用的concurrent fetcher
// 和downloader之间
type headerQueue Downloader

// waker returns a notification channel that gets pinged in case more header
// fetches have been queued up, so the fetcher might assign it to idle peers.
// waker返回一个notification channel，当有更多的header fetches已经入队时，这样fetcher
// 可能将它赋值给idle peers
func (q *headerQueue) waker() chan bool {
	return q.queue.headerContCh
}

// pending returns the number of headers that are currently queued for fetching
// by the concurrent downloader.
// pending返回当前排队的一系列headers用于fetching，通过concurrent downloader
func (q *headerQueue) pending() int {
	return q.queue.PendingHeaders()
}

// capacity is responsible for calculating how many headers a particular peer is
// estimated to be able to retrieve within the alloted round trip time.
func (q *headerQueue) capacity(peer *peerConnection, rtt time.Duration) int {
	return peer.HeaderCapacity(rtt)
}

// updateCapacity is responsible for updating how many headers a particular peer
// is estimated to be able to retrieve in a unit time.
func (q *headerQueue) updateCapacity(peer *peerConnection, items int, span time.Duration) {
	peer.UpdateHeaderRate(items, span)
}

// reserve is responsible for allocating a requested number of pending headers
// from the download queue to the specified peer.
// reserve负责申请分配一系列pending headers，从download queue到指定的peer
func (q *headerQueue) reserve(peer *peerConnection, items int) (*fetchRequest, bool, bool) {
	return q.queue.ReserveHeaders(peer, items), false, false
}

// unreserve is resposible for removing the current header retrieval allocation
// assigned to a specific peer and placing it back into the pool to allow
// reassigning to some other peer.
func (q *headerQueue) unreserve(peer string) int {
	fails := q.queue.ExpireHeaders(peer)
	if fails > 2 {
		log.Trace("Header delivery timed out", "peer", peer)
	} else {
		log.Debug("Header delivery stalling", "peer", peer)
	}
	return fails
}

// request is responsible for converting a generic fetch request into a header
// one and sending it to the remote peer for fulfillment.
// request负责转换一个generic fetch request到一个header类的并且将它们发送到remote peer用于填充
func (q *headerQueue) request(peer *peerConnection, req *fetchRequest, resCh chan *eth.Response) (*eth.Request, error) {
	peer.log.Trace("Requesting new batch of headers", "from", req.From)
	return peer.peer.RequestHeadersByNumber(req.From, MaxHeaderFetch, 0, false, resCh)
}

// deliver is responsible for taking a generic response packet from the concurrent
// fetcher, unpacking the header data and delivering it to the downloader's queue.
// deliver负责从concurrent fetcher获取一个通用的response packet，拆开header data并且发送它到
// downloader的queue
func (q *headerQueue) deliver(peer *peerConnection, packet *eth.Response) (int, error) {
	headers := *packet.Res.(*eth.BlockHeadersPacket)
	hashes := packet.Meta.([]common.Hash)

	// 发送headers
	accepted, err := q.queue.DeliverHeaders(peer.id, headers, hashes, q.headerProcCh)
	switch {
	case err == nil && len(headers) == 0:
		peer.log.Trace("Requested headers delivered")
	case err == nil:
		peer.log.Trace("Delivered new batch of headers", "count", len(headers), "accepted", accepted)
	default:
		peer.log.Debug("Failed to deliver retrieved headers", "err", err)
	}
	return accepted, err
}
