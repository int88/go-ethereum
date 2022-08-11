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
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/protocols/eth"
)

// fetchHeadersByHash is a blocking version of Peer.RequestHeadersByHash which
// handles all the cancellation, interruption and timeout mechanisms of a data
// retrieval to allow blocking API calls.
// fetchHeadersByHash是一个阻塞版本的Peer.RequestHeadersByHash，它处理cancellation, interruption
// 以及超时机制在data retrieval
func (d *Downloader) fetchHeadersByHash(p *peerConnection, hash common.Hash, amount int, skip int, reverse bool) ([]*types.Header, []common.Hash, error) {
	// Create the response sink and send the network request
	// 创建response sink并且发送network request
	start := time.Now()
	resCh := make(chan *eth.Response)

	req, err := p.peer.RequestHeadersByHash(hash, amount, skip, reverse, resCh)
	if err != nil {
		return nil, nil, err
	}
	defer req.Close()

	// Wait until the response arrives, the request is cancelled or times out
	// 等待直到response到来，request被取消或者超时
	ttl := d.peers.rates.TargetTimeout()

	timeoutTimer := time.NewTimer(ttl)
	defer timeoutTimer.Stop()

	select {
	case <-d.cancelCh:
		return nil, nil, errCanceled

	case <-timeoutTimer.C:
		// Header retrieval timed out, update the metrics
		// Header retrieval超时，更新metrics
		p.log.Debug("Header request timed out", "elapsed", ttl)
		headerTimeoutMeter.Mark(1)

		return nil, nil, errTimeout

	// 同步获取header
	case res := <-resCh:
		// Headers successfully retrieved, update the metrics
		// 成功获取了headers，更新metric
		headerReqTimer.Update(time.Since(start))
		headerInMeter.Mark(int64(len(*res.Res.(*eth.BlockHeadersPacket))))

		// Don't reject the packet even if it turns out to be bad, downloader will
		// disconnect the peer on its own terms. Simply delivery the headers to
		// be processed by the caller
		res.Done <- nil

		// 从res中解析出BlockHeadersPacket
		return *res.Res.(*eth.BlockHeadersPacket), res.Meta.([]common.Hash), nil
	}
}

// fetchHeadersByNumber is a blocking version of Peer.RequestHeadersByNumber which
// handles all the cancellation, interruption and timeout mechanisms of a data
// retrieval to allow blocking API calls.
// fetchHeadersByNumber是一个阻塞版本的Peer.RequestHeadersByNumber，它负责处理所有的cancellation
// interruption以及timeout机制，对于一个data retrieval，从而允许blocking API调用
func (d *Downloader) fetchHeadersByNumber(p *peerConnection, number uint64, amount int, skip int, reverse bool) ([]*types.Header, []common.Hash, error) {
	// Create the response sink and send the network request
	// 创建response sink并且发送network request
	start := time.Now()
	resCh := make(chan *eth.Response)

	req, err := p.peer.RequestHeadersByNumber(number, amount, skip, reverse, resCh)
	if err != nil {
		return nil, nil, err
	}
	defer req.Close()

	// Wait until the response arrives, the request is cancelled or times out
	ttl := d.peers.rates.TargetTimeout()

	timeoutTimer := time.NewTimer(ttl)
	defer timeoutTimer.Stop()

	select {
	case <-d.cancelCh:
		return nil, nil, errCanceled

	case <-timeoutTimer.C:
		// Header retrieval timed out, update the metrics
		p.log.Debug("Header request timed out", "elapsed", ttl)
		headerTimeoutMeter.Mark(1)

		return nil, nil, errTimeout

	case res := <-resCh:
		// Headers successfully retrieved, update the metrics
		// 成功获取了Headers，更新metrics
		headerReqTimer.Update(time.Since(start))
		headerInMeter.Mark(int64(len(*res.Res.(*eth.BlockHeadersPacket))))

		// Don't reject the packet even if it turns out to be bad, downloader will
		// disconnect the peer on its own terms. Simply delivery the headers to
		// be processed by the caller
		// 不要拒绝packet，即使它是坏的，downloader会以自己的方式和downloader断开连接
		// 简单地传递headers来给caller调用
		res.Done <- nil

		return *res.Res.(*eth.BlockHeadersPacket), res.Meta.([]common.Hash), nil
	}
}
