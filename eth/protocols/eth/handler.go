// Copyright 2020 The go-ethereum Authors
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

package eth

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/params"
)

const (
	// softResponseLimit is the target maximum size of replies to data retrievals.
	softResponseLimit = 2 * 1024 * 1024

	// maxHeadersServe is the maximum number of block headers to serve. This number
	// is there to limit the number of disk lookups.
	// maxHeadersServe是服务的最大的block headers的数目，这个number用于限制disk lookups
	maxHeadersServe = 1024

	// maxBodiesServe is the maximum number of block bodies to serve. This number
	// is mostly there to limit the number of disk lookups. With 24KB block sizes
	// nowadays, the practical limit will always be softResponseLimit.
	maxBodiesServe = 1024

	// maxReceiptsServe is the maximum number of block receipts to serve. This
	// number is mostly there to limit the number of disk lookups. With block
	// containing 200+ transactions nowadays, the practical limit will always
	// be softResponseLimit.
	maxReceiptsServe = 1024
)

// Handler is a callback to invoke from an outside runner after the boilerplate
// exchanges have passed.
// Handler是一个回调，从outside runner被调用，在boilerplate exchanges通过之后
type Handler func(peer *Peer) error

// Backend defines the data retrieval methods to serve remote requests and the
// callback methods to invoke on remote deliveries.
// Backend定义了data retrieval方法，用来服务remote requests并且在remote deliveries的时候调用callback
type Backend interface {
	// Chain retrieves the blockchain object to serve data.
	Chain() *core.BlockChain

	// TxPool retrieves the transaction pool object to serve data.
	TxPool() TxPool

	// AcceptTxs retrieves whether transaction processing is enabled on the node
	// or if inbound transactions should simply be dropped.
	AcceptTxs() bool

	// RunPeer is invoked when a peer joins on the `eth` protocol. The handler
	// should do any peer maintenance work, handshakes and validations. If all
	// is passed, control should be given back to the `handler` to process the
	// inbound messages going forward.
	// RunPeer被调用，当一个peer加入`eth` protocol，handler应该做任何的peer维护工作，握手
	// 以及校验，如果所有都通过，control应该给回`handler`来处理inbound messages的转发
	RunPeer(peer *Peer, handler Handler) error

	// PeerInfo retrieves all known `eth` information about a peer.
	PeerInfo(id enode.ID) interface{}

	// Handle is a callback to be invoked when a data packet is received from
	// the remote peer. Only packets not consumed by the protocol handler will
	// be forwarded to the backend.
	// Handle是一个callback，当从remote peer接收到一个data packet的时候被调用，只有没有被packet
	// handler处理的packets才会转发到backend
	Handle(peer *Peer, packet Packet) error
}

// TxPool defines the methods needed by the protocol handler to serve transactions.
// TxPool定义了protocol handler需要的方法，来服务txs
type TxPool interface {
	// Get retrieves the transaction from the local txpool with the given hash.
	Get(hash common.Hash) *types.Transaction
}

// MakeProtocols constructs the P2P protocol definitions for `eth`.
// MakeProtocols构建P2P协议，对于`eth`的定义
func MakeProtocols(backend Backend, network uint64, dnsdisc enode.Iterator) []p2p.Protocol {
	protocols := make([]p2p.Protocol, 0, len(ProtocolVersions))
	for _, version := range ProtocolVersions {
		// Blob transactions require eth/68 announcements, disable everything else
		if version <= ETH67 && backend.Chain().Config().CancunTime != nil {
			continue
		}
		version := version // Closure

		protocols = append(protocols, p2p.Protocol{
			Name:    ProtocolName,
			Version: version,
			Length:  protocolLengths[version],
			Run: func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
				// 构建新的peer
				peer := NewPeer(version, p, rw, backend.TxPool())
				defer peer.Close()

				return backend.RunPeer(peer, func(peer *Peer) error {
					return Handle(backend, peer)
				})
			},
			NodeInfo: func() interface{} {
				return nodeInfo(backend.Chain(), network)
			},
			PeerInfo: func(id enode.ID) interface{} {
				return backend.PeerInfo(id)
			},
			Attributes:     []enr.Entry{currentENREntry(backend.Chain())},
			DialCandidates: dnsdisc,
		})
	}
	return protocols
}

// NodeInfo represents a short summary of the `eth` sub-protocol metadata
// known about the host peer.
// NodeInfo代表一个`eth`子协议的元数据，host peer已知
type NodeInfo struct {
	Network    uint64              `json:"network"`    // Ethereum network ID (1=Mainnet, Goerli=5)
	Difficulty *big.Int            `json:"difficulty"` // Total difficulty of the host's blockchain
	Genesis    common.Hash         `json:"genesis"`    // SHA3 hash of the host's genesis block
	Config     *params.ChainConfig `json:"config"`     // Chain configuration for the fork rules
	Head       common.Hash         `json:"head"`       // Hex hash of the host's best owned block
}

// nodeInfo retrieves some `eth` protocol metadata about the running host node.
// nodeInfo获取正在运行的host node的一些`eth`协议的元数据
func nodeInfo(chain *core.BlockChain, network uint64) *NodeInfo {
	head := chain.CurrentBlock()
	hash := head.Hash()

	return &NodeInfo{
		Network:    network,
		Difficulty: chain.GetTd(hash, head.Number.Uint64()),
		Genesis:    chain.Genesis().Hash(),
		Config:     chain.Config(),
		Head:       hash,
	}
}

// Handle is invoked whenever an `eth` connection is made that successfully passes
// the protocol handshake. This method will keep processing messages until the
// connection is torn down.
// Handle被调用，当一个`eth`连接成功建立，通过协议的握手，这个方法保持处理连接，直到连接断开
func Handle(backend Backend, peer *Peer) error {
	for {
		if err := handleMessage(backend, peer); err != nil {
			peer.Log().Debug("Message handling failed in `eth`", "err", err)
			return err
		}
	}
}

type msgHandler func(backend Backend, msg Decoder, peer *Peer) error
type Decoder interface {
	Decode(val interface{}) error
	Time() time.Time
}

var eth67 = map[uint64]msgHandler{
	// 对于各种message的处理方法
	NewBlockHashesMsg:             handleNewBlockhashes,
	NewBlockMsg:                   handleNewBlock,
	TransactionsMsg:               handleTransactions,
	NewPooledTransactionHashesMsg: handleNewPooledTransactionHashes67,
	GetBlockHeadersMsg:            handleGetBlockHeaders,
	BlockHeadersMsg:               handleBlockHeaders,
	GetBlockBodiesMsg:             handleGetBlockBodies,
	BlockBodiesMsg:                handleBlockBodies,
	GetReceiptsMsg:                handleGetReceipts,
	ReceiptsMsg:                   handleReceipts,
	GetPooledTransactionsMsg:      handleGetPooledTransactions,
	PooledTransactionsMsg:         handlePooledTransactions,
}

var eth68 = map[uint64]msgHandler{
	NewBlockHashesMsg:             handleNewBlockhashes,
	NewBlockMsg:                   handleNewBlock,
	TransactionsMsg:               handleTransactions,
	NewPooledTransactionHashesMsg: handleNewPooledTransactionHashes68,
	GetBlockHeadersMsg:            handleGetBlockHeaders,
	BlockHeadersMsg:               handleBlockHeaders,
	GetBlockBodiesMsg:             handleGetBlockBodies,
	BlockBodiesMsg:                handleBlockBodies,
	GetReceiptsMsg:                handleGetReceipts,
	ReceiptsMsg:                   handleReceipts,
	GetPooledTransactionsMsg:      handleGetPooledTransactions,
	PooledTransactionsMsg:         handlePooledTransactions,
}

// handleMessage is invoked whenever an inbound message is received from a remote
// peer. The remote connection is torn down upon returning any error.
// handleMessage被调用，当从remote peer接收到一个inbound message，在接收到任何error的时候
// remote connection关闭
func handleMessage(backend Backend, peer *Peer) error {
	// Read the next message from the remote peer, and ensure it's fully consumed
	// 从remtoe读取下一个message，并且确保它完全被消费
	msg, err := peer.rw.ReadMsg()
	if err != nil {
		return err
	}
	if msg.Size > maxMessageSize {
		return fmt.Errorf("%w: %v > %v", errMsgTooLarge, msg.Size, maxMessageSize)
	}
	defer msg.Discard()

	var handlers = eth67
	if peer.Version() >= ETH68 {
		handlers = eth68
	}
	// Track the amount of time it takes to serve the request and run the handler
	// 追踪处理请求的时间并且运行handler
	if metrics.Enabled {
		h := fmt.Sprintf("%s/%s/%d/%#02x", p2p.HandleHistName, ProtocolName, peer.Version(), msg.Code)
		defer func(start time.Time) {
			sampler := func() metrics.Sample {
				return metrics.ResettingSample(
					metrics.NewExpDecaySample(1028, 0.015),
				)
			}
			metrics.GetOrRegisterHistogramLazy(h, nil, sampler).Update(time.Since(start).Microseconds())
		}(time.Now())
	}
	if handler := handlers[msg.Code]; handler != nil {
		return handler(backend, msg, peer)
	}
	return fmt.Errorf("%w: %v", errInvalidMsgCode, msg.Code)
}
