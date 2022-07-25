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
	"errors"
	"fmt"
	"io"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/forkid"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
)

// Constants to match up protocol versions and messages
// 用于匹配protocol版本以及messages的常量
const (
	ETH66 = 66
)

// ProtocolName is the official short name of the `eth` protocol used during
// devp2p capability negotiation.
const ProtocolName = "eth"

// ProtocolVersions are the supported versions of the `eth` protocol (first
// is primary).
var ProtocolVersions = []uint{ETH66}

// protocolLengths are the number of implemented message corresponding to
// different protocol versions.
var protocolLengths = map[uint]uint64{ETH66: 17}

// maxMessageSize is the maximum cap on the size of a protocol message.
const maxMessageSize = 10 * 1024 * 1024

// MSG的类型
const (
	StatusMsg                     = 0x00
	NewBlockHashesMsg             = 0x01
	TransactionsMsg               = 0x02
	GetBlockHeadersMsg            = 0x03
	BlockHeadersMsg               = 0x04
	GetBlockBodiesMsg             = 0x05
	BlockBodiesMsg                = 0x06
	NewBlockMsg                   = 0x07
	GetNodeDataMsg                = 0x0d
	NodeDataMsg                   = 0x0e
	GetReceiptsMsg                = 0x0f
	ReceiptsMsg                   = 0x10
	NewPooledTransactionHashesMsg = 0x08
	GetPooledTransactionsMsg      = 0x09
	PooledTransactionsMsg         = 0x0a
)

var (
	errNoStatusMsg             = errors.New("no status message")
	errMsgTooLarge             = errors.New("message too long")
	errDecode                  = errors.New("invalid message")
	errInvalidMsgCode          = errors.New("invalid message code")
	errProtocolVersionMismatch = errors.New("protocol version mismatch")
	errNetworkIDMismatch       = errors.New("network ID mismatch")
	errGenesisMismatch         = errors.New("genesis mismatch")
	errForkIDRejected          = errors.New("fork ID rejected")
)

// Packet represents a p2p message in the `eth` protocol.
// Packet代表了一个在`eth`协议中的p2p message
type Packet interface {
	Name() string // Name returns a string corresponding to the message type.
	Kind() byte   // Kind returns the message type.
}

// StatusPacket is the network packet for the status message for eth/64 and later.
// StatusPacket是network packet用来对于eth/64以及之后的status message
type StatusPacket struct {
	ProtocolVersion uint32
	NetworkID       uint64
	TD              *big.Int
	Head            common.Hash
	Genesis         common.Hash
	ForkID          forkid.ID
}

// NewBlockHashesPacket is the network packet for the block announcements.
// NewBlockHashesPacket是network packet用于block announcements
type NewBlockHashesPacket []struct {
	Hash   common.Hash // Hash of one particular block being announced // 被宣告的特定block的hash
	Number uint64      // Number of one particular block being announced // 被宣告的特定block的编号
}

// Unpack retrieves the block hashes and numbers from the announcement packet
// and returns them in a split flat format that's more consistent with the
// internal data structures.
func (p *NewBlockHashesPacket) Unpack() ([]common.Hash, []uint64) {
	var (
		hashes  = make([]common.Hash, len(*p))
		numbers = make([]uint64, len(*p))
	)
	for i, body := range *p {
		hashes[i], numbers[i] = body.Hash, body.Number
	}
	return hashes, numbers
}

// TransactionsPacket is the network packet for broadcasting new transactions.
// TransactionsPacket是network packet用于广播transactions
type TransactionsPacket []*types.Transaction

// GetBlockHeadersPacket represents a block header query.
// GetBlockHeadersPacket代表了一个block header query
type GetBlockHeadersPacket struct {
	Origin HashOrNumber // Block from which to retrieve headers // 获取headers的Block
	Amount uint64       // Maximum number of headers to retrieve // 获取的headers的最大数目
	Skip   uint64       // Blocks to skip between consecutive headers // 连续的headers之间跳过的Blocks
	// 查询的方向（false表示向latest遍历，true表示向genesis遍历）
	Reverse bool // Query direction (false = rising towards latest, true = falling towards genesis)
}

// GetBlockHeadersPacket66 represents a block header query over eth/66
type GetBlockHeadersPacket66 struct {
	RequestId uint64
	*GetBlockHeadersPacket
}

// HashOrNumber is a combined field for specifying an origin block.
// HashOrNumber是一个聚合的字段用于指定一个origin block
type HashOrNumber struct {
	Hash   common.Hash // Block hash from which to retrieve headers (excludes Number)
	Number uint64      // Block hash from which to retrieve headers (excludes Hash)
}

// EncodeRLP is a specialized encoder for HashOrNumber to encode only one of the
// two contained union fields.
func (hn *HashOrNumber) EncodeRLP(w io.Writer) error {
	if hn.Hash == (common.Hash{}) {
		return rlp.Encode(w, hn.Number)
	}
	if hn.Number != 0 {
		return fmt.Errorf("both origin hash (%x) and number (%d) provided", hn.Hash, hn.Number)
	}
	return rlp.Encode(w, hn.Hash)
}

// DecodeRLP is a specialized decoder for HashOrNumber to decode the contents
// into either a block hash or a block number.
func (hn *HashOrNumber) DecodeRLP(s *rlp.Stream) error {
	_, size, err := s.Kind()
	switch {
	case err != nil:
		return err
	case size == 32:
		hn.Number = 0
		return s.Decode(&hn.Hash)
	case size <= 8:
		hn.Hash = common.Hash{}
		return s.Decode(&hn.Number)
	default:
		return fmt.Errorf("invalid input size %d for origin", size)
	}
}

// BlockHeadersPacket represents a block header response.
// BlockHeadersPacket代表一个block header的response
type BlockHeadersPacket []*types.Header

// BlockHeadersPacket66 represents a block header response over eth/66.
// BlockHeadersPacket66代表一个block header response，位于eth/66之上
type BlockHeadersPacket66 struct {
	RequestId uint64
	BlockHeadersPacket
}

// BlockHeadersRLPPacket represents a block header response, to use when we already
// have the headers rlp encoded.
type BlockHeadersRLPPacket []rlp.RawValue

// BlockHeadersRLPPacket66 represents a block header response over eth/66.
type BlockHeadersRLPPacket66 struct {
	RequestId uint64
	BlockHeadersRLPPacket
}

// NewBlockPacket is the network packet for the block propagation message.
// NewBlockPacket是对于block propagation message的network packet
type NewBlockPacket struct {
	Block *types.Block
	TD    *big.Int
}

// sanityCheck verifies that the values are reasonable, as a DoS protection
func (request *NewBlockPacket) sanityCheck() error {
	if err := request.Block.SanityCheck(); err != nil {
		return err
	}
	//TD at mainnet block #7753254 is 76 bits. If it becomes 100 million times
	// larger, it will still fit within 100 bits
	if tdlen := request.TD.BitLen(); tdlen > 100 {
		return fmt.Errorf("too large block TD: bitlen %d", tdlen)
	}
	return nil
}

// GetBlockBodiesPacket represents a block body query.
// GetBlockBodiesPacket代表一个block body的query
type GetBlockBodiesPacket []common.Hash

// GetBlockBodiesPacket66 represents a block body query over eth/66.
type GetBlockBodiesPacket66 struct {
	RequestId uint64
	GetBlockBodiesPacket
}

// BlockBodiesPacket is the network packet for block content distribution.
// BlockBodiesPacket是用于block content分发的network packet
type BlockBodiesPacket []*BlockBody

// BlockBodiesPacket66 is the network packet for block content distribution over eth/66.
type BlockBodiesPacket66 struct {
	RequestId uint64
	BlockBodiesPacket
}

// BlockBodiesRLPPacket is used for replying to block body requests, in cases
// where we already have them RLP-encoded, and thus can avoid the decode-encode
// roundtrip.
type BlockBodiesRLPPacket []rlp.RawValue

// BlockBodiesRLPPacket66 is the BlockBodiesRLPPacket over eth/66
type BlockBodiesRLPPacket66 struct {
	RequestId uint64
	BlockBodiesRLPPacket
}

// BlockBody represents the data content of a single block.
// BlockBody代表了单个的block的内容
type BlockBody struct {
	Transactions []*types.Transaction // Transactions contained within a block
	Uncles       []*types.Header      // Uncles contained within a block
}

// Unpack retrieves the transactions and uncles from the range packet and returns
// them in a split flat format that's more consistent with the internal data structures.
func (p *BlockBodiesPacket) Unpack() ([][]*types.Transaction, [][]*types.Header) {
	var (
		txset    = make([][]*types.Transaction, len(*p))
		uncleset = make([][]*types.Header, len(*p))
	)
	for i, body := range *p {
		txset[i], uncleset[i] = body.Transactions, body.Uncles
	}
	return txset, uncleset
}

// GetNodeDataPacket represents a trie node data query.
// GetNodeDataPacket代表对于一个trie node data的查询
type GetNodeDataPacket []common.Hash

// GetNodeDataPacket66 represents a trie node data query over eth/66.
type GetNodeDataPacket66 struct {
	RequestId uint64
	GetNodeDataPacket
}

// NodeDataPacket is the network packet for trie node data distribution.
type NodeDataPacket [][]byte

// NodeDataPacket66 is the network packet for trie node data distribution over eth/66.
type NodeDataPacket66 struct {
	RequestId uint64
	NodeDataPacket
}

// GetReceiptsPacket represents a block receipts query.
// GetReceiptsPacket代表了对于一个block receipts的查询
type GetReceiptsPacket []common.Hash

// GetReceiptsPacket66 represents a block receipts query over eth/66.
type GetReceiptsPacket66 struct {
	RequestId uint64
	GetReceiptsPacket
}

// ReceiptsPacket is the network packet for block receipts distribution.
type ReceiptsPacket [][]*types.Receipt

// ReceiptsPacket66 is the network packet for block receipts distribution over eth/66.
type ReceiptsPacket66 struct {
	RequestId uint64
	ReceiptsPacket
}

// ReceiptsRLPPacket is used for receipts, when we already have it encoded
type ReceiptsRLPPacket []rlp.RawValue

// ReceiptsRLPPacket66 is the eth-66 version of ReceiptsRLPPacket
type ReceiptsRLPPacket66 struct {
	RequestId uint64
	ReceiptsRLPPacket
}

// NewPooledTransactionHashesPacket represents a transaction announcement packet.
// NewPooledTransactionHashesPacket代表一个transaction announcement的包
type NewPooledTransactionHashesPacket []common.Hash

// GetPooledTransactionsPacket represents a transaction query.
type GetPooledTransactionsPacket []common.Hash

type GetPooledTransactionsPacket66 struct {
	RequestId uint64
	GetPooledTransactionsPacket
}

// PooledTransactionsPacket is the network packet for transaction distribution.
// PooledTransactionsPacket是用于transaction分发的network packet
type PooledTransactionsPacket []*types.Transaction

// PooledTransactionsPacket66 is the network packet for transaction distribution over eth/66.
type PooledTransactionsPacket66 struct {
	RequestId uint64
	PooledTransactionsPacket
}

// PooledTransactionsRLPPacket is the network packet for transaction distribution, used
// in the cases we already have them in rlp-encoded form
// 按照rlp编码的形式的transaction分发
type PooledTransactionsRLPPacket []rlp.RawValue

// PooledTransactionsRLPPacket66 is the eth/66 form of PooledTransactionsRLPPacket
type PooledTransactionsRLPPacket66 struct {
	RequestId uint64
	PooledTransactionsRLPPacket
}

// 从peer获取status,block hashes, block header, block body, node data, receipts, transactions等等
func (*StatusPacket) Name() string { return "Status" }
func (*StatusPacket) Kind() byte   { return StatusMsg }

func (*NewBlockHashesPacket) Name() string { return "NewBlockHashes" }
func (*NewBlockHashesPacket) Kind() byte   { return NewBlockHashesMsg }

func (*TransactionsPacket) Name() string { return "Transactions" }
func (*TransactionsPacket) Kind() byte   { return TransactionsMsg }

func (*GetBlockHeadersPacket) Name() string { return "GetBlockHeaders" }
func (*GetBlockHeadersPacket) Kind() byte   { return GetBlockHeadersMsg }

func (*BlockHeadersPacket) Name() string { return "BlockHeaders" }
func (*BlockHeadersPacket) Kind() byte   { return BlockHeadersMsg }

func (*GetBlockBodiesPacket) Name() string { return "GetBlockBodies" }
func (*GetBlockBodiesPacket) Kind() byte   { return GetBlockBodiesMsg }

func (*BlockBodiesPacket) Name() string { return "BlockBodies" }
func (*BlockBodiesPacket) Kind() byte   { return BlockBodiesMsg }

func (*NewBlockPacket) Name() string { return "NewBlock" }
func (*NewBlockPacket) Kind() byte   { return NewBlockMsg }

func (*GetNodeDataPacket) Name() string { return "GetNodeData" }
func (*GetNodeDataPacket) Kind() byte   { return GetNodeDataMsg }

func (*NodeDataPacket) Name() string { return "NodeData" }
func (*NodeDataPacket) Kind() byte   { return NodeDataMsg }

func (*GetReceiptsPacket) Name() string { return "GetReceipts" }
func (*GetReceiptsPacket) Kind() byte   { return GetReceiptsMsg }

func (*ReceiptsPacket) Name() string { return "Receipts" }
func (*ReceiptsPacket) Kind() byte   { return ReceiptsMsg }

func (*NewPooledTransactionHashesPacket) Name() string { return "NewPooledTransactionHashes" }
func (*NewPooledTransactionHashesPacket) Kind() byte   { return NewPooledTransactionHashesMsg }

func (*GetPooledTransactionsPacket) Name() string { return "GetPooledTransactions" }
func (*GetPooledTransactionsPacket) Kind() byte   { return GetPooledTransactionsMsg }

func (*PooledTransactionsPacket) Name() string { return "PooledTransactions" }
func (*PooledTransactionsPacket) Kind() byte   { return PooledTransactionsMsg }
