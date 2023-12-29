// Copyright 2020 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package ethtest

import (
	"crypto/rand"
	"math/big"
	"reflect"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/misc/eip4844"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/crypto/kzg4844"
	"github.com/ethereum/go-ethereum/eth/protocols/eth"
	"github.com/ethereum/go-ethereum/internal/utesting"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/holiman/uint256"
)

// Suite represents a structure used to test a node's conformance
// to the eth protocol.
// Suit代表一个结构用于测试一个node对于eth协议的兼容性
type Suite struct {
	Dest   *enode.Node
	chain  *Chain
	engine *EngineClient
}

// NewSuite creates and returns a new eth-test suite that can
// be used to test the given node against the given blockchain
// data.
// NewSuite创建并且返回一个新的eth-test suit，可以用于测试给定的node，对于给定的blockchain data
func NewSuite(dest *enode.Node, chainDir, engineURL, jwt string) (*Suite, error) {
	chain, err := NewChain(chainDir)
	if err != nil {
		return nil, err
	}
	engine, err := NewEngineClient(chainDir, engineURL, jwt)
	if err != nil {
		return nil, err
	}

	return &Suite{
		Dest:   dest,
		chain:  chain,
		engine: engine,
	}, nil
}

func (s *Suite) EthTests() []utesting.Test {
	return []utesting.Test{
		// status
		{Name: "TestStatus", Fn: s.TestStatus},
		// get block headers
		// 获取block headers
		{Name: "TestGetBlockHeaders", Fn: s.TestGetBlockHeaders},
		{Name: "TestSimultaneousRequests", Fn: s.TestSimultaneousRequests},
		{Name: "TestSameRequestID", Fn: s.TestSameRequestID},
		{Name: "TestZeroRequestID", Fn: s.TestZeroRequestID},
		// get block bodies
		// 获取block bodies
		{Name: "TestGetBlockBodies", Fn: s.TestGetBlockBodies},
		// // malicious handshakes + status
		// 恶意的handshakes和status
		{Name: "TestMaliciousHandshake", Fn: s.TestMaliciousHandshake},
		{Name: "TestMaliciousStatus", Fn: s.TestMaliciousStatus},
		// test transactions
		// 测试txs
		{Name: "TestTransaction", Fn: s.TestTransaction},
		{Name: "TestInvalidTxs", Fn: s.TestInvalidTxs},
		{Name: "TestLargeTxRequest", Fn: s.TestLargeTxRequest},
		{Name: "TestNewPooledTxs", Fn: s.TestNewPooledTxs},
		{Name: "TestBlobViolations", Fn: s.TestBlobViolations},
	}
}

func (s *Suite) SnapTests() []utesting.Test {
	return []utesting.Test{
		{Name: "Status", Fn: s.TestSnapStatus},
		{Name: "AccountRange", Fn: s.TestSnapGetAccountRange},
		{Name: "GetByteCodes", Fn: s.TestSnapGetByteCodes},
		{Name: "GetTrieNodes", Fn: s.TestSnapTrieNodes},
		{Name: "GetStorageRanges", Fn: s.TestSnapGetStorageRanges},
	}
}

// TestStatus attempts to connect to the given node and exchange a status
// message with it on the eth protocol.
// TestStatus试着连接给定的node并且交换一个status message，用eth protocol
func (s *Suite) TestStatus(t *utesting.T) {
	conn, err := s.dial()
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	if err := conn.peer(s.chain, nil); err != nil {
		t.Fatalf("peering failed: %v", err)
	}
}

// headersMatch returns whether the received headers match the given request
// headersMatch返回是否接受到的headers匹配给定的request
func headersMatch(expected []*types.Header, headers []*types.Header) bool {
	return reflect.DeepEqual(expected, headers)
}

// TestGetBlockHeaders tests whether the given node can respond to an eth
// `GetBlockHeaders` request and that the response is accurate.
// TestGetBlockHeaders测试是否给定的node可以回复一个eth的`GetBlockHeaders`请求并且response是正确的
func (s *Suite) TestGetBlockHeaders(t *utesting.T) {
	conn, err := s.dial()
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	if err = conn.peer(s.chain, nil); err != nil {
		t.Fatalf("peering failed: %v", err)
	}
	// Send headers request.
	// 发送headers request
	req := &eth.GetBlockHeadersPacket{
		RequestId: 33,
		GetBlockHeadersRequest: &eth.GetBlockHeadersRequest{
			Origin:  eth.HashOrNumber{Hash: s.chain.blocks[1].Hash()},
			Amount:  2,
			Skip:    1,
			Reverse: false,
		},
	}
	// Read headers response.
	if err := conn.Write(ethProto, eth.GetBlockHeadersMsg, req); err != nil {
		t.Fatalf("could not write to connection: %v", err)
	}
	headers := new(eth.BlockHeadersPacket)
	if err := conn.ReadMsg(ethProto, eth.BlockHeadersMsg, &headers); err != nil {
		t.Fatalf("error reading msg: %v", err)
	}
	if got, want := headers.RequestId, req.RequestId; got != want {
		t.Fatalf("unexpected request id")
	}
	// Check for correct headers.
	// 检查正确的headers
	expected, err := s.chain.GetHeaders(req)
	if err != nil {
		t.Fatalf("failed to get headers for given request: %v", err)
	}
	if !headersMatch(expected, headers.BlockHeadersRequest) {
		t.Fatalf("header mismatch: \nexpected %v \ngot %v", expected, headers)
	}
}

// TestSimultaneousRequests sends two simultaneous `GetBlockHeader` requests
// from the same connection with different request IDs and checks to make sure
// the node responds with the correct headers per request.
// TestSimultaneousRequests发送两个同时的`GetBlockHeader`请求，来自同一个连接，有着不同的request IDs并且检查node用正确的headers
// 回复，对于每个request
func (s *Suite) TestSimultaneousRequests(t *utesting.T) {
	conn, err := s.dial()
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	if err := conn.peer(s.chain, nil); err != nil {
		t.Fatalf("peering failed: %v", err)
	}

	// Create two different requests.
	req1 := &eth.GetBlockHeadersPacket{
		RequestId: uint64(111),
		GetBlockHeadersRequest: &eth.GetBlockHeadersRequest{
			Origin: eth.HashOrNumber{
				Hash: s.chain.blocks[1].Hash(),
			},
			Amount:  2,
			Skip:    1,
			Reverse: false,
		},
	}
	req2 := &eth.GetBlockHeadersPacket{
		RequestId: uint64(222),
		GetBlockHeadersRequest: &eth.GetBlockHeadersRequest{
			Origin: eth.HashOrNumber{
				Hash: s.chain.blocks[1].Hash(),
			},
			Amount:  4,
			Skip:    1,
			Reverse: false,
		},
	}

	// Send both requests.
	if err := conn.Write(ethProto, eth.GetBlockHeadersMsg, req1); err != nil {
		t.Fatalf("failed to write to connection: %v", err)
	}
	if err := conn.Write(ethProto, eth.GetBlockHeadersMsg, req2); err != nil {
		t.Fatalf("failed to write to connection: %v", err)
	}

	// Wait for responses.
	headers1 := new(eth.BlockHeadersPacket)
	if err := conn.ReadMsg(ethProto, eth.BlockHeadersMsg, &headers1); err != nil {
		t.Fatalf("error reading block headers msg: %v", err)
	}
	if got, want := headers1.RequestId, req1.RequestId; got != want {
		t.Fatalf("unexpected request id in response: got %d, want %d", got, want)
	}
	headers2 := new(eth.BlockHeadersPacket)
	if err := conn.ReadMsg(ethProto, eth.BlockHeadersMsg, &headers2); err != nil {
		t.Fatalf("error reading block headers msg: %v", err)
	}
	if got, want := headers2.RequestId, req2.RequestId; got != want {
		t.Fatalf("unexpected request id in response: got %d, want %d", got, want)
	}

	// Check received headers for accuracy.
	if expected, err := s.chain.GetHeaders(req1); err != nil {
		t.Fatalf("failed to get expected headers for request 1: %v", err)
	} else if !headersMatch(expected, headers1.BlockHeadersRequest) {
		t.Fatalf("header mismatch: \nexpected %v \ngot %v", expected, headers1)
	}
	if expected, err := s.chain.GetHeaders(req2); err != nil {
		t.Fatalf("failed to get expected headers for request 2: %v", err)
	} else if !headersMatch(expected, headers2.BlockHeadersRequest) {
		t.Fatalf("header mismatch: \nexpected %v \ngot %v", expected, headers2)
	}
}

// TestSameRequestID sends two requests with the same request ID to a single
// node.
// TestSameRequestID发送两个requests，用同样的request ID，到单个的node
func (s *Suite) TestSameRequestID(t *utesting.T) {
	conn, err := s.dial()
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	if err := conn.peer(s.chain, nil); err != nil {
		t.Fatalf("peering failed: %v", err)
	}

	// Create two different requests with the same ID.
	reqID := uint64(1234)
	request1 := &eth.GetBlockHeadersPacket{
		RequestId: reqID,
		GetBlockHeadersRequest: &eth.GetBlockHeadersRequest{
			Origin: eth.HashOrNumber{
				Number: 1,
			},
			Amount: 2,
		},
	}
	request2 := &eth.GetBlockHeadersPacket{
		RequestId: reqID,
		GetBlockHeadersRequest: &eth.GetBlockHeadersRequest{
			Origin: eth.HashOrNumber{
				Number: 33,
			},
			Amount: 2,
		},
	}

	// Send the requests.
	if err = conn.Write(ethProto, eth.GetBlockHeadersMsg, request1); err != nil {
		t.Fatalf("failed to write to connection: %v", err)
	}
	if err = conn.Write(ethProto, eth.GetBlockHeadersMsg, request2); err != nil {
		t.Fatalf("failed to write to connection: %v", err)
	}

	// Wait for the responses.
	headers1 := new(eth.BlockHeadersPacket)
	if err := conn.ReadMsg(ethProto, eth.BlockHeadersMsg, &headers1); err != nil {
		t.Fatalf("error reading from connection: %v", err)
	}
	if got, want := headers1.RequestId, request1.RequestId; got != want {
		t.Fatalf("unexpected request id: got %d, want %d", got, want)
	}
	headers2 := new(eth.BlockHeadersPacket)
	if err := conn.ReadMsg(ethProto, eth.BlockHeadersMsg, &headers2); err != nil {
		t.Fatalf("error reading from connection: %v", err)
	}
	if got, want := headers2.RequestId, request2.RequestId; got != want {
		t.Fatalf("unexpected request id: got %d, want %d", got, want)
	}

	// Check if headers match.
	// 检查headers是否匹配
	if expected, err := s.chain.GetHeaders(request1); err != nil {
		t.Fatalf("failed to get expected block headers: %v", err)
	} else if !headersMatch(expected, headers1.BlockHeadersRequest) {
		t.Fatalf("header mismatch: \nexpected %v \ngot %v", expected, headers1)
	}
	if expected, err := s.chain.GetHeaders(request2); err != nil {
		t.Fatalf("failed to get expected block headers: %v", err)
	} else if !headersMatch(expected, headers2.BlockHeadersRequest) {
		t.Fatalf("header mismatch: \nexpected %v \ngot %v", expected, headers2)
	}
}

// TestZeroRequestID checks that a message with a request ID of zero is still handled
// by the node.
// TestZeroRequestID检查一个message有着zero的request ID，依然能够被node处理
func (s *Suite) TestZeroRequestID(t *utesting.T) {
	conn, err := s.dial()
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	if err := conn.peer(s.chain, nil); err != nil {
		t.Fatalf("peering failed: %v", err)
	}
	req := &eth.GetBlockHeadersPacket{
		GetBlockHeadersRequest: &eth.GetBlockHeadersRequest{
			Origin: eth.HashOrNumber{Number: 0},
			Amount: 2,
		},
	}
	// Read headers response.
	if err := conn.Write(ethProto, eth.GetBlockHeadersMsg, req); err != nil {
		t.Fatalf("could not write to connection: %v", err)
	}
	headers := new(eth.BlockHeadersPacket)
	if err := conn.ReadMsg(ethProto, eth.BlockHeadersMsg, &headers); err != nil {
		t.Fatalf("error reading msg: %v", err)
	}
	if got, want := headers.RequestId, req.RequestId; got != want {
		t.Fatalf("unexpected request id")
	}
	if expected, err := s.chain.GetHeaders(req); err != nil {
		t.Fatalf("failed to get expected block headers: %v", err)
	} else if !headersMatch(expected, headers.BlockHeadersRequest) {
		t.Fatalf("header mismatch: \nexpected %v \ngot %v", expected, headers)
	}
}

// TestGetBlockBodies tests whether the given node can respond to a
// `GetBlockBodies` request and that the response is accurate.
// TestGetBlockBodies测试是否给定的node可以回复一个`GetBlockBodies` requst并且reponse是正确的
func (s *Suite) TestGetBlockBodies(t *utesting.T) {
	conn, err := s.dial()
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	if err := conn.peer(s.chain, nil); err != nil {
		t.Fatalf("peering failed: %v", err)
	}
	// Create block bodies request.
	req := &eth.GetBlockBodiesPacket{
		RequestId: 55,
		GetBlockBodiesRequest: eth.GetBlockBodiesRequest{
			s.chain.blocks[54].Hash(),
			s.chain.blocks[75].Hash(),
		},
	}
	if err := conn.Write(ethProto, eth.GetBlockBodiesMsg, req); err != nil {
		t.Fatalf("could not write to connection: %v", err)
	}
	// Wait for response.
	// 等待response
	resp := new(eth.BlockBodiesPacket)
	if err := conn.ReadMsg(ethProto, eth.BlockBodiesMsg, &resp); err != nil {
		t.Fatalf("error reading block bodies msg: %v", err)
	}
	if got, want := resp.RequestId, req.RequestId; got != want {
		t.Fatalf("unexpected request id in respond", got, want)
	}
	bodies := resp.BlockBodiesResponse
	if len(bodies) != len(req.GetBlockBodiesRequest) {
		t.Fatalf("wrong bodies in response: expected %d bodies, got %d", len(req.GetBlockBodiesRequest), len(bodies))
	}
}

// randBuf makes a random buffer size kilobytes large.
// randBuf构建一个随机的buffer，有size k那么大
func randBuf(size int) []byte {
	buf := make([]byte, size*1024)
	rand.Read(buf)
	return buf
}

// TestMaliciousHandshake tries to send malicious data during the handshake.
// TestMaliciousHandshake试着在握手期间发送malicious data
func (s *Suite) TestMaliciousHandshake(t *utesting.T) {
	key, _ := crypto.GenerateKey()

	// Write hello to client.
	// 写hello到client
	var (
		pub0    = crypto.FromECDSAPub(&key.PublicKey)[1:]
		version = eth.ProtocolVersions[0]
	)
	handshakes := []*protoHandshake{
		{
			Version: 5,
			Caps: []p2p.Cap{
				{Name: string(randBuf(2)), Version: version},
			},
			ID: pub0,
		},
		{
			Version: 5,
			Caps: []p2p.Cap{
				{Name: "eth", Version: version},
			},
			ID: append(pub0, byte(0)),
		},
		{
			Version: 5,
			Caps: []p2p.Cap{
				{Name: "eth", Version: version},
			},
			ID: append(pub0, pub0...),
		},
		{
			Version: 5,
			Caps: []p2p.Cap{
				{Name: "eth", Version: version},
			},
			ID: randBuf(2),
		},
		{
			Version: 5,
			Caps: []p2p.Cap{
				{Name: string(randBuf(2)), Version: version},
			},
			ID: randBuf(2),
		},
	}
	for _, handshake := range handshakes {
		conn, err := s.dialAs(key)
		if err != nil {
			t.Fatalf("dial failed: %v", err)
		}
		defer conn.Close()

		if err := conn.Write(ethProto, handshakeMsg, handshake); err != nil {
			t.Fatalf("could not write to connection: %v", err)
		}
		// Check that the peer disconnected
		// 检查peer是disconnected
		for i := 0; i < 2; i++ {
			code, _, err := conn.Read()
			if err != nil {
				// Client may have disconnected without sending disconnect msg.
				// Client可能已经断连了，而没有发送disconnect msg
				continue
			}
			switch code {
			case discMsg:
			case handshakeMsg:
				// Discard one hello as Hello's are sent concurrently
				// 丢弃一个hello，因为Hello是并行发送的
				continue
			default:
				t.Fatalf("unexpected msg: code %d", code)
			}
		}
	}
}

// TestMaliciousStatus sends a status package with a large total difficulty.
// TestMaliciousStatus发送一个status pkg，有一个大的td
func (s *Suite) TestMaliciousStatus(t *utesting.T) {
	conn, err := s.dial()
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	if err := conn.handshake(); err != nil {
		t.Fatalf("handshake failed: %v", err)
	}
	// Create status with large total difficulty.
	// 创建status，有着大的td
	status := &eth.StatusPacket{
		ProtocolVersion: uint32(conn.negotiatedProtoVersion),
		NetworkID:       s.chain.config.ChainID.Uint64(),
		TD:              new(big.Int).SetBytes(randBuf(2048)),
		Head:            s.chain.Head().Hash(),
		Genesis:         s.chain.GetBlock(0).Hash(),
		ForkID:          s.chain.ForkID(),
	}
	if err := conn.statusExchange(s.chain, status); err != nil {
		t.Fatalf("status exchange failed: %v", err)
	}
	// Wait for disconnect.
	// 等待disconnect
	code, _, err := conn.Read()
	if err != nil {
		t.Fatalf("error reading from connection: %v", err)
	}
	switch code {
	case discMsg:
		break
	default:
		t.Fatalf("expected disconnect, got: %d", code)
	}
}

// TestTransaction sends a valid transaction to the node and checks if the
// transaction gets propagated.
// TestTransaction发送一个合法的tx到node并且检查是否tx被传播了
func (s *Suite) TestTransaction(t *utesting.T) {
	// Nudge client out of syncing mode to accept pending txs.
	// Nudge client在syncing mode之外接收pending txs
	if err := s.engine.sendForkchoiceUpdated(); err != nil {
		t.Fatalf("failed to send next block: %v", err)
	}
	from, nonce := s.chain.GetSender(0)
	inner := &types.DynamicFeeTx{
		ChainID:   s.chain.config.ChainID,
		Nonce:     nonce,
		GasTipCap: common.Big1,
		GasFeeCap: s.chain.Head().BaseFee(),
		Gas:       30000,
		To:        &common.Address{0xaa},
		Value:     common.Big1,
	}
	tx, err := s.chain.SignTx(from, types.NewTx(inner))
	if err != nil {
		t.Fatalf("failed to sign tx: %v", err)
	}
	// 发送txs
	if err := s.sendTxs([]*types.Transaction{tx}); err != nil {
		t.Fatal(err)
	}
	s.chain.IncNonce(from, 1)
}

// TestInvalidTxs sends several invalid transactions and tests whether
// the node will propagate them.
// TestInvalidTxs发送若干非法的txs并且测试是否node会传播他们
func (s *Suite) TestInvalidTxs(t *utesting.T) {
	// Nudge client out of syncing mode to accept pending txs.
	// 接收pending txs
	if err := s.engine.sendForkchoiceUpdated(); err != nil {
		t.Fatalf("failed to send next block: %v", err)
	}

	from, nonce := s.chain.GetSender(0)
	inner := &types.DynamicFeeTx{
		ChainID:   s.chain.config.ChainID,
		Nonce:     nonce,
		GasTipCap: common.Big1,
		GasFeeCap: s.chain.Head().BaseFee(),
		Gas:       30000,
		To:        &common.Address{0xaa},
	}
	tx, err := s.chain.SignTx(from, types.NewTx(inner))
	if err != nil {
		t.Fatalf("failed to sign tx: %v", err)
	}
	if err := s.sendTxs([]*types.Transaction{tx}); err != nil {
		t.Fatalf("failed to send txs: %v", err)
	}
	s.chain.IncNonce(from, 1)

	inners := []*types.DynamicFeeTx{
		// Nonce already used
		// 已经使用的Nonce
		{
			ChainID:   s.chain.config.ChainID,
			Nonce:     nonce - 1,
			GasTipCap: common.Big1,
			GasFeeCap: s.chain.Head().BaseFee(),
			Gas:       100000,
		},
		// Value exceeds balance
		// Value超过了balance
		{
			Nonce:     nonce,
			GasTipCap: common.Big1,
			GasFeeCap: s.chain.Head().BaseFee(),
			Gas:       100000,
			Value:     s.chain.Balance(from),
		},
		// Gas limit too low
		// Gas limit太低
		{
			Nonce:     nonce,
			GasTipCap: common.Big1,
			GasFeeCap: s.chain.Head().BaseFee(),
			Gas:       1337,
		},
		// Code size too large
		// Code size太大
		{
			Nonce:     nonce,
			GasTipCap: common.Big1,
			GasFeeCap: s.chain.Head().BaseFee(),
			Data:      randBuf(50),
			Gas:       1_000_000,
		},
		// Data too large
		// Data太大
		{
			Nonce:     nonce,
			GasTipCap: common.Big1,
			GasFeeCap: s.chain.Head().BaseFee(),
			To:        &common.Address{0xaa},
			Data:      randBuf(128),
			Gas:       5_000_000,
		},
	}

	var txs []*types.Transaction
	for _, inner := range inners {
		tx, err := s.chain.SignTx(from, types.NewTx(inner))
		if err != nil {
			t.Fatalf("failed to sign tx: %v", err)
		}
		txs = append(txs, tx)
	}
	if err := s.sendInvalidTxs(txs); err != nil {
		t.Fatalf("failed to send invalid txs: %v", err)
	}
}

// TestLargeTxRequest tests whether a node can fulfill a large GetPooledTransactions
// request.
// TestLargeTxRequest测试是否一个node可以填充一个大的GetPooledTransactions请求
func (s *Suite) TestLargeTxRequest(t *utesting.T) {
	// Nudge client out of syncing mode to accept pending txs.
	if err := s.engine.sendForkchoiceUpdated(); err != nil {
		t.Fatalf("failed to send next block: %v", err)
	}

	// Generate many transactions to seed target with.
	// 生成很多的txs来填充target
	var (
		from, nonce = s.chain.GetSender(1)
		count       = 2000
		txs         []*types.Transaction
		hashes      []common.Hash
		set         = make(map[common.Hash]struct{})
	)
	for i := 0; i < count; i++ {
		inner := &types.DynamicFeeTx{
			ChainID:   s.chain.config.ChainID,
			Nonce:     nonce + uint64(i),
			GasTipCap: common.Big1,
			GasFeeCap: s.chain.Head().BaseFee(),
			Gas:       75000,
		}
		tx, err := s.chain.SignTx(from, types.NewTx(inner))
		if err != nil {
			t.Fatalf("failed to sign tx: err")
		}
		txs = append(txs, tx)
		set[tx.Hash()] = struct{}{}
		hashes = append(hashes, tx.Hash())
	}
	s.chain.IncNonce(from, uint64(count))

	// Send txs.
	if err := s.sendTxs(txs); err != nil {
		t.Fatalf("failed to send txs: %v", err)
	}

	// Set up receive connection to ensure node is peered with the receiving
	// connection before tx request is sent.
	// 设置receive connection来确保node已经有了receiving connection，在发送tx之前
	conn, err := s.dial()
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	if err = conn.peer(s.chain, nil); err != nil {
		t.Fatalf("peering failed: %v", err)
	}
	// Create and send pooled tx request.
	// 创建并且发送pooled tx请求
	req := &eth.GetPooledTransactionsPacket{
		RequestId:                    1234,
		GetPooledTransactionsRequest: hashes,
	}
	if err = conn.Write(ethProto, eth.GetPooledTransactionsMsg, req); err != nil {
		t.Fatalf("could not write to conn: %v", err)
	}
	// Check that all received transactions match those that were sent to node.
	// 检查所有接收到的txs匹配那些发送node的
	msg := new(eth.PooledTransactionsPacket)
	if err := conn.ReadMsg(ethProto, eth.PooledTransactionsMsg, &msg); err != nil {
		t.Fatalf("error reading from connection: %v", err)
	}
	if got, want := msg.RequestId, req.RequestId; got != want {
		t.Fatalf("unexpected request id in response: got %d, want %d", got, want)
	}
	for _, got := range msg.PooledTransactionsResponse {
		if _, exists := set[got.Hash()]; !exists {
			t.Fatalf("unexpected tx received: %v", got.Hash())
		}
	}
}

// TestNewPooledTxs tests whether a node will do a GetPooledTransactions request
// upon receiving a NewPooledTransactionHashes announcement.
// TestNewPooledTxs测试是否一个node会做一个GetPooledTransactions请求，当接收到一个NewPooledTransactionHashes
// 的announcement
func (s *Suite) TestNewPooledTxs(t *utesting.T) {
	// Nudge client out of syncing mode to accept pending txs.
	if err := s.engine.sendForkchoiceUpdated(); err != nil {
		t.Fatalf("failed to send next block: %v", err)
	}
	var (
		count       = 50
		from, nonce = s.chain.GetSender(1)
		hashes      = make([]common.Hash, count)
		txTypes     = make([]byte, count)
		sizes       = make([]uint32, count)
	)
	for i := 0; i < count; i++ {
		inner := &types.DynamicFeeTx{
			ChainID:   s.chain.config.ChainID,
			Nonce:     nonce + uint64(i),
			GasTipCap: common.Big1,
			GasFeeCap: s.chain.Head().BaseFee(),
			Gas:       75000,
		}
		tx, err := s.chain.SignTx(from, types.NewTx(inner))
		if err != nil {
			t.Fatalf("failed to sign tx: err")
		}
		hashes[i] = tx.Hash()
		txTypes[i] = tx.Type()
		sizes[i] = uint32(tx.Size())
	}
	s.chain.IncNonce(from, uint64(count))

	// Connect to peer.
	conn, err := s.dial()
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	if err = conn.peer(s.chain, nil); err != nil {
		t.Fatalf("peering failed: %v", err)
	}

	// Send announcement.
	// 发送announcement
	ann := eth.NewPooledTransactionHashesPacket68{Types: txTypes, Sizes: sizes, Hashes: hashes}
	err = conn.Write(ethProto, eth.NewPooledTransactionHashesMsg, ann)
	if err != nil {
		t.Fatalf("failed to write to connection: %v", err)
	}

	// Wait for GetPooledTxs request.
	// 等待GetPooledTxs请求
	for {
		msg, err := conn.ReadEth()
		if err != nil {
			t.Fatalf("failed to read eth msg: %v", err)
		}
		switch msg := msg.(type) {
		case *eth.GetPooledTransactionsPacket:
			if len(msg.GetPooledTransactionsRequest) != len(hashes) {
				t.Fatalf("unexpected number of txs requested: wanted %d, got %d", len(hashes), len(msg.GetPooledTransactionsRequest))
			}
			return
		case *eth.NewPooledTransactionHashesPacket68:
			continue
		case *eth.TransactionsPacket:
			continue
		default:
			t.Fatalf("unexpected %s", pretty.Sdump(msg))
		}
	}
}

func makeSidecar(data ...byte) *types.BlobTxSidecar {
	var (
		blobs       = make([]kzg4844.Blob, len(data))
		commitments []kzg4844.Commitment
		proofs      []kzg4844.Proof
	)
	for i := range blobs {
		blobs[i][0] = data[i]
		c, _ := kzg4844.BlobToCommitment(blobs[i])
		p, _ := kzg4844.ComputeBlobProof(blobs[i], c)
		commitments = append(commitments, c)
		proofs = append(proofs, p)
	}
	return &types.BlobTxSidecar{
		Blobs:       blobs,
		Commitments: commitments,
		Proofs:      proofs,
	}
}

func (s *Suite) makeBlobTxs(count, blobs int, discriminator byte) (txs types.Transactions) {
	from, nonce := s.chain.GetSender(5)
	for i := 0; i < count; i++ {
		// Make blob data, max of 2 blobs per tx.
		// 构建blob data，每个tx最多2个blobs
		blobdata := make([]byte, blobs%2)
		for i := range blobdata {
			blobdata[i] = discriminator
			blobs -= 1
		}
		inner := &types.BlobTx{
			ChainID:    uint256.MustFromBig(s.chain.config.ChainID),
			Nonce:      nonce + uint64(i),
			GasTipCap:  uint256.NewInt(1),
			GasFeeCap:  uint256.MustFromBig(s.chain.Head().BaseFee()),
			Gas:        100000,
			BlobFeeCap: uint256.MustFromBig(eip4844.CalcBlobFee(*s.chain.Head().ExcessBlobGas())),
			BlobHashes: makeSidecar(blobdata...).BlobHashes(),
			Sidecar:    makeSidecar(blobdata...),
		}
		tx, err := s.chain.SignTx(from, types.NewTx(inner))
		if err != nil {
			panic("blob tx signing failed")
		}
		txs = append(txs, tx)
	}
	return txs
}

func (s *Suite) TestBlobViolations(t *utesting.T) {
	if err := s.engine.sendForkchoiceUpdated(); err != nil {
		t.Fatalf("send fcu failed: %v", err)
	}
	// Create blob txs for each tests with unqiue tx hashes.
	// 对于每个测试创建blob txs，有着独特的tx hashes
	var (
		t1 = s.makeBlobTxs(2, 3, 0x1)
		t2 = s.makeBlobTxs(2, 3, 0x2)
	)
	for _, test := range []struct {
		ann  eth.NewPooledTransactionHashesPacket68
		resp eth.PooledTransactionsResponse
	}{
		// Invalid tx size.
		// 非法的tx大小
		{
			ann: eth.NewPooledTransactionHashesPacket68{
				Types:  []byte{types.BlobTxType, types.BlobTxType},
				Sizes:  []uint32{uint32(t1[0].Size()), uint32(t1[1].Size() + 10)},
				Hashes: []common.Hash{t1[0].Hash(), t1[1].Hash()},
			},
			resp: eth.PooledTransactionsResponse(t1),
		},
		// Wrong tx type.
		// 错误的tx类型
		{
			ann: eth.NewPooledTransactionHashesPacket68{
				Types:  []byte{types.DynamicFeeTxType, types.BlobTxType},
				Sizes:  []uint32{uint32(t2[0].Size()), uint32(t2[1].Size())},
				Hashes: []common.Hash{t2[0].Hash(), t2[1].Hash()},
			},
			resp: eth.PooledTransactionsResponse(t2),
		},
	} {
		conn, err := s.dial()
		if err != nil {
			t.Fatalf("dial fail: %v", err)
		}
		if err := conn.peer(s.chain, nil); err != nil {
			t.Fatalf("peering failed: %v", err)
		}
		if err := conn.Write(ethProto, eth.NewPooledTransactionHashesMsg, test.ann); err != nil {
			t.Fatalf("sending announcement failed: %v", err)
		}
		req := new(eth.GetPooledTransactionsPacket)
		if err := conn.ReadMsg(ethProto, eth.GetPooledTransactionsMsg, req); err != nil {
			t.Fatalf("reading pooled tx request failed: %v", err)
		}
		resp := eth.PooledTransactionsPacket{RequestId: req.RequestId, PooledTransactionsResponse: test.resp}
		if err := conn.Write(ethProto, eth.PooledTransactionsMsg, resp); err != nil {
			t.Fatalf("writing pooled tx response failed: %v", err)
		}
		if code, _, err := conn.Read(); err != nil {
			t.Fatalf("expected disconnect on blob violation, got err: %v", err)
		} else if code != discMsg {
			t.Fatalf("expected disconnect on blob violation, got msg code: %d", code)
		}
		conn.Close()
	}
}
