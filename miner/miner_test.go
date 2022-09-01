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

// Package miner implements Ethereum block creation and mining.
package miner

import (
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/clique"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/eth/downloader"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/trie"
)

type mockBackend struct {
	bc     *core.BlockChain
	txPool *core.TxPool
}

func NewMockBackend(bc *core.BlockChain, txPool *core.TxPool) *mockBackend {
	return &mockBackend{
		bc:     bc,
		txPool: txPool,
	}
}

func (m *mockBackend) BlockChain() *core.BlockChain {
	return m.bc
}

func (m *mockBackend) TxPool() *core.TxPool {
	return m.txPool
}

func (m *mockBackend) StateAtBlock(block *types.Block, reexec uint64, base *state.StateDB, checkLive bool, preferDisk bool) (statedb *state.StateDB, err error) {
	return nil, errors.New("not supported")
}

type testBlockChain struct {
	statedb       *state.StateDB
	gasLimit      uint64
	chainHeadFeed *event.Feed
}

func (bc *testBlockChain) CurrentBlock() *types.Block {
	return types.NewBlock(&types.Header{
		GasLimit: bc.gasLimit,
	}, nil, nil, nil, trie.NewStackTrie(nil))
}

func (bc *testBlockChain) GetBlock(hash common.Hash, number uint64) *types.Block {
	return bc.CurrentBlock()
}

func (bc *testBlockChain) StateAt(common.Hash) (*state.StateDB, error) {
	return bc.statedb, nil
}

func (bc *testBlockChain) SubscribeChainHeadEvent(ch chan<- core.ChainHeadEvent) event.Subscription {
	return bc.chainHeadFeed.Subscribe(ch)
}

func TestMiner(t *testing.T) {
	miner, mux, cleanup := createMiner(t)
	defer cleanup(false)
	miner.Start(common.HexToAddress("0x12345"))
	waitForMiningState(t, miner, true)
	// Start the downloader
	// 启动downloader
	mux.Post(downloader.StartEvent{})
	waitForMiningState(t, miner, false)
	// Stop the downloader and wait for the update loop to run
	// 停止downloader并且等待update loop运行
	mux.Post(downloader.DoneEvent{})
	waitForMiningState(t, miner, true)

	// Subsequent downloader events after a successful DoneEvent should not cause the
	// miner to start or stop. This prevents a security vulnerability
	// that would allow entities to present fake high blocks that would
	// stop mining operations by causing a downloader sync
	// until it was discovered they were invalid, whereon mining would resume.
	// 在一个成功的DoneEvent之后的downloader events不应该导致minter开始或停止
	// 这防止了一个security vulnerability，它会允许entities展现fake high blocks
	// 它会停止mining操作，导致一个downloader sync
	mux.Post(downloader.StartEvent{})
	waitForMiningState(t, miner, true)

	mux.Post(downloader.FailedEvent{})
	waitForMiningState(t, miner, true)
}

// TestMinerDownloaderFirstFails tests that mining is only
// permitted to run indefinitely once the downloader sees a DoneEvent (success).
// An initial FailedEvent should allow mining to stop on a subsequent
// downloader StartEvent.
// TestMinerDownloaderFirstFails测试只有在downlaoder看到一个DoneEvent (success)的时候，miner
// 才允许一直运行
func TestMinerDownloaderFirstFails(t *testing.T) {
	miner, mux, cleanup := createMiner(t)
	defer cleanup(false)
	miner.Start(common.HexToAddress("0x12345"))
	waitForMiningState(t, miner, true)
	// Start the downloader
	mux.Post(downloader.StartEvent{})
	waitForMiningState(t, miner, false)

	// Stop the downloader and wait for the update loop to run
	// 停止downloader并且等待update loop运行
	mux.Post(downloader.FailedEvent{})
	waitForMiningState(t, miner, true)

	// Since the downloader hasn't yet emitted a successful DoneEvent,
	// we expect the miner to stop on next StartEvent.
	// 因为downloader还没有发出一个成功的DoneEvent，我们期望miner在下一个StartEvent
	// 的时候停止
	mux.Post(downloader.StartEvent{})
	waitForMiningState(t, miner, false)

	// Downloader finally succeeds.
	// Downloader最终成功了
	mux.Post(downloader.DoneEvent{})
	waitForMiningState(t, miner, true)

	// Downloader starts again.
	// Since it has achieved a DoneEvent once, we expect miner
	// state to be unchanged.
	// Downloader又启动了，因为它已经实现了一次DoneEvent，我们期望
	// miner的state不再改变
	mux.Post(downloader.StartEvent{})
	waitForMiningState(t, miner, true)

	mux.Post(downloader.FailedEvent{})
	waitForMiningState(t, miner, true)
}

func TestMinerStartStopAfterDownloaderEvents(t *testing.T) {
	miner, mux, cleanup := createMiner(t)
	defer cleanup(false)
	miner.Start(common.HexToAddress("0x12345"))
	waitForMiningState(t, miner, true)
	// Start the downloader
	mux.Post(downloader.StartEvent{})
	waitForMiningState(t, miner, false)

	// Downloader finally succeeds.
	mux.Post(downloader.DoneEvent{})
	waitForMiningState(t, miner, true)

	miner.Stop()
	waitForMiningState(t, miner, false)

	miner.Start(common.HexToAddress("0x678910"))
	waitForMiningState(t, miner, true)

	miner.Stop()
	waitForMiningState(t, miner, false)
}

func TestStartWhileDownload(t *testing.T) {
	miner, mux, cleanup := createMiner(t)
	defer cleanup(false)
	waitForMiningState(t, miner, false)
	miner.Start(common.HexToAddress("0x12345"))
	waitForMiningState(t, miner, true)
	// Stop the downloader and wait for the update loop to run
	mux.Post(downloader.StartEvent{})
	waitForMiningState(t, miner, false)
	// Starting the miner after the downloader should not work
	miner.Start(common.HexToAddress("0x12345"))
	waitForMiningState(t, miner, false)
}

func TestStartStopMiner(t *testing.T) {
	miner, _, cleanup := createMiner(t)
	defer cleanup(false)
	waitForMiningState(t, miner, false)
	miner.Start(common.HexToAddress("0x12345"))
	waitForMiningState(t, miner, true)
	miner.Stop()
	waitForMiningState(t, miner, false)

}

func TestCloseMiner(t *testing.T) {
	miner, _, cleanup := createMiner(t)
	defer cleanup(true)
	waitForMiningState(t, miner, false)
	miner.Start(common.HexToAddress("0x12345"))
	waitForMiningState(t, miner, true)
	// Terminate the miner and wait for the update loop to run
	miner.Close()
	waitForMiningState(t, miner, false)
}

// TestMinerSetEtherbase checks that etherbase becomes set even if mining isn't
// possible at the moment
// TestMinerSetEtherbase检查etherbase能够被设置，即使mining当前是不可能的
func TestMinerSetEtherbase(t *testing.T) {
	miner, mux, cleanup := createMiner(t)
	defer cleanup(false)
	// Start with a 'bad' mining address
	// 以一个'bad'mining地址启动
	miner.Start(common.HexToAddress("0xdead"))
	waitForMiningState(t, miner, true)
	// Start the downloader
	mux.Post(downloader.StartEvent{})
	waitForMiningState(t, miner, false)
	// Now user tries to configure proper mining address
	// 现在用户尝试设置合适的mining address
	miner.Start(common.HexToAddress("0x1337"))
	// Stop the downloader and wait for the update loop to run
	// 停止downloader并且等待update loop运行
	mux.Post(downloader.DoneEvent{})

	waitForMiningState(t, miner, true)
	// The miner should now be using the good address
	// miner应该使用good address
	if got, exp := miner.coinbase, common.HexToAddress("0x1337"); got != exp {
		t.Fatalf("Wrong coinbase, got %x expected %x", got, exp)
	}
}

// waitForMiningState waits until either
// waitForMiningState等待直到：
// * 达到期望的mining state
// * 超时之后则测试失败
// * the desired mining state was reached
// * a timeout was reached which fails the test
func waitForMiningState(t *testing.T, m *Miner, mining bool) {
	t.Helper()

	var state bool
	for i := 0; i < 100; i++ {
		time.Sleep(10 * time.Millisecond)
		if state = m.Mining(); state == mining {
			return
		}
	}
	t.Fatalf("Mining() == %t, want %t", state, mining)
}

func createMiner(t *testing.T) (*Miner, *event.TypeMux, func(skipMiner bool)) {
	// Create Ethash config
	config := Config{
		Etherbase: common.HexToAddress("123456789"),
	}
	// Create chainConfig
	// 创建chainConfig
	memdb := memorydb.New()
	chainDB := rawdb.NewDatabase(memdb)
	// 构建genesis
	genesis := core.DeveloperGenesisBlock(15, 11_500_000, common.HexToAddress("12345"))
	chainConfig, _, err := core.SetupGenesisBlock(chainDB, genesis)
	if err != nil {
		t.Fatalf("can't create new chain config: %v", err)
	}
	// Create consensus engine
	// 创建共识引擎
	engine := clique.New(chainConfig.Clique, chainDB)
	// Create Ethereum backend
	// 创建Ethereum后端
	bc, err := core.NewBlockChain(chainDB, nil, chainConfig, engine, vm.Config{}, nil, nil)
	if err != nil {
		t.Fatalf("can't create new chain %v", err)
	}
	statedb, _ := state.New(common.Hash{}, state.NewDatabase(chainDB), nil)
	blockchain := &testBlockChain{statedb, 10000000, new(event.Feed)}

	// 构建tx pool
	pool := core.NewTxPool(testTxPoolConfig, chainConfig, blockchain)
	backend := NewMockBackend(bc, pool)
	// Create event Mux
	// 创建event Mux
	mux := new(event.TypeMux)
	// Create Miner
	// 创建Miner
	miner := New(backend, &config, chainConfig, mux, engine, nil)
	cleanup := func(skipMiner bool) {
		// 停止chain
		bc.Stop()
		// 关闭engine
		engine.Close()
		// 停止pool
		pool.Stop()
		if !skipMiner {
			// 关闭miner
			miner.Close()
		}
	}
	return miner, mux, cleanup
}
