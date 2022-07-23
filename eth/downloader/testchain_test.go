// Copyright 2018 The go-ethereum Authors
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
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
)

// Test chain parameters.
var (
	testKey, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	testAddress = crypto.PubkeyToAddress(testKey.PublicKey)
	testDB      = rawdb.NewMemoryDatabase()
	testGenesis = core.GenesisBlockForTesting(testDB, testAddress, big.NewInt(1000000000000000))
)

// The common prefix of all test chains:
// 所有测试链的公共前缀
var testChainBase *testChain

// Different forks on top of the base chain:
var testChainForkLightA, testChainForkLightB, testChainForkHeavy *testChain

var pregenerated bool

func init() {
	// Reduce some of the parameters to make the tester faster
	// 缩小一些参数让tester更快
	fullMaxForkAncestry = 10000
	lightMaxForkAncestry = 10000
	blockCacheMaxItems = 1024
	fsHeaderSafetyNet = 256
	fsHeaderContCheck = 500 * time.Millisecond

	// 构建test chain
	testChainBase = newTestChain(blockCacheMaxItems+200, testGenesis)

	var forkLen = int(fullMaxForkAncestry + 50)
	var wg sync.WaitGroup

	// Generate the test chains to seed the peers with
	wg.Add(3)
	go func() { testChainForkLightA = testChainBase.makeFork(forkLen, false, 1); wg.Done() }()
	go func() { testChainForkLightB = testChainBase.makeFork(forkLen, false, 2); wg.Done() }()
	go func() { testChainForkHeavy = testChainBase.makeFork(forkLen, true, 3); wg.Done() }()
	wg.Wait()

	// Generate the test peers used by the tests to avoid overloading during testing.
	// These seemingly random chains are used in various downloader tests. We're just
	// pre-generating them here.
	// 生成供tests使用的test peers，来避免测试期间的重载，这些看起来随机的chains，是用于各种downlaoder测试
	// 我们在这里提前生成它们
	chains := []*testChain{
		testChainBase,
		testChainForkLightA,
		testChainForkLightB,
		testChainForkHeavy,
		testChainBase.shorten(1),
		testChainBase.shorten(blockCacheMaxItems - 15),
		testChainBase.shorten((blockCacheMaxItems - 15) / 2),
		testChainBase.shorten(blockCacheMaxItems - 15 - 5),
		testChainBase.shorten(MaxHeaderFetch),
		testChainBase.shorten(800),
		// 缩小并且拷贝chain
		testChainBase.shorten(800 / 2),
		testChainBase.shorten(800 / 3),
		testChainBase.shorten(800 / 4),
		testChainBase.shorten(800 / 5),
		testChainBase.shorten(800 / 6),
		testChainBase.shorten(800 / 7),
		testChainBase.shorten(800 / 8),
		testChainBase.shorten(3*fsHeaderSafetyNet + 256 + fsMinFullBlocks),
		testChainBase.shorten(fsMinFullBlocks + 256 - 1),
		testChainForkLightA.shorten(len(testChainBase.blocks) + 80),
		testChainForkLightB.shorten(len(testChainBase.blocks) + 81),
		testChainForkLightA.shorten(len(testChainBase.blocks) + MaxHeaderFetch),
		testChainForkLightB.shorten(len(testChainBase.blocks) + MaxHeaderFetch),
		testChainForkHeavy.shorten(len(testChainBase.blocks) + 79),
	}
	wg.Add(len(chains))
	for _, chain := range chains {
		go func(blocks []*types.Block) {
			newTestBlockchain(blocks)
			wg.Done()
		}(chain.blocks[1:])
	}
	wg.Wait()

	// Mark the chains pregenerated. Generating a new one will lead to a panic.
	pregenerated = true
}

type testChain struct {
	blocks []*types.Block
}

// newTestChain creates a blockchain of the given length.
// newTestChain创建一个给定长度的blockchain
func newTestChain(length int, genesis *types.Block) *testChain {
	tc := &testChain{
		blocks: []*types.Block{genesis},
	}
	tc.generate(length-1, 0, genesis, false)
	return tc
}

// makeFork creates a fork on top of the test chain.
// makeFork在test chain之上创建一个fork
func (tc *testChain) makeFork(length int, heavy bool, seed byte) *testChain {
	fork := tc.copy(len(tc.blocks) + length)
	fork.generate(length, seed, tc.blocks[len(tc.blocks)-1], heavy)
	return fork
}

// shorten creates a copy of the chain with the given length. It panics if the
// length is longer than the number of available blocks.
// shorten创建一个给定长度的chain的拷贝，它会panic，如果length大于可用的blocks
func (tc *testChain) shorten(length int) *testChain {
	if length > len(tc.blocks) {
		panic(fmt.Errorf("can't shorten test chain to %d blocks, it's only %d blocks long", length, len(tc.blocks)))
	}
	return tc.copy(length)
}

func (tc *testChain) copy(newlen int) *testChain {
	if newlen > len(tc.blocks) {
		newlen = len(tc.blocks)
	}
	// 拷贝newlen个blocks
	cpy := &testChain{
		blocks: append([]*types.Block{}, tc.blocks[:newlen]...),
	}
	return cpy
}

// generate creates a chain of n blocks starting at and including parent.
// the returned hash chain is ordered head->parent. In addition, every 22th block
// contains a transaction and every 5th an uncle to allow testing correct block
// reassembly.
// generate创建一个由n个blocks组成的chain，从parent开始并且包含parent
// 每22个block包含一个transaction，每5个一个uncle，允许测试正确的block组装
func (tc *testChain) generate(n int, seed byte, parent *types.Block, heavy bool) {
	blocks, _ := core.GenerateChain(params.TestChainConfig, parent, ethash.NewFaker(), testDB, n, func(i int, block *core.BlockGen) {
		block.SetCoinbase(common.Address{seed})
		// If a heavy chain is requested, delay blocks to raise difficulty
		// 如果请求了一个heavy chain，延迟blocks来提高difficulty
		if heavy {
			block.OffsetTime(-9)
		}
		// Include transactions to the miner to make blocks more interesting.
		// 包含了transactions到miner中，让blocks更有趣
		if parent == tc.blocks[0] && i%22 == 0 {
			signer := types.MakeSigner(params.TestChainConfig, block.Number())
			tx, err := types.SignTx(types.NewTransaction(block.TxNonce(testAddress), common.Address{seed}, big.NewInt(1000), params.TxGas, block.BaseFee(), nil), signer, testKey)
			if err != nil {
				panic(err)
			}
			// 添加transactions
			block.AddTx(tx)
		}
		// if the block number is a multiple of 5, add a bonus uncle to the block
		// 如果block number是5的倍数，添加一个bonus uncle到block
		if i > 0 && i%5 == 0 {
			block.AddUncle(&types.Header{
				ParentHash: block.PrevBlock(i - 2).Hash(),
				Number:     big.NewInt(block.Number().Int64() - 1),
			})
		}
	})
	tc.blocks = append(tc.blocks, blocks...)
}

var (
	testBlockchains     = make(map[common.Hash]*testBlockchain)
	testBlockchainsLock sync.Mutex
)

type testBlockchain struct {
	chain *core.BlockChain
	gen   sync.Once
}

// newTestBlockchain creates a blockchain database built by running the given blocks,
// either actually running them, or reusing a previously created one. The returned
// chains are *shared*, so *do not* mutate them.
// newTestBlockchain创建一个blockchain database，通过运行给定的blocks创建，要么实际运行它们
// 要么重用一个之前创建的，返回的chain是共享的，不要修改它们
func newTestBlockchain(blocks []*types.Block) *core.BlockChain {
	// Retrieve an existing database, or create a new one
	// 获取一个已经存在的database，或者创建一个新的
	head := testGenesis.Hash()
	if len(blocks) > 0 {
		head = blocks[len(blocks)-1].Hash()
	}
	testBlockchainsLock.Lock()
	if _, ok := testBlockchains[head]; !ok {
		testBlockchains[head] = new(testBlockchain)
	}
	tbc := testBlockchains[head]
	testBlockchainsLock.Unlock()

	// Ensure that the database is generated
	// 确保database被创建
	tbc.gen.Do(func() {
		if pregenerated {
			panic("Requested chain generation outside of init")
		}
		db := rawdb.NewMemoryDatabase()
		core.GenesisBlockForTesting(db, testAddress, big.NewInt(1000000000000000))

		chain, err := core.NewBlockChain(db, nil, params.TestChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil)
		if err != nil {
			panic(err)
		}
		// 在链中加入blocks
		if n, err := chain.InsertChain(blocks); err != nil {
			panic(fmt.Sprintf("block %d: %v", n, err))
		}
		tbc.chain = chain
	})
	return tbc.chain
}
