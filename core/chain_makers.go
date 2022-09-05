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

package core

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

// BlockGen creates blocks for testing.
// BlockGen创建blocks用于测试
// See GenerateChain for a detailed explanation.
// 查看GenerateChain，关于更详细的描述
type BlockGen struct {
	i       int
	parent  *types.Block
	chain   []*types.Block
	header  *types.Header
	statedb *state.StateDB

	gasPool  *GasPool
	txs      []*types.Transaction
	receipts []*types.Receipt
	uncles   []*types.Header

	config *params.ChainConfig
	engine consensus.Engine
}

// SetCoinbase sets the coinbase of the generated block.
// It can be called at most once.
// SetCoinbase设置生成的block的coinbase，它最多可以被调用一次
func (b *BlockGen) SetCoinbase(addr common.Address) {
	if b.gasPool != nil {
		if len(b.txs) > 0 {
			panic("coinbase must be set before adding transactions")
		}
		panic("coinbase can only be set once")
	}
	b.header.Coinbase = addr
	b.gasPool = new(GasPool).AddGas(b.header.GasLimit)
}

// SetExtra sets the extra data field of the generated block.
// SetExtra设置生成的block的extra data字段
func (b *BlockGen) SetExtra(data []byte) {
	b.header.Extra = data
}

// SetNonce sets the nonce field of the generated block.
func (b *BlockGen) SetNonce(nonce types.BlockNonce) {
	b.header.Nonce = nonce
}

// SetDifficulty sets the difficulty field of the generated block. This method is
// useful for Clique tests where the difficulty does not depend on time. For the
// ethash tests, please use OffsetTime, which implicitly recalculates the diff.
// SetDifficulty设置了生成的block的difficulty字段，这个方法在Clique测试中是有用的
// 因为difficulty不依赖时间，对于ethash测试，请使用OffsetTime，它隐式地重计算diff
func (b *BlockGen) SetDifficulty(diff *big.Int) {
	b.header.Difficulty = diff
}

// AddTx adds a transaction to the generated block. If no coinbase has
// been set, the block's coinbase is set to the zero address.
// AddTx添加一个transaction到生成的block，如果没有设置coinbase，block的coinbase会设置为0
//
// AddTx panics if the transaction cannot be executed. In addition to
// the protocol-imposed limitations (gas limit, etc.), there are some
// further limitations on the content of transactions that can be
// added. Notably, contract code relying on the BLOCKHASH instruction
// will panic during execution.
// AddTx会pancis，如果transaction不能被执行，除了protocol施加的limitations（gas limit等）
// 对于添加的transactions还有额外的限制，值得注意的是，依赖BLOCKHASH指令的contract code在执行
// 的时候会panic
// 直接就会执行transaction
func (b *BlockGen) AddTx(tx *types.Transaction) {
	b.AddTxWithChain(nil, tx)
}

// AddTxWithChain adds a transaction to the generated block. If no coinbase has
// been set, the block's coinbase is set to the zero address.
// AddTxWithChain添加一个transaction到生成的block，如果没有设置coinbase，block的coinbase
// 会被设置为zero
//
// AddTxWithChain panics if the transaction cannot be executed. In addition to
// the protocol-imposed limitations (gas limit, etc.), there are some
// further limitations on the content of transactions that can be
// added. If contract code relies on the BLOCKHASH instruction,
// the block in chain will be returned.
func (b *BlockGen) AddTxWithChain(bc *BlockChain, tx *types.Transaction) {
	if b.gasPool == nil {
		b.SetCoinbase(common.Address{})
	}
	// 准备statedb，第二个参数是tx的索引
	b.statedb.Prepare(tx.Hash(), len(b.txs))
	// 应用transaction
	receipt, err := ApplyTransaction(b.config, bc, &b.header.Coinbase, b.gasPool, b.statedb, b.header, tx, &b.header.GasUsed, vm.Config{})
	if err != nil {
		panic(err)
	}
	// 扩展txs和receipts
	b.txs = append(b.txs, tx)
	b.receipts = append(b.receipts, receipt)
}

// GetBalance returns the balance of the given address at the generated block.
func (b *BlockGen) GetBalance(addr common.Address) *big.Int {
	return b.statedb.GetBalance(addr)
}

// AddUncheckedTx forcefully adds a transaction to the block without any
// validation.
//
// AddUncheckedTx will cause consensus failures when used during real
// chain processing. This is best used in conjunction with raw block insertion.
func (b *BlockGen) AddUncheckedTx(tx *types.Transaction) {
	b.txs = append(b.txs, tx)
}

// Number returns the block number of the block being generated.
func (b *BlockGen) Number() *big.Int {
	return new(big.Int).Set(b.header.Number)
}

// BaseFee returns the EIP-1559 base fee of the block being generated.
func (b *BlockGen) BaseFee() *big.Int {
	return new(big.Int).Set(b.header.BaseFee)
}

// AddUncheckedReceipt forcefully adds a receipts to the block without a
// backing transaction.
//
// AddUncheckedReceipt will cause consensus failures when used during real
// chain processing. This is best used in conjunction with raw block insertion.
func (b *BlockGen) AddUncheckedReceipt(receipt *types.Receipt) {
	b.receipts = append(b.receipts, receipt)
}

// TxNonce returns the next valid transaction nonce for the
// account at addr. It panics if the account does not exist.
func (b *BlockGen) TxNonce(addr common.Address) uint64 {
	if !b.statedb.Exist(addr) {
		panic("account does not exist")
	}
	return b.statedb.GetNonce(addr)
}

// AddUncle adds an uncle header to the generated block.
// AddUncle添加一个uncle header到生成的block中
func (b *BlockGen) AddUncle(h *types.Header) {
	// The uncle will have the same timestamp and auto-generated difficulty
	// uncle有着同样的时间戳和自动生成的difficulty
	h.Time = b.header.Time

	var parent *types.Header
	for i := b.i - 1; i >= 0; i-- {
		if b.chain[i].Hash() == h.ParentHash {
			parent = b.chain[i].Header()
			break
		}
	}
	chainreader := &fakeChainReader{config: b.config}
	h.Difficulty = b.engine.CalcDifficulty(chainreader, b.header.Time, parent)

	// The gas limit and price should be derived from the parent
	// gas limit和price应该继承自parent
	h.GasLimit = parent.GasLimit
	if b.config.IsLondon(h.Number) {
		h.BaseFee = misc.CalcBaseFee(b.config, parent)
		if !b.config.IsLondon(parent.Number) {
			parentGasLimit := parent.GasLimit * params.ElasticityMultiplier
			h.GasLimit = CalcGasLimit(parentGasLimit, parentGasLimit)
		}
	}
	b.uncles = append(b.uncles, h)
}

// PrevBlock returns a previously generated block by number. It panics if
// num is greater or equal to the number of the block being generated.
// For index -1, PrevBlock returns the parent block given to GenerateChain.
// PrevBlock通过number返回一个之前生成的block，它会panic如果number大于等于已经生成的block的number
// 对于index为-1，PrevBlock返回给定GenerateChain的parent block
func (b *BlockGen) PrevBlock(index int) *types.Block {
	if index >= b.i {
		panic(fmt.Errorf("block index %d out of range (%d,%d)", index, -1, b.i))
	}
	if index == -1 {
		return b.parent
	}
	return b.chain[index]
}

// OffsetTime modifies the time instance of a block, implicitly changing its
// associated difficulty. It's useful to test scenarios where forking is not
// tied to chain length directly.
// OffsetTime修改一个block的time instance，隐式地修改了它相关的difficulty
func (b *BlockGen) OffsetTime(seconds int64) {
	b.header.Time += uint64(seconds)
	if b.header.Time <= b.parent.Header().Time {
		panic("block time out of range")
	}
	chainreader := &fakeChainReader{config: b.config}
	// 对difficulty进行重新计算
	b.header.Difficulty = b.engine.CalcDifficulty(chainreader, b.header.Time, b.parent.Header())
}

// GenerateChain creates a chain of n blocks. The first block's
// parent will be the provided parent. db is used to store
// intermediate states and should contain the parent's state trie.
// GenerateChain创建一个由n个blocks组成的chain，第一个block的parent是参数中提供的parent
// db用于存储中间状态并且应该包含parent的state trie
//
// The generator function is called with a new block generator for
// every block. Any transactions and uncles added to the generator
// become part of the block. If gen is nil, the blocks will be empty
// and their coinbase will be the zero address.
// generator函数会为每个block调用，用一个新的block generator，任何添加到
// generator的transactions以及uncles会变为block的一部分，如果gen为nil，则blocks
// 为空并且他们的coinbase会为zero address
//
// Blocks created by GenerateChain do not contain valid proof of work
// values. Inserting them into BlockChain requires use of FakePow or
// a similar non-validating proof of work implementation.
// 由GenerateChain创建的Blocks不包含合法的pow value，将他们插入BlockChain，需要FakePoW
// 或者类似的non-validating proof of work实现
func GenerateChain(config *params.ChainConfig, parent *types.Block, engine consensus.Engine, db ethdb.Database, n int, gen func(int, *BlockGen)) ([]*types.Block, []types.Receipts) {
	if config == nil {
		config = params.TestChainConfig
	}
	blocks, receipts := make(types.Blocks, n), make([]types.Receipts, n)
	chainreader := &fakeChainReader{config: config}
	genblock := func(i int, parent *types.Block, statedb *state.StateDB) (*types.Block, types.Receipts) {
		b := &BlockGen{i: i, chain: blocks, parent: parent, statedb: statedb, config: config, engine: engine}
		// 构建header
		b.header = makeHeader(chainreader, parent, statedb, b.engine)

		// Set the difficulty for clique block. The chain maker doesn't have access
		// to a chain, so the difficulty will be left unset (nil). Set it here to the
		// correct value.
		// 为clique block设置difficulty，chain maker没有对于一个chain的访问权限，这样difficulty
		// 会被遗留为未设置（nil），这里将它设置为正确值
		if b.header.Difficulty == nil {
			if config.TerminalTotalDifficulty == nil {
				// Clique chain
				b.header.Difficulty = big.NewInt(2)
			} else {
				// Post-merge chain
				// merge之后的chain
				b.header.Difficulty = big.NewInt(0)
			}
		}
		// Mutate the state and block according to any hard-fork specs
		// 修改state以及block，根据所有hard-fork specs
		if daoBlock := config.DAOForkBlock; daoBlock != nil {
			limit := new(big.Int).Add(daoBlock, params.DAOForkExtraRange)
			if b.header.Number.Cmp(daoBlock) >= 0 && b.header.Number.Cmp(limit) < 0 {
				if config.DAOForkSupport {
					b.header.Extra = common.CopyBytes(params.DAOForkBlockExtra)
				}
			}
		}
		if config.DAOForkSupport && config.DAOForkBlock != nil && config.DAOForkBlock.Cmp(b.header.Number) == 0 {
			misc.ApplyDAOHardFork(statedb)
		}
		// Execute any user modifications to the block
		// 执行所有用户对于block的修改
		if gen != nil {
			gen(i, b)
		}
		if b.engine != nil {
			// Finalize and seal the block
			// 调用引擎完成并且密封block
			block, _ := b.engine.FinalizeAndAssemble(chainreader, b.header, statedb, b.txs, b.uncles, b.receipts)

			// Write state changes to db
			// 将状态的变更写入db
			root, err := statedb.Commit(config.IsEIP158(b.header.Number))
			if err != nil {
				panic(fmt.Sprintf("state write error: %v", err))
			}
			// 将TrieDB提交
			if err := statedb.Database().TrieDB().Commit(root, false, nil); err != nil {
				panic(fmt.Sprintf("trie write error: %v", err))
			}
			// 真正生成了block
			return block, b.receipts
		}
		return nil, nil
	}
	for i := 0; i < n; i++ {
		log.Info("GenerateChain generate block", "i", i)
		// 对于每个block都构建新的statedb
		statedb, err := state.New(parent.Root(), state.NewDatabase(db), nil)
		if err != nil {
			panic(err)
		}
		// 创建block，返回receipt
		block, receipt := genblock(i, parent, statedb)
		blocks[i] = block
		receipts[i] = receipt
		// 重新设置parent
		parent = block
	}
	return blocks, receipts
}

func makeHeader(chain consensus.ChainReader, parent *types.Block, state *state.StateDB, engine consensus.Engine) *types.Header {
	var time uint64
	if parent.Time() == 0 {
		time = 10
	} else {
		// block的时间固定在10秒
		time = parent.Time() + 10 // block time is fixed at 10 seconds
	}
	header := &types.Header{
		Root:       state.IntermediateRoot(chain.Config().IsEIP158(parent.Number())),
		ParentHash: parent.Hash(),
		Coinbase:   parent.Coinbase(),
		// Difficulty根据genine进行计算
		Difficulty: engine.CalcDifficulty(chain, time, &types.Header{
			Number: parent.Number(),
			Time:   time - 10,
			// 获取parent的difficulty
			Difficulty: parent.Difficulty(),
			UncleHash:  parent.UncleHash(),
		}),
		GasLimit: parent.GasLimit(),
		Number:   new(big.Int).Add(parent.Number(), common.Big1),
		Time:     time,
	}
	if chain.Config().IsLondon(header.Number) {
		header.BaseFee = misc.CalcBaseFee(chain.Config(), parent.Header())
		if !chain.Config().IsLondon(parent.Number()) {
			parentGasLimit := parent.GasLimit() * params.ElasticityMultiplier
			header.GasLimit = CalcGasLimit(parentGasLimit, parentGasLimit)
		}
	}
	return header
}

// makeHeaderChain creates a deterministic chain of headers rooted at parent.
// makeHeaderChain创建一个确定性的chain of headers，将parent作为root
func makeHeaderChain(parent *types.Header, n int, engine consensus.Engine, db ethdb.Database, seed int) []*types.Header {
	blocks := makeBlockChain(types.NewBlockWithHeader(parent), n, engine, db, seed)
	headers := make([]*types.Header, len(blocks))
	// 获取blocks中的header
	for i, block := range blocks {
		headers[i] = block.Header()
	}
	return headers
}

// makeBlockChain creates a deterministic chain of blocks rooted at parent.
// makeBlockChain创建一个确定的chain of blocks，将parent作为root
func makeBlockChain(parent *types.Block, n int, engine consensus.Engine, db ethdb.Database, seed int) []*types.Block {
	blocks, _ := GenerateChain(params.TestChainConfig, parent, engine, db, n, func(i int, b *BlockGen) {
		// 设置coinbase
		b.SetCoinbase(common.Address{0: byte(seed), 19: byte(i)})
	})
	return blocks
}

type fakeChainReader struct {
	config *params.ChainConfig
}

// Config returns the chain configuration.
func (cr *fakeChainReader) Config() *params.ChainConfig {
	return cr.config
}

func (cr *fakeChainReader) CurrentHeader() *types.Header                            { return nil }
func (cr *fakeChainReader) GetHeaderByNumber(number uint64) *types.Header           { return nil }
func (cr *fakeChainReader) GetHeaderByHash(hash common.Hash) *types.Header          { return nil }
func (cr *fakeChainReader) GetHeader(hash common.Hash, number uint64) *types.Header { return nil }
func (cr *fakeChainReader) GetBlock(hash common.Hash, number uint64) *types.Block   { return nil }
func (cr *fakeChainReader) GetTd(hash common.Hash, number uint64) *big.Int          { return nil }
