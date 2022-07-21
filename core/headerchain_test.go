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

package core

import (
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

func verifyUnbrokenCanonchain(hc *HeaderChain) error {
	h := hc.CurrentHeader()
	for {
		// 从数据库中获取canonical hahs
		canonHash := rawdb.ReadCanonicalHash(hc.chainDb, h.Number.Uint64())
		if exp := h.Hash(); canonHash != exp {
			return fmt.Errorf("Canon hash chain broken, block %d got %x, expected %x",
				h.Number, canonHash[:8], exp[:8])
		}
		// Verify that we have the TD
		// 确认我们有TD
		if td := rawdb.ReadTd(hc.chainDb, canonHash, h.Number.Uint64()); td == nil {
			return fmt.Errorf("Canon TD missing at block %d", h.Number)
		}
		if h.Number.Uint64() == 0 {
			break
		}
		h = hc.GetHeader(h.ParentHash, h.Number.Uint64()-1)
	}
	return nil
}

func testInsert(t *testing.T, hc *HeaderChain, chain []*types.Header, wantStatus WriteStatus, wantErr error, forker *ForkChoice) {
	t.Helper()

	// 插入header chain
	status, err := hc.InsertHeaderChain(chain, time.Now(), forker)
	if status != wantStatus {
		t.Errorf("wrong write status from InsertHeaderChain: got %v, want %v", status, wantStatus)
	}
	// Always verify that the header chain is unbroken
	// 总是确保header chain没有broken
	if err := verifyUnbrokenCanonchain(hc); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("unexpected error from InsertHeaderChain: %v", err)
	}
}

// This test checks status reporting of InsertHeaderChain.
// 这个测试InsertHeaderChain的状态上报
func TestHeaderInsertion(t *testing.T) {
	var (
		db      = rawdb.NewMemoryDatabase()
		genesis = (&Genesis{BaseFee: big.NewInt(params.InitialBaseFee)}).MustCommit(db)
	)

	// 构建一个header chain
	hc, err := NewHeaderChain(db, params.AllEthashProtocolChanges, ethash.NewFaker(), func() bool { return false })
	if err != nil {
		t.Fatal(err)
	}
	// chain A: G->A1->A2...A128
	// 构建chain A
	chainA := makeHeaderChain(genesis.Header(), 128, ethash.NewFaker(), db, 10)
	// chain B: G->A1->B1...B128
	// 在chain A的基础上构建chain B
	chainB := makeHeaderChain(chainA[0], 128, ethash.NewFaker(), db, 10)
	// 设置log handler
	log.Root().SetHandler(log.StdoutHandler)

	forker := NewForkChoice(hc, nil)
	// Inserting 64 headers on an empty chain, expecting
	// 1 callbacks, 1 canon-status, 0 sidestatus,
	// 在一个empty chain上插入64个headers，期望一个callbacks，1个canon-status，0个sidestatus
	testInsert(t, hc, chainA[:64], CanonStatTy, nil, forker)

	// Inserting 64 identical headers, expecting
	// 0 callbacks, 0 canon-status, 0 sidestatus,
	testInsert(t, hc, chainA[:64], NonStatTy, nil, forker)

	// Inserting the same some old, some new headers
	// 1 callbacks, 1 canon, 0 side
	testInsert(t, hc, chainA[32:96], CanonStatTy, nil, forker)

	// Inserting side blocks, but not overtaking the canon chain
	// 插入side blocks，但是没有超过canon chain
	testInsert(t, hc, chainB[0:32], SideStatTy, nil, forker)

	// Inserting more side blocks, but we don't have the parent
	// 插入更多的side blocks，但是我们没有parent
	testInsert(t, hc, chainB[34:36], NonStatTy, consensus.ErrUnknownAncestor, forker)

	// Inserting more sideblocks, overtaking the canon chain
	// 插入更多sideblocks，获取canon chain
	testInsert(t, hc, chainB[32:97], CanonStatTy, nil, forker)

	// Inserting more A-headers, taking back the canonicality
	// 插入更多的A headers，拿回canonicality
	testInsert(t, hc, chainA[90:100], CanonStatTy, nil, forker)

	// And B becomes canon again
	// B再次变成canon
	testInsert(t, hc, chainB[97:107], CanonStatTy, nil, forker)

	// And B becomes even longer
	// B变得更长
	testInsert(t, hc, chainB[107:128], CanonStatTy, nil, forker)
}
