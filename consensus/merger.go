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

package consensus

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

// transitionStatus describes the status of eth1/2 transition. This switch
// between modes is a one-way action which is triggered by corresponding
// consensus-layer message.
// transitionStatus描述了eth1/2转换的状态，模式之间的转换是单向的，它由共识层相关的message
// 触发
type transitionStatus struct {
	// 第一次收到NewHead的时候被设置
	LeftPoW bool // The flag is set when the first NewHead message received
	// 第一次收到FinalisedBlock的时候被设置
	EnteredPoS bool // The flag is set when the first FinalisedBlock message received
}

// Merger is an internal help structure used to track the eth1/2 transition status.
// It's a common structure can be used in both full node and light client.
// Merge是一个内部的helper用于追踪eth1/2的过渡状态，它是一个通用结构可用用于full node以及light client
type Merger struct {
	db     ethdb.KeyValueStore
	status transitionStatus
	mu     sync.RWMutex
}

// NewMerger creates a new Merger which stores its transition status in the provided db.
// NewMerger创建一个新的Merger，它在提供的db中存储过渡状态
func NewMerger(db ethdb.KeyValueStore) *Merger {
	var status transitionStatus
	blob := rawdb.ReadTransitionStatus(db)
	if len(blob) != 0 {
		if err := rlp.DecodeBytes(blob, &status); err != nil {
			log.Crit("Failed to decode the transition status", "err", err)
		}
	}
	return &Merger{
		db:     db,
		status: status,
	}
}

// ReachTTD is called whenever the first NewHead message received
// from the consensus-layer.
// ReachTTD被调用，当第一个NewHead message从共识层被接收到
func (m *Merger) ReachTTD() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.status.LeftPoW {
		return
	}
	// 到达TTD的时候，则设置LeftPoW
	m.status = transitionStatus{LeftPoW: true}
	blob, err := rlp.EncodeToBytes(m.status)
	if err != nil {
		panic(fmt.Sprintf("Failed to encode the transition status: %v", err))
	}
	rawdb.WriteTransitionStatus(m.db, blob)
	log.Info("Left PoW stage")
}

// FinalizePoS is called whenever the first FinalisedBlock message received
// from the consensus-layer.
// FinalizePoS被调用，当第一个FinalisedBlock message从共识层被接收到
func (m *Merger) FinalizePoS() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.status.EnteredPoS {
		return
	}
	m.status = transitionStatus{LeftPoW: true, EnteredPoS: true}
	blob, err := rlp.EncodeToBytes(m.status)
	if err != nil {
		panic(fmt.Sprintf("Failed to encode the transition status: %v", err))
	}
	rawdb.WriteTransitionStatus(m.db, blob)
	log.Info("Entered PoS stage")
}

// TDDReached reports whether the chain has left the PoW stage.
// TDDReached会report，当chain已经离开PoW阶段
func (m *Merger) TDDReached() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.status.LeftPoW
}

// PoSFinalized reports whether the chain has entered the PoS stage.
// PoSFinalized汇报，是否chain已经进入了PoS阶段
func (m *Merger) PoSFinalized() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.status.EnteredPoS
}
