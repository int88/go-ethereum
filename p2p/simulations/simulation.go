// Copyright 2017 The go-ethereum Authors
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

package simulations

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
)

// Simulation provides a framework for running actions in a simulated network
// and then waiting for expectations to be met
// Simulation提供了一个framework用于在simulated network中运行actions并且等待
// 期望的事件发生
type Simulation struct {
	network *Network
}

// NewSimulation returns a new simulation which runs in the given network
// NewSimulation返回一个新的simulation，运行给定的network
func NewSimulation(network *Network) *Simulation {
	return &Simulation{
		network: network,
	}
}

// Run performs a step of the simulation by performing the step's action and
// then waiting for the step's expectation to be met
// Run执行一个step of the simulation，通过执行step的action并且只有等待step的expection被满足
func (s *Simulation) Run(ctx context.Context, step *Step) (result *StepResult) {
	result = newStepResult()

	result.StartedAt = time.Now()
	defer func() { result.FinishedAt = time.Now() }()

	// watch network events for the duration of the step
	// 在step期间等待network events
	stop := s.watchNetwork(result)
	defer stop()

	// perform the action
	// 执行action
	if err := step.Action(ctx); err != nil {
		result.Error = err
		return
	}

	// wait for all node expectations to either pass, error or timeout
	// 等待所有的node expectations，不管是pass, error或者超时
	nodes := make(map[enode.ID]struct{}, len(step.Expect.Nodes))
	for _, id := range step.Expect.Nodes {
		nodes[id] = struct{}{}
	}
	for len(result.Passes) < len(nodes) {
		select {
		case id := <-step.Trigger:
			// skip if we aren't checking the node
			if _, ok := nodes[id]; !ok {
				continue
			}

			// skip if the node has already passed
			if _, ok := result.Passes[id]; ok {
				continue
			}

			// run the node expectation check
			// 运行node expectation check
			pass, err := step.Expect.Check(ctx, id)
			if err != nil {
				result.Error = err
				return
			}
			if pass {
				result.Passes[id] = time.Now()
			}
		case <-ctx.Done():
			result.Error = ctx.Err()
			return
		}
	}

	return
}

func (s *Simulation) watchNetwork(result *StepResult) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	events := make(chan *Event)
	sub := s.network.Events().Subscribe(events)
	go func() {
		defer close(done)
		defer sub.Unsubscribe()
		for {
			select {
			case event := <-events:
				result.NetworkEvents = append(result.NetworkEvents, event)
			case <-stop:
				return
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

type Step struct {
	// Action is the action to perform for this step
	// Action是这个step中执行的行动
	Action func(context.Context) error

	// Trigger is a channel which receives node ids and triggers an
	// expectation check for that node
	// Trigger是一个channel，它接收node ids并且触发这个节点的expection check
	Trigger chan enode.ID

	// Expect is the expectation to wait for when performing this step
	// 当执行这个step时，期望的expectation
	Expect *Expectation
}

type Expectation struct {
	// Nodes is a list of nodes to check
	// 一系列检查的Nodes
	Nodes []enode.ID

	// Check checks whether a given node meets the expectation
	// Check检查给定的node是否满足期望
	Check func(context.Context, enode.ID) (bool, error)
}

func newStepResult() *StepResult {
	return &StepResult{
		Passes: make(map[enode.ID]time.Time),
	}
}

type StepResult struct {
	// Error is the error encountered whilst running the step
	Error error

	// StartedAt is the time the step started
	StartedAt time.Time

	// FinishedAt is the time the step finished
	FinishedAt time.Time

	// Passes are the timestamps of the successful node expectations
	Passes map[enode.ID]time.Time

	// NetworkEvents are the network events which occurred during the step
	NetworkEvents []*Event
}
