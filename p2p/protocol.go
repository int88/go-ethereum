// Copyright 2014 The go-ethereum Authors
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

package p2p

import (
	"fmt"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

// Protocol represents a P2P subprotocol implementation.
// Protocol代表一个P2P子协议的实现
type Protocol struct {
	// Name should contain the official protocol name,
	// often a three-letter word.
	Name string

	// Version should contain the version number of the protocol.
	Version uint

	// Length should contain the number of message codes used
	// by the protocol.
	// Length应该包含在protocol使用的message codes的数目
	Length uint64

	// Run is called in a new goroutine when the protocol has been
	// negotiated with a peer. It should read and write messages from
	// rw. The Payload for each message must be fully consumed.
	// Run在一个新的goroutine之中被调用，当protocol已经和一个peer协商好之后
	// 它应该从rw中读写messages，每个message的Payload必须被完全消费
	//
	// The peer connection is closed when Start returns. It should return
	// any protocol-level error (such as an I/O error) that is
	// encountered.
	// 当Start返回的时候，peer connection被关闭，它应该返回所有遇到的协议级别的错误
	// （例如一个I/O错误）
	Run func(peer *Peer, rw MsgReadWriter) error

	// NodeInfo is an optional helper method to retrieve protocol specific metadata
	// about the host node.
	// NodeInfo是一个可选的helper method，用于获取协议特定的元数据，关于host node
	NodeInfo func() interface{}

	// PeerInfo is an optional helper method to retrieve protocol specific metadata
	// about a certain peer in the network. If an info retrieval function is set,
	// but returns nil, it is assumed that the protocol handshake is still running.
	// PeerInfo是一个可选的 helper方法用于获取协议特定的元数据，关于network中一个特定的peer
	// 如果设置了一个info retrieval函数，但是返回nil，这就假设协议握手还在进行
	PeerInfo func(id enode.ID) interface{}

	// DialCandidates, if non-nil, is a way to tell Server about protocol-specific nodes
	// that should be dialed. The server continuously reads nodes from the iterator and
	// attempts to create connections to them.
	// DialCandidates，如果为non-nil，是一个办法告诉Server，关于协议特定的nodes应该被dialed
	// server持续从iterator中读取nodes并且试着和他们建立连接
	DialCandidates enode.Iterator

	// Attributes contains protocol specific information for the node record.
	// Attributes包含了对于node record的协议特定的信息
	Attributes []enr.Entry
}

func (p Protocol) cap() Cap {
	// 协议的名字以及版本
	return Cap{p.Name, p.Version}
}

// Cap is the structure of a peer capability.
// Cap是一个关于peer capability的结构
type Cap struct {
	Name    string
	Version uint
}

func (cap Cap) String() string {
	return fmt.Sprintf("%s/%d", cap.Name, cap.Version)
}

type capsByNameAndVersion []Cap

func (cs capsByNameAndVersion) Len() int      { return len(cs) }
func (cs capsByNameAndVersion) Swap(i, j int) { cs[i], cs[j] = cs[j], cs[i] }
func (cs capsByNameAndVersion) Less(i, j int) bool {
	return cs[i].Name < cs[j].Name || (cs[i].Name == cs[j].Name && cs[i].Version < cs[j].Version)
}
