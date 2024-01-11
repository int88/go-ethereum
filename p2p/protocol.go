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
	"strings"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

// Protocol represents a P2P subprotocol implementation.
// Protocol代表一个P2P子协议的实现
type Protocol struct {
	// Name should contain the official protocol name,
	// often a three-letter word.
	// Name应该包含官方的协议名称，通常是三个字母的单词
	Name string

	// Version should contain the version number of the protocol.
	Version uint

	// Length should contain the number of message codes used
	// by the protocol.
	// Length应该包含协议使用的message codes的number
	Length uint64

	// Run is called in a new goroutine when the protocol has been
	// negotiated with a peer. It should read and write messages from
	// rw. The Payload for each message must be fully consumed.
	// Run在一个新的goroutine中被调用，当protocol已经和一个peer协商完成，他应该从rw中读写messages
	// 对于每个message的Payload应该完全被消费
	//
	// The peer connection is closed when Start returns. It should return
	// any protocol-level error (such as an I/O error) that is
	// encountered.
	// 返回时，peer connection被关闭，他应该返回任何协议层面遇到的错误（例如一个I/O error）
	Run func(peer *Peer, rw MsgReadWriter) error

	// NodeInfo is an optional helper method to retrieve protocol specific metadata
	// about the host node.
	// NodeInfo是一个可选的hepler方法，来获取host node相关的协议特定的元数据
	NodeInfo func() interface{}

	// PeerInfo is an optional helper method to retrieve protocol specific metadata
	// about a certain peer in the network. If an info retrieval function is set,
	// but returns nil, it is assumed that the protocol handshake is still running.
	// PeerInfo是一个可选的helper方法来获取协议特定的元数据，关于network中一个特定的peer，如果设置了一个info retrieval function
	// 但是返回nil，假设协议握手依然在运行
	PeerInfo func(id enode.ID) interface{}

	// DialCandidates, if non-nil, is a way to tell Server about protocol-specific nodes
	// that should be dialed. The server continuously reads nodes from the iterator and
	// attempts to create connections to them.
	// DialCandidates，如果非nil，是一种方法告诉Server，关于protocol特定的nodes，应该被dialed，server
	// 持续从iterator读取nodes的信息并且试着向他们创建连接
	DialCandidates enode.Iterator

	// Attributes contains protocol specific information for the node record.
	// Attributes包含协议特定的信息，对于node record
	Attributes []enr.Entry
}

func (p Protocol) cap() Cap {
	return Cap{p.Name, p.Version}
}

// Cap is the structure of a peer capability.
// Cap是一个peer capability的结构
type Cap struct {
	Name    string
	Version uint
}

func (cap Cap) String() string {
	return fmt.Sprintf("%s/%d", cap.Name, cap.Version)
}

// Cmp defines the canonical sorting order of capabilities.
// Cmp定义了capabilities的官方的排序
func (cap Cap) Cmp(other Cap) int {
	if cap.Name == other.Name {
		if cap.Version < other.Version {
			return -1
		}
		if cap.Version > other.Version {
			return 1
		}
		return 0
	}
	return strings.Compare(cap.Name, other.Name)
}
