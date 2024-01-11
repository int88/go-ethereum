// Copyright 2019 The go-ethereum Authors
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

package discover

import (
	"crypto/ecdsa"
	"net"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/p2p/netutil"
)

// UDPConn is a network connection on which discovery can operate.
type UDPConn interface {
	ReadFromUDP(b []byte) (n int, addr *net.UDPAddr, err error)
	WriteToUDP(b []byte, addr *net.UDPAddr) (n int, err error)
	Close() error
	LocalAddr() net.Addr
}

// Config holds settings for the discovery listener.
// Config维护discovery listener的设置
type Config struct {
	// These settings are required and configure the UDP listener:
	// 这些设置是配置UDP listener需要的
	PrivateKey *ecdsa.PrivateKey

	// All remaining settings are optional.
	// 所有剩余的配置都是可选的

	// Packet handling configuration:
	// Packet处理的配置
	NetRestrict *netutil.Netlist  // list of allowed IP networks，允许的IP networks的列表
	Unhandled   chan<- ReadPacket // unhandled packets are sent on this channel，unhandled packets被送到这个channel

	// Node table configuration:
	// Node table的配置
	Bootnodes       []*enode.Node // list of bootstrap nodes，一系列的bootstrap nodes
	PingInterval    time.Duration // speed of node liveness check，node liveness check的速度
	RefreshInterval time.Duration // used in bucket refresh，在bucket refresh的时候使用

	// The options below are useful in very specific cases, like in unit tests.
	// 下面的options在特定的场景很有用，例如unit tests
	V5ProtocolID *[6]byte
	Log          log.Logger         // if set, log messages go here
	ValidSchemes enr.IdentityScheme // allowed identity schemes
	Clock        mclock.Clock
}

func (cfg Config) withDefaults() Config {
	// Node table configuration:
	if cfg.PingInterval == 0 {
		cfg.PingInterval = 10 * time.Second
	}
	if cfg.RefreshInterval == 0 {
		cfg.RefreshInterval = 30 * time.Minute
	}

	// Debug/test settings:
	if cfg.Log == nil {
		cfg.Log = log.Root()
	}
	if cfg.ValidSchemes == nil {
		cfg.ValidSchemes = enode.ValidSchemes
	}
	if cfg.Clock == nil {
		cfg.Clock = mclock.System{}
	}
	return cfg
}

// ListenUDP starts listening for discovery packets on the given UDP socket.
func ListenUDP(c UDPConn, ln *enode.LocalNode, cfg Config) (*UDPv4, error) {
	return ListenV4(c, ln, cfg)
}

// ReadPacket is a packet that couldn't be handled. Those packets are sent to the unhandled
// channel if configured.
// ReadPacket是一个不能被处理的packet，这些packet会被发送到unhandled channel，如果配置的话
type ReadPacket struct {
	Data []byte
	Addr *net.UDPAddr
}

func min(x, y int) int {
	if x > y {
		return y
	}
	return x
}
