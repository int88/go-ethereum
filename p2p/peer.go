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
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/exp/slices"
)

var (
	ErrShuttingDown = errors.New("shutting down")
)

const (
	baseProtocolVersion    = 5
	baseProtocolLength     = uint64(16)
	baseProtocolMaxMsgSize = 2 * 1024

	snappyProtocolVersion = 5

	pingInterval = 15 * time.Second
)

const (
	// devp2p message codes
	handshakeMsg = 0x00
	discMsg      = 0x01
	pingMsg      = 0x02
	pongMsg      = 0x03
)

// protoHandshake is the RLP structure of the protocol handshake.
// protoHandshake是协议握手的RLP结构
type protoHandshake struct {
	Version    uint64
	Name       string
	Caps       []Cap
	ListenPort uint64
	ID         []byte // secp256k1 public key

	// Ignore additional fields (for forward compatibility).
	Rest []rlp.RawValue `rlp:"tail"`
}

// PeerEventType is the type of peer events emitted by a p2p.Server
// PeerEventType是一个p2p.Server发射的peer events的类型
type PeerEventType string

const (
	// PeerEventTypeAdd is the type of event emitted when a peer is added
	// to a p2p.Server
	// PeerEventTypeAdd当一个peer被加入到p2p.Server时发射的事件类型
	PeerEventTypeAdd PeerEventType = "add"

	// PeerEventTypeDrop is the type of event emitted when a peer is
	// dropped from a p2p.Server
	// PeerEventTypeDrop是被发射的event类型，当一个Peer被drop，当来自一个p2p.Server
	PeerEventTypeDrop PeerEventType = "drop"

	// PeerEventTypeMsgSend is the type of event emitted when a
	// message is successfully sent to a peer
	// 当一个message被成功发送到一个peer时发射的事件
	PeerEventTypeMsgSend PeerEventType = "msgsend"

	// PeerEventTypeMsgRecv is the type of event emitted when a
	// message is received from a peer
	// 当从peer成功接收到一个message的时候发生
	PeerEventTypeMsgRecv PeerEventType = "msgrecv"
)

// PeerEvent is an event emitted when peers are either added or dropped from
// a p2p.Server or when a message is sent or received on a peer connection
// PeerEvent是peers加入或者从p2p.Server丢弃之后发射的事件，或者从一个peer connection发送或者接收一个message
type PeerEvent struct {
	Type          PeerEventType `json:"type"`
	Peer          enode.ID      `json:"peer"`
	Error         string        `json:"error,omitempty"`
	Protocol      string        `json:"protocol,omitempty"`
	MsgCode       *uint64       `json:"msg_code,omitempty"`
	MsgSize       *uint32       `json:"msg_size,omitempty"`
	LocalAddress  string        `json:"local,omitempty"`
	RemoteAddress string        `json:"remote,omitempty"`
}

// Peer represents a connected remote node.
// Peer代表一个连接的remote node
type Peer struct {
	rw      *conn
	running map[string]*protoRW
	log     log.Logger
	created mclock.AbsTime

	wg       sync.WaitGroup
	protoErr chan error
	closed   chan struct{}
	pingRecv chan struct{}
	disc     chan DiscReason

	// events receives message send / receive events if set
	// events接收message 发送/接收 事件，如果设置的话
	events   *event.Feed
	testPipe *MsgPipeRW // for testing
}

// NewPeer returns a peer for testing purposes.
// NewPeer返回一个peer用于测试
func NewPeer(id enode.ID, name string, caps []Cap) *Peer {
	// Generate a fake set of local protocols to match as running caps. Almost
	// no fields needs to be meaningful here as we're only using it to cross-
	// check with the "remote" caps array.
	// 生成一个fake的local protocols用于匹配running caps，几乎没有字段需要有意义，因为我们只使用它
	// 来crosscheck，和"remote" cap array
	protos := make([]Protocol, len(caps))
	for i, cap := range caps {
		protos[i].Name = cap.Name
		protos[i].Version = cap.Version
	}
	pipe, _ := net.Pipe()
	node := enode.SignNull(new(enr.Record), id)
	conn := &conn{fd: pipe, transport: nil, node: node, caps: caps, name: name}
	peer := newPeer(log.Root(), conn, protos)
	// 确保Disconnect不会block
	close(peer.closed) // ensures Disconnect doesn't block
	return peer
}

// NewPeerPipe creates a peer for testing purposes.
// The message pipe given as the last parameter is closed when
// Disconnect is called on the peer.
func NewPeerPipe(id enode.ID, name string, caps []Cap, pipe *MsgPipeRW) *Peer {
	p := NewPeer(id, name, caps)
	p.testPipe = pipe
	return p
}

// ID returns the node's public key.
func (p *Peer) ID() enode.ID {
	return p.rw.node.ID()
}

// Node returns the peer's node descriptor.
func (p *Peer) Node() *enode.Node {
	return p.rw.node
}

// Name returns an abbreviated form of the name
func (p *Peer) Name() string {
	s := p.rw.name
	if len(s) > 20 {
		return s[:20] + "..."
	}
	return s
}

// Fullname returns the node name that the remote node advertised.
func (p *Peer) Fullname() string {
	return p.rw.name
}

// Caps returns the capabilities (supported subprotocols) of the remote peer.
func (p *Peer) Caps() []Cap {
	// TODO: maybe return copy
	return p.rw.caps
}

// RunningCap returns true if the peer is actively connected using any of the
// enumerated versions of a specific protocol, meaning that at least one of the
// versions is supported by both this node and the peer p.
func (p *Peer) RunningCap(protocol string, versions []uint) bool {
	if proto, ok := p.running[protocol]; ok {
		for _, ver := range versions {
			if proto.Version == ver {
				return true
			}
		}
	}
	return false
}

// RemoteAddr returns the remote address of the network connection.
func (p *Peer) RemoteAddr() net.Addr {
	return p.rw.fd.RemoteAddr()
}

// LocalAddr returns the local address of the network connection.
func (p *Peer) LocalAddr() net.Addr {
	return p.rw.fd.LocalAddr()
}

// Disconnect terminates the peer connection with the given reason.
// Disconnect终止peer connection，用给定的原因
// It returns immediately and does not wait until the connection is closed.
// 它立刻返回并且不等待，直到connection关闭
func (p *Peer) Disconnect(reason DiscReason) {
	if p.testPipe != nil {
		p.testPipe.Close()
	}

	select {
	case p.disc <- reason:
	case <-p.closed:
	}
}

// String implements fmt.Stringer.
func (p *Peer) String() string {
	id := p.ID()
	return fmt.Sprintf("Peer %x %v", id[:8], p.RemoteAddr())
}

// Inbound returns true if the peer is an inbound connection
func (p *Peer) Inbound() bool {
	return p.rw.is(inboundConn)
}

func newPeer(log log.Logger, conn *conn, protocols []Protocol) *Peer {
	protomap := matchProtocols(protocols, conn.caps, conn)
	// 构建peer
	p := &Peer{
		rw:       conn,
		running:  protomap,
		created:  mclock.Now(),
		disc:     make(chan DiscReason),
		protoErr: make(chan error, len(protomap)+1), // protocols + pingLoop
		closed:   make(chan struct{}),
		pingRecv: make(chan struct{}, 16),
		log:      log.New("id", conn.node.ID(), "conn", conn.flags),
	}
	return p
}

func (p *Peer) Log() log.Logger {
	return p.log
}

func (p *Peer) run() (remoteRequested bool, err error) {
	var (
		writeStart = make(chan struct{}, 1)
		writeErr   = make(chan error, 1)
		readErr    = make(chan error, 1)
		reason     DiscReason // sent to the peer
	)
	p.wg.Add(2)
	// read的loop
	go p.readLoop(readErr)
	// ping的loop
	go p.pingLoop()

	// Start all protocol handlers.
	// 启动所有的protocol handlers
	writeStart <- struct{}{}
	p.startProtocols(writeStart, writeErr)

	// Wait for an error or disconnect.
	// 等待一个error或者disconnect
loop:
	for {
		select {
		case err = <-writeErr:
			// A write finished. Allow the next write to start if
			// there was no error.
			// 一个write结束，允许下一个write开始，如果没有error
			if err != nil {
				reason = DiscNetworkError
				break loop
			}
			writeStart <- struct{}{}
		case err = <-readErr:
			if r, ok := err.(DiscReason); ok {
				remoteRequested = true
				reason = r
			} else {
				reason = DiscNetworkError
			}
			break loop
		case err = <-p.protoErr:
			reason = discReasonForError(err)
			break loop
		case err = <-p.disc:
			reason = discReasonForError(err)
			break loop
		}
	}

	close(p.closed)
	p.rw.close(reason)
	p.wg.Wait()
	return remoteRequested, err
}

func (p *Peer) pingLoop() {
	defer p.wg.Done()

	// 设置ping的timer
	ping := time.NewTimer(pingInterval)
	defer ping.Stop()

	for {
		select {
		case <-ping.C:
			// 发送ping
			if err := SendItems(p.rw, pingMsg); err != nil {
				p.protoErr <- err
				return
			}
			ping.Reset(pingInterval)

		case <-p.pingRecv:
			// 发送pong
			SendItems(p.rw, pongMsg)

		case <-p.closed:
			return
		}
	}
}

func (p *Peer) readLoop(errc chan<- error) {
	defer p.wg.Done()
	for {
		// 读取msg
		msg, err := p.rw.ReadMsg()
		if err != nil {
			errc <- err
			return
		}
		msg.ReceivedAt = time.Now()
		// 处理msg
		if err = p.handle(msg); err != nil {
			errc <- err
			return
		}
	}
}

func (p *Peer) handle(msg Msg) error {
	switch {
	case msg.Code == pingMsg:
		msg.Discard()
		select {
		case p.pingRecv <- struct{}{}:
		case <-p.closed:
		}
	case msg.Code == discMsg:
		// This is the last message. We don't need to discard or
		// check errors because, the connection will be closed after it.
		// 这是最后的message，我们不需要丢弃或者检查错误，因为connection会在之后关闭
		var m struct{ R DiscReason }
		rlp.Decode(msg.Payload, &m)
		return m.R
	case msg.Code < baseProtocolLength:
		// ignore other base protocol messages
		// 忽略其他基础的Protocol messages
		return msg.Discard()
	default:
		// it's a subprotocol message
		// 这是一个自协议的message
		proto, err := p.getProto(msg.Code)
		if err != nil {
			return fmt.Errorf("msg code out of range: %v", msg.Code)
		}
		if metrics.Enabled {
			m := fmt.Sprintf("%s/%s/%d/%#02x", ingressMeterName, proto.Name, proto.Version, msg.Code-proto.offset)
			metrics.GetOrRegisterMeter(m, nil).Mark(int64(msg.meterSize))
			metrics.GetOrRegisterMeter(m+"/packets", nil).Mark(1)
		}
		select {
		case proto.in <- msg:
			return nil
		case <-p.closed:
			return io.EOF
		}
	}
	return nil
}

func countMatchingProtocols(protocols []Protocol, caps []Cap) int {
	n := 0
	for _, cap := range caps {
		for _, proto := range protocols {
			if proto.Name == cap.Name && proto.Version == cap.Version {
				n++
			}
		}
	}
	return n
}

// matchProtocols creates structures for matching named subprotocols.
// matchProtocols创建结构体，用于匹配的named subprotocols
func matchProtocols(protocols []Protocol, caps []Cap, rw MsgReadWriter) map[string]*protoRW {
	slices.SortFunc(caps, Cap.Cmp)
	offset := baseProtocolLength
	result := make(map[string]*protoRW)

outer:
	for _, cap := range caps {
		for _, proto := range protocols {
			if proto.Name == cap.Name && proto.Version == cap.Version {
				// If an old protocol version matched, revert it
				if old := result[cap.Name]; old != nil {
					offset -= old.Length
				}
				// Assign the new match
				result[cap.Name] = &protoRW{Protocol: proto, offset: offset, in: make(chan Msg), w: rw}
				offset += proto.Length

				continue outer
			}
		}
	}
	return result
}

func (p *Peer) startProtocols(writeStart <-chan struct{}, writeErr chan<- error) {
	p.wg.Add(len(p.running))
	for _, proto := range p.running {
		proto := proto
		proto.closed = p.closed
		proto.wstart = writeStart
		proto.werr = writeErr
		var rw MsgReadWriter = proto
		if p.events != nil {
			rw = newMsgEventer(rw, p.events, p.ID(), proto.Name, p.Info().Network.RemoteAddress, p.Info().Network.LocalAddress)
		}
		p.log.Trace(fmt.Sprintf("Starting protocol %s/%d", proto.Name, proto.Version))
		go func() {
			// 运行proto
			defer p.wg.Done()
			err := proto.Run(p, rw)
			if err == nil {
				p.log.Trace(fmt.Sprintf("Protocol %s/%d returned", proto.Name, proto.Version))
				err = errProtocolReturned
			} else if !errors.Is(err, io.EOF) {
				p.log.Trace(fmt.Sprintf("Protocol %s/%d failed", proto.Name, proto.Version), "err", err)
			}
			p.protoErr <- err
		}()
	}
}

// getProto finds the protocol responsible for handling
// the given message code.
// getProto找到协议，负责处理给定message
func (p *Peer) getProto(code uint64) (*protoRW, error) {
	for _, proto := range p.running {
		if code >= proto.offset && code < proto.offset+proto.Length {
			return proto, nil
		}
	}
	return nil, newPeerError(errInvalidMsgCode, "%d", code)
}

type protoRW struct {
	Protocol
	// 接受read messages
	in     chan Msg        // receives read messages
	closed <-chan struct{} // receives when peer is shutting down
	wstart <-chan struct{} // receives when write may start
	werr   chan<- error    // for write results
	offset uint64
	w      MsgWriter
}

func (rw *protoRW) WriteMsg(msg Msg) (err error) {
	if msg.Code >= rw.Length {
		return newPeerError(errInvalidMsgCode, "not handled")
	}
	msg.meterCap = rw.cap()
	msg.meterCode = msg.Code

	msg.Code += rw.offset

	select {
	case <-rw.wstart:
		err = rw.w.WriteMsg(msg)
		// Report write status back to Peer.run. It will initiate
		// shutdown if the error is non-nil and unblock the next write
		// otherwise. The calling protocol code should exit for errors
		// as well but we don't want to rely on that.
		rw.werr <- err
	case <-rw.closed:
		err = ErrShuttingDown
	}
	return err
}

func (rw *protoRW) ReadMsg() (Msg, error) {
	select {
	case msg := <-rw.in:
		msg.Code -= rw.offset
		return msg, nil
	case <-rw.closed:
		return Msg{}, io.EOF
	}
}

// PeerInfo represents a short summary of the information known about a connected
// peer. Sub-protocol independent fields are contained and initialized here, with
// protocol specifics delegated to all connected sub-protocols.
type PeerInfo struct {
	ENR     string   `json:"enr,omitempty"` // Ethereum Node Record
	Enode   string   `json:"enode"`         // Node URL
	ID      string   `json:"id"`            // Unique node identifier
	Name    string   `json:"name"`          // Name of the node, including client type, version, OS, custom data
	Caps    []string `json:"caps"`          // Protocols advertised by this peer
	Network struct {
		LocalAddress  string `json:"localAddress"`  // Local endpoint of the TCP data connection
		RemoteAddress string `json:"remoteAddress"` // Remote endpoint of the TCP data connection
		Inbound       bool   `json:"inbound"`
		Trusted       bool   `json:"trusted"`
		Static        bool   `json:"static"`
	} `json:"network"`
	Protocols map[string]interface{} `json:"protocols"` // Sub-protocol specific metadata fields
}

// Info gathers and returns a collection of metadata known about a peer.
func (p *Peer) Info() *PeerInfo {
	// Gather the protocol capabilities
	var caps []string
	for _, cap := range p.Caps() {
		caps = append(caps, cap.String())
	}
	// Assemble the generic peer metadata
	info := &PeerInfo{
		Enode:     p.Node().URLv4(),
		ID:        p.ID().String(),
		Name:      p.Fullname(),
		Caps:      caps,
		Protocols: make(map[string]interface{}, len(p.running)),
	}
	if p.Node().Seq() > 0 {
		info.ENR = p.Node().String()
	}
	info.Network.LocalAddress = p.LocalAddr().String()
	info.Network.RemoteAddress = p.RemoteAddr().String()
	info.Network.Inbound = p.rw.is(inboundConn)
	info.Network.Trusted = p.rw.is(trustedConn)
	info.Network.Static = p.rw.is(staticDialedConn)

	// Gather all the running protocol infos
	for _, proto := range p.running {
		protoInfo := interface{}("unknown")
		if query := proto.Protocol.PeerInfo; query != nil {
			if metadata := query(p.ID()); metadata != nil {
				protoInfo = metadata
			} else {
				protoInfo = "handshake"
			}
		}
		info.Protocols[proto.Name] = protoInfo
	}
	return info
}
