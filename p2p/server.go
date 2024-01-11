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

// Package p2p implements the Ethereum p2p network protocols.
// p2p包实现了ethereum的p2p network协议
package p2p

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/p2p/nat"
	"github.com/ethereum/go-ethereum/p2p/netutil"
	"golang.org/x/exp/slices"
)

const (
	defaultDialTimeout = 15 * time.Second

	// This is the fairness knob for the discovery mixer. When looking for peers, we'll
	// wait this long for a single source of candidates before moving on and trying other
	// sources.
	discmixTimeout = 5 * time.Second

	// Connectivity defaults.
	// 默认的连接设置
	defaultMaxPendingPeers = 50
	defaultDialRatio       = 3

	// This time limits inbound connection attempts per source IP.
	inboundThrottleTime = 30 * time.Second

	// Maximum time allowed for reading a complete message.
	// This is effectively the amount of time a connection can be idle.
	frameReadTimeout = 30 * time.Second

	// Maximum amount of time allowed for writing a complete message.
	frameWriteTimeout = 20 * time.Second
)

var (
	errServerStopped       = errors.New("server stopped")
	errEncHandshakeError   = errors.New("rlpx enc error")
	errProtoHandshakeError = errors.New("rlpx proto error")
)

// Config holds Server options.
// Config维护Server的options
type Config struct {
	// This field must be set to a valid secp256k1 private key.
	PrivateKey *ecdsa.PrivateKey `toml:"-"`

	// MaxPeers is the maximum number of peers that can be
	// connected. It must be greater than zero.
	// MaxPeers是可以连接的最大数目的peers
	MaxPeers int

	// MaxPendingPeers is the maximum number of peers that can be pending in the
	// handshake phase, counted separately for inbound and outbound connections.
	// Zero defaults to preset values.
	// MaxPendingPeers是最大的可以在握手阶段pending的peers数目，对于inbound和outbound connections分别统计
	// 0则为默认配置
	MaxPendingPeers int `toml:",omitempty"`

	// DialRatio controls the ratio of inbound to dialed connections.
	// Example: a DialRatio of 2 allows 1/2 of connections to be dialed.
	// Setting DialRatio to zero defaults it to 3.
	DialRatio int `toml:",omitempty"`

	// NoDiscovery can be used to disable the peer discovery mechanism.
	// Disabling is useful for protocol debugging (manual topology).
	// NoDiscovery可以被用于禁止peer discovery机制，对于协议的debug很有用
	NoDiscovery bool

	// DiscoveryV4 specifies whether V4 discovery should be started.
	DiscoveryV4 bool `toml:",omitempty"`

	// DiscoveryV5 specifies whether the new topic-discovery based V5 discovery
	// protocol should be started or not.
	DiscoveryV5 bool `toml:",omitempty"`

	// Name sets the node name of this server.
	Name string `toml:"-"`

	// BootstrapNodes are used to establish connectivity
	// with the rest of the network.
	// BootstrapNodes用于和network的剩余部分建立连接
	BootstrapNodes []*enode.Node

	// BootstrapNodesV5 are used to establish connectivity
	// with the rest of the network using the V5 discovery
	// protocol.
	// BootstrapNodesV5用于和network的剩余部分建立连接，使用V5的discovery protocol
	BootstrapNodesV5 []*enode.Node `toml:",omitempty"`

	// Static nodes are used as pre-configured connections which are always
	// maintained and re-connected on disconnects.
	// Static nodes作为提前配置的connections，总是被维护并且在断连的时候重连
	StaticNodes []*enode.Node

	// Trusted nodes are used as pre-configured connections which are always
	// allowed to connect, even above the peer limit.
	// Trusted作为提前配置的连接，总是允许去连接，即使超过了peer limit
	TrustedNodes []*enode.Node

	// Connectivity can be restricted to certain IP networks.
	// If this option is set to a non-nil value, only hosts which match one of the
	// IP networks contained in the list are considered.
	// Connectivity可以被限制到特定的IP networks，如果这个option设置为non-nil，只有匹配这个IP networks的才会被考虑
	NetRestrict *netutil.Netlist `toml:",omitempty"`

	// NodeDatabase is the path to the database containing the previously seen
	// live nodes in the network.
	// NodeDatabase是数据库的路径，包含了之前在network中看到的live nodes
	NodeDatabase string `toml:",omitempty"`

	// Protocols should contain the protocols supported
	// by the server. Matching protocols are launched for
	// each peer.
	// Protocols应该包含这个server支持的protocols，对于每个peer匹配的protocols都启动
	Protocols []Protocol `toml:"-" json:"-"`

	// If ListenAddr is set to a non-nil address, the server
	// will listen for incoming connections.
	//
	// If the port is zero, the operating system will pick a port. The
	// ListenAddr field will be updated with the actual address when
	// the server is started.
	ListenAddr string

	// If DiscAddr is set to a non-nil value, the server will use ListenAddr
	// for TCP and DiscAddr for the UDP discovery protocol.
	// 如果DiscAddr设置为non-nil，server会使用ListenAddr作为TCP，用DiscAddr作为UDP的发现协议
	DiscAddr string

	// If set to a non-nil value, the given NAT port mapper
	// is used to make the listening port available to the
	// Internet.
	// 如果设置为non-nil，给定的NAT port mapper会被使用，让listening port在Internet可访问
	NAT nat.Interface `toml:",omitempty"`

	// If Dialer is set to a non-nil value, the given Dialer
	// is used to dial outbound peer connections.
	// 如果Dialer设置为non-nil，给定的Dialer会用于dial outbound peer connections
	Dialer NodeDialer `toml:"-"`

	// If NoDial is true, the server will not dial any peers.
	// 如果NoDial为true，server不会dial任何的peers
	NoDial bool `toml:",omitempty"`

	// If EnableMsgEvents is set then the server will emit PeerEvents
	// whenever a message is sent to or received from a peer
	// 如果EnableMsgEvents被设置，那么server会发射PeerEvents，无论何时一个message被发送，或者从一个peer接收
	EnableMsgEvents bool

	// Logger is a custom logger to use with the p2p.Server.
	Logger log.Logger `toml:",omitempty"`

	clock mclock.Clock
}

// Server manages all peer connections.
// Server管理所有的peer connections
type Server struct {
	// Config fields may not be modified while the server is running.
	// Config字段当server运行的时候不能被修改
	Config

	// Hooks for testing. These are useful because we can inhibit
	// the whole protocol stack.
	// Hooks用于测试，这些很有用，因为我们可以抑制整个协议栈
	newTransport func(net.Conn, *ecdsa.PublicKey) transport
	newPeerHook  func(*Peer)
	listenFunc   func(network, addr string) (net.Listener, error)

	lock    sync.Mutex // protects running
	running bool

	listener     net.Listener
	ourHandshake *protoHandshake
	loopWG       sync.WaitGroup // loop, listenLoop
	peerFeed     event.Feed
	log          log.Logger

	nodedb    *enode.DB
	localnode *enode.LocalNode
	ntab      *discover.UDPv4
	DiscV5    *discover.UDPv5
	discmix   *enode.FairMix
	dialsched *dialScheduler

	// This is read by the NAT port mapping loop.
	// 这被NAT port mapping loop读取
	portMappingRegister chan *portMapping

	// Channels into the run loop.
	// run loop中的channels
	quit                    chan struct{}
	addtrusted              chan *enode.Node
	removetrusted           chan *enode.Node
	peerOp                  chan peerOpFunc
	peerOpDone              chan struct{}
	delpeer                 chan peerDrop
	checkpointPostHandshake chan *conn
	checkpointAddPeer       chan *conn

	// State of run loop and listenLoop.
	// run loop以及listenLoop的状态
	inboundHistory expHeap
}

type peerOpFunc func(map[enode.ID]*Peer)

type peerDrop struct {
	*Peer
	err       error
	requested bool // true if signaled by the peer
}

type connFlag int32

const (
	dynDialedConn connFlag = 1 << iota
	staticDialedConn
	inboundConn
	trustedConn
)

// conn wraps a network connection with information gathered
// during the two handshakes.
// conn封装一个network connection，用在两次握手期间获取到的信息
type conn struct {
	fd net.Conn
	transport
	node  *enode.Node
	flags connFlag
	cont  chan error // The run loop uses cont to signal errors to SetupConn.
	// 在protocol handshake之后合法
	caps []Cap  // valid after the protocol handshake
	name string // valid after the protocol handshake
}

type transport interface {
	// The two handshakes.
	// 两种握手
	doEncHandshake(prv *ecdsa.PrivateKey) (*ecdsa.PublicKey, error)
	doProtoHandshake(our *protoHandshake) (*protoHandshake, error)
	// The MsgReadWriter can only be used after the encryption
	// handshake has completed. The code uses conn.id to track this
	// by setting it to a non-nil value after the encryption handshake.
	// MsgReadWriter可以在entryption握手完成之后使用，代码使用conn.id追踪，通过设置他为non-nil
	// 在握手之后
	MsgReadWriter
	// transports must provide Close because we use MsgPipe in some of
	// the tests. Closing the actual network connection doesn't do
	// anything in those tests because MsgPipe doesn't use it.
	// transports必须提供Close，因为我们使用MsgPipe，在一些测试中，关闭真正的连接
	// 不会在这些测试中做任何事，因为MsgPipe不使用它
	close(err error)
}

func (c *conn) String() string {
	s := c.flags.String()
	if (c.node.ID() != enode.ID{}) {
		s += " " + c.node.ID().String()
	}
	s += " " + c.fd.RemoteAddr().String()
	return s
}

func (f connFlag) String() string {
	s := ""
	if f&trustedConn != 0 {
		s += "-trusted"
	}
	if f&dynDialedConn != 0 {
		s += "-dyndial"
	}
	if f&staticDialedConn != 0 {
		s += "-staticdial"
	}
	if f&inboundConn != 0 {
		s += "-inbound"
	}
	if s != "" {
		s = s[1:]
	}
	return s
}

func (c *conn) is(f connFlag) bool {
	flags := connFlag(atomic.LoadInt32((*int32)(&c.flags)))
	return flags&f != 0
}

func (c *conn) set(f connFlag, val bool) {
	for {
		oldFlags := connFlag(atomic.LoadInt32((*int32)(&c.flags)))
		flags := oldFlags
		if val {
			flags |= f
		} else {
			flags &= ^f
		}
		if atomic.CompareAndSwapInt32((*int32)(&c.flags), int32(oldFlags), int32(flags)) {
			return
		}
	}
}

// LocalNode returns the local node record.
func (srv *Server) LocalNode() *enode.LocalNode {
	return srv.localnode
}

// Peers returns all connected peers.
func (srv *Server) Peers() []*Peer {
	var ps []*Peer
	srv.doPeerOp(func(peers map[enode.ID]*Peer) {
		for _, p := range peers {
			ps = append(ps, p)
		}
	})
	return ps
}

// PeerCount returns the number of connected peers.
func (srv *Server) PeerCount() int {
	var count int
	srv.doPeerOp(func(ps map[enode.ID]*Peer) {
		count = len(ps)
	})
	return count
}

// AddPeer adds the given node to the static node set. When there is room in the peer set,
// the server will connect to the node. If the connection fails for any reason, the server
// will attempt to reconnect the peer.
// AddPeer将给定的node加入到static node set，当peer set中还有空间的时候，server会连接这个node，如果connection失败
// 由于任何原因，server会试着重新连接peer
func (srv *Server) AddPeer(node *enode.Node) {
	srv.dialsched.addStatic(node)
}

// RemovePeer removes a node from the static node set. It also disconnects from the given
// node if it is currently connected as a peer.
// RemovePeer将一个node移出static node set，它同时对给定的node断连，如果它当前作为一个peer连接
//
// This method blocks until all protocols have exited and the peer is removed. Do not use
// RemovePeer in protocol implementations, call Disconnect on the Peer instead.
// 这个方法阻塞直到所有的协议都已经退出并且peer被移除，不要使用协议实现的RemovePeer，而是使用Peer的Disconnect
func (srv *Server) RemovePeer(node *enode.Node) {
	var (
		ch  chan *PeerEvent
		sub event.Subscription
	)
	// Disconnect the peer on the main loop.
	// 在main loop断开peer
	srv.doPeerOp(func(peers map[enode.ID]*Peer) {
		// 从dialsched移除static
		srv.dialsched.removeStatic(node)
		if peer := peers[node.ID()]; peer != nil {
			ch = make(chan *PeerEvent, 1)
			// 获取订阅
			sub = srv.peerFeed.Subscribe(ch)
			// 断开连接
			peer.Disconnect(DiscRequested)
		}
	})
	// Wait for the peer connection to end.
	// 等待peer connection结束
	if ch != nil {
		defer sub.Unsubscribe()
		for ev := range ch {
			if ev.Peer == node.ID() && ev.Type == PeerEventTypeDrop {
				return
			}
		}
	}
}

// AddTrustedPeer adds the given node to a reserved trusted list which allows the
// node to always connect, even if the slot are full.
// AddTrustedPeer将给定的node加入到保留的trusted list，这允许node总是连接，即使slot满了
func (srv *Server) AddTrustedPeer(node *enode.Node) {
	select {
	case srv.addtrusted <- node:
	case <-srv.quit:
	}
}

// RemoveTrustedPeer removes the given node from the trusted peer set.
func (srv *Server) RemoveTrustedPeer(node *enode.Node) {
	select {
	case srv.removetrusted <- node:
	case <-srv.quit:
	}
}

// SubscribeEvents subscribes the given channel to peer events
// SubscribeEvents订阅给定的channel，对于peer events
func (srv *Server) SubscribeEvents(ch chan *PeerEvent) event.Subscription {
	return srv.peerFeed.Subscribe(ch)
}

// Self returns the local node's endpoint information.
// Self返回local node自己的endpoint信息
func (srv *Server) Self() *enode.Node {
	srv.lock.Lock()
	ln := srv.localnode
	srv.lock.Unlock()

	if ln == nil {
		return enode.NewV4(&srv.PrivateKey.PublicKey, net.ParseIP("0.0.0.0"), 0, 0)
	}
	return ln.Node()
}

// Stop terminates the server and all active peer connections.
// Stop终止server以及所有活跃的peer connections
// It blocks until all active connections have been closed.
// 它阻塞直到所有活跃的connecitons都已经被关闭
func (srv *Server) Stop() {
	srv.lock.Lock()
	if !srv.running {
		srv.lock.Unlock()
		return
	}
	srv.running = false
	if srv.listener != nil {
		// this unblocks listener Accept
		// 这会unblocks listener Accept
		srv.listener.Close()
	}
	close(srv.quit)
	srv.lock.Unlock()
	srv.loopWG.Wait()
}

// sharedUDPConn implements a shared connection. Write sends messages to the underlying connection while read returns
// messages that were found unprocessable and sent to the unhandled channel by the primary listener.
type sharedUDPConn struct {
	*net.UDPConn
	unhandled chan discover.ReadPacket
}

// ReadFromUDP implements discover.UDPConn
func (s *sharedUDPConn) ReadFromUDP(b []byte) (n int, addr *net.UDPAddr, err error) {
	packet, ok := <-s.unhandled
	if !ok {
		return 0, nil, errors.New("connection was closed")
	}
	l := len(packet.Data)
	if l > len(b) {
		l = len(b)
	}
	copy(b[:l], packet.Data[:l])
	return l, packet.Addr, nil
}

// Close implements discover.UDPConn
func (s *sharedUDPConn) Close() error {
	return nil
}

// Start starts running the server.
// Start开始运行server
// Servers can not be re-used after stopping.
// Servers在停止之后不能重用
func (srv *Server) Start() (err error) {
	srv.lock.Lock()
	defer srv.lock.Unlock()
	if srv.running {
		return errors.New("server already running")
	}
	srv.running = true
	srv.log = srv.Logger
	if srv.log == nil {
		srv.log = log.Root()
	}
	if srv.clock == nil {
		srv.clock = mclock.System{}
	}
	if srv.NoDial && srv.ListenAddr == "" {
		// P2P server会没有用，既不dialing，又不listening
		srv.log.Warn("P2P server will be useless, neither dialing nor listening")
	}

	// static fields
	if srv.PrivateKey == nil {
		// 必须设置private key
		return errors.New("Server.PrivateKey must be set to a non-nil key")
	}
	if srv.newTransport == nil {
		srv.newTransport = newRLPX
	}
	if srv.listenFunc == nil {
		srv.listenFunc = net.Listen
	}
	srv.quit = make(chan struct{})
	srv.delpeer = make(chan peerDrop)
	srv.checkpointPostHandshake = make(chan *conn)
	srv.checkpointAddPeer = make(chan *conn)
	srv.addtrusted = make(chan *enode.Node)
	srv.removetrusted = make(chan *enode.Node)
	srv.peerOp = make(chan peerOpFunc)
	srv.peerOpDone = make(chan struct{})

	// 设置local node
	if err := srv.setupLocalNode(); err != nil {
		return err
	}
	// 设置prot mapping
	srv.setupPortMapping()

	if srv.ListenAddr != "" {
		// 设置listening
		if err := srv.setupListening(); err != nil {
			return err
		}
	}
	// 设置discovery
	if err := srv.setupDiscovery(); err != nil {
		return err
	}
	// 设置dial scheduler
	srv.setupDialScheduler()

	srv.loopWG.Add(1)
	go srv.run()
	return nil
}

func (srv *Server) setupLocalNode() error {
	// Create the devp2p handshake.
	// 创建devp2p的握手
	pubkey := crypto.FromECDSAPub(&srv.PrivateKey.PublicKey)
	srv.ourHandshake = &protoHandshake{Version: baseProtocolVersion, Name: srv.Name, ID: pubkey[1:]}
	for _, p := range srv.Protocols {
		srv.ourHandshake.Caps = append(srv.ourHandshake.Caps, p.cap())
	}
	slices.SortFunc(srv.ourHandshake.Caps, Cap.Cmp)

	// Create the local node.
	// 创建local node
	db, err := enode.OpenDB(srv.NodeDatabase)
	if err != nil {
		return err
	}
	srv.nodedb = db
	// 构建local node
	srv.localnode = enode.NewLocalNode(db, srv.PrivateKey)
	srv.localnode.SetFallbackIP(net.IP{127, 0, 0, 1})
	// TODO: check conflicts
	for _, p := range srv.Protocols {
		for _, e := range p.Attributes {
			srv.localnode.Set(e)
		}
	}
	return nil
}

func (srv *Server) setupDiscovery() error {
	srv.discmix = enode.NewFairMix(discmixTimeout)

	// Don't listen on UDP endpoint if DHT is disabled.
	// 不要监听UDP endpoint，如果DHT被禁止
	if srv.NoDiscovery {
		return nil
	}
	conn, err := srv.setupUDPListening()
	if err != nil {
		return err
	}

	var (
		sconn     discover.UDPConn = conn
		unhandled chan discover.ReadPacket
	)
	// If both versions of discovery are running, setup a shared
	// connection, so v5 can read unhandled messages from v4.
	// 如果两个版本的discovery都在运行，设置一个shared connection，这与v5可以从v4读取unhandled messages
	if srv.DiscoveryV4 && srv.DiscoveryV5 {
		unhandled = make(chan discover.ReadPacket, 100)
		sconn = &sharedUDPConn{conn, unhandled}
	}

	// Start discovery services.
	// 启动discovery services
	if srv.DiscoveryV4 {
		cfg := discover.Config{
			PrivateKey:  srv.PrivateKey,
			NetRestrict: srv.NetRestrict,
			Bootnodes:   srv.BootstrapNodes,
			Unhandled:   unhandled,
			Log:         srv.log,
		}
		// 监听discv4
		ntab, err := discover.ListenV4(conn, srv.localnode, cfg)
		if err != nil {
			return err
		}
		srv.ntab = ntab
		srv.discmix.AddSource(ntab.RandomNodes())
	}
	if srv.DiscoveryV5 {
		// 启动discovery v5 service
		cfg := discover.Config{
			PrivateKey:  srv.PrivateKey,
			NetRestrict: srv.NetRestrict,
			Bootnodes:   srv.BootstrapNodesV5,
			Log:         srv.log,
		}
		srv.DiscV5, err = discover.ListenV5(sconn, srv.localnode, cfg)
		if err != nil {
			return err
		}
	}

	// Add protocol-specific discovery sources.
	// 添加协议特定的discovery sources
	added := make(map[string]bool)
	for _, proto := range srv.Protocols {
		if proto.DialCandidates != nil && !added[proto.Name] {
			srv.discmix.AddSource(proto.DialCandidates)
			added[proto.Name] = true
		}
	}
	return nil
}

func (srv *Server) setupDialScheduler() {
	config := dialConfig{
		self:           srv.localnode.ID(),
		maxDialPeers:   srv.maxDialedConns(),
		maxActiveDials: srv.MaxPendingPeers,
		log:            srv.Logger,
		netRestrict:    srv.NetRestrict,
		dialer:         srv.Dialer,
		clock:          srv.clock,
	}
	if srv.ntab != nil {
		config.resolver = srv.ntab
	}
	if config.dialer == nil {
		config.dialer = tcpDialer{&net.Dialer{Timeout: defaultDialTimeout}}
	}
	srv.dialsched = newDialScheduler(config, srv.discmix, srv.SetupConn)
	for _, n := range srv.StaticNodes {
		srv.dialsched.addStatic(n)
	}
}

func (srv *Server) maxInboundConns() int {
	return srv.MaxPeers - srv.maxDialedConns()
}

func (srv *Server) maxDialedConns() (limit int) {
	if srv.NoDial || srv.MaxPeers == 0 {
		return 0
	}
	if srv.DialRatio == 0 {
		limit = srv.MaxPeers / defaultDialRatio
	} else {
		limit = srv.MaxPeers / srv.DialRatio
	}
	if limit == 0 {
		limit = 1
	}
	return limit
}

func (srv *Server) setupListening() error {
	// Launch the listener.
	// 启动listener
	listener, err := srv.listenFunc("tcp", srv.ListenAddr)
	if err != nil {
		return err
	}
	srv.listener = listener
	srv.ListenAddr = listener.Addr().String()

	// Update the local node record and map the TCP listening port if NAT is configured.
	// 更新本地的node record并且映射TCP的监听端口，如果配置了NAT
	tcp, isTCP := listener.Addr().(*net.TCPAddr)
	if isTCP {
		srv.localnode.Set(enr.TCP(tcp.Port))
		if !tcp.IP.IsLoopback() && !tcp.IP.IsPrivate() {
			srv.portMappingRegister <- &portMapping{
				protocol: "TCP",
				name:     "ethereum p2p",
				port:     tcp.Port,
			}
		}
	}

	srv.loopWG.Add(1)
	go srv.listenLoop()
	return nil
}

func (srv *Server) setupUDPListening() (*net.UDPConn, error) {
	listenAddr := srv.ListenAddr

	// Use an alternate listening address for UDP if
	// a custom discovery address is configured.
	if srv.DiscAddr != "" {
		listenAddr = srv.DiscAddr
	}
	addr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	laddr := conn.LocalAddr().(*net.UDPAddr)
	srv.localnode.SetFallbackUDP(laddr.Port)
	srv.log.Debug("UDP listener up", "addr", laddr)
	if !laddr.IP.IsLoopback() && !laddr.IP.IsPrivate() {
		srv.portMappingRegister <- &portMapping{
			protocol: "UDP",
			name:     "ethereum peer discovery",
			port:     laddr.Port,
		}
	}

	return conn, nil
}

// doPeerOp runs fn on the main loop.
// doPeerOp在main loop运行
func (srv *Server) doPeerOp(fn peerOpFunc) {
	select {
	case srv.peerOp <- fn:
		<-srv.peerOpDone
	case <-srv.quit:
	}
}

// run is the main loop of the server.
// run是server运行的main loop
func (srv *Server) run() {
	// 开始P2P networking
	srv.log.Info("Started P2P networking", "self", srv.localnode.Node().URLv4())
	defer srv.loopWG.Done()
	defer srv.nodedb.Close()
	defer srv.discmix.Close()
	defer srv.dialsched.stop()

	var (
		peers        = make(map[enode.ID]*Peer)
		inboundCount = 0
		trusted      = make(map[enode.ID]bool, len(srv.TrustedNodes))
	)
	// Put trusted nodes into a map to speed up checks.
	// 将trusted nodes加入map来加速检查
	// Trusted peers are loaded on startup or added via AddTrustedPeer RPC.
	// Trusted peers在启动的时候加载或者通过AddTrustedPeer RPC
	for _, n := range srv.TrustedNodes {
		trusted[n.ID()] = true
	}

running:
	for {
		select {
		case <-srv.quit:
			// The server was stopped. Run the cleanup logic.
			break running

		case n := <-srv.addtrusted:
			// This channel is used by AddTrustedPeer to add a node
			// to the trusted node set.
			// 这个channel被AddTrustedPeer使用来添加一个node到trusted node set
			srv.log.Trace("Adding trusted node", "node", n)
			trusted[n.ID()] = true
			if p, ok := peers[n.ID()]; ok {
				p.rw.set(trustedConn, true)
			}

		case n := <-srv.removetrusted:
			// This channel is used by RemoveTrustedPeer to remove a node
			// from the trusted node set.
			// 这个channel被RemoveTrustedPeer使用来移除一个node，从trusted node set
			srv.log.Trace("Removing trusted node", "node", n)
			delete(trusted, n.ID())
			if p, ok := peers[n.ID()]; ok {
				p.rw.set(trustedConn, false)
			}

		case op := <-srv.peerOp:
			// This channel is used by Peers and PeerCount.
			// 这个channel被Peers以及PeerCount使用
			op(peers)
			srv.peerOpDone <- struct{}{}

		case c := <-srv.checkpointPostHandshake:
			// A connection has passed the encryption handshake so
			// the remote identity is known (but hasn't been verified yet).
			// 一个connection已经传递entryption handshake，这与remote identity已经知道（但是还没有校验）
			if trusted[c.node.ID()] {
				// Ensure that the trusted flag is set before checking against MaxPeers.
				c.flags |= trustedConn
			}
			// TODO: track in-progress inbound node IDs (pre-Peer) to avoid dialing them.
			c.cont <- srv.postHandshakeChecks(peers, inboundCount, c)

		case c := <-srv.checkpointAddPeer:
			// At this point the connection is past the protocol handshake.
			// Its capabilities are known and the remote identity is verified.
			// 这个时候connction已经过了protocol handshake，它的capabilities已经知道，并且remote identity已经被校验
			err := srv.addPeerChecks(peers, inboundCount, c)
			if err == nil {
				// The handshakes are done and it passed all checks.
				// handshakes已经完成并且它通过了所有校验
				p := srv.launchPeer(c)
				peers[c.node.ID()] = p
				srv.log.Debug("Adding p2p peer", "peercount", len(peers), "id", p.ID(), "conn", c.flags, "addr", p.RemoteAddr(), "name", p.Name())
				srv.dialsched.peerAdded(c)
				if p.Inbound() {
					inboundCount++
					serveSuccessMeter.Mark(1)
				} else {
					dialSuccessMeter.Mark(1)
				}
				activePeerGauge.Inc(1)
			}
			c.cont <- err

		case pd := <-srv.delpeer:
			// A peer disconnected.
			// 一个peer已经disconnected
			d := common.PrettyDuration(mclock.Now() - pd.created)
			delete(peers, pd.ID())
			// 移除p2p peer
			srv.log.Debug("Removing p2p peer", "peercount", len(peers), "id", pd.ID(), "duration", d, "req", pd.requested, "err", pd.err)
			srv.dialsched.peerRemoved(pd.rw)
			if pd.Inbound() {
				inboundCount--
			}
			activePeerGauge.Dec(1)
		}
	}

	srv.log.Trace("P2P networking is spinning down")

	// Terminate discovery. If there is a running lookup it will terminate soon.
	// 终止discovery，如果有一个正在运行的lookup，它会立刻终止
	if srv.ntab != nil {
		srv.ntab.Close()
	}
	if srv.DiscV5 != nil {
		srv.DiscV5.Close()
	}
	// Disconnect all peers.
	// 断开所有的peers
	for _, p := range peers {
		p.Disconnect(DiscQuitting)
	}
	// Wait for peers to shut down. Pending connections and tasks are
	// not handled here and will terminate soon-ish because srv.quit
	// is closed.
	// 等待所有peers被关闭，Pending connections以及tasks没有在这里被处理并且会终止，因为srv.quit被关闭
	for len(peers) > 0 {
		p := <-srv.delpeer
		p.log.Trace("<-delpeer (spindown)")
		delete(peers, p.ID())
	}
}

func (srv *Server) postHandshakeChecks(peers map[enode.ID]*Peer, inboundCount int, c *conn) error {
	switch {
	case !c.is(trustedConn) && len(peers) >= srv.MaxPeers:
		// 大于max peers
		return DiscTooManyPeers
	case !c.is(trustedConn) && c.is(inboundConn) && inboundCount >= srv.maxInboundConns():
		return DiscTooManyPeers
	case peers[c.node.ID()] != nil:
		return DiscAlreadyConnected
	case c.node.ID() == srv.localnode.ID():
		return DiscSelf
	default:
		return nil
	}
}

func (srv *Server) addPeerChecks(peers map[enode.ID]*Peer, inboundCount int, c *conn) error {
	// Drop connections with no matching protocols.
	// 丢弃连接，如果没有匹配的protocols
	if len(srv.Protocols) > 0 && countMatchingProtocols(srv.Protocols, c.caps) == 0 {
		return DiscUselessPeer
	}
	// Repeat the post-handshake checks because the
	// peer set might have changed since those checks were performed.
	// 重复post-handshake checks，因为peer set可能在这些检查发生之后有变化
	return srv.postHandshakeChecks(peers, inboundCount, c)
}

// listenLoop runs in its own goroutine and accepts
// inbound connections.
// listenLoop运行它自己的goroutine并且接受inbound connections
func (srv *Server) listenLoop() {
	srv.log.Debug("TCP listener up", "addr", srv.listener.Addr())

	// The slots channel limits accepts of new connections.
	// slots channel限制新连接的接收
	tokens := defaultMaxPendingPeers
	if srv.MaxPendingPeers > 0 {
		tokens = srv.MaxPendingPeers
	}
	slots := make(chan struct{}, tokens)
	for i := 0; i < tokens; i++ {
		slots <- struct{}{}
	}

	// Wait for slots to be returned on exit. This ensures all connection goroutines
	// are down before listenLoop returns.
	// 等待slots在退出的时候返回，这确保所有的connection goroutines在listenLoop退出的时候down
	defer srv.loopWG.Done()
	defer func() {
		for i := 0; i < cap(slots); i++ {
			<-slots
		}
	}()

	for {
		// Wait for a free slot before accepting.
		// 等待一个free slot，在接受之前
		<-slots

		var (
			fd      net.Conn
			err     error
			lastLog time.Time
		)
		for {
			fd, err = srv.listener.Accept()
			if netutil.IsTemporaryError(err) {
				if time.Since(lastLog) > 1*time.Second {
					srv.log.Debug("Temporary read error", "err", err)
					lastLog = time.Now()
				}
				time.Sleep(time.Millisecond * 200)
				continue
			} else if err != nil {
				srv.log.Debug("Read error", "err", err)
				slots <- struct{}{}
				return
			}
			break
		}

		remoteIP := netutil.AddrIP(fd.RemoteAddr())
		// 检查inbound connection
		if err := srv.checkInboundConn(remoteIP); err != nil {
			// 拒绝inbound connection
			srv.log.Debug("Rejected inbound connection", "addr", fd.RemoteAddr(), "err", err)
			fd.Close()
			slots <- struct{}{}
			continue
		}
		if remoteIP != nil {
			fd = newMeteredConn(fd)
			serveMeter.Mark(1)
			srv.log.Trace("Accepted connection", "addr", fd.RemoteAddr())
		}
		go func() {
			// 设置connection
			srv.SetupConn(fd, inboundConn, nil)
			slots <- struct{}{}
		}()
	}
}

func (srv *Server) checkInboundConn(remoteIP net.IP) error {
	if remoteIP == nil {
		return nil
	}
	// Reject connections that do not match NetRestrict.
	// 拒绝connections，如果不匹配NetRestrict
	if srv.NetRestrict != nil && !srv.NetRestrict.Contains(remoteIP) {
		return fmt.Errorf("not in netrestrict list")
	}
	// Reject Internet peers that try too often.
	// 拒绝重试太频繁的Internet peers
	now := srv.clock.Now()
	srv.inboundHistory.expire(now, nil)
	if !netutil.IsLAN(remoteIP) && srv.inboundHistory.contains(remoteIP.String()) {
		return fmt.Errorf("too many attempts")
	}
	// 添加到inbound history
	srv.inboundHistory.add(remoteIP.String(), now.Add(inboundThrottleTime))
	return nil
}

// SetupConn runs the handshakes and attempts to add the connection
// as a peer. It returns when the connection has been added as a peer
// or the handshakes have failed.
// SetupConn运行握手并且试着将连接作为一个peer添加，它返回什么时候连接作为peer被添加或者握手失败
func (srv *Server) SetupConn(fd net.Conn, flags connFlag, dialDest *enode.Node) error {
	c := &conn{fd: fd, flags: flags, cont: make(chan error)}
	if dialDest == nil {
		c.transport = srv.newTransport(fd, nil)
	} else {
		c.transport = srv.newTransport(fd, dialDest.Pubkey())
	}

	err := srv.setupConn(c, flags, dialDest)
	if err != nil {
		if !c.is(inboundConn) {
			markDialError(err)
		}
		c.close(err)
	}
	return err
}

func (srv *Server) setupConn(c *conn, flags connFlag, dialDest *enode.Node) error {
	// Prevent leftover pending conns from entering the handshake.
	// 防止进入握手之后，剩下pending conns
	srv.lock.Lock()
	running := srv.running
	srv.lock.Unlock()
	if !running {
		return errServerStopped
	}

	// If dialing, figure out the remote public key.
	// 如果dialing，搞清楚remote public key
	if dialDest != nil {
		dialPubkey := new(ecdsa.PublicKey)
		if err := dialDest.Load((*enode.Secp256k1)(dialPubkey)); err != nil {
			// dial destination没有一个secp256k1 public key
			err = fmt.Errorf("%w: dial destination doesn't have a secp256k1 public key", errEncHandshakeError)
			srv.log.Trace("Setting up connection failed", "addr", c.fd.RemoteAddr(), "conn", c.flags, "err", err)
			return err
		}
	}

	// Run the RLPx handshake.
	// 运行RLPx握手
	remotePubkey, err := c.doEncHandshake(srv.PrivateKey)
	if err != nil {
		srv.log.Trace("Failed RLPx handshake", "addr", c.fd.RemoteAddr(), "conn", c.flags, "err", err)
		return fmt.Errorf("%w: %v", errEncHandshakeError, err)
	}
	if dialDest != nil {
		c.node = dialDest
	} else {
		c.node = nodeFromConn(remotePubkey, c.fd)
	}
	clog := srv.log.New("id", c.node.ID(), "addr", c.fd.RemoteAddr(), "conn", c.flags)
	err = srv.checkpoint(c, srv.checkpointPostHandshake)
	if err != nil {
		clog.Trace("Rejected peer", "err", err)
		return err
	}

	// Run the capability negotiation handshake.
	// 运行capability协商握手
	phs, err := c.doProtoHandshake(srv.ourHandshake)
	if err != nil {
		clog.Trace("Failed p2p handshake", "err", err)
		return fmt.Errorf("%w: %v", errProtoHandshakeError, err)
	}
	if id := c.node.ID(); !bytes.Equal(crypto.Keccak256(phs.ID), id[:]) {
		// 错误的devp2p握手标识
		clog.Trace("Wrong devp2p handshake identity", "phsid", hex.EncodeToString(phs.ID))
		return DiscUnexpectedIdentity
	}
	c.caps, c.name = phs.Caps, phs.Name
	err = srv.checkpoint(c, srv.checkpointAddPeer)
	if err != nil {
		clog.Trace("Rejected peer", "err", err)
		return err
	}

	return nil
}

func nodeFromConn(pubkey *ecdsa.PublicKey, conn net.Conn) *enode.Node {
	var ip net.IP
	var port int
	if tcp, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		ip = tcp.IP
		port = tcp.Port
	}
	// 构建enode
	return enode.NewV4(pubkey, ip, port, port)
}

// checkpoint sends the conn to run, which performs the
// post-handshake checks for the stage (posthandshake, addpeer).
// checkpoint发送conn去运行，为这个stage执行post-handshake检查（posthandshake，addpeer）
func (srv *Server) checkpoint(c *conn, stage chan<- *conn) error {
	select {
	// 将connection发给stage
	case stage <- c:
	case <-srv.quit:
		return errServerStopped
	}
	return <-c.cont
}

func (srv *Server) launchPeer(c *conn) *Peer {
	// 构建一个新的peer
	p := newPeer(srv.log, c, srv.Protocols)
	if srv.EnableMsgEvents {
		// If message events are enabled, pass the peerFeed
		// to the peer.
		// 如果message events被使能，传递peerFeed到peer
		p.events = &srv.peerFeed
	}
	go srv.runPeer(p)
	return p
}

// runPeer runs in its own goroutine for each peer.
// runPeer运行在自己的goroutine，对于每个peer
func (srv *Server) runPeer(p *Peer) {
	if srv.newPeerHook != nil {
		// 运行peer hook
		srv.newPeerHook(p)
	}
	// 发送peer event
	srv.peerFeed.Send(&PeerEvent{
		Type:          PeerEventTypeAdd,
		Peer:          p.ID(),
		RemoteAddress: p.RemoteAddr().String(),
		LocalAddress:  p.LocalAddr().String(),
	})

	// Run the per-peer main loop.
	// 运行每个peer的main loop
	remoteRequested, err := p.run()

	// Announce disconnect on the main loop to update the peer set.
	// 在main loop声明disconnect，更新peer set
	// The main loop waits for existing peers to be sent on srv.delpeer
	// before returning, so this send should not select on srv.quit.
	// main loop等待已经存在的peers发送到srv.delpeer，在返回之前，这与这个send不应该在srv.quit被选择
	srv.delpeer <- peerDrop{p, err, remoteRequested}

	// Broadcast peer drop to external subscribers. This needs to be
	// after the send to delpeer so subscribers have a consistent view of
	// the peer set (i.e. Server.Peers() doesn't include the peer when the
	// event is received).
	srv.peerFeed.Send(&PeerEvent{
		Type:          PeerEventTypeDrop,
		Peer:          p.ID(),
		Error:         err.Error(),
		RemoteAddress: p.RemoteAddr().String(),
		LocalAddress:  p.LocalAddr().String(),
	})
}

// NodeInfo represents a short summary of the information known about the host.
type NodeInfo struct {
	ID    string `json:"id"`    // Unique node identifier (also the encryption key)
	Name  string `json:"name"`  // Name of the node, including client type, version, OS, custom data
	Enode string `json:"enode"` // Enode URL for adding this peer from remote peers
	ENR   string `json:"enr"`   // Ethereum Node Record
	IP    string `json:"ip"`    // IP address of the node
	Ports struct {
		Discovery int `json:"discovery"` // UDP listening port for discovery protocol
		Listener  int `json:"listener"`  // TCP listening port for RLPx
	} `json:"ports"`
	ListenAddr string                 `json:"listenAddr"`
	Protocols  map[string]interface{} `json:"protocols"`
}

// NodeInfo gathers and returns a collection of metadata known about the host.
func (srv *Server) NodeInfo() *NodeInfo {
	// Gather and assemble the generic node infos
	node := srv.Self()
	info := &NodeInfo{
		Name:       srv.Name,
		Enode:      node.URLv4(),
		ID:         node.ID().String(),
		IP:         node.IP().String(),
		ListenAddr: srv.ListenAddr,
		Protocols:  make(map[string]interface{}),
	}
	info.Ports.Discovery = node.UDP()
	info.Ports.Listener = node.TCP()
	info.ENR = node.String()

	// Gather all the running protocol infos (only once per protocol type)
	for _, proto := range srv.Protocols {
		if _, ok := info.Protocols[proto.Name]; !ok {
			nodeInfo := interface{}("unknown")
			if query := proto.NodeInfo; query != nil {
				nodeInfo = proto.NodeInfo()
			}
			info.Protocols[proto.Name] = nodeInfo
		}
	}
	return info
}

// PeersInfo returns an array of metadata objects describing connected peers.
func (srv *Server) PeersInfo() []*PeerInfo {
	// Gather all the generic and sub-protocol specific infos
	infos := make([]*PeerInfo, 0, srv.PeerCount())
	for _, peer := range srv.Peers() {
		if peer != nil {
			infos = append(infos, peer.Info())
		}
	}
	// Sort the result array alphabetically by node identifier
	for i := 0; i < len(infos); i++ {
		for j := i + 1; j < len(infos); j++ {
			if infos[i].ID > infos[j].ID {
				infos[i], infos[j] = infos[j], infos[i]
			}
		}
	}
	return infos
}
