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

package adapters

import (
	"crypto/ecdsa"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/internal/reexec"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/gorilla/websocket"
	"golang.org/x/exp/slog"
)

// Node represents a node in a simulation network which is created by a
// NodeAdapter, for example:
// Node代表一个node，在一个simulation network，通过NodeAdapter创建，例如：
//
//   - SimNode, an in-memory node in the same process
//   - SimNode，一个内存中的node，在同一个进程中
//   - ExecNode, a child process node
//   - ExecNode，一个child process节点
//   - DockerNode, a node running in a Docker container
//   - DockerNode，在Docker container中运行的node
type Node interface {
	// Addr returns the node's address (e.g. an Enode URL)
	Addr() []byte

	// Client returns the RPC client which is created once the node is
	// up and running
	// Client返回RPC client，在node up以及running的时候被创建
	Client() (*rpc.Client, error)

	// ServeRPC serves RPC requests over the given connection
	// ServeRPC通过给定的连接服务RPC请求
	ServeRPC(*websocket.Conn) error

	// Start starts the node with the given snapshots
	// Start启动node，用给定的snapshots
	Start(snapshots map[string][]byte) error

	// Stop stops the node
	Stop() error

	// NodeInfo returns information about the node
	NodeInfo() *p2p.NodeInfo

	// Snapshots creates snapshots of the running services
	// Snapshots创建运行的services的snapshots
	Snapshots() (map[string][]byte, error)
}

// NodeAdapter is used to create Nodes in a simulation network
// NodeAdapter用于在一个simulation network中创建Nodes
type NodeAdapter interface {
	// Name returns the name of the adapter for logging purposes
	// Name返回adapter的名字，用于日志
	Name() string

	// NewNode creates a new node with the given configuration
	// NewNode用给定的配置创建一个新的node
	NewNode(config *NodeConfig) (Node, error)
}

// NodeConfig is the configuration used to start a node in a simulation
// network
// NodeConfig是配置用于启动一个node，在simulation network
type NodeConfig struct {
	// ID is the node's ID which is used to identify the node in the
	// simulation network
	ID enode.ID

	// PrivateKey is the node's private key which is used by the devp2p
	// stack to encrypt communications
	PrivateKey *ecdsa.PrivateKey

	// Enable peer events for Msgs
	EnableMsgEvents bool

	// Name is a human friendly name for the node like "node01"
	Name string

	// Use an existing database instead of a temporary one if non-empty
	DataDir string

	// Lifecycles are the names of the service lifecycles which should be run when
	// starting the node (for SimNodes it should be the names of service lifecycles
	// contained in SimAdapter.lifecycles, for other nodes it should be
	// service lifecycles registered by calling the RegisterLifecycle function)
	// Lifecycles是service lifecycles的名字，当启动node的时候应该运行（对于SimNodes，它应该是
	// 包含在SimAdapter.lifecycles的service lifecycles的名字，对于其他nodes，它应该是通过
	// 调用RegisterLifecycle函数注册的service lifecycles）
	Lifecycles []string

	// Properties are the names of the properties this node should hold
	// within running services (e.g. "bootnode", "lightnode" or any custom values)
	// These values need to be checked and acted upon by node Services
	// Properties是这个node的running services中应该包含的properties的名字（例如，"bootnode", "lightnode"或者任何自定义的名字）
	// 这些值应该被检查并且由node Services执行
	Properties []string

	// ExternalSigner specifies an external URI for a clef-type signer
	ExternalSigner string

	// Enode
	node *enode.Node

	// ENR Record with entries to overwrite
	Record enr.Record

	// function to sanction or prevent suggesting a peer
	Reachable func(id enode.ID) bool

	Port uint16

	// LogFile is the log file name of the p2p node at runtime.
	//
	// The default value is empty so that the default log writer
	// is the system standard output.
	LogFile string

	// LogVerbosity is the log verbosity of the p2p node at runtime.
	//
	// The default verbosity is INFO.
	LogVerbosity slog.Level
}

// nodeConfigJSON is used to encode and decode NodeConfig as JSON by encoding
// all fields as strings
type nodeConfigJSON struct {
	ID              string   `json:"id"`
	PrivateKey      string   `json:"private_key"`
	Name            string   `json:"name"`
	Lifecycles      []string `json:"lifecycles"`
	Properties      []string `json:"properties"`
	EnableMsgEvents bool     `json:"enable_msg_events"`
	Port            uint16   `json:"port"`
	LogFile         string   `json:"logfile"`
	LogVerbosity    int      `json:"log_verbosity"`
}

// MarshalJSON implements the json.Marshaler interface by encoding the config
// fields as strings
func (n *NodeConfig) MarshalJSON() ([]byte, error) {
	confJSON := nodeConfigJSON{
		ID:              n.ID.String(),
		Name:            n.Name,
		Lifecycles:      n.Lifecycles,
		Properties:      n.Properties,
		Port:            n.Port,
		EnableMsgEvents: n.EnableMsgEvents,
		LogFile:         n.LogFile,
		LogVerbosity:    int(n.LogVerbosity),
	}
	if n.PrivateKey != nil {
		confJSON.PrivateKey = hex.EncodeToString(crypto.FromECDSA(n.PrivateKey))
	}
	return json.Marshal(confJSON)
}

// UnmarshalJSON implements the json.Unmarshaler interface by decoding the json
// string values into the config fields
func (n *NodeConfig) UnmarshalJSON(data []byte) error {
	var confJSON nodeConfigJSON
	if err := json.Unmarshal(data, &confJSON); err != nil {
		return err
	}

	if confJSON.ID != "" {
		if err := n.ID.UnmarshalText([]byte(confJSON.ID)); err != nil {
			return err
		}
	}

	if confJSON.PrivateKey != "" {
		key, err := hex.DecodeString(confJSON.PrivateKey)
		if err != nil {
			return err
		}
		privKey, err := crypto.ToECDSA(key)
		if err != nil {
			return err
		}
		n.PrivateKey = privKey
	}

	n.Name = confJSON.Name
	n.Lifecycles = confJSON.Lifecycles
	n.Properties = confJSON.Properties
	n.Port = confJSON.Port
	n.EnableMsgEvents = confJSON.EnableMsgEvents
	n.LogFile = confJSON.LogFile
	n.LogVerbosity = slog.Level(confJSON.LogVerbosity)

	return nil
}

// Node returns the node descriptor represented by the config.
func (n *NodeConfig) Node() *enode.Node {
	return n.node
}

// RandomNodeConfig returns node configuration with a randomly generated ID and
// PrivateKey
func RandomNodeConfig() *NodeConfig {
	prvkey, err := crypto.GenerateKey()
	if err != nil {
		panic("unable to generate key")
	}

	port, err := assignTCPPort()
	if err != nil {
		panic("unable to assign tcp port")
	}

	enodId := enode.PubkeyToIDV4(&prvkey.PublicKey)
	return &NodeConfig{
		PrivateKey:      prvkey,
		ID:              enodId,
		Name:            fmt.Sprintf("node_%s", enodId.String()),
		Port:            port,
		EnableMsgEvents: true,
		LogVerbosity:    log.LvlInfo,
	}
}

func assignTCPPort() (uint16, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	l.Close()
	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		return 0, err
	}
	p, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return 0, err
	}
	return uint16(p), nil
}

// ServiceContext is a collection of options and methods which can be utilised
// ServiceContext是一系列的options以及方法，可以被使用，当开始services的时候
// when starting services
type ServiceContext struct {
	RPCDialer

	Config   *NodeConfig
	Snapshot []byte
}

// RPCDialer is used when initialising services which need to connect to
// other nodes in the network (for example a simulated Swarm node which needs
// to connect to a Geth node to resolve ENS names)
type RPCDialer interface {
	DialRPC(id enode.ID) (*rpc.Client, error)
}

// LifecycleConstructor allows a Lifecycle to be constructed during node start-up.
// LifecycleConstructor允许一个Lifecycle被构建，在node的start-up
// While the service-specific package usually takes care of Lifecycle creation and registration,
// for testing purposes, it is useful to be able to construct a Lifecycle on spot.
// 当service特定的包通常关心Lifecycle的创建以及注册，用于测试目的，这是有用的，能够构建一个Lifecycle
type LifecycleConstructor func(ctx *ServiceContext, stack *node.Node) (node.Lifecycle, error)

// LifecycleConstructors stores LifecycleConstructor functions to call during node start-up.
// LifecycleConstructors存储LifecycleConstructor 函数来调用，在node启动期间
type LifecycleConstructors map[string]LifecycleConstructor

// lifecycleConstructorFuncs is a map of registered services which are used to boot devp2p
// nodes
var lifecycleConstructorFuncs = make(LifecycleConstructors)

// RegisterLifecycles registers the given Services which can then be used to
// start devp2p nodes using either the Exec or Docker adapters.
// RegisterLifecycles注册给定Services，可以在启动devp2p nodes的时候启动，使用Exec或者Docker adapters
//
// It should be called in an init function so that it has the opportunity to
// execute the services before main() is called.
// 它应该在init函数中调用，这它有机会执行services，在main()被调用之前
func RegisterLifecycles(lifecycles LifecycleConstructors) {
	for name, f := range lifecycles {
		if _, exists := lifecycleConstructorFuncs[name]; exists {
			panic(fmt.Sprintf("node service already exists: %q", name))
		}
		lifecycleConstructorFuncs[name] = f
	}

	// now we have registered the services, run reexec.Init() which will
	// potentially start one of the services if the current binary has
	// been exec'd with argv[0] set to "p2p-node"
	// 现在我们已经注册了services，运行reexec.Init()，会潜在运行其中一个service，如果
	// 当前的binary已经exec'd。将argv[0]设置为"p2p-node"
	if reexec.Init() {
		os.Exit(0)
	}
}

// adds the host part to the configuration's ENR, signs it
// 添加host part到配置的ENR，签名
// creates and  the corresponding enode object to the configuration
// 创建对应的enode对象，在配置中
func (n *NodeConfig) initEnode(ip net.IP, tcpport int, udpport int) error {
	enrIp := enr.IP(ip)
	n.Record.Set(&enrIp)
	enrTcpPort := enr.TCP(tcpport)
	n.Record.Set(&enrTcpPort)
	enrUdpPort := enr.UDP(udpport)
	n.Record.Set(&enrUdpPort)

	err := enode.SignV4(&n.Record, n.PrivateKey)
	if err != nil {
		return fmt.Errorf("unable to generate ENR: %v", err)
	}
	nod, err := enode.New(enode.V4ID{}, &n.Record)
	if err != nil {
		return fmt.Errorf("unable to create enode: %v", err)
	}
	log.Trace("simnode new", "record", n.Record)
	n.node = nod
	return nil
}

func (n *NodeConfig) initDummyEnode() error {
	return n.initEnode(net.IPv4(127, 0, 0, 1), int(n.Port), 0)
}
