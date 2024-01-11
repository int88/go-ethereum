# devp2p Simulations

The `p2p/simulations` package implements a simulation framework that supports
creating a collection of devp2p nodes, connecting them to form a
simulation network, performing simulation actions in that network and then
extracting useful information.
`p2p/simulations`包实现一个模拟框架，支持创建一系列的devp2p nodes，链接他们来组成一个模拟网络，
执行模拟操作，在network中并且之后抽取有用的信息

## Nodes

Each node in a simulation network runs multiple services by wrapping a collection
of objects which implement the `node.Service` interface meaning they:
在simulation network中的每个node，运行多个services，通过封装一系列的对象，实现了`node.Service`接口意味着它们：

* can be started and stopped
* 可以被开始和启动
* run p2p protocols
* 运行p2p协议
* expose RPC APIs
* 暴露RCP APIs

This means that any object which implements the `node.Service` interface can be
used to run a node in the simulation.
这意味着任何对象，实现了`node.Service`接口，可以用于在simulation运行一个node

## Services

Before running a simulation, a set of service initializers must be registered
which can then be used to run nodes in the network.
在运行一个模拟之前，一系列的service initializers必须被注册，在他们可以用于在network中运行nodes之前

A service initializer is a function with the following signature:

```go
func(ctx *adapters.ServiceContext) (node.Service, error)
```

These initializers should be registered by calling the `adapters.RegisterServices`
function in an `init()` hook:

```go
func init() {
	adapters.RegisterServices(adapters.Services{
		"service1": initService1,
		"service2": initService2,
	})
}
```

## Node Adapters

The simulation framework includes multiple "node adapters" which are
responsible for creating an environment in which a node runs.
simulation framework包含多个"node adapters"，负责创建一个环境，一个node可以运行

### SimAdapter

The `SimAdapter` runs nodes in-memory, connecting them using an in-memory,
synchronous `net.Pipe` and connecting to their RPC server using an in-memory
`rpc.Client`.
`SimAdapter`在内存中运行nodes，使用内存中的，同步的`net.Pipe`连接它们，并且使用一个内存中的`rpc.Client`连接它们的RPC server

### ExecAdapter

The `ExecAdapter` runs nodes as child processes of the running simulation.
`ExecAdapter`作为运行的模拟的子程序运行nodes

It does this by executing the binary which is running the simulation but
setting `argv[0]` (i.e. the program name) to `p2p-node` which is then
detected by an init hook in the child process which runs the `node.Service`
using the devp2p node stack rather than executing `main()`.
通过执行二进制，运行simulation，但是设置`argv[0]`为`p2p-node`，之后被一个init hook检测到
在child进程，运行`node.Service`使用devp2p node stack而不是执行main

The nodes listen for devp2p connections and WebSocket RPC clients on random
localhost ports.
node监听devp2p连接以及WebSocket RPC客户端，在随机的localhost端口

## Network

A simulation network is created with an ID and default service. The default
service is used if a node is created without an explicit service. The 
network has exposed methods for creating, starting, stopping, connecting 
and disconnecting nodes. It also emits events when certain actions occur.
一个simulation network用一个ID以及默认的service创建，默认的service被使用，如果node被创建
而没有一个显式的service，network已经暴露了方法用于创建、启动、停止，连接以及断开nodes
它同时发射events，当特定的actions发生的时候

### Events

A simulation network emits the following events:

* node event       - when nodes are created / started / stopped
* connection event - when nodes are connected / disconnected
* message event    - when a protocol message is sent between two nodes 当一个protocol message在两个nodes之间发送

The events have a "control" flag which when set indicates that the event is the
outcome of a controlled simulation action (e.g. creating a node or explicitly
connecting two nodes).
events有一个"control" flag，被设置的时候表明event是一个控制的simulation action的结果（
例如创建一个node或者显式连接两个nodes）

This is in contrast to a non-control event, otherwise called a "live" event,
which is the outcome of something happening in the network as a result of a
control event (e.g. a node actually started up or a connection was actually
established between two nodes).
这是和non-control event相反，否则叫做一个"live" event，这是发生在网络内的事件的结果
作为control event的结果（例如，一个node真正启动或者一个连接在两个nodes间真正建立）

Live events are detected by the simulation network by subscribing to node peer
events via RPC when the nodes start up.
Live events被检测到，通过simulation network，通过订阅node peer事件，通过RPC，当node启动的时候

## Testing Framework

The `Simulation` type can be used in tests to perform actions in a simulation
network and then wait for expectations to be met.
`Simulation`类型可以在测试中使用，来执行actions，在一个simulation network，之后等待期望被满足

With a running simulation network, the `Simulation.Run` method can be called
with a `Step` which has the following fields:
有一个在运行的simulation network，`Simulation.Run`方法可以被一个`Step`调用，有以下字段：

* `Action` - a function that performs some action in the network 一个函数在network中执行一些action

* `Expect` - an expectation function which returns whether or not a 一个expectation函数返回是否一个给定的node满足期望
    given node meets the expectation

* `Trigger` - a channel that receives node IDs which then trigger a check 一个channel接收node ID，之后会触发一个expectation函数的check，在给定的node执行
    of the expectation function to be performed against that node

As a concrete example, consider a simulated network of Ethereum nodes. An
`Action` could be the sending of a transaction, `Expect` it being included in
a block, and `Trigger` a check for every block that is mined.
作为一个具体的例子，考虑一个Ethereum nodes的simulated network，一个`Action`可以为发送
一个tx，`Expect`它被包含在一个block，并且`Trigger`一个check，对于每个被挖掘的block

On return, the `Simulation.Run` method returns a `StepResult` which can be used
to determine if all nodes met the expectation, how long it took them to meet
the expectation and what network events were emitted during the step run.
返回的时候，`Simulation.Run`方法返回一个`StepResult`，可以被用于是否所有的nodes满足期望，
花多长时间满足期望并且在每步运行的时候发射什么network events

## HTTP API

The simulation framework includes a HTTP API that can be used to control the
simulation.

The API is initialised with a particular node adapter and has the following
endpoints:

```
GET    /                            Get network information
POST   /start                       Start all nodes in the network
POST   /stop                        Stop all nodes in the network
GET    /events                      Stream network events
GET    /snapshot                    Take a network snapshot
POST   /snapshot                    Load a network snapshot
POST   /nodes                       Create a node
GET    /nodes                       Get all nodes in the network
GET    /nodes/:nodeid               Get node information
POST   /nodes/:nodeid/start         Start a node
POST   /nodes/:nodeid/stop          Stop a node
POST   /nodes/:nodeid/conn/:peerid  Connect two nodes
DELETE /nodes/:nodeid/conn/:peerid  Disconnect two nodes
GET    /nodes/:nodeid/rpc           Make RPC requests to a node via WebSocket
```

For convenience, `nodeid` in the URL can be the name of a node rather than its
ID.

## Command line client

`p2psim` is a command line client for the HTTP API, located in
`cmd/p2psim`.

It provides the following commands:

```
p2psim show
p2psim events [--current] [--filter=FILTER]
p2psim snapshot
p2psim load
p2psim node create [--name=NAME] [--services=SERVICES] [--key=KEY]
p2psim node list
p2psim node show <node>
p2psim node start <node>
p2psim node stop <node>
p2psim node connect <node> <peer>
p2psim node disconnect <node> <peer>
p2psim node rpc <node> <method> [<args>] [--subscribe]
```

## Example

See [p2p/simulations/examples/README.md](examples/README.md).
