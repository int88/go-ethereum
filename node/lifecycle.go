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

package node

// Lifecycle encompasses the behavior of services that can be started and stopped
// on the node. Lifecycle management is delegated to the node, but it is the
// responsibility of the service-specific package to configure and register the
// service on the node using the `RegisterLifecycle` method.
// Lifecycle包含services的行为，可以在node中开始和结束，Lifecycle management被委托到node，但是这是
// service特定包的责任来配置和注册service到node，使用`RegisterLifecycle`的方法
type Lifecycle interface {
	// Start is called after all services have been constructed and the networking
	// layer was also initialized to spawn any goroutines required by the service.
	// Start被调用，在所有services已经被构建并且networking layer已经被初始化，生成service需要的goroutines
	Start() error

	// Stop terminates all goroutines belonging to the service, blocking until they
	// are all terminated.
	// Stop终止所有的goroutines，属于这个service，阻塞直到他们被全部终结
	Stop() error
}
