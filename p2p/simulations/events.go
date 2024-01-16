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
	"fmt"
	"time"
)

// EventType is the type of event emitted by a simulation network
type EventType string

const (
	// EventTypeNode is the type of event emitted when a node is either
	// created, started or stopped
	// EventTypeNode是event的类型，当一个node被创建，启动或者停止的时候
	EventTypeNode EventType = "node"

	// EventTypeConn is the type of event emitted when a connection is
	// is either established or dropped between two nodes
	// EventTypeConn是发射的事件类型，当一个连接被建立或者dropped，在两个nodes之间
	EventTypeConn EventType = "conn"

	// EventTypeMsg is the type of event emitted when a p2p message it
	// sent between two nodes
	// EventTypeMsg是发射的event，当一个p2p message在两个Node之间传输
	EventTypeMsg EventType = "msg"
)

// Event is an event emitted by a simulation network
// Event是一个simulation network发射的event
type Event struct {
	// Type is the type of the event
	Type EventType `json:"type"`

	// Time is the time the event happened
	Time time.Time `json:"time"`

	// Control indicates whether the event is the result of a controlled
	// action in the network
	// Control表明event是否是network中的一个controlled action的结果
	Control bool `json:"control"`

	// Node is set if the type is EventTypeNode
	Node *Node `json:"node,omitempty"`

	// Conn is set if the type is EventTypeConn
	Conn *Conn `json:"conn,omitempty"`

	// Msg is set if the type is EventTypeMsg
	Msg *Msg `json:"msg,omitempty"`

	//Optionally provide data (currently for simulation frontends only)
	Data interface{} `json:"data"`
}

// NewEvent creates a new event for the given object which should be either a
// Node, Conn or Msg.
// NewEvent创建一个新的event，对于给定的对象，应该为一个Node，Conn或者Msg
//
// The object is copied so that the event represents the state of the object
// when NewEvent is called.
// object被拷贝，因此event代表object的状态，当NewEvent被调用的时候
func NewEvent(v interface{}) *Event {
	event := &Event{Time: time.Now()}
	switch v := v.(type) {
	case *Node:
		event.Type = EventTypeNode
		event.Node = v.copy()
	case *Conn:
		event.Type = EventTypeConn
		conn := *v
		event.Conn = &conn
	case *Msg:
		event.Type = EventTypeMsg
		msg := *v
		event.Msg = &msg
	default:
		panic(fmt.Sprintf("invalid event type: %T", v))
	}
	return event
}

// ControlEvent creates a new control event
// ControlEvent创建一个新的control event
func ControlEvent(v interface{}) *Event {
	event := NewEvent(v)
	event.Control = true
	return event
}

// String returns the string representation of the event
func (e *Event) String() string {
	switch e.Type {
	case EventTypeNode:
		return fmt.Sprintf("<node-event> id: %s up: %t", e.Node.ID().TerminalString(), e.Node.Up())
	case EventTypeConn:
		return fmt.Sprintf("<conn-event> nodes: %s->%s up: %t", e.Conn.One.TerminalString(), e.Conn.Other.TerminalString(), e.Conn.Up)
	case EventTypeMsg:
		return fmt.Sprintf("<msg-event> nodes: %s->%s proto: %s, code: %d, received: %t", e.Msg.One.TerminalString(), e.Msg.Other.TerminalString(), e.Msg.Protocol, e.Msg.Code, e.Msg.Received)
	default:
		return ""
	}
}
