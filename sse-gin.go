package main

import (
	"encoding/json"
)

type EventType string

type EventBroker[T comparable] struct {
	Notifier       chan Event[T]
	newClients     chan Connection[T]
	closingClients chan Connection[T]
	clients        map[T]map[Connection[T]]bool
}

type Event[T comparable] struct {
	Type    EventType       `json:"type"`
	Targets []T             `json:"targets"`
	Payload json.RawMessage `json:"payload"`
}

type Connection[T comparable] struct {
	Target T
	Chan   chan Event[T]
}

func NewEventBroker[T comparable]() *EventBroker[T] {
	return &EventBroker[T]{
		Notifier:       make(chan Event[T]),
		newClients:     make(chan Connection[T]),
		closingClients: make(chan Connection[T]),
		clients:        make(map[T]map[Connection[T]]bool),
	}
}

func (b *EventBroker[T]) AddClient(c Connection[T]) {
	b.newClients <- c
}

func (b *EventBroker[T]) RemoveClient(c Connection[T]) {
	b.closingClients <- c
}

func (b *EventBroker[T]) Listen() {
	for {
		select {
		case c := <-b.newClients:
			// A client has made a new connection for the first time
			_, ok := b.clients[c.Target]
			if !ok {
				b.clients[c.Target] = map[Connection[T]]bool{c: true}
			} else {
				b.clients[c.Target][c] = true
			}
		case c := <-b.closingClients:
			delete(b.clients[c.Target], c)
			// If no connections remain, remove the client
			if len(b.clients[c.Target]) == 0 {
				// remove client
				delete(b.clients, c.Target)
			}
		case e := <-b.Notifier:
			// Check if the user is online, get all connections
			for _, target := range e.Targets {
				b.SendEventTo(target, e)
			}
		}
	}
}

func (b *EventBroker[T]) SendEventTo(target T, e Event[T]) {
	if cm, ok := b.clients[target]; ok {
		// Relay the event to each connection
		for cn := range cm {
			cn.Chan <- e
		}
	}
}

type EventHub[T comparable, X comparable] struct {
	newClients     chan Presence[T, X]
	closingClients chan Presence[T, X]
	groups         map[T]map[X]Presence[T, X]
}

type Entity[T comparable] struct {
	Id   T
	Meta any
}

type Presence[T comparable, X comparable] struct {
	Target T
	Entity Entity[X]
	Online chan []Entity[X]
}

func NewEventHub[T comparable, X comparable]() *EventHub[T, X] {
	return &EventHub[T, X]{
		newClients:     make(chan Presence[T, X]),
		closingClients: make(chan Presence[T, X]),
		groups:         make(map[T]map[X]Presence[T, X]),
	}
}

func (h *EventHub[T, X]) AddClient(p Presence[T, X]) {
	h.newClients <- p
}

func (h *EventHub[T, X]) RemoveClient(p Presence[T, X]) {
	h.closingClients <- p
}

func (h *EventHub[T, X]) UpdateConnections(g map[X]Presence[T, X]) {
	entities := make([]Entity[X], len(g))
	i := 0
	for _, p := range g {
		entities[i] = p.Entity
		i += 1
	}
	for _, p := range g {
		// Non-blocking send with default on select block
		// we don't care if there are no consumers it's likely conn will be closed soon
		select {
		case p.Online <- entities:
		default:
		}
	}
}

func (h *EventHub[T, X]) Manage() {
	for {
		select {
		case p := <-h.newClients:
			g, ok := h.groups[p.Target]
			// There is no group, so we create one with our first entity
			if !ok {
				h.groups[p.Target] = map[X]Presence[T, X]{
					p.Entity.Id: p,
				}
			} else {
				oc, ok := g[p.Entity.Id]
				// Group exists but there is no other similar entity
				if !ok {
					g[p.Entity.Id] = p
				} else {
					// Group exists and there is already a connection made by a similar entity
					// Remove old connection and add new one
					close(oc.Online)
					delete(g, p.Entity.Id)
					g[p.Entity.Id] = p
				}
				h.UpdateConnections(g)
			}
		case p := <-h.closingClients:
			delete(h.groups[p.Target], p.Entity.Id)
			if len(h.groups[p.Target]) == 0 {
				delete(h.groups, p.Target)
			} else {
				h.UpdateConnections(h.groups[p.Target])
			}
		}
	}
}
