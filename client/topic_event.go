package client

import "github.com/libp2p/go-libp2p/core/peer"

// TopicEventType describes the type of event delivered to a topic handler.
type TopicEventType int

const (
	TopicEventData TopicEventType = iota
	TopicEventPeerSubscribed
	TopicEventPeerUnsubscribed
)

// TopicEvent represents an event on a topic.
type TopicEvent struct {
	Type TopicEventType
	Data []byte
	Peer peer.ID
}

// TopicHandler is the callback signature invoked for topic events.
type TopicHandler func(topic string, ev TopicEvent)
