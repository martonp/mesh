package client

import (
	"testing"
	"time"
)

func TestTopicRegistry(t *testing.T) {
	tr := newTopicRegistry()

	// Ensure the registry and register a topic and its handler.
	pingTopic := "ping"

	pingHandlerSignals := make(chan struct{}, 1)
	pingHandler := func(_ string, ev TopicEvent) {
		pingHandlerSignals <- struct{}{}
	}

	if !tr.register(pingTopic, pingHandler) {
		t.Fatalf("Expected ping topic to be registered")
	}

	// Ensure the registry returns an error when fetching the handler for an unknown topic.
	_, err := tr.fetchHandler("unknown")
	if err == nil {
		t.Fatalf("Expected an unknown topic error")
	}

	// Ensure the handler of a topic can be fetched.
	handleFunc, err := tr.fetchHandler(pingTopic)
	if err != nil {
		t.Fatalf("Unexpected error fetching topic handler: %v", err)
	}

	handleFunc(pingTopic, TopicEvent{})

	select {
	case <-pingHandlerSignals:
	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting for handler signal")
	}

	// Ensure the topics in the registry can be fetched.
	topics := tr.fetchTopics()
	if len(topics) != 1 {
		t.Fatalf("Expected a single registry entry, got %d", len(topics))
	}

	// Ensure the registry can unregister a topic.
	if !tr.unregister(pingTopic) {
		t.Fatalf("Expected ping topic to be unregistered")
	}
}
