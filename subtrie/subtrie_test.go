package subtrie

import (
	"reflect"
	"sort"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestSubTrie_subscribePeer_isSubscribed(t *testing.T) {
	sm := New()

	peerID := peer.ID("peer1")

	added := sm.SubscribePeer(peerID, []string{"feed:market:dcr_btc:candles", "feed:market:eth_btc:trades"})
	if len(added) != 2 {
		t.Errorf("expected 2 newly added topics, got %v", added)
	}

	// Try adding an existing one and a new one
	added2 := sm.SubscribePeer(peerID, []string{"feed:market:dcr_btc:candles", "new:topic:here"})
	if len(added2) != 1 || added2[0] != "new:topic:here" {
		t.Errorf("expected 1 newly added topic, got %v", added2)
	}

	if !sm.IsSubscribed(peerID, "feed:market:dcr_btc:candles") {
		t.Errorf("expected peer to be subscribed to feed:market:dcr_btc:candles")
	}
	if !sm.IsSubscribed(peerID, "feed:market:eth_btc:trades") {
		t.Errorf("expected peer to be subscribed to feed:market:eth_btc:trades")
	}
	if sm.IsSubscribed(peerID, "feed:market:dcr_btc") {
		t.Errorf("expected peer to NOT be subscribed to partial path feed:market:dcr_btc")
	}
	if sm.IsSubscribed(peerID, "feed:market:btc_usd:candles") {
		t.Errorf("expected peer to NOT be subscribed to feed:market:btc_usd:candles")
	}
	if sm.IsSubscribed(peer.ID("peer2"), "feed:market:dcr_btc:candles") {
		t.Errorf("expected peer2 to NOT be subscribed")
	}
}

func TestSubTrie_peersForTopic(t *testing.T) {
	sm := New()

	sm.SubscribePeer(peer.ID("peer1"), []string{"feed:market:dcr_btc:candles"})
	sm.SubscribePeer(peer.ID("peer2"), []string{"feed:market:dcr_btc:candles", "feed:market:eth_btc:candles"})
	sm.SubscribePeer(peer.ID("peer3"), []string{"feed:market:eth_btc:candles"})

	subs1 := sm.PeersForTopic("feed:market:dcr_btc:candles")
	sort.Slice(subs1, func(i, j int) bool {
		return subs1[i] < subs1[j]
	})
	expected1 := []peer.ID{peer.ID("peer1"), peer.ID("peer2")}
	if !reflect.DeepEqual(subs1, expected1) {
		t.Errorf("expected %v, got %v", expected1, subs1)
	}

	subs2 := sm.PeersForTopic("feed:market:eth_btc:candles")
	sort.Slice(subs2, func(i, j int) bool {
		return subs2[i] < subs2[j]
	})
	expected2 := []peer.ID{peer.ID("peer2"), peer.ID("peer3")}
	if !reflect.DeepEqual(subs2, expected2) {
		t.Errorf("expected %v, got %v", expected2, subs2)
	}

	subs3 := sm.PeersForTopic("feed:market:unknown")
	if len(subs3) != 0 {
		t.Errorf("expected 0 subscribers, got %v", subs3)
	}
}

func TestSubTrie_topicsForPeer(t *testing.T) {
	sm := New()

	sm.SubscribePeer(peer.ID("peer1"), []string{
		"feed:market:dcr_btc:candles",
		"feed:market:eth_btc:trades",
		"feed:market:btc_usd:orderbook",
		"other:topic:here",
	})

	topics := sm.TopicsForPeer(peer.ID("peer1"), "feed:market")
	sort.Strings(topics)
	expected := []string{"btc_usd", "dcr_btc", "eth_btc"}

	if !reflect.DeepEqual(topics, expected) {
		t.Errorf("expected %v, got %v", expected, topics)
	}

	if len(sm.TopicsForPeer(peer.ID("peer2"), "feed:market")) != 0 {
		t.Errorf("expected empty topics for peer2")
	}

	topLevel := sm.TopicsForPeer(peer.ID("peer1"), "")
	sort.Strings(topLevel)
	expectedTop := []string{"feed", "other"}
	if !reflect.DeepEqual(topLevel, expectedTop) {
		t.Errorf("expected %v, got %v", expectedTop, topLevel)
	}
}

func TestSubTrie_removePeer(t *testing.T) {
	sm := New()

	sm.SubscribePeer(peer.ID("peer1"), []string{
		"feed:market:dcr_btc:candles",
		"feed:market:eth_btc:trades",
	})
	sm.SubscribePeer(peer.ID("peer2"), []string{
		"feed:market:dcr_btc:candles",
	})

	// Verify initial state
	if len(sm.PeersForTopic("feed:market:dcr_btc:candles")) != 2 {
		t.Errorf("expected 2 subscribers")
	}

	// Delete peer1
	sm.RemovePeer(peer.ID("peer1"))

	// Verify peer1 is gone from isSubscribed
	if sm.IsSubscribed(peer.ID("peer1"), "feed:market:dcr_btc:candles") {
		t.Errorf("expected peer1 to not be subscribed to anything")
	}
	if sm.IsSubscribed(peer.ID("peer1"), "feed:market:eth_btc:trades") {
		t.Errorf("expected peer1 to not be subscribed to anything")
	}

	// Verify peer1 is gone from Global Trie
	subs1 := sm.PeersForTopic("feed:market:dcr_btc:candles")
	if len(subs1) != 1 || subs1[0] != peer.ID("peer2") {
		t.Errorf("expected only peer2, got %v", subs1)
	}

	subs2 := sm.PeersForTopic("feed:market:eth_btc:trades")
	if len(subs2) != 0 {
		t.Errorf("expected 0 subscribers, got %v", subs2)
	}

	// Verify collectTopics returns empty
	if len(sm.TopicsForPeer(peer.ID("peer1"), "feed:market")) != 0 {
		t.Errorf("expected empty topics for peer1 after delete")
	}
}

func TestSubTrie_ConcurrentAccess(t *testing.T) {
	sm := New()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			sm.SubscribePeer(peer.ID("peer1"), []string{"feed:market:dcr_btc:candles", "feed:market:eth_btc:trades"})
			sm.RemovePeer(peer.ID("peer1"))
		}
		done <- struct{}{}
	}()

	go func() {
		for i := 0; i < 1000; i++ {
			sm.IsSubscribed(peer.ID("peer1"), "feed:market:dcr_btc:candles")
			sm.PeersForTopic("feed:market:dcr_btc:candles")
			sm.TopicsForPeer(peer.ID("peer1"), "feed:market")
		}
		done <- struct{}{}
	}()

	<-done
	<-done
}

func TestSubTrie_unsubscribePeer(t *testing.T) {
	sm := New()

	peerID := peer.ID("peer1")
	sm.SubscribePeer(peerID, []string{
		"feed:market:dcr_btc:candles",
		"feed:market:eth_btc:trades",
		"feed:market:btc_usd:orderbook",
	})

	sm.SubscribePeer(peer.ID("peer2"), []string{
		"feed:market:dcr_btc:candles",
	})

	// Delete specific subsets
	unsubbed := sm.UnsubscribePeer(peerID, []string{
		"feed:market:dcr_btc:candles",
		"feed:market:btc_usd:orderbook",
		"feed:market:never:subbed",
	})
	if len(unsubbed) != 2 || unsubbed[0] != "feed:market:dcr_btc:candles" || unsubbed[1] != "feed:market:btc_usd:orderbook" {
		t.Errorf("expected only valid subs to be returned, got %v", unsubbed)
	}

	if sm.IsSubscribed(peerID, "feed:market:dcr_btc:candles") {
		t.Errorf("expected peer1 to be unsubscribed from dcr_btc")
	}
	if sm.IsSubscribed(peerID, "feed:market:btc_usd:orderbook") {
		t.Errorf("expected peer1 to be unsubscribed from btc_usd")
	}
	if !sm.IsSubscribed(peerID, "feed:market:eth_btc:trades") {
		t.Errorf("expected peer1 to remain subscribed to eth_btc")
	}

	// Verify global trie cleanup
	subs := sm.PeersForTopic("feed:market:dcr_btc:candles")
	if len(subs) != 1 || subs[0] != peer.ID("peer2") {
		t.Errorf("expected only peer2 to be subscribed, got %v", subs)
	}

	if len(sm.PeersForTopic("feed:market:btc_usd:orderbook")) != 0 {
		t.Errorf("expected no subscribers for btc_usd")
	}

	// Verify collectTopics doesn't return deleted ones
	topics := sm.TopicsForPeer(peerID, "feed:market")
	if len(topics) != 1 || topics[0] != "eth_btc" {
		t.Errorf("expected only eth_btc in collectTopics, got %v", topics)
	}

	// Verify zombie node cleanup by seeing if empty peerTrie drops on final delete
	unsubbed2 := sm.UnsubscribePeer(peerID, []string{"feed:market:eth_btc:trades"})
	if len(unsubbed2) != 1 {
		t.Errorf("expected 1 unsubbed, got %v", unsubbed2)
	}
	sm.mtx.RLock()
	_, ok := sm.peerTries[peerID]
	sm.mtx.RUnlock()
	if ok {
		t.Errorf("expected peer1 trie to be entirely deleted when empty")
	}
}

func TestSubTrie_searchTopics(t *testing.T) {

	test := func(name string, topics, filters, expected []string) {
		sm := New()
		sm.SubscribePeer(peer.ID("peer1"), topics)
		results := sm.SearchTopics(filters)
		if len(results) != len(expected) {
			t.Fatalf("[%s] expected %v topics, got %v", name, expected, results)
		}
		lookup := make(map[string]bool)
		for _, r := range results {
			lookup[r] = true
		}
		for _, r := range expected {
			if !lookup[r] {
				t.Fatalf("[%s] expected topic %s not found", name, r)
			}
		}
	}

	inputs := []string{
		"market:dcr_btc:candles",
		"market:eth_btc",
		"feerates:BTC",
		"prices:USDC",
		"splitticket",
	}

	// Test empty
	test("empty", inputs, []string{}, []string{})

	// Test single topic
	filters := []string{
		"market:dcr_btc:candles",
	}
	exp := []string{
		"market:dcr_btc:candles",
	}
	test("single leaf", inputs, filters, exp)

	// Test root topic
	filters = []string{
		"market",
	}
	exp = []string{
		"market",
		"market:dcr_btc",
		"market:dcr_btc:candles",
		"market:eth_btc",
	}
	test("root", inputs, filters, exp)

	// Test no dupes.
	filters = []string{
		"market",
		"market:dcr_btc",
	}
	// Same exp
	test("no dupes", inputs, filters, exp)

	// Test non-existent topics
	filters = []string{
		"non-existent",
	}
	exp = []string{}
	test("no matches", inputs, filters, exp)

}
