package subtrie

import (
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

// topicNode represents a single node in a peer's topic trie.
type topicNode struct {
	subTopics map[string]*topicNode
	isEnd     bool
}

func newTopicNode() *topicNode {
	return &topicNode{
		subTopics: make(map[string]*topicNode),
	}
}

// trie represents a node in the single global topic trie.
type trie struct {
	children    map[string]*trie
	subscribers map[peer.ID]struct{}
}

func newTrie() *trie {
	return &trie{
		subscribers: make(map[peer.ID]struct{}),
	}
}

// SubTrie provides a highly optimized, thread-safe, memory-based
// bi-directional subscriber-topic indexing system.
type SubTrie struct {
	mtx        sync.RWMutex
	peerTries  map[peer.ID]*topicNode
	globalTrie *trie
}

// New initializes and returns a ready-to-use subscriptionManager.
func New() *SubTrie {
	return &SubTrie{
		peerTries:  make(map[peer.ID]*topicNode),
		globalTrie: newTrie(),
	}
}

// IsSubscribed traverses the peer's personal trie to check if they are
// subscribed to the specific path.
func (sm *SubTrie) IsSubscribed(peerID peer.ID, topic string) bool {
	sm.mtx.RLock()
	defer sm.mtx.RUnlock()

	peerTrie, ok := sm.peerTries[peerID]
	if !ok {
		return false
	}

	parts := strings.Split(topic, ":")
	node := peerTrie
	for _, part := range parts {
		if node.subTopics == nil {
			return false
		}
		child, exists := node.subTopics[part]
		if !exists {
			return false
		}
		node = child
	}

	return node.isEnd
}

// PeersForTopic traverses the global topic trie to the specified node
// and returns the list of peer IDs.
func (sm *SubTrie) PeersForTopic(topic string) []peer.ID {
	sm.mtx.RLock()
	defer sm.mtx.RUnlock()

	parts := strings.Split(topic, ":")
	node := sm.globalTrie
	for _, part := range parts {
		if node.children == nil {
			return nil
		}
		child, exists := node.children[part]
		if !exists {
			return nil
		}
		node = child
	}

	var subscribers []peer.ID
	if node.subscribers != nil {
		subscribers = make([]peer.ID, 0, len(node.subscribers))
		for peerID := range node.subscribers {
			subscribers = append(subscribers, peerID)
		}
	}

	return subscribers
}

// TopicsForPeer traverses the peer's personal trie to the specified path
// and returns all the keys of the children at that node.
func (sm *SubTrie) TopicsForPeer(peerID peer.ID, path string) []string {
	sm.mtx.RLock()
	defer sm.mtx.RUnlock()

	peerTrie, ok := sm.peerTries[peerID]
	if !ok {
		return nil
	}

	node := peerTrie
	if path != "" {
		parts := strings.Split(path, ":")
		for _, part := range parts {
			if node.subTopics == nil {
				return nil
			}
			child, exists := node.subTopics[part]
			if !exists {
				return nil
			}
			node = child
		}
	}

	if node.subTopics == nil {
		return nil
	}

	topics := make([]string, 0, len(node.subTopics))
	for topic := range node.subTopics {
		topics = append(topics, topic)
	}

	return topics
}

// SubscribePeer parses each topic by the ":" delimiter, adds the topics to the peer's
// personal trie, and synchronizes by adding the peerID to the subscribers set
// at the corresponding nodes in the global topic trie. Returns the list of newly added topics.
func (sm *SubTrie) SubscribePeer(peerID peer.ID, topics []string) (added []string) {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	peerTrie, ok := sm.peerTries[peerID]
	if !ok {
		peerTrie = newTopicNode()
		sm.peerTries[peerID] = peerTrie
	}

	for _, topic := range topics {
		if topic == "" {
			continue
		}
		parts := strings.Split(topic, ":")

		// 1. Add to the peer's personal trie
		uNode := peerTrie
		for _, part := range parts {
			if uNode.subTopics == nil {
				uNode.subTopics = make(map[string]*topicNode)
			}
			child, exists := uNode.subTopics[part]
			if !exists {
				child = newTopicNode()
				uNode.subTopics[part] = child
			}
			uNode = child
		}

		if uNode.isEnd {
			// Already subscribed, skip global trie synchronization
			continue
		}
		uNode.isEnd = true
		added = append(added, topic)

		// 2. Add to the global topic trie
		gNode := sm.globalTrie
		for _, part := range parts {
			if gNode.children == nil {
				gNode.children = make(map[string]*trie)
			}
			child, exists := gNode.children[part]
			if !exists {
				child = newTrie()
				gNode.children[part] = child
			}
			gNode = child
		}
		if gNode.subscribers == nil {
			gNode.subscribers = make(map[peer.ID]struct{})
		}
		gNode.subscribers[peerID] = struct{}{}
	}
	return added
}

// UnsubscribePeer parses each topic by the ":" delimiter, removes the topics from the peer's
// personal trie, and synchronizes by removing the peerID from the subscribers set
// at the corresponding nodes in the global topic trie. It actively prunes empty zombie nodes.
// Returns the list of topics that were successfully unsubscribed.
func (sm *SubTrie) UnsubscribePeer(peerID peer.ID, topics []string) (unsubbed []string) {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	peerTrie, ok := sm.peerTries[peerID]
	if !ok {
		return nil
	}
	// Helper for bottom-up pruning of personal trie
	var prunePeerTrie func(node *topicNode, parts []string, depth int) (keep bool, wasSubbed bool)
	prunePeerTrie = func(node *topicNode, parts []string, depth int) (bool, bool) {
		if depth == len(parts) {
			wasSubbed := node.isEnd
			node.isEnd = false
			return len(node.subTopics) > 0, wasSubbed
		}
		part := parts[depth]
		child, exists := node.subTopics[part]
		if !exists {
			return len(node.subTopics) > 0 || node.isEnd, false
		}

		keepChild, subbed := prunePeerTrie(child, parts, depth+1)
		if !keepChild {
			delete(node.subTopics, part)
		}

		return len(node.subTopics) > 0 || node.isEnd, subbed
	}

	// Helper for bottom-up pruning of global trie
	var pruneGlobalTrie func(node *trie, parts []string, depth int) bool
	pruneGlobalTrie = func(node *trie, parts []string, depth int) bool {
		if depth == len(parts) {
			if node.subscribers != nil {
				delete(node.subscribers, peerID)
			}
			return len(node.children) > 0 || len(node.subscribers) > 0
		}
		part := parts[depth]
		child, exists := node.children[part]
		if !exists {
			return len(node.children) > 0 || len(node.subscribers) > 0
		}

		keep := pruneGlobalTrie(child, parts, depth+1)
		if !keep {
			delete(node.children, part)
		}

		return len(node.children) > 0 || len(node.subscribers) > 0
	}

	for _, topic := range topics {
		if topic == "" {
			continue
		}
		parts := strings.Split(topic, ":")

		_, wasSubbed := prunePeerTrie(peerTrie, parts, 0)
		if wasSubbed {
			pruneGlobalTrie(sm.globalTrie, parts, 0)
			unsubbed = append(unsubbed, topic)
		}
	}

	// Clean up the peer's overall state if they have zero active subscriptions remaining
	if len(peerTrie.subTopics) == 0 && !peerTrie.isEnd {
		delete(sm.peerTries, peerID)
	}

	return unsubbed
}

// Subscribers returns all subscribers for each of the provided topics.
func (sm *SubTrie) Subscribers(topics []string) map[string][]peer.ID {
	sm.mtx.RLock()
	defer sm.mtx.RUnlock()

	result := make(map[string][]peer.ID, len(topics))
	for _, topic := range topics {
		if topic == "" {
			continue
		}
		parts := strings.Split(topic, ":")
		gNode := sm.globalTrie
		exactMatch := true
		for _, part := range parts {
			if gNode.children == nil {
				exactMatch = false
				break
			}
			child, exists := gNode.children[part]
			if !exists {
				exactMatch = false
				break
			}
			gNode = child
		}
		if exactMatch && gNode.subscribers != nil {
			result[topic] = slices.Collect(maps.Keys(gNode.subscribers))
		}
	}
	return result
}

// RemovePeer completely removes a peer's subscriptions by traversing their personal
// trie to identify fully qualified topics, removes their ID from the corresponding
// global topic trie nodes, and deletes the peer's personal trie to free memory.
func (sm *SubTrie) RemovePeer(peerID peer.ID) {
	sm.mtx.Lock()
	defer sm.mtx.Unlock()

	peerTrie, ok := sm.peerTries[peerID]
	if !ok {
		return
	}

	// Remove the peer from matching global trie nodes and prune empty branches.
	var pruneGlobal func(peerNode *topicNode, globalNode *trie) bool
	pruneGlobal = func(peerNode *topicNode, globalNode *trie) bool {
		if peerNode.isEnd && globalNode.subscribers != nil {
			delete(globalNode.subscribers, peerID)
		}

		for part, peerChild := range peerNode.subTopics {
			if globalNode.children == nil {
				continue
			}
			globalChild, exists := globalNode.children[part]
			if !exists {
				continue
			}
			if keepChild := pruneGlobal(peerChild, globalChild); !keepChild {
				delete(globalNode.children, part)
			}
		}

		return len(globalNode.children) > 0 || len(globalNode.subscribers) > 0
	}

	pruneGlobal(peerTrie, sm.globalTrie)

	delete(sm.peerTries, peerID)
}

// SearchTopics takes a list of namespaced topics and returns a list of all topics
// that are an exact match or a child of one of the topics in the input list, without duplicates.
func (sm *SubTrie) SearchTopics(filters []string) []string {
	sm.mtx.RLock()
	defer sm.mtx.RUnlock()

	seen := make(map[string]struct{})
	results := make([]string, 0)

	add := func(topic string) {
		if topic == "" {
			return
		}
		if _, exists := seen[topic]; exists {
			return
		}
		seen[topic] = struct{}{}
		results = append(results, topic)
	}

	for _, filter := range filters {
		if filter == "" {
			continue
		}

		node := sm.globalTrie
		parts := strings.Split(filter, ":")
		matched := make([]string, 0, len(parts))

		ok := true
		for _, part := range parts {
			child, exists := node.children[part]
			if !exists {
				ok = false
				break
			}
			node = child
			matched = append(matched, part)
		}
		if !ok {
			continue
		}

		rootTopic := strings.Join(matched, ":")
		walkTrie(node, rootTopic, func(topic string, _ *trie) {
			add(topic)
		})
	}

	return results
}

func walkTrie(node *trie, topic string, f func(topic string, node *trie)) {
	f(topic, node)
	for part, child := range node.children {
		childTopic := topic
		if childTopic != "" {
			childTopic += ":"
		}
		childTopic += part
		walkTrie(child, childTopic, f)
	}
}
