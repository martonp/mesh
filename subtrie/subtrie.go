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
		parts := strings.Split(topic, ":")
		gNode := sm.globalTrie
		for _, part := range parts {
			if gNode.children == nil {
				break
			}
			child, exists := gNode.children[part]
			if !exists {
				break
			}
			gNode = child
		}
		if gNode.subscribers != nil {
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

	// Helper for DFS traversal to find all subscribed paths for the peer
	var dfs func(node *topicNode, path []string)
	dfs = func(node *topicNode, path []string) {
		if node.isEnd {
			// Find this path in the global topic trie to remove the peerID
			gNode := sm.globalTrie
			validPath := true
			for _, part := range path {
				if gNode.children == nil {
					validPath = false
					break
				}
				child, exists := gNode.children[part]
				if !exists {
					validPath = false
					break
				}
				gNode = child
			}

			// Remove the peerID if the global node exists
			if validPath && gNode.subscribers != nil {
				delete(gNode.subscribers, peerID)
			}
		}

		// Continue traversing down the tree
		if node.subTopics != nil {
			for part, child := range node.subTopics {
				// Passing the appended path for deep recursive checks
				dfs(child, append(path, part))
			}
		}
	}

	// 1. Traverse and remove the peer from the global topic trie.
	dfs(peerTrie, nil)

	// 2. Delete the peer from the personal tries map.
	delete(sm.peerTries, peerID)
}

// SearchTopics takes a list of namespaced topics and returns a list of all topics
// that are an exact match or a child of one of the topics in the input list, without duplicates.
func (sm *SubTrie) SearchTopics(topics []string) []string {
	sm.mtx.RLock()
	defer sm.mtx.RUnlock()

	// Grab only the root of hierarchical topics with matching non-zero root.
	topicFilters := make([]string, 0, len(topics))
nexttopic: // skip duplicate topics
	for _, topic := range topics {
		for i, filteredTopic := range topicFilters {
			if filteredTopic == "" {
				topicFilters = []string{""}
				break nexttopic
			}
			parts, fParts := strings.Split(topic, ":"), strings.Split(filteredTopic, ":")
			matchParts := make([]string, 0, 1)
			for j := 0; j < len(parts) && j < len(fParts); j++ {
				if parts[j] == fParts[j] {
					matchParts = append(matchParts, parts[j])
				} else {
					break
				}
			}
			if len(matchParts) > 0 {
				// Either a duplicate or a shared root.
				topicFilters[i] = strings.Join(matchParts, ":")
				continue nexttopic
			}
		}
		// no matching root
		topicFilters = append(topicFilters, topic)
	}

	results := make([]string, 0, len(topics))

	for _, filter := range topicFilters {
		if filter == "" {
			continue
		}
		parts := strings.Split(filter, ":")
		root := sm.globalTrie
		rootParts := make([]string, 0, len(parts))
		for _, part := range parts {
			if child, exists := root.children[part]; exists {
				rootParts = append(rootParts, part)
				root = child
			} else {
				break
			}
		}
		rootTopic := strings.Join(rootParts, ":")
		if rootTopic == "" && filter != "" {
			// filter is not a prefix of any topic
			continue
		}

		walkTrie(root, rootTopic, func(topic string, _ *trie) {
			if topic == "" { // Don't include the global root
				return
			}
			results = append(results, topic)
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
