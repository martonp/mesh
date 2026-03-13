package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bisoncraft/mesh/bond"
	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"

	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	defaultHost = "0.0.0.0"
)

var (
	// ErrRedundantSubscription indicates a redundant subscription has been made.
	ErrRedundantSubscription = errors.New("redundant subscription")
	// ErrRedundantUnsubscription indicates an unsubscription from a topic that is not subscribed.
	ErrRedundantUnsubscription = errors.New("redundant unsubscription")
	// errEmptyTopic indicates an empty topic name was provided.
	errEmptyTopic = errors.New("topic name cannot be empty")
	// errNoMeshConnection indicates that there is no mesh connection established.
	errNoMeshConnection = errors.New("no mesh connection established")
)

// Config represents a tatanka client configuration.
type Config struct {
	Host            string
	Port            int
	PrivateKey      crypto.PrivKey
	RemotePeerAddrs []string
	Logger          slog.Logger
	Bonds           []*bond.BondParams
}

// Client represents a tatanka client.
type Client struct {
	cfg           *Config
	host          host.Host
	topicRegistry *topicRegistry
	bondInfo      *bond.BondInfo
	log           slog.Logger
	connFactory   meshConnFactory
	connManager   *meshConnectionManager
}

// PeerID returns the peer ID of the client. If the client has not yet been
// initialized, an empty string is returned.
func (c *Client) PeerID() peer.ID {
	if c.host == nil {
		return ""
	}
	return c.host.ID()
}

// ConnectedTatankaNodePeerID returns the peer ID of the currently connected
// tatanka node. If no tatanka node connection is active, an empty string
// is returned.
func (c *Client) ConnectedTatankaNodePeerID() string {
	mc, err := c.primaryMeshConnection()
	if err != nil {
		return ""
	}
	return mc.remotePeerID().String()
}

// NewClient initializes a new tatanka client.
func NewClient(cfg *Config) (*Client, error) {
	c := &Client{
		cfg:           cfg,
		topicRegistry: newTopicRegistry(),
		log:           cfg.Logger,
	}

	bi := bond.NewBondInfo()
	bi.AddBonds(cfg.Bonds, time.Now())
	c.bondInfo = bi

	// Add a placeholder bond to validate client. Remove once bond pipeline is fully implemented.
	c.bondInfo.AddBonds([]*bond.BondParams{}, time.Now())

	if c.cfg.Host == "" {
		c.cfg.Host = defaultHost
	}

	return c, nil
}

func (c *Client) primaryMeshConnection() (meshConn, error) {
	if c.connManager == nil {
		return nil, errNoMeshConnection
	}
	return c.connManager.primaryConnection()
}

// Broadcast publishes the provided message bytes on a mesh topic.
func (c *Client) Broadcast(ctx context.Context, topic string, data []byte) error {
	if topic == "" {
		return errEmptyTopic
	}

	if data == nil {
		return errors.New("data cannot be nil")
	}

	mc, err := c.primaryMeshConnection()
	if err != nil {
		return fmt.Errorf("failed to broadcast message: %w", err)
	}

	return mc.broadcast(ctx, topic, data)
}

// Topics returns the list of topics the client is currently subscribed to.
func (c *Client) Topics() []string {
	return c.topicRegistry.fetchTopics()
}

// Subscribe subscribes the client to the provided topic.
func (c *Client) Subscribe(ctx context.Context, topics []string, handlerFunc TopicHandler) error {
	if len(topics) == 0 {
		return errors.New("no topics provided")
	}
	for _, topic := range topics {
		if topic == "" {
			return errEmptyTopic
		}
	}

	if handlerFunc == nil {
		return errors.New("handler function cannot be nil")
	}

	mc, err := c.primaryMeshConnection()
	if err != nil {
		return err
	}

	for _, topic := range topics {
		if !c.topicRegistry.register(topic, handlerFunc) {
			return ErrRedundantSubscription
		}
	}

	err = mc.subscribe(ctx, topics)
	if err != nil {
		// Unregister the topic if subscription fails.
		for _, topic := range topics {
			if !c.topicRegistry.unregister(topic) {
				c.log.Warnf("Failed to unregister topic %s", topic)
			}
		}

		return err
	}

	return nil
}

// Unsubscribe unsubscribes the client from the provided topic.
func (c *Client) Unsubscribe(ctx context.Context, topic string) error {
	if topic == "" {
		return errEmptyTopic
	}

	mc, err := c.primaryMeshConnection()
	if err != nil {
		return err
	}

	// Fetch the handler for the topic before unregistering.
	topicHandler, err := c.topicRegistry.fetchHandler(topic)
	if err != nil {
		return ErrRedundantUnsubscription
	}

	if !c.topicRegistry.unregister(topic) {
		return ErrRedundantUnsubscription
	}

	err = mc.unsubscribe(ctx, []string{topic})
	if err != nil {
		// Re-register the topic if unsubscription fails.
		if !c.topicRegistry.register(topic, topicHandler) {
			c.log.Warnf("Failed to re-register topic %s", topic)
		}

		return err
	}

	return nil
}

// handlePushMessage processes incoming pushed messages. The messages are processed based on their topics.
func (c *Client) handlePushMessage(msg *protocolsPb.PushMessage) {
	handlerFunc, err := c.topicRegistry.fetchHandler(msg.Topic)
	if err != nil {
		c.log.Warnf("Failed to fetch handler for topic %s: %v", msg.Topic, err)
		return
	}

	event := TopicEvent{
		Peer: peer.ID(msg.GetSender()),
		Type: TopicEventData,
	}

	switch msg.GetMessageType() {
	case protocolsPb.PushMessage_BROADCAST:
		event.Type = TopicEventData
		event.Data = msg.GetData()
	case protocolsPb.PushMessage_SUBSCRIBE:
		event.Type = TopicEventPeerSubscribed
	case protocolsPb.PushMessage_UNSUBSCRIBE:
		event.Type = TopicEventPeerUnsubscribed
	default:
		c.log.Warnf("Received push message with unknown type %v on topic %s", msg.GetMessageType(), msg.Topic)
		return
	}

	handlerFunc(msg.Topic, event)
}

// PostAllBonds posts the client's bonds.
func (c *Client) PostAllBonds(ctx context.Context) error {
	mc, err := c.primaryMeshConnection()
	if err != nil {
		return err
	}
	return mc.postAllBonds(ctx)
}

// AddBonds adds the provided bond parameters to the client.
func (c *Client) AddBonds(ctx context.Context, bonds []*bond.BondParams) error {
	mc, err := c.primaryMeshConnection()
	if err != nil {
		return err
	}

	// Client and *meshConnection share a *BondInfo. The bond will be added to the
	// *BondInfo by *meshConnection.
	return mc.addBonds(ctx, bonds)
}

// SearchTopics searches for topics on mesh. Topics names are hierarchical, of
// the form <level1>:<level2>:...:<levelN>. You can request all the subtopics
// of a level by specifying just the level name. For example, if you have topics
// "a:b:c" and "a:b:d", and you search for "a:b", you will get both "a:b:c" and
// "a:b:d".
func (c *Client) SearchTopics(ctx context.Context, filters []string) ([]string, error) {
	mc, err := c.primaryMeshConnection()
	if err != nil {
		return nil, err
	}
	return mc.searchTopics(ctx, filters)
}

// newPeerStore parses a list of multiaddr strings into a peerstore.
// Multiple addresses for the same peer ID are combined into a single AddrInfo.
func newPeerStore(addrs []string) (peerstore.Peerstore, error) {
	peerMap := make(map[peer.ID]*peer.AddrInfo)

	for _, addrStr := range addrs {
		maddr, err := ma.NewMultiaddr(addrStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse address %q: %w", addrStr, err)
		}
		info, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse peer info from %q: %w", addrStr, err)
		}

		if existing, ok := peerMap[info.ID]; ok {
			existing.Addrs = append(existing.Addrs, info.Addrs...)
		} else {
			peerMap[info.ID] = info
		}
	}

	ps, err := pstoremem.NewPeerstore()
	if err != nil {
		return nil, fmt.Errorf("NewPeerstore error: %v", err)
	}

	for _, info := range peerMap {
		ps.AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)
	}
	return ps, nil
}

// Run starts the mesh client.
func (c *Client) Run(ctx context.Context) error {
	listenAddr := fmt.Sprintf("/ip4/%s/tcp/%d", c.cfg.Host, c.cfg.Port)

	if c.cfg.PrivateKey == nil {
		return fmt.Errorf("no private key provided for client")
	}

	ps, err := newPeerStore(c.cfg.RemotePeerAddrs)
	if err != nil {
		return fmt.Errorf("failed to create peerstore: %w", err)
	}

	c.host, err = libp2p.New(libp2p.ListenAddrStrings(listenAddr), libp2p.Identity(c.cfg.PrivateKey), libp2p.Peerstore(ps))
	if err != nil {
		return fmt.Errorf("failed to create host: %w", err)
	}
	defer func() { _ = c.host.Close() }()

	// Set default connection factory if not already set (tests may override).
	if c.connFactory == nil {
		c.connFactory = func(peerID peer.ID) meshConn {
			return newMeshConnection(c.host, peerID, c.log, c.bondInfo, c.topicRegistry.fetchTopics, c.handlePushMessage)
		}
	}

	// Create the connection manager.
	c.connManager = newMeshConnectionManager(&meshConnectionManagerConfig{
		host:        c.host,
		log:         c.log,
		connFactory: c.connFactory,
	})

	c.connManager.run(ctx)

	return nil
}
