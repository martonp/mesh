package tatanka

import (
	"context"
	"errors"
	"fmt"

	"github.com/bisoncraft/mesh/oracle"
	"github.com/bisoncraft/mesh/oracle/sources"
	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	pb "github.com/bisoncraft/mesh/tatanka/pb"
	"github.com/bisoncraft/mesh/tatanka/types"
	"github.com/decred/slog"
	"github.com/klauspost/compress/zstd"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

const (
	// clientMessageTopicName is the name of the pubsub topic for client messages.
	clientMessageTopicName = "client_messages"

	// clientConnectionsTopicName is the name of the pubsub topic used to
	// propagate client connection and disconnection messages between tatanka
	// nodes.
	clientConnectionsTopicName = "client_connections"

	// oracleUpdatesTopicName is the name of the pubsub topic used to
	// propagate oracle updates between tatanka nodes.
	oracleUpdatesTopicName = "oracle_updates"

	// quotaHeartbeatTopicName is the name of the pubsub topic used to
	// periodically share quota information between tatanka nodes.
	quotaHeartbeatTopicName = "quota_heartbeat"

	// whitelistUpdatesTopicName is the pubsub topic for whitelist updates.
	whitelistUpdatesTopicName = "whitelist_updates"
)

type clientConnectionUpdate struct {
	clientID   peer.ID
	reporterID peer.ID
	timestamp  int64
	connected  bool
}

func newClientConnectionUpdateFromPb(msg *pb.ClientConnectionMsg) (*clientConnectionUpdate, error) {
	clientID, err := peer.IDFromBytes(msg.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to parse client ID: %w", err)
	}

	reporterID, err := peer.IDFromBytes(msg.ReporterId)
	if err != nil {
		return nil, fmt.Errorf("failed to parse reporter ID: %w", err)
	}

	return &clientConnectionUpdate{
		clientID:   clientID,
		reporterID: reporterID,
		timestamp:  msg.Timestamp,
		connected:  msg.Connected,
	}, nil
}

func (c *clientConnectionUpdate) toPb() *pb.ClientConnectionMsg {
	return &pb.ClientConnectionMsg{
		Id:         []byte(c.clientID),
		ReporterId: []byte(c.reporterID),
		Timestamp:  c.timestamp,
		Connected:  c.connected,
	}
}

type gossipSubCfg struct {
	node                          host.Host
	log                           slog.Logger
	getWhitelistPeers             func() map[peer.ID]struct{}
	handleBroadcastMessage        func(msg *protocolsPb.PushMessage)
	handleClientConnectionMessage func(update *clientConnectionUpdate)
	handleOracleUpdate            func(senderID peer.ID, update *pb.NodeOracleUpdate)
	handleQuotaHeartbeat          func(senderID peer.ID, heartbeat *pb.QuotaHandshake)
	handleWhitelistUpdate         func(senderID peer.ID, ws *types.WhitelistState, timestamp int64)
}

// gossipSub manages the nodes connection to a gossip sub network between tatanka
// nodes. This network is used to gossip all client broadcast messages,
// client connections, and oracle updates.
type gossipSub struct {
	log                    slog.Logger
	ps                     *pubsub.PubSub
	cfg                    *gossipSubCfg
	clientMessageTopic     *pubsub.Topic
	clientConnectionsTopic *pubsub.Topic
	oracleUpdatesTopic     *pubsub.Topic
	quotaHeartbeatTopic    *pubsub.Topic
	whitelistUpdatesTopic  *pubsub.Topic
	zstdEncoder            *zstd.Encoder
	zstdDecoder            *zstd.Decoder
}

func newGossipSub(ctx context.Context, cfg *gossipSubCfg) (*gossipSub, error) {
	peerFilter := func(pid peer.ID, topic string) bool {
		_, ok := cfg.getWhitelistPeers()[pid]
		return ok
	}

	ps, err := pubsub.NewGossipSub(ctx, cfg.node,
		pubsub.WithPeerFilter(peerFilter),
		// FloodPublish optimizes for quick delivery at the cost of duplicates.
		pubsub.WithFloodPublish(true),
	)
	if err != nil {
		return nil, err
	}

	clientMessageTopic, err := ps.Join(clientMessageTopicName)
	if err != nil {
		return nil, fmt.Errorf("failed to join client message topic: %w", err)
	}

	clientConnectionsTopic, err := ps.Join(clientConnectionsTopicName)
	if err != nil {
		return nil, fmt.Errorf("failed to join client connections topic: %w", err)
	}

	oracleUpdatesTopic, err := ps.Join(oracleUpdatesTopicName)
	if err != nil {
		return nil, fmt.Errorf("failed to join oracle updates topic: %w", err)
	}

	quotaHeartbeatTopic, err := ps.Join(quotaHeartbeatTopicName)
	if err != nil {
		return nil, fmt.Errorf("failed to join quota heartbeat topic: %w", err)
	}

	whitelistUpdatesTopic, err := ps.Join(whitelistUpdatesTopicName)
	if err != nil {
		return nil, fmt.Errorf("failed to join whitelist updates topic: %w", err)
	}

	zstdEncoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
	}

	zstdDecoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}

	return &gossipSub{
		log:                    cfg.log,
		ps:                     ps,
		cfg:                    cfg,
		clientMessageTopic:     clientMessageTopic,
		clientConnectionsTopic: clientConnectionsTopic,
		oracleUpdatesTopic:     oracleUpdatesTopic,
		quotaHeartbeatTopic:    quotaHeartbeatTopic,
		whitelistUpdatesTopic:  whitelistUpdatesTopic,
		zstdEncoder:            zstdEncoder,
		zstdDecoder:            zstdDecoder,
	}, nil
}

// listenForClientMessages subscribes to the pubsub client messages topic, and
// distributes messages to subscribed clients as they come in.
func (gs *gossipSub) listenForClientMessages(ctx context.Context) error {
	sub, err := gs.clientMessageTopic.Subscribe()
	if err != nil {
		return fmt.Errorf("failed to subscribe to client message topic: %w", err)
	}

	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		if msg != nil {
			pushMessage := &protocolsPb.PushMessage{}
			if err := proto.Unmarshal(msg.Data, pushMessage); err != nil {
				gs.log.Errorf("Failed to unmarshal push message: %v", err)
				continue
			}

			gs.cfg.handleBroadcastMessage(pushMessage)
		}
	}
}

func (gs *gossipSub) listenForClientConnections(ctx context.Context) error {
	sub, err := gs.clientConnectionsTopic.Subscribe()
	if err != nil {
		return fmt.Errorf("failed to subscribe to client connections topic: %w", err)
	}

	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		if msg != nil {
			connMsg := &pb.ClientConnectionMsg{}
			if err := proto.Unmarshal(msg.Data, connMsg); err != nil {
				gs.log.Errorf("Failed to unmarshal client connection message: %v", err)
				continue
			}

			update, err := newClientConnectionUpdateFromPb(connMsg)
			if err != nil {
				gs.log.Errorf("Failed to create client connection update from message: %v", err)
				continue
			}

			gs.cfg.handleClientConnectionMessage(update)
		}
	}
}

func (gs *gossipSub) listenForOracleUpdates(ctx context.Context) error {
	sub, err := gs.oracleUpdatesTopic.Subscribe()
	if err != nil {
		return fmt.Errorf("failed to subscribe to oracle updates topic: %w", err)
	}

	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		if msg != nil {
			decompressed, err := gs.zstdDecoder.DecodeAll(msg.Data, nil)
			if err != nil {
				gs.log.Errorf("Failed to decompress oracle update: %v", err)
				continue
			}

			oracleUpdate := &pb.NodeOracleUpdate{}
			if err := proto.Unmarshal(decompressed, oracleUpdate); err != nil {
				gs.log.Errorf("Failed to unmarshal oracle update: %v", err)
				continue
			}

			gs.cfg.handleOracleUpdate(msg.GetFrom(), oracleUpdate)
		}
	}
}

func (gs *gossipSub) publishClientMessage(ctx context.Context, msg *protocolsPb.PushMessage) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal client push message: %w", err)
	}
	return gs.clientMessageTopic.Publish(ctx, data)
}

func (gs *gossipSub) publishClientConnectionMessage(ctx context.Context, msg *clientConnectionUpdate) error {
	data, err := proto.Marshal(msg.toPb())
	if err != nil {
		return fmt.Errorf("failed to marshal client connection message: %w", err)
	}

	return gs.clientConnectionsTopic.Publish(ctx, data)
}

func (gs *gossipSub) publishOracleUpdate(ctx context.Context, update *oracle.OracleUpdate) error {
	pbUpdate, err := oracleUpdateToPb(update)
	if err != nil {
		return err
	}

	data, err := proto.Marshal(pbUpdate)
	if err != nil {
		return fmt.Errorf("failed to marshal oracle update: %w", err)
	}

	compressed := gs.zstdEncoder.EncodeAll(data, nil)

	return gs.oracleUpdatesTopic.Publish(ctx, compressed)
}

func (gs *gossipSub) listenForQuotaHeartbeats(ctx context.Context) error {
	sub, err := gs.quotaHeartbeatTopic.Subscribe()
	if err != nil {
		return fmt.Errorf("failed to subscribe to quota heartbeat topic: %w", err)
	}

	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		if msg != nil && gs.cfg.handleQuotaHeartbeat != nil {
			heartbeat := &pb.QuotaHandshake{}
			if err := proto.Unmarshal(msg.Data, heartbeat); err != nil {
				gs.log.Errorf("Failed to unmarshal quota heartbeat: %v", err)
				continue
			}
			gs.cfg.handleQuotaHeartbeat(msg.GetFrom(), heartbeat)
		}
	}
}

func (gs *gossipSub) listenForWhitelistUpdates(ctx context.Context) error {
	sub, err := gs.whitelistUpdatesTopic.Subscribe()
	if err != nil {
		return fmt.Errorf("failed to subscribe to whitelist updates topic: %w", err)
	}

	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		if msg != nil && gs.cfg.handleWhitelistUpdate != nil {
			update := &pb.WhitelistState{}
			if err := proto.Unmarshal(msg.Data, update); err != nil {
				gs.log.Errorf("Failed to unmarshal whitelist update: %v", err)
				continue
			}
			gs.cfg.handleWhitelistUpdate(msg.GetFrom(), pbToWhitelistState(update), update.Timestamp)
		}
	}
}

func (gs *gossipSub) publishQuotaHeartbeat(ctx context.Context, quotas map[string]*sources.QuotaStatus) error {
	heartbeat := &pb.QuotaHandshake{
		Quotas: quotaStatusesToPb(quotas),
	}
	data, err := proto.Marshal(heartbeat)
	if err != nil {
		return fmt.Errorf("failed to marshal quota heartbeat: %w", err)
	}
	return gs.quotaHeartbeatTopic.Publish(ctx, data)
}

func (gs *gossipSub) publishWhitelistUpdate(ctx context.Context, update *pb.WhitelistState) error {
	data, err := proto.Marshal(update)
	if err != nil {
		return fmt.Errorf("failed to marshal whitelist update: %w", err)
	}
	return gs.whitelistUpdatesTopic.Publish(ctx, data)
}

func (gs *gossipSub) run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		err := gs.listenForClientMessages(ctx)
		gs.log.Debug("Client broadcast messages listener stopped.")
		return err
	})

	g.Go(func() error {
		err := gs.listenForClientConnections(ctx)
		gs.log.Debug("Client connection listener stopped.")
		return err
	})

	g.Go(func() error {
		err := gs.listenForOracleUpdates(ctx)
		gs.log.Debug("Oracle updates listener stopped.")
		return err
	})

	g.Go(func() error {
		err := gs.listenForQuotaHeartbeats(ctx)
		gs.log.Debug("Quota heartbeat listener stopped.")
		return err
	})

	g.Go(func() error {
		err := gs.listenForWhitelistUpdates(ctx)
		gs.log.Debug("Whitelist proposals listener stopped.")
		return err
	})

	return g.Wait()
}
