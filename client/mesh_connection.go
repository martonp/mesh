package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/bisoncraft/mesh/bond"
	"github.com/bisoncraft/mesh/codec"
	"github.com/bisoncraft/mesh/protocols"
	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"
)

const (
	maxPostBondRetries = 3
)

var (
	errInvalidBondIndex = errors.New("invalid bond index")
)

// meshConnection represents a connection to a mesh peer.
type meshConnection struct {
	peerID        peer.ID
	host          host.Host
	handleMessage func(*protocolsPb.PushMessage)
	fetchTopics   func() []string
	bondInfo      *bond.BondInfo
	log           slog.Logger

	cancelFuncMtx sync.RWMutex
	cancelFunc    context.CancelFunc

	readyCh   chan struct{}
	readyOnce sync.Once
}

var _ meshConn = (*meshConnection)(nil)

func newMeshConnection(host host.Host, peerID peer.ID, logger slog.Logger, bondInfo *bond.BondInfo, fetchTopics func() []string, handleMessage func(*protocolsPb.PushMessage)) *meshConnection {
	return &meshConnection{
		host:          host,
		peerID:        peerID,
		log:           logger,
		handleMessage: handleMessage,
		fetchTopics:   fetchTopics,
		bondInfo:      bondInfo,
		readyCh:       make(chan struct{}, 1),
	}
}

func (m *meshConnection) remotePeerID() peer.ID {
	return m.peerID
}

func (m *meshConnection) waitReady(ctx context.Context) {
	select {
	case <-m.readyCh:
		return
	case <-ctx.Done():
		return
	}
}

// markReady signals that the connection attempt completed. Sends nil on success
// or the error on failure. Only the first call has effect.
func (m *meshConnection) markReady() {
	m.readyOnce.Do(func() {
		close(m.readyCh)
	})
}

// run starts the mesh connection and maintains it until the context is done.
func (m *meshConnection) run(ctx context.Context) error {
	if m.handleMessage == nil {
		return fmt.Errorf("%s: no handler function provided", m.host.ID())
	}

	err := m.host.Connect(ctx, peer.AddrInfo{ID: m.peerID})
	if err != nil {
		return fmt.Errorf("failed to connect to peer %s: %w", m.peerID, err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	m.cancelFuncMtx.Lock()
	m.cancelFunc = cancel
	m.cancelFuncMtx.Unlock()

	if m.bondInfo.BondStrength() > 0 {
		// Post the bonds associated with the connection.The mesh connection is required to have
		// adequate bond strength in order to establish a push stream.
		if err := m.postAllBonds(runCtx); err != nil {
			return err
		}
	}

	return m.runPushStream(runCtx)
}

// subscribe subscribes to the provided topic.
func (m *meshConnection) subscribe(ctx context.Context, topics []string) error {
	s, err := m.host.NewStream(ctx, m.peerID, protocols.ClientSubscribeProtocol)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}
	defer func() { _ = s.Close() }()

	req := &protocolsPb.SubscribeRequest{Topics: topics}
	if err := codec.WriteLengthPrefixedMessage(s, req); err != nil {
		return fmt.Errorf("failed to write subscribe request: %w", err)
	}

	resp := &protocolsPb.Success{}
	if err := codec.ReadLengthPrefixedMessage(s, resp); err != nil {
		return fmt.Errorf("failed to read subscribe response: %w", err)
	}

	return nil
}

// broadcast publishes the provided message bytes on a mesh topic.
func (m *meshConnection) broadcast(ctx context.Context, topic string, data []byte) error {
	if m.bondInfo.BondStrength() < bond.MinRequiredBondStrength {
		return fmt.Errorf("%s: connection does not have the minimum required bond strength to publish", m.host.ID())
	}
	s, err := m.host.NewStream(ctx, m.peerID, protocols.ClientPublishProtocol)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}
	defer func() { _ = s.Close() }()

	req := &protocolsPb.PublishRequest{
		Topic: topic,
		Data:  data,
	}
	return codec.WriteLengthPrefixedMessage(s, req)
}

// unsubscribe unsubscribes from the provided topic.
func (m *meshConnection) unsubscribe(ctx context.Context, topics []string) error {
	s, err := m.host.NewStream(ctx, m.peerID, protocols.ClientSubscribeProtocol)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}
	defer func() { _ = s.Close() }()

	req := &protocolsPb.SubscribeRequest{Topics: topics}
	if err := codec.WriteLengthPrefixedMessage(s, req); err != nil {
		return fmt.Errorf("failed to write unsubscribe request: %w", err)
	}

	resp := &protocolsPb.Success{}
	if err := codec.ReadLengthPrefixedMessage(s, resp); err != nil {
		return fmt.Errorf("failed to read unsubscribe response: %w", err)
	}

	return nil
}

// postBondsInternal posts the provided bond request.
func (m *meshConnection) postBondsInternal(ctx context.Context, req *protocolsPb.PostBondRequest) error {
	// Post the provided bond.
	s, err := m.host.NewStream(ctx, m.peerID, protocols.PostBondsProtocol)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}
	defer func() { _ = s.Close() }()

	err = codec.WriteLengthPrefixedMessage(s, req)
	if err != nil {
		return fmt.Errorf("failed to write post bond request: %w", err)
	}

	// Process the post bond response.
	hostID := m.host.ID()
	resp := &protocolsPb.Response{}
	err = codec.ReadLengthPrefixedMessage(s, resp)
	if err != nil {
		return fmt.Errorf("failed to read message bytes: %w", err)
	}

	switch v := resp.Response.(type) {
	case *protocolsPb.Response_Error:
		switch v.Error.GetError().(type) {
		case *protocolsPb.Error_PostBondError:
			bondErr := v.Error.GetPostBondError()
			idx := int(bondErr.InvalidBondIndex)
			if idx >= len(req.Bonds) {
				return fmt.Errorf("invalid bond index %d in post bond error", bondErr.InvalidBondIndex)
			}
			invalidBond := string(req.Bonds[idx].BondID)
			m.log.Infof("%s rejected bond %s at request index %d", hostID, invalidBond, idx)
			return errInvalidBondIndex

		case *protocolsPb.Error_Message:
			errMsg := v.Error.GetMessage()
			return fmt.Errorf("%s: %v", hostID, errMsg)

		default:
			return fmt.Errorf("%s: unknown error type %T", hostID, v)
		}

	case *protocolsPb.Response_PostBondResponse:
		postedStrength := v.PostBondResponse.BondStrength
		m.log.Infof("posted %d bonds with strength %d to %s. new strength: %d",
			len(req.Bonds), postedStrength, hostID, m.bondInfo.BondStrength())
		return nil

	default:
		return fmt.Errorf("%s: unexpected response for post bond request: %T", hostID, v)
	}
}

// postAllBonds posts the connection's bond, retrying on invalid bond index errors. This needs to be
// called before the connection's push stream is established.
func (m *meshConnection) postAllBonds(ctx context.Context) error {
	hostID := m.host.ID()
	for range maxPostBondRetries {
		req, err := bond.PostBondReqFromBondInfo(m.bondInfo)
		if err != nil {
			return fmt.Errorf("failed to create post bond request: %w", err)
		}

		err = m.postBondsInternal(ctx, req)
		if err == nil {
			return nil
		}

		switch {
		case errors.Is(err, errInvalidBondIndex):
			// Retry if an invalid bond index error is returned.
			continue

		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			return fmt.Errorf("%s: post bond retry cancelled due to context: %w", hostID, err)

		default:
			return fmt.Errorf("%s: failed to post bond: %w", hostID, err)
		}
	}

	return fmt.Errorf("%s: maximum retry attempts for posting bond reached", hostID)
}

func (m *meshConnection) addBonds(ctx context.Context, bonds []*bond.BondParams) error {
	if len(bonds) == 0 {
		return fmt.Errorf("cannot add 0 bonds")
	}

	req := &protocolsPb.PostBondRequest{}
	for _, bp := range bonds {
		req.Bonds = append(req.Bonds, &protocolsPb.Bond{BondID: []byte(bp.ID)})
	}

	if err := m.postBondsInternal(ctx, req); err != nil {
		return fmt.Errorf("failed to post bonds: %w", err)
	}

	m.bondInfo.AddBonds(bonds, time.Now())

	return nil
}

// kill terminates the mesh connection.
func (m *meshConnection) kill() {
	m.cancelFuncMtx.RLock()
	defer m.cancelFuncMtx.RUnlock()

	if m.cancelFunc == nil {
		// Do nothing if the context cancellation is not set.
		return
	}

	m.log.Infof("%s: terminating mesh connection to peer with ID %s", m.host.ID(), m.peerID)
	m.cancelFunc()
}

// fetchAvailableMeshNodes fetches the list of available mesh nodes from the
// connected tatanka node.
func (m *meshConnection) fetchAvailableMeshNodes(ctx context.Context) ([]peer.AddrInfo, error) {
	s, err := m.host.NewStream(ctx, m.peerID, protocols.AvailableMeshNodesProtocol)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}
	defer func() { _ = s.Close() }()

	resp := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(s, resp); err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	switch v := resp.Response.(type) {
	case *protocolsPb.Response_AvailableMeshNodesResponse:
		meshNodesResp := v.AvailableMeshNodesResponse
		peers := make([]peer.AddrInfo, 0, len(meshNodesResp.Peers))

		for _, pbPeer := range meshNodesResp.Peers {
			peerID, err := peer.IDFromBytes(pbPeer.Id)
			if err != nil {
				m.log.Warnf("Failed to parse peer ID: %v", err)
				continue
			}

			addrs := make([]ma.Multiaddr, 0, len(pbPeer.Addrs))
			for _, addrBytes := range pbPeer.Addrs {
				addr, err := ma.NewMultiaddrBytes(addrBytes)
				if err != nil {
					m.log.Warnf("Failed to parse multiaddr: %v", err)
					continue
				}
				addrs = append(addrs, addr)
			}

			peers = append(peers, peer.AddrInfo{ID: peerID, Addrs: addrs})
		}

		return peers, nil

	case *protocolsPb.Response_Error:
		return nil, fmt.Errorf("error response: %v", v.Error)

	default:
		return nil, fmt.Errorf("unexpected response type: %T", v)
	}
}

func (m *meshConnection) searchTopics(ctx context.Context, filters []string) ([]string, error) {
	// Post the provided bond.
	s, err := m.host.NewStream(ctx, m.peerID, protocols.SearchTopicsProtocol)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}
	defer func() { _ = s.Close() }()

	req := &protocolsPb.SearchTopicsRequest{
		Filters: filters,
	}

	err = codec.WriteLengthPrefixedMessage(s, req)
	if err != nil {
		return nil, fmt.Errorf("failed to write search topics request: %w", err)
	}

	resp := &protocolsPb.SearchTopicsResponse{}
	if err := codec.ReadLengthPrefixedMessage(s, resp); err != nil {
		return nil, fmt.Errorf("failed to read search topics response: %w", err)
	}

	return resp.Topics, nil
}

// openPushStream creates a new push stream to the remote peer, sends the initial
// subscriptions, and waits for the server acknowledgement. Returns the stream on
// success or an error on failure.
func (m *meshConnection) openPushStream(ctx context.Context) (network.Stream, error) {
	stream, err := m.host.NewStream(ctx, m.peerID, protocols.ClientPushProtocol)
	if err != nil {
		return nil, fmt.Errorf("failed to create push stream: %w", err)
	}

	initialSubs := &protocolsPb.InitialSubscriptions{
		Topics: m.fetchTopics(),
	}
	if err := codec.WriteLengthPrefixedMessage(stream, initialSubs); err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("failed to send initial subscriptions: %w", err)
	}

	resp := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(stream, resp); err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("failed to read push stream response: %w", err)
	}

	switch v := resp.Response.(type) {
	case *protocolsPb.Response_Success:
		m.log.Infof("%s: push stream established", m.host.ID())
		return stream, nil

	case *protocolsPb.Response_Error:
		_ = stream.Close()
		if v.Error.GetUnauthorized() != nil {
			return nil, errors.New("unauthorized: cannot establish push stream")
		}
		return nil, fmt.Errorf("push stream error: %T", v.Error.GetError())

	default:
		_ = stream.Close()
		return nil, fmt.Errorf("unexpected push stream response: %T", v)
	}
}

// runPushStream establishes a push stream and listens for messages. It blocks
// until the context is done or the push stream is closed.
// - If the initial attempt to establish the push stream fails, it returns an error.
// - If the push stream is closed, and the peer is no longer connected, it returns
// an error.
// - If the push stream is closed, and the peer is still connected, one attempt will
// be made to reopen the stream, if that fails, it returns an error.
func (m *meshConnection) runPushStream(ctx context.Context) error {
	stream, err := m.openPushStream(ctx)
	if err != nil {
		return err
	}

	m.markReady()

	for {
		m.handlePushedData(ctx, stream)

		if ctx.Err() != nil {
			return nil
		}

		// If the peer is no longer connected, return an error.
		if m.host.Network().Connectedness(m.peerID) != network.Connected {
			return fmt.Errorf("peer %s disconnected", m.peerID.ShortString())
		}

		// Attempt to reopen the stream once.
		m.log.Infof("%s: push stream closed, attempting to reopen", m.host.ID())
		stream, err = m.openPushStream(ctx)
		if err != nil {
			return fmt.Errorf("failed to reopen push stream: %w", err)
		}
	}
}

// readPayload wraps the result of the blocking push stream read.
type readPayload struct {
	data []byte
	err  error
}

// handlePushedData listens for messages on the push stream and processes them using the
// handler function. This function will block until the context is done or the push
// stream is closed.
func (m *meshConnection) handlePushedData(ctx context.Context, pushStream network.Stream) {
	defer func() { _ = pushStream.Close() }()

	for {
		readCh := make(chan readPayload, 1)

		go func() {
			// Timeout is 0, so no deadline is set.
			data, err := codec.ReadLengthPrefixedBytes(pushStream, 0)
			readCh <- readPayload{
				data: data,
				err:  err,
			}
		}()

		select {
		case <-ctx.Done():
			m.kill()
			return
		case payload := <-readCh:
			if payload.err != nil {
				if errors.Is(payload.err, io.EOF) || errors.Is(payload.err, network.ErrReset) {
					return
				}

				m.log.Errorf("%s: failed to read message: %v", m.host.ID(), payload.err)
				continue
			}

			msg := &protocolsPb.PushMessage{}
			if err := proto.Unmarshal(payload.data, msg); err == nil {
				m.handleMessage(msg)
			}
		}
	}
}
