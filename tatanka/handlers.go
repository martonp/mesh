package tatanka

import (
	"context"
	"encoding/binary"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/bisoncraft/mesh/bond"
	"github.com/bisoncraft/mesh/codec"
	"github.com/bisoncraft/mesh/oracle"
	"github.com/bisoncraft/mesh/protocols"
	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	"github.com/bisoncraft/mesh/tatanka/pb"
	"github.com/bisoncraft/mesh/tatanka/types"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

const defaultTimeout = time.Second * 30

// handleClientPush is called when the client opens a push stream to the node.
func (t *TatankaNode) handleClientPush(s network.Stream) {
	client := s.Conn().RemotePeer()

	var success bool
	defer func() {
		if !success {
			_ = s.Close()
		}
	}()

	initialSubs := &protocolsPb.InitialSubscriptions{}
	if err := codec.ReadLengthPrefixedMessage(s, initialSubs); err != nil {
		t.log.Errorf("Failed to read initial subscriptions from client %s: %v", client.ShortString(), err)
		return
	}

	added := t.subTrie.SubscribePeer(client, initialSubs.Topics)

	for _, topic := range added {
		t.publishClientSubscriptionEvent(client, topic, true)
	}

	if err := codec.WriteLengthPrefixedMessage(s, pbResponseSuccess()); err != nil {
		t.log.Errorf("Failed to write success response: %v", err)
		return
	}

	success = true

	t.pushStreamManager.newPushStream(s)
}

func (t *TatankaNode) publishClientSubscriptionEvent(client peer.ID, topic string, subscribed bool) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	message := pbPushMessageSubscription(topic, client, subscribed)
	err := t.gossipSub.publishClientMessage(ctx, message)
	if err != nil {
		t.log.Errorf("Failed to publish subscription event: %v", err)
	}
}

// handleClientSubscribe handles a client subscribe request.
func (t *TatankaNode) handleClientSubscribe(s network.Stream) {
	defer func() { _ = s.Close() }()

	client := s.Conn().RemotePeer()

	subscribeMessage := &protocolsPb.SubscribeRequest{}
	if err := codec.ReadLengthPrefixedMessage(s, subscribeMessage); err != nil {
		t.log.Debugf("Failed to read/unmarshal subscribe message from client %s: %v.", client.ShortString(), err)
		// TODO: client sent invalid message, remove client?
		return
	}
	added := t.subTrie.SubscribePeer(client, subscribeMessage.Topics)
	for _, topic := range added {
		if strings.HasPrefix(topic, protocols.PriceTopicPrefix) || strings.HasPrefix(topic, protocols.FeeRateTopicPrefix) {
			// We don't care about the other nodes view of fee rate and prices.
			continue
		}
		t.publishClientSubscriptionEvent(client, topic, true)
	}

	for _, topic := range added {
		if strings.HasPrefix(topic, protocols.PriceTopicPrefix) {
			t.sendCurrentOracleUpdate(client, topic)
		} else if strings.HasPrefix(topic, protocols.FeeRateTopicPrefix) {
			t.sendCurrentOracleUpdate(client, topic)
		}
	}

	if err := codec.WriteLengthPrefixedMessage(s, pbResponseSuccess()); err != nil {
		t.log.Errorf("Failed to write success response for subscribe: %v", err)
	}
}

// handleClientUnsubscribe handles a client unsubscribe request.
func (t *TatankaNode) handleClientUnsubscribe(s network.Stream) {
	defer func() { _ = s.Close() }()

	client := s.Conn().RemotePeer()
	unsubMsg := &protocolsPb.UnsubscribeRequest{}

	unsubscribed := t.subTrie.UnsubscribePeer(client, unsubMsg.Topics)
	for _, topic := range unsubscribed {
		// Skip price and fee rates topics.
		if strings.HasPrefix(topic, protocols.PriceTopicPrefix) || strings.HasPrefix(topic, protocols.FeeRateTopicPrefix) {
			continue
		}
		t.publishClientSubscriptionEvent(client, topic, false)
	}
	if err := codec.WriteLengthPrefixedMessage(s, pbResponseSuccess()); err != nil {
		t.log.Errorf("Failed to write success response for unsubscribe: %v", err)
	}
}

// sendCurrentOracleUpdate sends the current oracle state to a newly subscribed client.
func (t *TatankaNode) sendCurrentOracleUpdate(client peer.ID, topic string) {
	var data []byte

	// Check for prefixed price subscription.
	if strings.HasPrefix(topic, protocols.PriceTopicPrefix) {
		ticker := topic[len(protocols.PriceTopicPrefix):]
		if price, ok := t.oracle.Price(oracle.Ticker(ticker)); ok {
			data = encodeFloat64(price)
		}
	} else if strings.HasPrefix(topic, protocols.FeeRateTopicPrefix) {
		// Check for prefixed fee rate subscription.
		network := topic[len(protocols.FeeRateTopicPrefix):]
		if feeRate, ok := t.oracle.FeeRate(oracle.Network(network)); ok {
			data = bigIntToBytes(feeRate)
		}
	}

	if data != nil {
		t.pushStreamManager.send(t.nodeID, client, topic, data)
	}
}

// handleClientPublish handles a request by a client the publish a message to a
// topic.
func (t *TatankaNode) handleClientPublish(s network.Stream) {
	defer func() { _ = s.Close() }()

	client := s.Conn().RemotePeer()

	publishMessage := &protocolsPb.PublishRequest{}
	if err := codec.ReadLengthPrefixedMessage(s, publishMessage); err != nil {
		t.log.Debugf("Failed to read/unmarshal publish message from client %s: %v.", client.ShortString(), err)
		// TODO: remove client?
		return
	}

	if strings.HasPrefix(publishMessage.Topic, protocols.PriceTopicPrefix) ||
		strings.HasPrefix(publishMessage.Topic, protocols.FeeRateTopicPrefix) {
		t.log.Warnf("Client %s attempted to publish to restricted oracle topic %s",
			client.ShortString(), publishMessage.Topic)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	message := pbPushMessageBroadcast(publishMessage.Topic, publishMessage.Data, client)
	err := t.gossipSub.publishClientMessage(ctx, message)
	if err != nil {
		t.log.Errorf("Failed to publish client message: %w", err)
	}
}

func (t *TatankaNode) handlePostBonds(s network.Stream) {
	defer func() { _ = s.Close() }()

	client := s.Conn().RemotePeer()

	postBondMessage := &protocolsPb.PostBondRequest{}
	if err := codec.ReadLengthPrefixedMessage(s, postBondMessage); err != nil {
		t.log.Debugf("Failed to read/unmarshal post bond message from client %s: %v.", client.ShortString(), err)
		// TODO: remove client?
		return
	}

	sendInvalidBondIndex := func(index uint32) {
		responseMessage := pbResponsePostBondError(index)
		if err := codec.WriteLengthPrefixedMessage(s, responseMessage); err != nil {
			t.log.Errorf("Failed to write invalid bond index response: %v.", err)
		}
	}

	sendErrorResponse := func(err error) {
		responseMessage := pbResponseError(err)
		if err := codec.WriteLengthPrefixedMessage(s, responseMessage); err != nil {
			t.log.Errorf("Failed to write post bond error response: %v.", err)
		}
	}

	bondsParams := make([]*bond.BondParams, 0, len(postBondMessage.Bonds))
	for i, bp := range postBondMessage.Bonds {
		valid, expiry, strength, err := t.bondVerifier.verifyBond(bp.AssetID, bp.BondID, client)
		if err != nil {
			sendErrorResponse(err)
			return
		}
		if !valid {
			sendInvalidBondIndex(uint32(i))
			return
		}
		bondsParams = append(bondsParams, &bond.BondParams{
			ID:       string(bp.BondID),
			Expiry:   expiry,
			Strength: strength,
		})
	}

	totalStrength := t.bondStorage.addBonds(client, bondsParams)
	successResponse := pbResponsePostBond(totalStrength)
	if err := codec.WriteLengthPrefixedMessage(s, successResponse); err != nil {
		t.log.Errorf("Failed to write post bond success response: %v.", err)
	}
}

// relayMessageToCounterparty sends a relay message to a counterparty client and
// returns the response payload or a protocol error.
func (t *TatankaNode) relayMessageToCounterparty(counterpartyID, initiatorID peer.ID, message []byte) (respData []byte, protoErr *protocolsPb.Error, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	counterpartyStream, err := t.node.NewStream(ctx, counterpartyID, protocols.TatankaRelayMessageProtocol)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = counterpartyStream.Close() }()

	req := &protocolsPb.TatankaRelayMessageRequest{
		PeerID:  []byte(initiatorID),
		Message: message,
	}
	if err := codec.WriteLengthPrefixedMessage(counterpartyStream, req); err != nil {
		return nil, nil, err
	}

	counterpartyResp := &protocolsPb.TatankaRelayMessageResponse{}
	if err := codec.ReadLengthPrefixedMessage(counterpartyStream, counterpartyResp); err != nil {
		return nil, nil, err
	}

	if errResp := counterpartyResp.GetError(); errResp != nil {
		return nil, errResp, nil
	}

	return counterpartyResp.GetMessage(), nil, nil
}

// handleClientRelayMessage processes a client relay message request and returns
// the counterparty's response payload or an error, then closes the stream.
func (t *TatankaNode) handleClientRelayMessage(s network.Stream) {
	defer func() { _ = s.Close() }()

	client := s.Conn().RemotePeer()

	requestMessage := &protocolsPb.ClientRelayMessageRequest{}
	if err := codec.ReadLengthPrefixedMessage(s, requestMessage); err != nil {
		t.log.Debugf("Failed to read/unmarshal relay message request from client %s: %v.", client.ShortString(), err)
		return
	}

	counterpartyID, err := peer.IDFromBytes(requestMessage.PeerID)
	if err != nil {
		t.log.Debugf("Failed to parse counterparty ID from request: %v.", err)
		return
	}

	writeResponse := func(resp *protocolsPb.ClientRelayMessageResponse) {
		if err := codec.WriteLengthPrefixedMessage(s, resp); err != nil {
			t.log.Warnf("Failed to write relay message response to client %s: %v.", client.ShortString(), err)
		}
	}

	if t.node.Network().Connectedness(counterpartyID) == network.Connected {
		respData, protoErr, err := t.relayMessageToCounterparty(counterpartyID, client, requestMessage.Message)
		if err != nil {
			writeResponse(pbClientRelayMessageErrorMessage("failed to contact counterparty client"))
			return
		}
		if protoErr != nil {
			switch {
			case protoErr.GetCpRejectedError() != nil:
				writeResponse(pbClientRelayMessageCounterpartyRejected())
			case protoErr.GetCpNotFoundError() != nil:
				writeResponse(pbClientRelayMessageCounterpartyNotFound())
			case protoErr.GetMessage() != "":
				writeResponse(pbClientRelayMessageErrorMessage(protoErr.GetMessage()))
			default:
				writeResponse(pbClientRelayMessageErrorMessage("counterparty relay failed"))
			}
			return
		}

		writeResponse(pbClientRelayMessageSuccess(respData))
		return
	}

	// Counterparty is not directly connected; forward the request to a tatanka node that has the counterparty connected.
	tatankaPeers := t.clientConnectionManager.getTatankaPeersForClient(counterpartyID)
	if len(tatankaPeers) == 0 {
		t.log.Debugf("No tatanka peers found for counterparty %s.", counterpartyID.ShortString())
		writeResponse(pbClientRelayMessageCounterpartyNotFound())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	tatankaPeer := tatankaPeers[0]
	forwardStream, err := t.node.NewStream(ctx, tatankaPeer, forwardRelayProtocol)
	if err != nil {
		t.log.Warnf("Failed to open forward relay stream to tatanka peer %s: %v", tatankaPeer.ShortString(), err)
		writeResponse(pbClientRelayMessageErrorMessage("failed to contact counterparty node"))
		return
	}
	defer func() { _ = forwardStream.Close() }()

	forwardReq := &pb.TatankaForwardRelayRequest{
		InitiatorId:    []byte(client),
		CounterpartyId: []byte(counterpartyID),
		Message:        requestMessage.Message,
	}
	if err := codec.WriteLengthPrefixedMessage(forwardStream, forwardReq); err != nil {
		t.log.Warnf("Failed to write forward relay request to tatanka peer %s: %v", tatankaPeer.ShortString(), err)
		writeResponse(pbClientRelayMessageErrorMessage("failed to forward relay request"))
		return
	}

	forwardResp := &pb.TatankaForwardRelayResponse{}
	if err := codec.ReadLengthPrefixedMessage(forwardStream, forwardResp); err != nil {
		t.log.Warnf("Failed to read forward relay response from tatanka peer %s: %v", tatankaPeer.ShortString(), err)
		writeResponse(pbClientRelayMessageErrorMessage("failed to receive response from counterparty node"))
		return
	}

	if errMsg := forwardResp.GetError(); errMsg != "" {
		writeResponse(pbClientRelayMessageErrorMessage(errMsg))
		return
	}
	if forwardResp.GetClientNotFound() != nil {
		writeResponse(pbClientRelayMessageCounterpartyNotFound())
		return
	}
	if forwardResp.GetClientRejected() != nil {
		writeResponse(pbClientRelayMessageCounterpartyRejected())
		return
	}

	writeResponse(pbClientRelayMessageSuccess(forwardResp.GetSuccess()))
}

// handleForwardRelay handles a request from another tatanka node to forward a relay message to a client.
func (t *TatankaNode) handleForwardRelay(s network.Stream) {
	defer func() { _ = s.Close() }()

	peerID := s.Conn().RemotePeer()

	requestMessage := &pb.TatankaForwardRelayRequest{}
	if err := codec.ReadLengthPrefixedMessage(s, requestMessage); err != nil {
		t.log.Warnf("Failed to read/unmarshal forward relay request message from peer %s: %v.", peerID.ShortString(), err)
		return
	}

	counterpartyID, err := peer.IDFromBytes(requestMessage.CounterpartyId)
	if err != nil {
		t.log.Warnf("Failed to parse counterparty ID from request: %v.", err)
		return
	}

	writeResponse := func(resp *pb.TatankaForwardRelayResponse) {
		if err := codec.WriteLengthPrefixedMessage(s, resp); err != nil {
			t.log.Warnf("Failed to write forward relay response to peer %s: %v", peerID.ShortString(), err)
		}
	}

	// If the counterparty is not connected to us, return an error. Currently only one hop forwarding
	// is supported.
	if t.node.Network().Connectedness(counterpartyID) != network.Connected {
		t.log.Warnf("Unable to forward relay request to counterparty %s because they are not connected to us.", counterpartyID.ShortString())
		writeResponse(pbTatankaForwardRelayClientNotFound())
		return
	}

	respData, protoErr, err := t.relayMessageToCounterparty(counterpartyID, peer.ID(requestMessage.InitiatorId), requestMessage.Message)
	if err != nil {
		writeResponse(pbTatankaForwardRelayError("failed to contact counterparty client"))
		return
	}
	if protoErr != nil {
		switch {
		case protoErr.GetCpRejectedError() != nil:
			writeResponse(pbTatankaForwardRelayClientRejected())
		case protoErr.GetCpNotFoundError() != nil:
			writeResponse(pbTatankaForwardRelayClientNotFound())
		case protoErr.GetMessage() != "":
			writeResponse(pbTatankaForwardRelayError(protoErr.GetMessage()))
		default:
			writeResponse(pbTatankaForwardRelayError("counterparty relay failed"))
		}
		return
	}

	writeResponse(pbTatankaForwardRelaySuccess(respData))
}

func (t *TatankaNode) priceSubscribers(prices map[oracle.Ticker]float64) map[string][]peer.ID {
	candidates := make([]string, 0, len(prices))
	for ticker := range prices {
		candidates = append(candidates, protocols.PriceTopicPrefix+string(ticker))
	}

	return t.subTrie.Subscribers(candidates)
}

func (t *TatankaNode) findSubscribedFeeRateTopics(feeRates map[oracle.Network]*big.Int) map[string][]peer.ID {

	candidates := make([]string, 0, len(feeRates))
	for network := range feeRates {
		candidates = append(candidates, protocols.FeeRateTopicPrefix+string(network))
	}

	return t.subTrie.Subscribers(candidates)
}

func encodeFloat64(f float64) []byte {
	buf := make([]byte, 8)
	// Convert float64 bits to uint64
	bits := math.Float64bits(f)
	binary.BigEndian.PutUint64(buf, bits)
	return buf
}

func (t *TatankaNode) distributePriceUpdate(topic string, candidates []peer.ID, price float64) {
	t.pushStreamManager.broadcast(t.nodeID, candidates, topic, encodeFloat64(price))
}

func (t *TatankaNode) distributeFeeRateUpdate(topic string, candidates []peer.ID, feeRate *big.Int) {
	t.pushStreamManager.broadcast(t.nodeID, candidates, topic, bigIntToBytes(feeRate))
}

func (t *TatankaNode) handleOracleUpdate(senderID peer.ID, oracleUpdate *pb.NodeOracleUpdate) {
	if oracleUpdate.Source == "" {
		t.log.Warn("Skipping oracle update with empty source")
		return
	}
	if oracleUpdate.Timestamp <= 0 {
		t.log.Warnf("Skipping oracle update with invalid timestamp: %d", oracleUpdate.Timestamp)
		return
	}

	// Extract piggybacked quota status and forward to oracle.
	if oracleUpdate.Quota != nil {
		t.oracle.UpdatePeerSourceQuota(senderID.String(), pbToTimestampedQuotaStatus(oracleUpdate.Quota), oracleUpdate.Source)
	}

	update := pbToOracleUpdate(oracleUpdate)
	if len(update.Prices) == 0 && len(update.FeeRates) == 0 {
		t.log.Warn("Skipping oracle update with no prices or fee rates")
		return
	}

	result := t.oracle.Merge(update, senderID.String())
	if result == nil {
		return
	}

	t.distributePriceUpdates(result.Prices)
	t.distributeFeeRateUpdates(result.FeeRates)
}

func (t *TatankaNode) distributePriceUpdates(updatedPrices map[oracle.Ticker]float64) {
	if len(updatedPrices) == 0 {
		return
	}

	priceSubs := t.priceSubscribers(updatedPrices)
	if len(priceSubs) == 0 {
		return
	}

	for topic, candidates := range priceSubs {
		ticker := topic[len(protocols.PriceTopicPrefix):]
		price, ok := updatedPrices[oracle.Ticker(ticker)]
		if !ok {
			t.log.Errorf("No updated price found for %s", ticker)
			continue
		}
		go func(topic string, price float64, candidates []peer.ID) {
			t.distributePriceUpdate(topic, candidates, price)
		}(topic, price, candidates)
	}
}

func (t *TatankaNode) distributeFeeRateUpdates(updatedFeeRates map[oracle.Network]*big.Int) {
	if len(updatedFeeRates) == 0 {
		return
	}

	feeRateSubs := t.findSubscribedFeeRateTopics(updatedFeeRates)
	if len(feeRateSubs) == 0 {
		return
	}

	for topic, candidates := range feeRateSubs {
		network := topic[len(protocols.FeeRateTopicPrefix):]
		feeRate, ok := updatedFeeRates[oracle.Network(network)]
		if !ok {
			t.log.Errorf("No updated fee rate found for %s", network)
			continue
		}
		go func(topic string, feeRate *big.Int, candidates []peer.ID) {
			t.distributeFeeRateUpdate(topic, candidates, feeRate)
		}(topic, feeRate, candidates)
	}
}

// handleDiscovery handles a request from another tatanka node to discover
// addresses for a target peer. If the target peer is not connected to us, we
// send a not found response, otherwise we send the addresses of the target peer.
func (t *TatankaNode) handleDiscovery(s network.Stream) {
	defer func() { _ = s.Close() }()

	remotePeerID := s.Conn().RemotePeer()
	requestMessage := &pb.DiscoveryRequest{}
	if err := codec.ReadLengthPrefixedMessage(s, requestMessage); err != nil {
		t.log.Warnf("Failed to read/unmarshal discovery request message from peer %s: %v.", remotePeerID.ShortString(), err)
		return
	}

	targetPeerID, err := peer.IDFromBytes(requestMessage.Id)
	if err != nil {
		t.log.Warnf("Failed to parse target peer ID from request: %v.", err)
		return
	}

	// Only share addresses for whitelist peers.
	if _, ok := t.whitelistManager.getWhitelist().PeerIDs[targetPeerID]; !ok {
		t.log.Warnf("Tatanka peer %s attempted to discover addresses for non-whitelist peer %s.", remotePeerID.ShortString(), targetPeerID.ShortString())
		if err := codec.WriteLengthPrefixedMessage(s, pbDiscoveryResponseNotFound()); err != nil {
			t.log.Warnf("Failed to write discovery response to peer %s: %v.", remotePeerID.ShortString(), err)
		}
		return
	}

	if t.node.Network().Connectedness(targetPeerID) != network.Connected {
		if err := codec.WriteLengthPrefixedMessage(s, pbDiscoveryResponseNotFound()); err != nil {
			t.log.Warnf("Failed to write discovery response to peer %s: %v.", remotePeerID.ShortString(), err)
		}
		return
	}

	addrs := t.node.Peerstore().Addrs(targetPeerID)
	if err := codec.WriteLengthPrefixedMessage(s, pbDiscoveryResponseSuccess(addrs)); err != nil {
		t.log.Warnf("Failed to write discovery response to peer %s: %v.", remotePeerID.ShortString(), err)
	}
}

// handleWhitelist handles a symmetric whitelist handshake with another tatanka
// node. Both sides exchange a WhitelistState, check for a match, and protect
// or close the connection based on the result. The receiver waits a few seconds
// to close the connection to allow the initiator to read the mismatch result
func (t *TatankaNode) handleWhitelist(s network.Stream) {
	defer func() { _ = s.Close() }()

	remotePeerID := s.Conn().RemotePeer()

	// 1. Read peer's state.
	peerState := &pb.WhitelistState{}
	if err := codec.ReadLengthPrefixedMessage(s, peerState); err != nil {
		t.log.Warnf("Failed to read whitelist handshake from %s: %v", remotePeerID.ShortString(), err)
		return
	}

	// 2. Send our state.
	ownWs := t.whitelistManager.getLocalWhitelistState()
	if err := codec.WriteLengthPrefixedMessage(s, whitelistStateToPb(ownWs)); err != nil {
		t.log.Warnf("Failed to write whitelist handshake to %s: %v", remotePeerID.ShortString(), err)
		return
	}

	// 3. Check match independently.
	matched := flexibleWhitelistMatch(ownWs.Current, ownWs.Proposed, peerState.PeerIDs, peerState.ProposedPeerIDs)

	// 4. Protect if matched. On mismatch the initiator's verifyWhitelist
	// handles ClosePeer; doing it here synchronously races with the
	// client's stream read and can prevent the mismatch from being
	// detected. A delayed close acts as a safety net in case the
	// initiator never cleans up.
	if matched {
		t.node.ConnManager().Protect(remotePeerID, "tatanka-node")
	} else {
		go func() {
			time.Sleep(5 * time.Second)
			if !t.node.ConnManager().IsProtected(remotePeerID, "tatanka-node") {
				_ = t.node.Network().ClosePeer(remotePeerID)
			}
		}()
	}

	// 5. Record peer's whitelist info regardless of match.
	t.whitelistManager.updatePeerWhitelistState(remotePeerID, pbToWhitelistState(peerState), peerState.Timestamp)
}

// handleAvailableMeshNodes handles a request from a client to get a list of
// all mesh nodes that this tatanka node is connected to.
func (t *TatankaNode) handleAvailableMeshNodes(s network.Stream) {
	defer func() { _ = s.Close() }()

	whitelistPeers := t.whitelistManager.getWhitelist().PeerIDs
	peerStore := t.node.Peerstore()

	var peers []*protocolsPb.PeerInfo

	for pid := range whitelistPeers {
		// Include ourselves
		if pid == t.nodeID {
			peers = append(peers, libp2pPeerInfoToPb(peer.AddrInfo{
				ID:    t.nodeID,
				Addrs: t.node.Addrs(),
			}))
			continue
		}

		// Only include connected peers
		if t.node.Network().Connectedness(pid) != network.Connected {
			continue
		}

		addrs := peerStore.Addrs(pid)
		peers = append(peers, libp2pPeerInfoToPb(peer.AddrInfo{
			ID:    pid,
			Addrs: addrs,
		}))
	}

	if err := codec.WriteLengthPrefixedMessage(s, pbAvailableMeshNodesResponse(peers)); err != nil {
		t.log.Warnf("Failed to write available mesh nodes response: %v", err)
	}
}

// handleSearchTopics handles a request from a client to get a list of all
// topics that this tatanka node is subscribed to.
func (t *TatankaNode) handleSearchTopics(s network.Stream) {
	defer func() { _ = s.Close() }()

	client := s.Conn().RemotePeer()

	searchMsg := &protocolsPb.SearchTopicsRequest{}
	if err := codec.ReadLengthPrefixedMessage(s, searchMsg); err != nil {
		t.log.Debugf("Failed to read/unmarshal search topics request from client %s: %v.", client.ShortString(), err)
		// TODO: client sent invalid message, remove client?
		return
	}

	matches := t.subTrie.SearchTopics(searchMsg.Filters)

	if err := codec.WriteLengthPrefixedMessage(s, &protocolsPb.SearchTopicsResponse{Topics: matches}); err != nil {
		t.log.Warnf("Failed to write search topics response: %v", err)
	}
}

// handleQuotaHeartbeat handles a quota heartbeat message from another tatanka node.
// This is used to periodically share quota information via gossipsub.
func (t *TatankaNode) handleQuotaHeartbeat(senderID peer.ID, heartbeat *pb.QuotaHandshake) {
	for source, q := range heartbeat.Quotas {
		t.oracle.UpdatePeerSourceQuota(senderID.String(), pbToTimestampedQuotaStatus(q), source)
	}
}

func (t *TatankaNode) handleWhitelistUpdate(senderID peer.ID, ws *types.WhitelistState, timestamp int64) {
	if senderID == t.nodeID {
		return
	}
	if !t.whitelistManager.updatePeerWhitelistState(senderID, ws, timestamp) {
		return
	}
	pi := t.connectionManager.getPeerInfo(senderID)
	t.adminNotify.BroadcastPeerUpdate(pi)
}

// handleQuotaHandshake handles a quota handshake request from another tatanka node.
// This is used to exchange quota information on connection.
func (t *TatankaNode) handleQuotaHandshake(s network.Stream) {
	defer func() { _ = s.Close() }()
	peerID := s.Conn().RemotePeer()

	// Read peer's quotas
	req := &pb.QuotaHandshake{}
	if err := codec.ReadLengthPrefixedMessage(s, req); err != nil {
		t.log.Warnf("Failed to read quota handshake from %s: %v", peerID.ShortString(), err)
		return
	}

	// Process peer quotas
	for source, q := range req.Quotas {
		t.oracle.UpdatePeerSourceQuota(peerID.String(), pbToTimestampedQuotaStatus(q), source)
	}

	// Send our quotas
	localQuotas := quotaStatusesToPb(t.oracle.GetLocalQuotas())
	resp := &pb.QuotaHandshake{Quotas: localQuotas}
	if err := codec.WriteLengthPrefixedMessage(s, resp); err != nil {
		t.log.Warnf("Failed to send quota handshake to %s: %v", peerID.ShortString(), err)
	}
}
