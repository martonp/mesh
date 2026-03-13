package tatanka

import (
	"fmt"
	"math/big"
	"time"

	"github.com/bisoncraft/mesh/oracle"
	"github.com/bisoncraft/mesh/oracle/sources"
	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	"github.com/bisoncraft/mesh/tatanka/pb"
	"github.com/bisoncraft/mesh/tatanka/types"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// whitelistStateToPb converts a types.WhitelistState to a pb.WhitelistState.
func whitelistStateToPb(ws *types.WhitelistState) *pb.WhitelistState {
	state := &pb.WhitelistState{
		PeerIDs:   ws.Current.PeerIDsBytes(),
		Timestamp: time.Now().UnixNano(),
		Ready:     ws.Ready,
	}
	if ws.Proposed != nil {
		state.ProposedPeerIDs = ws.Proposed.PeerIDsBytes()
	}
	return state
}

// pbToWhitelistState converts a pb.WhitelistState to a types.WhitelistState.
func pbToWhitelistState(state *pb.WhitelistState) *types.WhitelistState {
	return &types.WhitelistState{
		Current:  types.DecodePeerIDBytes(state.PeerIDs),
		Proposed: types.DecodePeerIDBytes(state.ProposedPeerIDs),
		Ready:    state.Ready,
	}
}

// libp2pPeerInfoToPb converts a peer.AddrInfo to a protocolsPb.PeerInfo.
func libp2pPeerInfoToPb(peerInfo peer.AddrInfo) *protocolsPb.PeerInfo {
	addrBytes := make([][]byte, len(peerInfo.Addrs))
	for i, addr := range peerInfo.Addrs {
		addrBytes[i] = addr.Bytes()
	}

	return &protocolsPb.PeerInfo{
		Id:    []byte(peerInfo.ID),
		Addrs: addrBytes,
	}
}

// pbPeerInfoToLibp2p converts a protocolsPb.PeerInfo to a peer.AddrInfo.
func pbPeerInfoToLibp2p(pbPeer *protocolsPb.PeerInfo) (peer.AddrInfo, error) {
	peerID, err := peer.IDFromBytes(pbPeer.Id)
	if err != nil {
		return peer.AddrInfo{}, fmt.Errorf("failed to parse peer ID: %w", err)
	}

	addrs := make([]ma.Multiaddr, 0, len(pbPeer.Addrs))
	for _, addrBytes := range pbPeer.Addrs {
		addr, err := ma.NewMultiaddrBytes(addrBytes)
		if err != nil {
			return peer.AddrInfo{}, fmt.Errorf("failed to parse multiaddr: %w", err)
		}
		addrs = append(addrs, addr)
	}

	return peer.AddrInfo{
		ID:    peerID,
		Addrs: addrs,
	}, nil
}

func oracleUpdateToPb(update *oracle.OracleUpdate) (*pb.NodeOracleUpdate, error) {
	if update == nil {
		return nil, fmt.Errorf("oracle update is nil")
	}

	msg := &pb.NodeOracleUpdate{
		Source:    update.Source,
		Timestamp: update.Stamp.Unix(),
	}

	if len(update.Prices) > 0 {
		msg.Prices = make(map[string]float64, len(update.Prices))
		for ticker, price := range update.Prices {
			msg.Prices[string(ticker)] = price
		}
	}

	if len(update.FeeRates) > 0 {
		msg.FeeRates = make(map[string][]byte, len(update.FeeRates))
		for network, feeRate := range update.FeeRates {
			msg.FeeRates[string(network)] = bigIntToBytes(feeRate)
		}
	}

	if update.Quota != nil {
		msg.Quota = quotaStatusToPb(update.Quota)
	}

	return msg, nil
}

func pbToOracleUpdate(pbUpdate *pb.NodeOracleUpdate) *oracle.OracleUpdate {
	update := &oracle.OracleUpdate{
		Source: pbUpdate.Source,
		Stamp:  time.Unix(pbUpdate.Timestamp, 0),
	}

	if len(pbUpdate.Prices) > 0 {
		update.Prices = make(map[oracle.Ticker]float64, len(pbUpdate.Prices))
		for ticker, price := range pbUpdate.Prices {
			update.Prices[oracle.Ticker(ticker)] = price
		}
	}

	if len(pbUpdate.FeeRates) > 0 {
		update.FeeRates = make(map[oracle.Network]*big.Int, len(pbUpdate.FeeRates))
		for network, feeRateBytes := range pbUpdate.FeeRates {
			update.FeeRates[oracle.Network(network)] = new(big.Int).SetBytes(feeRateBytes)
		}
	}

	return update
}

func pbToTimestampedQuotaStatus(q *pb.QuotaStatus) *oracle.TimestampedQuotaStatus {
	return &oracle.TimestampedQuotaStatus{
		QuotaStatus: &sources.QuotaStatus{
			FetchesRemaining: q.FetchesRemaining,
			FetchesLimit:     q.FetchesLimit,
			ResetTime:        time.Unix(q.ResetTimestamp, 0),
		},
		ReceivedAt: time.Now(),
	}
}

func quotaStatusToPb(quota *sources.QuotaStatus) *pb.QuotaStatus {
	if quota == nil {
		return nil
	}
	return &pb.QuotaStatus{
		FetchesRemaining: quota.FetchesRemaining,
		FetchesLimit:     quota.FetchesLimit,
		ResetTimestamp:   quota.ResetTime.Unix(),
	}
}

func quotaStatusesToPb(quotas map[string]*sources.QuotaStatus) map[string]*pb.QuotaStatus {
	if len(quotas) == 0 {
		return nil
	}
	result := make(map[string]*pb.QuotaStatus, len(quotas))
	for source, quota := range quotas {
		if quota == nil {
			continue
		}
		result[source] = quotaStatusToPb(quota)
	}
	return result
}

func pbPushMessageSubscription(topic string, client peer.ID, subscribed bool) *protocolsPb.PushMessage {
	messageType := protocolsPb.PushMessage_SUBSCRIBE
	if !subscribed {
		messageType = protocolsPb.PushMessage_UNSUBSCRIBE
	}
	return &protocolsPb.PushMessage{
		MessageType: messageType,
		Topic:       topic,
		Sender:      []byte(client),
	}
}

func pbPushMessageBroadcast(topic string, data []byte, sender peer.ID) *protocolsPb.PushMessage {
	return &protocolsPb.PushMessage{
		MessageType: protocolsPb.PushMessage_BROADCAST,
		Topic:       topic,
		Data:        data,
		Sender:      []byte(sender),
	}
}

func pbResponseError(err error) *protocolsPb.Response {
	return &protocolsPb.Response{
		Response: &protocolsPb.Response_Error{
			Error: &protocolsPb.Error{
				Error: &protocolsPb.Error_Message{
					Message: err.Error(),
				},
			},
		},
	}
}

func pbResponseUnauthorizedError() *protocolsPb.Response {
	return &protocolsPb.Response{
		Response: &protocolsPb.Response_Error{
			Error: &protocolsPb.Error{
				Error: &protocolsPb.Error_Unauthorized{
					Unauthorized: &protocolsPb.UnauthorizedError{},
				},
			},
		},
	}
}

func pbResponseClientAddr(addrs [][]byte) *protocolsPb.Response {
	return &protocolsPb.Response{
		Response: &protocolsPb.Response_AddrResponse{
			AddrResponse: &protocolsPb.ClientAddrResponse{
				Addrs: addrs,
			},
		},
	}
}

func pbResponsePostBondError(index uint32) *protocolsPb.Response {
	return &protocolsPb.Response{
		Response: &protocolsPb.Response_Error{
			Error: &protocolsPb.Error{
				Error: &protocolsPb.Error_PostBondError{
					PostBondError: &protocolsPb.PostBondError{
						InvalidBondIndex: index,
					},
				},
			},
		},
	}
}

func pbResponsePostBond(bondStrength uint32) *protocolsPb.Response {
	return &protocolsPb.Response{
		Response: &protocolsPb.Response_PostBondResponse{
			PostBondResponse: &protocolsPb.PostBondResponse{
				BondStrength: bondStrength,
			},
		},
	}
}

func pbAvailableMeshNodesResponse(peers []*protocolsPb.PeerInfo) *protocolsPb.Response {
	return &protocolsPb.Response{
		Response: &protocolsPb.Response_AvailableMeshNodesResponse{
			AvailableMeshNodesResponse: &protocolsPb.AvailableMeshNodesResponse{
				Peers: peers,
			},
		},
	}
}

func pbResponseSuccess() *protocolsPb.Response {
	return &protocolsPb.Response{
		Response: &protocolsPb.Response_Success{
			Success: &protocolsPb.Success{},
		},
	}
}

func pbClientRelayMessageSuccess(message []byte) *protocolsPb.ClientRelayMessageResponse {
	return &protocolsPb.ClientRelayMessageResponse{
		Response: &protocolsPb.ClientRelayMessageResponse_Message{
			Message: message,
		},
	}
}

func pbClientRelayMessageError(err *protocolsPb.Error) *protocolsPb.ClientRelayMessageResponse {
	return &protocolsPb.ClientRelayMessageResponse{
		Response: &protocolsPb.ClientRelayMessageResponse_Error{
			Error: err,
		},
	}
}

func pbClientRelayMessageErrorMessage(message string) *protocolsPb.ClientRelayMessageResponse {
	return pbClientRelayMessageError(&protocolsPb.Error{
		Error: &protocolsPb.Error_Message{
			Message: message,
		},
	})
}

func pbClientRelayMessageCounterpartyNotFound() *protocolsPb.ClientRelayMessageResponse {
	return pbClientRelayMessageError(&protocolsPb.Error{
		Error: &protocolsPb.Error_CpNotFoundError{
			CpNotFoundError: &protocolsPb.CounterpartyNotFoundError{},
		},
	})
}

func pbClientRelayMessageCounterpartyRejected() *protocolsPb.ClientRelayMessageResponse {
	return pbClientRelayMessageError(&protocolsPb.Error{
		Error: &protocolsPb.Error_CpRejectedError{
			CpRejectedError: &protocolsPb.CounterpartyRejectedError{},
		},
	})
}

func pbTatankaForwardRelaySuccess(message []byte) *pb.TatankaForwardRelayResponse {
	return &pb.TatankaForwardRelayResponse{
		Response: &pb.TatankaForwardRelayResponse_Success{
			Success: message,
		},
	}
}

func pbTatankaForwardRelayClientNotFound() *pb.TatankaForwardRelayResponse {
	return &pb.TatankaForwardRelayResponse{
		Response: &pb.TatankaForwardRelayResponse_ClientNotFound_{
			ClientNotFound: &pb.TatankaForwardRelayResponse_ClientNotFound{},
		},
	}
}

func pbTatankaForwardRelayClientRejected() *pb.TatankaForwardRelayResponse {
	return &pb.TatankaForwardRelayResponse{
		Response: &pb.TatankaForwardRelayResponse_ClientRejected_{
			ClientRejected: &pb.TatankaForwardRelayResponse_ClientRejected{},
		},
	}
}

func pbTatankaForwardRelayError(message string) *pb.TatankaForwardRelayResponse {
	return &pb.TatankaForwardRelayResponse{
		Response: &pb.TatankaForwardRelayResponse_Error{
			Error: message,
		},
	}
}

func pbDiscoveryResponseNotFound() *pb.DiscoveryResponse {
	return &pb.DiscoveryResponse{
		Response: &pb.DiscoveryResponse_NotFound_{
			NotFound: &pb.DiscoveryResponse_NotFound{},
		},
	}
}

func pbDiscoveryResponseSuccess(addrs []ma.Multiaddr) *pb.DiscoveryResponse {
	addrBytes := make([][]byte, 0, len(addrs))
	for _, addr := range addrs {
		addrBytes = append(addrBytes, addr.Bytes())
	}

	return &pb.DiscoveryResponse{
		Response: &pb.DiscoveryResponse_Success_{
			Success: &pb.DiscoveryResponse_Success{
				Addrs: addrBytes,
			},
		},
	}
}

// bigIntToBytes converts big.Int to big-endian encoded bytes.
func bigIntToBytes(bi *big.Int) []byte {
	if bi == nil || bi.Sign() == 0 {
		return []byte{0}
	}
	return bi.Bytes()
}
