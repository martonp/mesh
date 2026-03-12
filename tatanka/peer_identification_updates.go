package tatanka

import (
	"context"
	"errors"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	eventbus "github.com/libp2p/go-libp2p/p2p/host/eventbus"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

var (
	errUnexpectedPeerRecordType = errors.New("signed peer record did not contain a peer.PeerRecord")
	errPeerRecordPeerMismatch   = errors.New("signed peer record peer ID did not match identify event peer")
)

// runPeerIdentificationUpdates reconciles identify-completed address updates
// into the peerstore, persisted peerstore cache, and published bootstrap list.
func (t *TatankaNode) runPeerIdentificationUpdates(ctx context.Context) {
	sub, err := t.node.EventBus().Subscribe(&event.EvtPeerIdentificationCompleted{}, eventbus.BufSize(32))
	if err != nil {
		t.log.Warnf("Failed to subscribe to peer identification updates; using timer fallback only: %v", err)
		return
	}
	defer sub.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-sub.Out():
			if !ok {
				return
			}
			t.handlePeerIdentificationCompleted(evt.(event.EvtPeerIdentificationCompleted))
		}
	}
}

func (t *TatankaNode) handlePeerIdentificationCompleted(evt event.EvtPeerIdentificationCompleted) {
	if evt.Peer == t.node.ID() {
		return
	}

	if _, ok := t.whitelistManager.getWhitelist().PeerIDs[evt.Peer]; !ok {
		return
	}

	addrs, source, err := authoritativePeerAddrs(evt)
	if err != nil {
		t.log.Warnf("Failed to decode identify addresses for %s; falling back to listen addrs: %v", evt.Peer, err)
		addrs = dedupeMultiaddrs(evt.ListenAddrs)
		source = "listen addrs"
	}

	var remote ma.Multiaddr
	if evt.Conn != nil {
		remote = evt.Conn.RemoteMultiaddr()
	}
	addrs = filterIdentifyAddrs(addrs, remote)

	ps := t.node.Peerstore()
	ps.UpdateAddrs(evt.Peer, peerstore.PermanentAddrTTL, 0)
	ps.AddAddrs(evt.Peer, addrs, peerstore.PermanentAddrTTL)

	if t.peerstoreCache != nil {
		t.peerstoreCache.save()
	}
	if t.bootstrapList != nil {
		t.bootstrapList.publish()
	}

	t.log.Debugf("Reconciled identified addresses for %s using %s (%d addresses)", evt.Peer, source, len(addrs))
}

func authoritativePeerAddrs(evt event.EvtPeerIdentificationCompleted) ([]ma.Multiaddr, string, error) {
	if evt.SignedPeerRecord != nil {
		rec, err := evt.SignedPeerRecord.Record()
		if err != nil {
			return nil, "", err
		}

		peerRec, ok := rec.(*peer.PeerRecord)
		if !ok {
			return nil, "", errUnexpectedPeerRecordType
		}
		if peerRec.PeerID != evt.Peer {
			return nil, "", errPeerRecordPeerMismatch
		}

		return dedupeMultiaddrs(peerRec.Addrs), "signed peer record", nil
	}

	return dedupeMultiaddrs(evt.ListenAddrs), "listen addrs", nil
}

func filterIdentifyAddrs(addrs []ma.Multiaddr, remote ma.Multiaddr) []ma.Multiaddr {
	if remote == nil {
		return addrs
	}

	switch {
	case manet.IsIPLoopback(remote):
		return addrs
	case manet.IsPrivateAddr(remote):
		return ma.FilterAddrs(addrs, func(addr ma.Multiaddr) bool {
			return !manet.IsIPLoopback(addr)
		})
	case manet.IsPublicAddr(remote):
		return ma.FilterAddrs(addrs, manet.IsPublicAddr)
	default:
		return addrs
	}
}

func dedupeMultiaddrs(addrs []ma.Multiaddr) []ma.Multiaddr {
	if len(addrs) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(addrs))
	deduped := make([]ma.Multiaddr, 0, len(addrs))
	for _, addr := range addrs {
		if addr == nil {
			continue
		}
		key := addr.String()
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		deduped = append(deduped, addr)
	}
	return deduped
}
