package tatanka

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	eventbus "github.com/libp2p/go-libp2p/p2p/host/eventbus"
	manet "github.com/multiformats/go-multiaddr/net"
)

// bootstrapPeerEntry is one element of the bootstrap list JSON array.
type bootstrapPeerEntry struct {
	PeerID string   `json:"peer_id"`
	Addrs  []string `json:"addrs"`
}

// bootstrapListPublisher periodically generates and publishes a bootstrap list
// containing all whitelist peers and their known addresses. The bootstrap list
// may be published to a file and/or an HTTP endpoint.
type bootstrapListPublisher struct {
	log          slog.Logger
	host         host.Host
	getWhitelist func() map[peer.ID]struct{}
	filePath     string // empty = file publishing disabled
	httpPort     int    // 0 = HTTP serving disabled

	cacheMtx sync.RWMutex
	cache    []byte // cached JSON output for HTTP endpoint

	httpServer *http.Server
}

func newBootstrapListPublisher(log slog.Logger, h host.Host, getWhitelist func() map[peer.ID]struct{}, filePath string, httpPort int) *bootstrapListPublisher {
	return &bootstrapListPublisher{
		log:          log,
		host:         h,
		getWhitelist: getWhitelist,
		filePath:     filePath,
		httpPort:     httpPort,
	}
}

// generate snapshots the whitelist and peerstore, producing a deterministically
// sorted JSON bootstrap list.
func (b *bootstrapListPublisher) generate() []byte {
	whitelist := b.getWhitelist()
	localID := b.host.ID()

	entries := make([]bootstrapPeerEntry, 0, len(whitelist))
	for pid := range whitelist {
		var addrStrs []string
		if pid == localID {
			// Only include publicly routable addresses for the local node.
			// These come from UPnP/NAT-PMP discovery via host.Addrs().
			for _, a := range b.host.Addrs() {
				if manet.IsPublicAddr(a) {
					addrStrs = append(addrStrs, a.String())
				}
			}
		} else {
			for _, a := range b.host.Peerstore().Addrs(pid) {
				addrStrs = append(addrStrs, a.String())
			}
		}
		if addrStrs == nil {
			addrStrs = []string{}
		}
		entries = append(entries, bootstrapPeerEntry{
			PeerID: pid.String(),
			Addrs:  addrStrs,
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].PeerID < entries[j].PeerID
	})

	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		b.log.Errorf("Failed to marshal bootstrap list: %v", err)
		return nil
	}
	return data
}

// publish generates the bootstrap list and writes it to all configured outputs.
func (b *bootstrapListPublisher) publish() {
	data := b.generate()
	if data == nil {
		return
	}

	b.cacheMtx.Lock()
	b.cache = data
	b.cacheMtx.Unlock()

	if b.filePath != "" {
		if err := atomicWriteFile(b.filePath, data); err != nil {
			b.log.Errorf("Failed to write bootstrap list file: %v", err)
		}
	}
}

// run starts the periodic publish loop and optional HTTP server.
func (b *bootstrapListPublisher) run(ctx context.Context) {
	// Subscribe to local address updates.
	var addrEvents <-chan interface{}
	sub, err := b.host.EventBus().Subscribe(&event.EvtLocalAddressesUpdated{}, eventbus.BufSize(8))
	if err != nil {
		b.log.Warnf("Failed to subscribe to local address updates; using timer fallback only: %v", err)
	} else {
		addrEvents = sub.Out()
		defer sub.Close()
	}

	// Publish immediately so the HTTP endpoint has something to serve.
	b.publish()

	if b.httpPort > 0 {
		mux := http.NewServeMux()
		mux.HandleFunc("/bootstrap", b.handleHTTP)
		b.httpServer = &http.Server{
			Addr:    fmt.Sprintf("0.0.0.0:%d", b.httpPort),
			Handler: mux,
		}
		go func() {
			b.log.Infof("Bootstrap list available on :%d/bootstrap", b.httpPort)
			if err := b.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				b.log.Errorf("Bootstrap list HTTP server failed: %v", err)
			}
		}()
	}

	// Do one startup fallback publish in case address change events are missed.
	startupFallback := time.NewTimer(30 * time.Second)
	defer func() {
		if !startupFallback.Stop() {
			select {
			case <-startupFallback.C:
			default:
			}
		}
	}()
	startupFallbackC := startupFallback.C

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-addrEvents:
			if !ok {
				addrEvents = nil
				continue
			}
			b.publish()
		case <-startupFallbackC:
			b.publish()
			startupFallbackC = nil
		case <-ticker.C:
			b.publish()
		}
	}
}

// shutdown gracefully stops the HTTP server if running.
func (b *bootstrapListPublisher) shutdown(ctx context.Context) error {
	if b.httpServer != nil {
		return b.httpServer.Shutdown(ctx)
	}
	return nil
}

// handleHTTP serves the cached bootstrap list JSON.
func (b *bootstrapListPublisher) handleHTTP(w http.ResponseWriter, r *http.Request) {
	b.cacheMtx.RLock()
	data := b.cache
	b.cacheMtx.RUnlock()

	if data == nil {
		http.Error(w, "bootstrap list not yet available", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}
