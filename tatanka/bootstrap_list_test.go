package tatanka

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	ma "github.com/multiformats/go-multiaddr"
)

func newTestLogger() slog.Logger {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")
	log.SetLevel(slog.LevelOff)
	return log
}

type lazyString func() string

func (ls lazyString) String() string {
	return ls()
}

func TestBootstrapListWhitelistUpdate(t *testing.T) {
	mnet, err := mocknet.WithNPeers(3)
	if err != nil {
		t.Fatal(err)
	}
	hosts := mnet.Hosts()
	h := hosts[0]

	addr1, _ := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/1234")
	addr2, _ := ma.NewMultiaddr("/ip4/5.6.7.8/tcp/5678")
	h.Peerstore().AddAddrs(hosts[1].ID(), []ma.Multiaddr{addr1}, 1<<62)
	h.Peerstore().AddAddrs(hosts[2].ID(), []ma.Multiaddr{addr2}, 1<<62)

	dir := t.TempDir()
	filePath := filepath.Join(dir, "bootstrap.json")
	port := reserveHTTPPort(t)
	url := fmt.Sprintf("http://127.0.0.1:%d/bootstrap", port)

	// Start with 2 peers.
	whitelist := map[peer.ID]struct{}{
		hosts[0].ID(): {},
		hosts[1].ID(): {},
	}
	wantInitial := map[string][]string{
		hosts[0].ID().String(): {},
		hosts[1].ID().String(): {addr1.String()},
	}
	wantUpdated := map[string][]string{
		hosts[0].ID().String(): {},
		hosts[1].ID().String(): {addr1.String()},
		hosts[2].ID().String(): {addr2.String()},
	}

	pub := newBootstrapListPublisher(newTestLogger(), h, func() map[peer.ID]struct{} {
		return whitelist
	}, filePath, port)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		pub.run(ctx)
	}()
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		if err := pub.shutdown(shutdownCtx); err != nil {
			t.Errorf("Failed to shutdown bootstrap list publisher: %v", err)
		}
		cancel()
		<-done
	}()

	waitForBootstrapListStateInFile(t, filePath, wantInitial)
	waitForBootstrapListStateOverHTTP(t, url, wantInitial)

	// Simulate whitelist update by adding peer 2.
	whitelist[hosts[2].ID()] = struct{}{}
	pub.publish()

	waitForBootstrapListStateInFile(t, filePath, wantUpdated)
	waitForBootstrapListStateOverHTTP(t, url, wantUpdated)
}

func waitForBootstrapListStateInFile(t *testing.T, filePath string, want map[string][]string) {
	t.Helper()

	wantState := normalizeBootstrapListWant(want)
	lastState := "no file read yet"

	requireEventually(t, func() bool {
		data, err := os.ReadFile(filePath)
		if err != nil {
			lastState = fmt.Sprintf("read failed: %v", err)
			return false
		}

		entries, err := decodeBootstrapListEntries(data)
		if err != nil {
			lastState = fmt.Sprintf("unmarshal failed: %v", err)
			return false
		}

		gotState := normalizeBootstrapListEntries(entries)
		lastState = fmt.Sprintf("got %v", gotState)
		return reflect.DeepEqual(gotState, wantState)
	}, 2*time.Second, 10*time.Millisecond, "bootstrap list file %q did not match %v (%v)", filePath, wantState, lazyString(func() string {
		return lastState
	}))
}

func waitForBootstrapListStateOverHTTP(t *testing.T, url string, want map[string][]string) {
	t.Helper()

	wantState := normalizeBootstrapListWant(want)
	lastState := "no HTTP response yet"

	requireEventually(t, func() bool {
		resp, err := http.Get(url)
		if err != nil {
			lastState = fmt.Sprintf("request failed: %v", err)
			return false
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			lastState = fmt.Sprintf("read failed: %v", err)
			return false
		}
		if resp.StatusCode != http.StatusOK {
			lastState = fmt.Sprintf("got status %d", resp.StatusCode)
			return false
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
			lastState = fmt.Sprintf("got Content-Type %q", ct)
			return false
		}

		entries, err := decodeBootstrapListEntries(body)
		if err != nil {
			lastState = fmt.Sprintf("unmarshal failed: %v", err)
			return false
		}

		gotState := normalizeBootstrapListEntries(entries)
		lastState = fmt.Sprintf("got %v", gotState)
		return reflect.DeepEqual(gotState, wantState)
	}, 2*time.Second, 10*time.Millisecond, "bootstrap HTTP %q did not match %v (%v)", url, wantState, lazyString(func() string {
		return lastState
	}))
}

func decodeBootstrapListEntries(data []byte) ([]bootstrapPeerEntry, error) {
	var entries []bootstrapPeerEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

func normalizeBootstrapListEntries(entries []bootstrapPeerEntry) map[string][]string {
	got := make(map[string][]string, len(entries))
	for _, entry := range entries {
		addrs := append([]string(nil), entry.Addrs...)
		sort.Strings(addrs)
		if len(addrs) == 0 {
			addrs = []string{}
		}
		got[entry.PeerID] = addrs
	}
	return got
}

func normalizeBootstrapListWant(want map[string][]string) map[string][]string {
	normalized := make(map[string][]string, len(want))
	for peerID, addrs := range want {
		copied := append([]string(nil), addrs...)
		sort.Strings(copied)
		if len(copied) == 0 {
			copied = []string{}
		}
		normalized[peerID] = copied
	}
	return normalized
}

func reserveHTTPPort(t *testing.T) int {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to reserve HTTP port: %v", err)
	}
	defer ln.Close()

	return ln.Addr().(*net.TCPAddr).Port
}
