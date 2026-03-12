package tatanka

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/record"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	ma "github.com/multiformats/go-multiaddr"
)

func TestPeerIdentificationReconnectUpdatesPeerstoreCacheAndBootstrapList(t *testing.T) {
	rootDir := t.TempDir()
	node1Dir := filepath.Join(rootDir, "node1")
	node2Dir := filepath.Join(rootDir, "node2")

	node1ID, err := InitTatankaNode(node1Dir)
	if err != nil {
		t.Fatalf("InitTatankaNode(node1): %v", err)
	}
	node2ID, err := InitTatankaNode(node2Dir)
	if err != nil {
		t.Fatalf("InitTatankaNode(node2): %v", err)
	}

	whitelist := []peer.ID{node1ID, node2ID}
	bootstrapPath := filepath.Join(node1Dir, "bootstrap.json")
	cachePath := filepath.Join(node1Dir, "peerstore.json")

	node1Cfg := &Config{
		DataDir:           node1Dir,
		Logger:            newTestLogger(),
		ListenPort:        0,
		MetricsPort:       0,
		AdminPort:         0,
		WhitelistPeers:    whitelist,
		ForceWhitelist:    true,
		BootstrapListFile: bootstrapPath,
	}

	node1, cancelNode1, doneNode1 := startConfiguredTestNode(t, node1Cfg)
	defer stopConfiguredTestNode(t, cancelNode1, doneNode1)

	node1BootstrapAddr := firstP2PAddrString(t, node1.node)
	oldListenPort := freeTCPPort(t)
	newListenPort := freeTCPPortExcluding(t, oldListenPort)

	node2Cfg := &Config{
		DataDir:        node2Dir,
		Logger:         newTestLogger(),
		ListenPort:     oldListenPort,
		MetricsPort:    0,
		AdminPort:      0,
		WhitelistPeers: whitelist,
		ForceWhitelist: true,
		BootstrapAddrs: []string{node1BootstrapAddr},
	}

	_, cancelNode2, doneNode2 := startConfiguredTestNode(t, node2Cfg)
	oldAddr := loopbackTCPAddr(oldListenPort)

	waitForConnectedness(t, node1, node2ID, network.Connected)
	waitForPeerAddrs(t, node1, node2ID, oldAddr)
	waitForCacheAddrs(t, cachePath, node2ID, oldAddr)
	waitForBootstrapPeerAddrs(t, bootstrapPath, node2ID, oldAddr)

	stopConfiguredTestNode(t, cancelNode2, doneNode2)
	waitForConnectedness(t, node1, node2ID, network.NotConnected)

	node2Cfg.ListenPort = newListenPort
	node2, cancelNode2, doneNode2 := startConfiguredTestNode(t, node2Cfg)
	defer stopConfiguredTestNode(t, cancelNode2, doneNode2)

	newAddr := loopbackTCPAddr(newListenPort)
	waitForConnectedness(t, node1, node2ID, network.Connected)
	waitForPeerAddrs(t, node1, node2ID, newAddr)
	waitForCacheAddrs(t, cachePath, node2ID, newAddr)
	waitForBootstrapPeerAddrs(t, bootstrapPath, node2ID, newAddr)

	if node2.node.ID() != node2ID {
		t.Fatalf("expected restarted node to keep peer ID %s, got %s", node2ID, node2.node.ID())
	}
}

func TestAuthoritativePeerAddrsPrefersSignedPeerRecord(t *testing.T) {
	mnet, err := mocknet.WithNPeers(1)
	if err != nil {
		t.Fatal(err)
	}
	remote := mnet.Hosts()[0]

	listenAddr := mustMultiaddr(t, "/ip4/1.1.1.1/tcp/1111")
	signedAddr := mustMultiaddr(t, "/ip4/2.2.2.2/tcp/2222")

	addrs, source, err := authoritativePeerAddrs(event.EvtPeerIdentificationCompleted{
		Peer:             remote.ID(),
		ListenAddrs:      []ma.Multiaddr{listenAddr},
		SignedPeerRecord: signPeerRecordForHost(t, remote, signedAddr),
	})
	if err != nil {
		t.Fatalf("authoritativePeerAddrs errored: %v", err)
	}
	if source != "signed peer record" {
		t.Fatalf("expected signed peer record source, got %q", source)
	}
	assertAddrStringsEqual(t, multiaddrStrings(addrs), []string{signedAddr.String()})
}

func TestFilterIdentifyAddrs(t *testing.T) {
	loopbackAddr := mustMultiaddr(t, "/ip4/127.0.0.1/tcp/1111")
	privateAddr := mustMultiaddr(t, "/ip4/10.0.0.8/tcp/2222")
	publicAddr := mustMultiaddr(t, "/ip4/8.8.8.8/tcp/3333")
	allAddrs := []ma.Multiaddr{loopbackAddr, privateAddr, publicAddr}

	tests := []struct {
		name   string
		remote ma.Multiaddr
		want   []string
	}{
		{
			name:   "loopback remote keeps all addrs",
			remote: mustMultiaddr(t, "/ip4/127.0.0.1/tcp/9999"),
			want:   []string{loopbackAddr.String(), privateAddr.String(), publicAddr.String()},
		},
		{
			name:   "private remote drops loopback addrs",
			remote: mustMultiaddr(t, "/ip4/10.0.0.9/tcp/9999"),
			want:   []string{privateAddr.String(), publicAddr.String()},
		},
		{
			name:   "public remote keeps only public addrs",
			remote: mustMultiaddr(t, "/ip4/9.9.9.9/tcp/9999"),
			want:   []string{publicAddr.String()},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := multiaddrStrings(filterIdentifyAddrs(allAddrs, test.remote))
			assertAddrStringsEqual(t, got, test.want)
		})
	}
}

func startConfiguredTestNode(t *testing.T, cfg *Config) (*TatankaNode, context.CancelFunc, <-chan struct{}) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	node, err := NewTatankaNode(cfg)
	if err != nil {
		cancel()
		t.Fatalf("NewTatankaNode(%s): %v", cfg.DataDir, err)
	}

	node.bondStorage = &testBondStorage{score: 1}
	node.oracle = &testOracle{}

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := node.Run(ctx); err != nil {
			t.Errorf("Run(%s): %v", cfg.DataDir, err)
		}
	}()

	if err := node.WaitReady(ctx); err != nil {
		cancel()
		<-done
		t.Fatalf("WaitReady(%s): %v", cfg.DataDir, err)
	}

	return node, cancel, done
}

func stopConfiguredTestNode(t *testing.T, cancel context.CancelFunc, done <-chan struct{}) {
	t.Helper()

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for test node shutdown")
	}
}

func waitForConnectedness(t *testing.T, node *TatankaNode, pid peer.ID, want network.Connectedness) {
	t.Helper()

	requireEventually(t, func() bool {
		return node.node.Network().Connectedness(pid) == want
	}, 10*time.Second, 50*time.Millisecond, "peer %s did not reach connectedness %v", pid, want)
}

func waitForPeerAddrs(t *testing.T, node *TatankaNode, pid peer.ID, want ...string) {
	t.Helper()

	requireEventually(t, func() bool {
		return addrStringsEqual(multiaddrStrings(node.node.Peerstore().Addrs(pid)), want)
	}, 10*time.Second, 50*time.Millisecond, "peerstore addrs for %s did not become %v", pid, want)
}

func firstP2PAddrString(t *testing.T, h host.Host) string {
	t.Helper()

	addrs, err := peer.AddrInfoToP2pAddrs(&peer.AddrInfo{
		ID:    h.ID(),
		Addrs: h.Addrs(),
	})
	if err != nil {
		t.Fatalf("AddrInfoToP2pAddrs: %v", err)
	}
	if len(addrs) == 0 {
		t.Fatal("host has no p2p addresses")
	}

	return addrs[0].String()
}

func loopbackTCPAddr(port int) string {
	return fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)
}

func freeTCPPort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	defer l.Close()

	tcpAddr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("unexpected listener addr type %T", l.Addr())
	}
	return tcpAddr.Port
}

func freeTCPPortExcluding(t *testing.T, exclude ...int) int {
	t.Helper()

	excluded := make(map[int]struct{}, len(exclude))
	for _, port := range exclude {
		excluded[port] = struct{}{}
	}

	for i := 0; i < 10; i++ {
		port := freeTCPPort(t)
		if _, found := excluded[port]; !found {
			return port
		}
	}

	t.Fatal("failed to find a distinct free TCP port")
	return 0
}

func signPeerRecordForHost(t *testing.T, h host.Host, addrs ...ma.Multiaddr) *record.Envelope {
	t.Helper()

	key := h.Peerstore().PrivKey(h.ID())
	if key == nil {
		t.Fatal("host private key not found in peerstore")
	}

	rec := peer.NewPeerRecord()
	rec.PeerID = h.ID()
	rec.Addrs = addrs

	signed, err := record.Seal(rec, key)
	if err != nil {
		t.Fatalf("Failed to sign peer record: %v", err)
	}
	return signed
}

func waitForCacheAddrs(t *testing.T, cachePath string, pid peer.ID, want ...string) {
	t.Helper()

	requireEventually(t, func() bool {
		data, err := os.ReadFile(cachePath)
		if err != nil {
			return false
		}

		var cache map[string][]string
		if err := json.Unmarshal(data, &cache); err != nil {
			return false
		}

		return addrStringsEqual(cache[pid.String()], want)
	}, 10*time.Second, 50*time.Millisecond, "cache addrs for %s did not become %v", pid, want)
}

func waitForBootstrapPeerAddrs(t *testing.T, bootstrapPath string, pid peer.ID, want ...string) {
	t.Helper()

	requireEventually(t, func() bool {
		data, err := os.ReadFile(bootstrapPath)
		if err != nil {
			return false
		}

		var entries []bootstrapPeerEntry
		if err := json.Unmarshal(data, &entries); err != nil {
			return false
		}

		for _, entry := range entries {
			if entry.PeerID == pid.String() {
				return addrStringsEqual(entry.Addrs, want)
			}
		}

		return false
	}, 10*time.Second, 50*time.Millisecond, "bootstrap addrs for %s did not become %v", pid, want)
}

func multiaddrStrings(addrs []ma.Multiaddr) []string {
	if len(addrs) == 0 {
		return nil
	}

	out := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if addr == nil {
			continue
		}
		out = append(out, addr.String())
	}
	return out
}

func assertAddrStringsEqual(t *testing.T, got, want []string) {
	t.Helper()
	if !addrStringsEqual(got, want) {
		t.Fatalf("unexpected addresses: got %v want %v", got, want)
	}
}

func addrStringsEqual(got, want []string) bool {
	gotCopy := append([]string(nil), got...)
	wantCopy := append([]string(nil), want...)
	sort.Strings(gotCopy)
	sort.Strings(wantCopy)
	if len(gotCopy) != len(wantCopy) {
		return false
	}
	for i := range gotCopy {
		if gotCopy[i] != wantCopy[i] {
			return false
		}
	}
	return true
}

func mustMultiaddr(t *testing.T, addr string) ma.Multiaddr {
	t.Helper()

	maddr, err := ma.NewMultiaddr(addr)
	if err != nil {
		t.Fatalf("Failed to parse multiaddr %q: %v", addr, err)
	}
	return maddr
}
