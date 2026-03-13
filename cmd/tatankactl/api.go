package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bisoncraft/mesh/oracle"
	"github.com/bisoncraft/mesh/tatanka/admin"
	"github.com/bisoncraft/mesh/tatanka/types"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/gorilla/websocket"
)

// --- Navigation messages ---

type viewID int

const (
	viewMenu viewID = iota
	viewConnections
	viewDiff
	viewOracleSources
	viewOracleDetail
	viewOracleAggregated
	viewAggregatedDetail
	viewWhitelist
	viewWhitelistEditor
)

type navigateMsg struct{ view viewID }
type navigateBackMsg struct{}
type navigateToDiffMsg struct{ node admin.PeerInfo }
type navigateToSourceDetailMsg struct {
	sourceName string
}
type navigateToAggregatedDetailMsg struct {
	dataType oracle.DataType
	key      string // ticker or network name
}
type navigateToWhitelistEditorMsg struct{}

type proposeResultMsg struct{ err error }
type clearProposalResultMsg struct{ err error }
type adoptResultMsg struct{ err error }

// --- Data messages ---

type adminStateMsg struct {
	state *admin.AdminState
}

type peerUpdateMsg struct{ peer admin.PeerInfo }
type whitelistStateUpdateMsg struct{ state *types.WhitelistState }
type whitelistUpdateMsg struct{ update admin.WhitelistUpdate }

type wsConnectedMsg struct{}
type wsErrorMsg struct{ err error }
type wsReconnectMsg struct{}

// oracleSnapshotMsg is received on WS connect as full state.
type oracleSnapshotMsg oracle.OracleSnapshot

// oracleUpdateMsg is a partial diff received as oracle_update.
type oracleUpdateMsg oracle.OracleSnapshot

// renderTickMsg triggers periodic re-rendering while oracle views are active.
type renderTickMsg time.Time

// --- Shared helpers ---

func navBack() tea.Cmd {
	return func() tea.Msg { return navigateBackMsg{} }
}

func sortedKeys[M ~map[string]V, V any](m M) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// --- Shared oracle data helpers ---

func newOracleSnapshot() *oracle.OracleSnapshot {
	return &oracle.OracleSnapshot{
		Prices:   make(map[string]*oracle.SnapshotRate),
		FeeRates: make(map[string]*oracle.SnapshotRate),
		Sources:  make(map[string]*oracle.SourceStatus),
	}
}

func getOrCreateSource(d *oracle.OracleSnapshot, name string) *oracle.SourceStatus {
	src, ok := d.Sources[name]
	if !ok {
		src = &oracle.SourceStatus{
			Fetches24h: make(map[string]int),
			Quotas:     make(map[string]*oracle.Quota),
		}
		d.Sources[name] = src
	}
	return src
}

func getOrCreateRate(m map[string]*oracle.SnapshotRate, key string) *oracle.SnapshotRate {
	r, ok := m[key]
	if !ok {
		r = &oracle.SnapshotRate{
			Contributions: make(map[string]*oracle.SourceContribution),
		}
		m[key] = r
	}
	return r
}

// mergeSnapshot applies a partial oracle.OracleSnapshot to the shared state.
func mergeSnapshot(d *oracle.OracleSnapshot, msg oracle.OracleSnapshot) {
	if msg.NodeID != "" {
		d.NodeID = msg.NodeID
	}

	for name, s := range msg.Sources {
		src := getOrCreateSource(d, name)
		if s.LastFetch != nil {
			src.LastFetch = s.LastFetch
		}
		if s.NextFetchTime != nil {
			src.NextFetchTime = s.NextFetchTime
		}
		if s.MinFetchInterval != nil {
			src.MinFetchInterval = s.MinFetchInterval
		}
		if s.NetworkSustainableRate != nil {
			src.NetworkSustainableRate = s.NetworkSustainableRate
		}
		if s.NetworkSustainablePeriod != nil {
			src.NetworkSustainablePeriod = s.NetworkSustainablePeriod
		}
		if s.NetworkNextFetchTime != nil {
			src.NetworkNextFetchTime = s.NetworkNextFetchTime
		}
		if s.NextFetchTime != nil {
			// Schedule updates from the diviner always carry error
			// state. Empty values mean the error was cleared.
			src.LastError = s.LastError
			src.LastErrorTime = s.LastErrorTime
		} else if s.LastError != "" || s.LastErrorTime != nil {
			src.LastError = s.LastError
			src.LastErrorTime = s.LastErrorTime
		}
		if s.OrderedNodes != nil {
			src.OrderedNodes = s.OrderedNodes
		}
		if s.Fetches24h != nil {
			src.Fetches24h = s.Fetches24h
		}
		if s.Quotas != nil {
			for nodeID, q := range s.Quotas {
				src.Quotas[nodeID] = q
			}
		}
		if s.LatestData != nil {
			if src.LatestData == nil {
				src.LatestData = make(map[string]map[string]string)
			}
			for dataType, entries := range s.LatestData {
				if src.LatestData[dataType] == nil {
					src.LatestData[dataType] = make(map[string]string)
				}
				for id, value := range entries {
					src.LatestData[dataType][id] = value
				}
			}
		}
	}

	for ticker, sr := range msg.Prices {
		rate := getOrCreateRate(d.Prices, ticker)
		rate.Value = sr.Value
		for source, c := range sr.Contributions {
			rate.Contributions[source] = c
		}
	}

	for network, sr := range msg.FeeRates {
		rate := getOrCreateRate(d.FeeRates, network)
		rate.Value = sr.Value
		for source, c := range sr.Contributions {
			rate.Contributions[source] = c
		}
	}
}

// updateOracleData applies a WS message to the shared oracle data.
func updateOracleData(d *oracle.OracleSnapshot, msg tea.Msg) {
	switch msg := msg.(type) {
	case oracleSnapshotMsg:
		d.Sources = make(map[string]*oracle.SourceStatus)
		d.Prices = make(map[string]*oracle.SnapshotRate)
		d.FeeRates = make(map[string]*oracle.SnapshotRate)
		mergeSnapshot(d, oracle.OracleSnapshot(msg))
	case oracleUpdateMsg:
		mergeSnapshot(d, oracle.OracleSnapshot(msg))
	}
}

// --- API client ---

type apiClient struct {
	address string
	http    *http.Client

	wsMu     sync.Mutex
	wsConn   *websocket.Conn
	wsCancel chan struct{}
}

func newAPIClient(address string) *apiClient {
	return &apiClient{
		address: normalizeAddress(address),
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

func normalizeAddress(addr string) string {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		return "http://" + addr
	}
	return addr
}

// wsMessage mirrors admin.WSMessage for client-side parsing.
type wsMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func (c *apiClient) connectWebSocket(ch chan<- tea.Msg) tea.Cmd {
	return func() tea.Msg {
		wsURL, err := url.Parse(c.address)
		if err != nil {
			return wsErrorMsg{err: fmt.Errorf("invalid address: %w", err)}
		}

		if wsURL.Scheme == "https" {
			wsURL.Scheme = "wss"
		} else {
			wsURL.Scheme = "ws"
		}
		wsURL.Path = "/admin/ws"

		conn, _, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
		if err != nil {
			return wsErrorMsg{err: fmt.Errorf("failed to connect: %w", err)}
		}

		cancel := make(chan struct{})

		c.wsMu.Lock()
		c.wsConn = conn
		c.wsCancel = cancel
		c.wsMu.Unlock()

		// Start reader goroutine
		go func() {
			defer conn.Close()
			for {
				select {
				case <-cancel:
					return
				default:
				}

				_, message, err := conn.ReadMessage()
				if err != nil {
					select {
					case <-cancel:
						return
					default:
					}
					ch <- wsErrorMsg{err: fmt.Errorf("connection lost: %w", err)}
					return
				}

				var envelope wsMessage
				if err := json.Unmarshal(message, &envelope); err != nil {
					continue
				}

				var msg tea.Msg
				switch envelope.Type {
				case "admin_state":
					var state admin.AdminState
					if err := json.Unmarshal(envelope.Data, &state); err != nil {
						continue
					}
					msg = adminStateMsg{state: &state}
				case "peer_update":
					var pi admin.PeerInfo
					if err := json.Unmarshal(envelope.Data, &pi); err != nil {
						continue
					}
					msg = peerUpdateMsg{peer: pi}
				case "whitelist_state":
					var ws types.WhitelistState
					if err := json.Unmarshal(envelope.Data, &ws); err != nil {
						continue
					}
					msg = whitelistStateUpdateMsg{state: &ws}
				case "whitelist_update":
					var wu admin.WhitelistUpdate
					if err := json.Unmarshal(envelope.Data, &wu); err != nil {
						continue
					}
					msg = whitelistUpdateMsg{update: wu}
				case "oracle_snapshot":
					var snapshot oracleSnapshotMsg
					if err := json.Unmarshal(envelope.Data, &snapshot); err != nil {
						continue
					}
					msg = snapshot
				case "oracle_update":
					var update oracleUpdateMsg
					if err := json.Unmarshal(envelope.Data, &update); err != nil {
						continue
					}
					msg = update
				default:
					continue
				}

				select {
				case ch <- msg:
				case <-cancel:
					return
				}
			}
		}()

		return wsConnectedMsg{}
	}
}

func (c *apiClient) disconnectWebSocket() {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()

	if c.wsCancel != nil {
		close(c.wsCancel)
		c.wsCancel = nil
	}
	if c.wsConn != nil {
		c.wsConn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		)
		c.wsConn.Close()
		c.wsConn = nil
	}
}

func listenForWSUpdates(ch <-chan tea.Msg) tea.Cmd {
	return func() tea.Msg {
		msg, ok := <-ch
		if !ok {
			return wsErrorMsg{err: fmt.Errorf("channel closed")}
		}
		return msg
	}
}

func (c *apiClient) proposeWhitelist(peers []string) tea.Cmd {
	return func() tea.Msg {
		body, _ := json.Marshal(map[string][]string{"peers": peers})
		resp, err := c.http.Post(c.address+"/admin/propose-whitelist", "application/json", bytes.NewReader(body))
		if err != nil {
			return proposeResultMsg{err: err}
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			return proposeResultMsg{err: fmt.Errorf("%s", strings.TrimSpace(string(b)))}
		}
		return proposeResultMsg{}
	}
}

func (c *apiClient) adoptWhitelist(peerID string) tea.Cmd {
	return func() tea.Msg {
		body, _ := json.Marshal(map[string]string{"peer_id": peerID})
		resp, err := c.http.Post(c.address+"/admin/adopt-whitelist", "application/json", bytes.NewReader(body))
		if err != nil {
			return adoptResultMsg{err: err}
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			return adoptResultMsg{err: fmt.Errorf("%s", strings.TrimSpace(string(b)))}
		}
		return adoptResultMsg{}
	}
}

func (c *apiClient) clearProposal() tea.Cmd {
	return func() tea.Msg {
		req, _ := http.NewRequest(http.MethodDelete, c.address+"/admin/propose-whitelist", nil)
		resp, err := c.http.Do(req)
		if err != nil {
			return clearProposalResultMsg{err: err}
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			return clearProposalResultMsg{err: fmt.Errorf("%s", strings.TrimSpace(string(b)))}
		}
		return clearProposalResultMsg{}
	}
}

// --- Whitelist helpers ---

// peerIDStrings returns sorted peer ID strings from a Whitelist.
func peerIDStrings(wl *types.Whitelist) []string {
	if wl == nil {
		return nil
	}
	strs := make([]string, 0, len(wl.PeerIDs))
	for pid := range wl.PeerIDs {
		strs = append(strs, pid.String())
	}
	sort.Strings(strs)
	return strs
}

// getOurWhitelist returns the sorted peer ID strings from our current whitelist.
func getOurWhitelist(state *admin.AdminState) []string {
	if state == nil || state.WhitelistState == nil {
		return nil
	}
	return peerIDStrings(state.WhitelistState.Current)
}

// getOurProposal returns the sorted peer ID strings from our proposed whitelist.
func getOurProposal(state *admin.AdminState) []string {
	if state == nil || state.WhitelistState == nil {
		return nil
	}
	return peerIDStrings(state.WhitelistState.Proposed)
}

// networkProposal represents a proposed whitelist with its supporters.
type networkProposal struct {
	proposedPeerIDs []string
	supporters      map[string]bool // peerID string -> ready
}

// computeNetworkProposals derives network proposals from peer states.
func computeNetworkProposals(state *admin.AdminState) []networkProposal {
	if state == nil {
		return nil
	}

	type proposal struct {
		peerIDs    []string
		supporters map[string]bool
	}
	proposals := make(map[string]*proposal)

	// Include our own proposal from the top-level WhitelistState.
	if state.WhitelistState != nil && state.WhitelistState.Proposed != nil {
		hash := state.WhitelistState.Proposed.Hash()
		np := &proposal{
			peerIDs:    peerIDStrings(state.WhitelistState.Proposed),
			supporters: make(map[string]bool),
		}
		np.supporters[state.OurPeerID] = state.WhitelistState.Ready
		proposals[hash] = np
	}

	for _, peer := range state.Peers {
		if peer.WhitelistState == nil || peer.WhitelistState.Proposed == nil {
			continue
		}
		hash := peer.WhitelistState.Proposed.Hash()
		np, ok := proposals[hash]
		if !ok {
			np = &proposal{
				peerIDs:    peerIDStrings(peer.WhitelistState.Proposed),
				supporters: make(map[string]bool),
			}
			proposals[hash] = np
		}
		np.supporters[peer.PeerID] = peer.WhitelistState.Ready
	}

	result := make([]networkProposal, 0, len(proposals))
	for _, np := range proposals {
		result = append(result, networkProposal{
			proposedPeerIDs: np.peerIDs,
			supporters:      np.supporters,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		if len(result[i].supporters) != len(result[j].supporters) {
			return len(result[i].supporters) > len(result[j].supporters)
		}
		return strings.Join(result[i].proposedPeerIDs, ",") < strings.Join(result[j].proposedPeerIDs, ",")
	})

	return result
}

// overlapPeerInfo describes one overlap peer's consensus status.
type overlapPeerInfo struct {
	peerID   string
	agreeing bool
	ready    bool
	online   bool
}

// consensusInfo summarises consensus progress for a whitelist proposal.
type consensusInfo struct {
	overlap   []overlapPeerInfo
	nOverlap  int
	threshold int
	agreeing  int
	blocking  int // online overlap peers not agreeing
}

// computeConsensusInfo computes consensus progress for a proposal relative
// to the current whitelist, given its set of supporters.
func computeConsensusInfo(state *admin.AdminState, currentWl, proposedWl []string, supporters map[string]bool) consensusInfo {
	currentSet := make(map[string]bool, len(currentWl))
	for _, id := range currentWl {
		currentSet[id] = true
	}
	proposedSet := make(map[string]bool, len(proposedWl))
	for _, id := range proposedWl {
		proposedSet[id] = true
	}

	// Overlap = peers in both current and proposed whitelists.
	var overlapIDs []string
	for id := range currentSet {
		if proposedSet[id] {
			overlapIDs = append(overlapIDs, id)
		}
	}
	sort.Strings(overlapIDs)

	nOverlap := len(overlapIDs)
	threshold := (2*nOverlap + 2) / 3

	info := consensusInfo{nOverlap: nOverlap, threshold: threshold}

	for _, id := range overlapIDs {
		opi := overlapPeerInfo{peerID: id}

		_, supports := supporters[id]
		if supports {
			opi.agreeing = true
			opi.ready = supporters[id]
			info.agreeing++
		}

		if id == state.OurPeerID {
			opi.online = true
			if !supports {
				info.blocking++
			}
		} else if peer, ok := state.Peers[id]; ok && (peer.State == admin.StateConnected || peer.State == admin.StateWhitelistMismatch) {
			opi.online = true
			if !supports {
				info.blocking++
			}
		}

		info.overlap = append(info.overlap, opi)
	}

	return info
}
