package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/gorilla/websocket"
	"github.com/bisoncraft/mesh/oracle"
	"github.com/bisoncraft/mesh/tatanka/admin"
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
)

type navigateMsg struct{ view viewID }
type navigateBackMsg struct{}
type navigateToDiffMsg struct{ node admin.NodeInfo }
type navigateToSourceDetailMsg struct {
	sourceName string
}
type navigateToAggregatedDetailMsg struct {
	dataType oracle.DataType
	key      string // ticker or network name
}

// --- Data messages ---

type adminStateMsg struct {
	state *admin.AdminState
}

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

	wsMu     sync.Mutex
	wsConn   *websocket.Conn
	wsCancel chan struct{}
}

func newAPIClient(address string) *apiClient {
	return &apiClient{
		address: normalizeAddress(address),
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
