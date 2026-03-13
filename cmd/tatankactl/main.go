package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/bisoncraft/mesh/oracle"
	"github.com/bisoncraft/mesh/tatanka/admin"
	tea "github.com/charmbracelet/bubbletea"
)

// rootModel is the top-level bubbletea model that routes between views.
type rootModel struct {
	api                    *apiClient
	oracleData             *oracle.OracleSnapshot
	activeView             viewID
	menu                   menuModel
	connections            connectionsModel
	diff                   diffModel
	whitelistView          whitelistViewModel
	whitelistEditor        whitelistEditorModel
	oracle                 oracleModel
	oracleDetail           oracleDetailModel
	oracleAggregated       oracleAggregatedModel
	oracleAggregatedDetail oracleAggregatedDetailModel
	height                 int
	wsCh                   chan tea.Msg
}

func newRootModel(api *apiClient) rootModel {
	return rootModel{
		api:        api,
		oracleData: newOracleSnapshot(),
		activeView: viewMenu,
		menu:       newMenuModel(),
		wsCh:       make(chan tea.Msg, 20),
	}
}

func (m rootModel) Init() tea.Cmd {
	return m.api.connectWebSocket(m.wsCh)
}

func renderTick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return renderTickMsg(t)
	})
}

func (m rootModel) isOracleView() bool {
	switch m.activeView {
	case viewOracleSources, viewOracleDetail, viewOracleAggregated, viewAggregatedDetail:
		return true
	}
	return false
}

func (m rootModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" {
			m.api.disconnectWebSocket()
			return m, tea.Quit
		}

	case tea.WindowSizeMsg:
		m.height = msg.Height
		// Propagate to active child
		var cmd tea.Cmd
		switch m.activeView {
		case viewMenu:
			m.menu, cmd = m.menu.Update(msg)
			return m, cmd
		case viewConnections:
			m.connections, cmd = m.connections.Update(msg)
			return m, cmd
		case viewDiff:
			m.diff, cmd = m.diff.Update(msg)
			return m, cmd
		case viewWhitelist:
			m.whitelistView, cmd = m.whitelistView.Update(msg)
			return m, cmd
		case viewWhitelistEditor:
			m.whitelistEditor, cmd = m.whitelistEditor.Update(msg)
			return m, cmd
		case viewOracleSources:
			m.oracle, cmd = m.oracle.Update(msg)
			return m, cmd
		case viewOracleDetail:
			m.oracleDetail, cmd = m.oracleDetail.Update(msg)
			return m, cmd
		case viewOracleAggregated:
			m.oracleAggregated, cmd = m.oracleAggregated.Update(msg)
			return m, cmd
		case viewAggregatedDetail:
			m.oracleAggregatedDetail, cmd = m.oracleAggregatedDetail.Update(msg)
			return m, cmd
		}
		return m, nil

	case navigateMsg:
		switch msg.view {
		case viewConnections:
			m.activeView = viewConnections
			m.connections.height = m.height
			return m, nil
		case viewWhitelist:
			m.whitelistView = newWhitelistViewModel(m.connections.state, m.api)
			m.whitelistView.height = m.height
			m.activeView = viewWhitelist
			return m, nil
		case viewOracleSources:
			m.activeView = viewOracleSources
			m.oracle = newOracleModel(m.oracleData)
			m.oracle.height = m.height
			return m, tea.Batch(m.oracle.Init(), renderTick())
		case viewOracleAggregated:
			m.activeView = viewOracleAggregated
			m.oracleAggregated = newOracleAggregatedModel(m.oracleData)
			m.oracleAggregated.height = m.height
			return m, tea.Batch(m.oracleAggregated.Init(), renderTick())
		}
		return m, nil

	case navigateBackMsg:
		switch m.activeView {
		case viewConnections:
			m.activeView = viewMenu
			return m, nil
		case viewWhitelist:
			m.activeView = viewMenu
			return m, nil
		case viewWhitelistEditor:
			m.whitelistView = newWhitelistViewModel(m.connections.state, m.api)
			m.whitelistView.height = m.height
			m.activeView = viewWhitelist
			return m, nil
		case viewDiff:
			m.activeView = viewConnections
			return m, nil
		case viewOracleSources:
			m.activeView = viewMenu
			return m, nil
		case viewOracleDetail:
			m.activeView = viewOracleSources
			m.oracle.rebuildSortedSources()
			return m, nil
		case viewOracleAggregated:
			m.activeView = viewMenu
			return m, nil
		case viewAggregatedDetail:
			m.activeView = viewOracleAggregated
			m.oracleAggregated.buildSections()
			return m, nil
		}
		return m, nil

	case wsConnectedMsg:
		return m, listenForWSUpdates(m.wsCh)

	case wsErrorMsg:
		// Reconnect after a brief delay.
		return m, tea.Tick(3*time.Second, func(t time.Time) tea.Msg {
			return wsReconnectMsg{}
		})

	case wsReconnectMsg:
		return m, m.api.connectWebSocket(m.wsCh)

	// Oracle WS messages — update shared data and trigger view rebuilds
	case oracleSnapshotMsg, oracleUpdateMsg:
		updateOracleData(m.oracleData, msg)
		var cmds []tea.Cmd
		cmds = append(cmds, listenForWSUpdates(m.wsCh))
		if m.activeView == viewOracleDetail {
			m.oracleDetail.buildSections()
		}
		if m.activeView == viewOracleAggregated {
			m.oracleAggregated.buildSections()
		}
		if m.activeView == viewOracleSources {
			m.oracle.rebuildSortedSources()
		}
		return m, tea.Batch(cmds...)

	case adminStateMsg:
		if msg.state != nil {
			m.menu.peerID = msg.state.OurPeerID
			m.connections.state = msg.state
			m.connections.sortNodes()
			m.connections.lastUpdate = time.Now()
			if m.activeView == viewWhitelist {
				m.whitelistView.state = msg.state
				m.whitelistView.lastUpdate = time.Now()
				m.whitelistView.buildSections()
			}
		}
		return m, listenForWSUpdates(m.wsCh)

	case peerUpdateMsg:
		if m.connections.state != nil && msg.peer.PeerID != m.connections.state.OurPeerID {
			m.connections.state.Peers[msg.peer.PeerID] = msg.peer
			m.connections.sortNodes()
			m.connections.lastUpdate = time.Now()
			if m.activeView == viewWhitelist {
				m.whitelistView.state = m.connections.state
				m.whitelistView.lastUpdate = time.Now()
				m.whitelistView.buildSections()
			}
		}
		return m, listenForWSUpdates(m.wsCh)

	case whitelistStateUpdateMsg:
		if m.connections.state != nil {
			m.connections.state.WhitelistState = msg.state
			if m.activeView == viewWhitelist {
				m.whitelistView.state = m.connections.state
				m.whitelistView.lastUpdate = time.Now()
				m.whitelistView.buildSections()
			}
		}
		return m, listenForWSUpdates(m.wsCh)

	case whitelistUpdateMsg:
		if m.connections.state != nil && msg.update.WhitelistState != nil && msg.update.WhitelistState.Current != nil {
			newWl := msg.update.WhitelistState.Current.PeerIDs
			// Build string set for O(1) lookup.
			newPeers := make(map[string]struct{}, len(newWl))
			for pid := range newWl {
				newPeers[pid.String()] = struct{}{}
			}
			// Remove peers no longer in the whitelist.
			for id := range m.connections.state.Peers {
				if _, ok := newPeers[id]; !ok {
					delete(m.connections.state.Peers, id)
				}
			}
			// Add new peers that aren't already tracked.
			for pid := range newWl {
				s := pid.String()
				if s == m.connections.state.OurPeerID {
					continue
				}
				if _, ok := m.connections.state.Peers[s]; !ok {
					m.connections.state.Peers[s] = admin.PeerInfo{
						PeerID: s,
						State:  admin.StateDisconnected,
					}
				}
			}
			m.connections.state.WhitelistState = msg.update.WhitelistState
			m.connections.sortNodes()
			m.connections.lastUpdate = time.Now()
			if m.activeView == viewWhitelist {
				m.whitelistView.state = m.connections.state
				m.whitelistView.lastUpdate = time.Now()
				m.whitelistView.buildSections()
			}
		}
		return m, listenForWSUpdates(m.wsCh)

	case navigateToWhitelistEditorMsg:
		m.whitelistEditor = newWhitelistEditorModel(m.connections.state, m.api)
		m.whitelistEditor.height = m.height
		m.activeView = viewWhitelistEditor
		return m, nil

	case proposeResultMsg:
		if msg.err != nil {
			m.whitelistView.statusMsg = fmt.Sprintf("Propose failed: %v", msg.err)
		} else {
			m.whitelistView.statusMsg = "Proposal submitted"
		}
		return m, nil

	case clearProposalResultMsg:
		if msg.err != nil {
			m.whitelistView.statusMsg = fmt.Sprintf("Clear failed: %v", msg.err)
		} else {
			m.whitelistView.statusMsg = "Proposal cleared"
		}
		return m, nil

	case renderTickMsg:
		if m.isOracleView() {
			// Re-render for relative time updates
			return m, renderTick()
		}
		return m, nil

	case navigateToSourceDetailMsg:
		m.oracleDetail = newOracleDetailModel(m.oracleData, msg.sourceName)
		m.oracleDetail.height = m.height
		m.activeView = viewOracleDetail
		return m, m.oracleDetail.Init()

	case navigateToAggregatedDetailMsg:
		m.oracleAggregatedDetail = newOracleAggregatedDetailModel(m.oracleData, msg.dataType, msg.key)
		m.oracleAggregatedDetail.height = m.height
		m.activeView = viewAggregatedDetail
		return m, m.oracleAggregatedDetail.Init()

	case navigateToDiffMsg:
		m.diff = newDiffModel(msg.node, m.connections.state, m.api)
		m.diff.height = m.height
		m.activeView = viewDiff
		return m, m.diff.Init()

	case adoptResultMsg:
		if msg.err != nil {
			m.diff.statusMsg = fmt.Sprintf("Adopt failed: %v", msg.err)
		} else {
			m.activeView = viewConnections
		}
		return m, nil
	}

	// Delegate to active view
	var cmd tea.Cmd
	switch m.activeView {
	case viewMenu:
		m.menu, cmd = m.menu.Update(msg)
	case viewConnections:
		m.connections, cmd = m.connections.Update(msg)
	case viewDiff:
		m.diff, cmd = m.diff.Update(msg)
	case viewWhitelist:
		m.whitelistView, cmd = m.whitelistView.Update(msg)
	case viewWhitelistEditor:
		m.whitelistEditor, cmd = m.whitelistEditor.Update(msg)
	case viewOracleSources:
		m.oracle, cmd = m.oracle.Update(msg)
	case viewOracleDetail:
		m.oracleDetail, cmd = m.oracleDetail.Update(msg)
	case viewOracleAggregated:
		m.oracleAggregated, cmd = m.oracleAggregated.Update(msg)
	case viewAggregatedDetail:
		m.oracleAggregatedDetail, cmd = m.oracleAggregatedDetail.Update(msg)
	}
	return m, cmd
}

func (m rootModel) View() string {
	switch m.activeView {
	case viewMenu:
		return m.menu.View()
	case viewConnections:
		return m.connections.View()
	case viewDiff:
		return m.diff.View()
	case viewWhitelist:
		return m.whitelistView.View()
	case viewWhitelistEditor:
		return m.whitelistEditor.View()
	case viewOracleSources:
		return m.oracle.View()
	case viewOracleDetail:
		return m.oracleDetail.View()
	case viewOracleAggregated:
		return m.oracleAggregated.View()
	case viewAggregatedDetail:
		return m.oracleAggregatedDetail.View()
	default:
		return ""
	}
}

func main() {
	address := flag.String("a", "localhost:12366", "Admin server address")
	flag.StringVar(address, "address", "localhost:12366", "Admin server address")
	flag.Parse()

	api := newAPIClient(*address)
	model := newRootModel(api)

	p := tea.NewProgram(model, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
