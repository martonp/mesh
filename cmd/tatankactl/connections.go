package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/bisoncraft/mesh/tatanka/admin"
)

type connectionsModel struct {
	state           *admin.AdminState
	nodes           []admin.NodeInfo
	mismatchIndices []int
	cursor          int // index into mismatchIndices
	lastUpdate      time.Time
	height          int
}

func (m connectionsModel) Update(msg tea.Msg) (connectionsModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.height = msg.Height
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			if len(m.mismatchIndices) > 0 && m.cursor > 0 {
				m.cursor--
			}
		case "down", "j":
			if len(m.mismatchIndices) > 0 && m.cursor < len(m.mismatchIndices)-1 {
				m.cursor++
			}
		case "enter":
			if len(m.mismatchIndices) > 0 {
				idx := m.mismatchIndices[m.cursor]
				node := m.nodes[idx]
				return m, func() tea.Msg {
					return navigateToDiffMsg{node: node}
				}
			}
		case "esc", "q":
			return m, navBack()
		}
	}
	return m, nil
}

func (m *connectionsModel) sortNodes() {
	nodes := make([]admin.NodeInfo, 0, len(m.state.Nodes))
	for _, node := range m.state.Nodes {
		nodes = append(nodes, node)
	}

	stateOrder := map[admin.NodeConnectionState]int{
		admin.StateConnected:         0,
		admin.StateWhitelistMismatch: 1,
		admin.StateDisconnected:      2,
	}
	sort.Slice(nodes, func(i, j int) bool {
		oi, oj := stateOrder[nodes[i].State], stateOrder[nodes[j].State]
		if oi != oj {
			return oi < oj
		}
		return nodes[i].PeerID < nodes[j].PeerID
	})

	m.nodes = nodes
	m.mismatchIndices = nil
	for i, n := range nodes {
		if n.State == admin.StateWhitelistMismatch {
			m.mismatchIndices = append(m.mismatchIndices, i)
		}
	}

	// Keep cursor in bounds
	if m.cursor >= len(m.mismatchIndices) {
		m.cursor = max(0, len(m.mismatchIndices)-1)
	}
}

func (m connectionsModel) View() string {
	var b strings.Builder

	// Header
	ts := ""
	if !m.lastUpdate.IsZero() {
		ts = dimStyle.Render(m.lastUpdate.Format("15:04:05"))
	}
	b.WriteString(fmt.Sprintf(" %s%s\n\n",
		headerStyle.Render("Connections"),
		pad(ts, 50)))

	if m.state == nil {
		b.WriteString(" Waiting for data...\n")
		b.WriteString(helpStyle.Render("\n Esc: Back"))
		return b.String()
	}

	// Summary counts
	counts := make(map[admin.NodeConnectionState]int)
	for _, node := range m.nodes {
		counts[node.State]++
	}
	b.WriteString(fmt.Sprintf(" Connected: %s | Mismatch: %s | Disconnected: %s\n\n",
		connectedStyle.Render(fmt.Sprintf("%d", counts[admin.StateConnected])),
		mismatchStyle.Render(fmt.Sprintf("%d", counts[admin.StateWhitelistMismatch])),
		disconnectedStyle.Render(fmt.Sprintf("%d", counts[admin.StateDisconnected])),
	))

	if len(m.nodes) == 0 {
		b.WriteString(" No nodes in whitelist\n")
		b.WriteString(helpStyle.Render("\n Esc: Back"))
		return b.String()
	}

	// Build a set of mismatch node indices for cursor display
	selectedNodeIdx := -1
	if len(m.mismatchIndices) > 0 {
		selectedNodeIdx = m.mismatchIndices[m.cursor]
	}

	for i, node := range m.nodes {
		icon := getStateIcon(node.State)
		stateStr := getStateString(node.State)

		cursorStr := ""
		if i == selectedNodeIdx {
			cursorStr = cursorStyle.Render(" \u25c0 [Enter for diff]")
		}

		b.WriteString(fmt.Sprintf(" %s %-25s %s%s\n",
			icon, stateStr, dimStyle.Render(node.PeerID), cursorStr))

		for _, addr := range node.Addresses {
			b.WriteString(fmt.Sprintf("    \u2502  %s\n", dimStyle.Render(addr)))
		}

		b.WriteString("\n")
	}

	// Help
	help := " \u2191\u2193 Navigate mismatch nodes   Enter: View diff   Esc: Back"
	if len(m.mismatchIndices) == 0 {
		help = " Esc: Back"
	}
	b.WriteString(helpStyle.Render(help))

	return fitToHeight(b.String(), m.height)
}

func pad(s string, width int) string {
	// Right-align s within width by prepending spaces
	w := lipgloss.Width(s)
	if w >= width {
		return "  " + s
	}
	return strings.Repeat(" ", width-w) + s
}
