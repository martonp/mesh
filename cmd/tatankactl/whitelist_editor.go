package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/bisoncraft/mesh/tatanka/admin"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/libp2p/go-libp2p/core/peer"
)

type editorEntry struct {
	peerID    string
	checked   bool
	inCurrent bool
	source    string // "current", "network", or "manual"
}

type whitelistEditorModel struct {
	entries      []editorEntry
	cursor       int
	confirming   bool
	adding       bool      // text input mode for adding a new peer ID
	addInput     textInput // text being typed
	addError     string    // validation error
	height       int
	api          *apiClient
	scrollOffset int
	ourPeerID    string // our own peer ID, cannot be unchecked
}

func newWhitelistEditorModel(state *admin.AdminState, api *apiClient) whitelistEditorModel {
	// Collect union of all known peer IDs.
	allIDs := make(map[string]string) // peerID -> source
	currentSet := make(map[string]bool)

	ourWl := getOurWhitelist(state)
	for _, id := range ourWl {
		allIDs[id] = "current"
		currentSet[id] = true
	}

	if state != nil {
		// Add peer whitelist from mismatch nodes.
		for _, p := range state.Peers {
			if p.State == admin.StateWhitelistMismatch && p.WhitelistState != nil && p.WhitelistState.Current != nil {
				for _, id := range peerIDStrings(p.WhitelistState.Current) {
					if _, ok := allIDs[id]; !ok {
						allIDs[id] = "network"
					}
				}
			}
		}
		// Add proposed peer IDs from all network proposals.
		for _, np := range computeNetworkProposals(state) {
			for _, id := range np.proposedPeerIDs {
				if _, ok := allIDs[id]; !ok {
					allIDs[id] = "network"
				}
			}
		}
	}

	// Pre-check the proposed whitelist if one exists, otherwise the current.
	preselected := getOurProposal(state)
	if preselected == nil {
		preselected = ourWl
	}
	checkedSet := make(map[string]bool, len(preselected))
	for _, id := range preselected {
		checkedSet[id] = true
	}

	// Sort and build entries.
	sorted := make([]string, 0, len(allIDs))
	for id := range allIDs {
		sorted = append(sorted, id)
	}
	sort.Strings(sorted)

	entries := make([]editorEntry, len(sorted))
	for i, id := range sorted {
		entries[i] = editorEntry{
			peerID:    id,
			checked:   checkedSet[id],
			inCurrent: currentSet[id],
			source:    allIDs[id],
		}
	}

	var ourPeerID string
	if state != nil {
		ourPeerID = state.OurPeerID
	}

	return whitelistEditorModel{
		entries:   entries,
		api:       api,
		ourPeerID: ourPeerID,
	}
}

func (m whitelistEditorModel) selectedCount() int {
	n := 0
	for _, e := range m.entries {
		if e.checked {
			n++
		}
	}
	return n
}

func (m whitelistEditorModel) selectedPeers() []string {
	var peers []string
	for _, e := range m.entries {
		if e.checked {
			peers = append(peers, e.peerID)
		}
	}
	return peers
}

func (m *whitelistEditorModel) addPeerID() {
	id := strings.TrimSpace(m.addInput.text)
	if id == "" {
		return
	}
	// Validate peer ID format.
	if _, err := peer.Decode(id); err != nil {
		m.addError = "invalid peer ID"
		return
	}
	// Check for duplicates.
	for _, e := range m.entries {
		if e.peerID == id {
			m.addError = "already in list"
			return
		}
	}
	m.entries = append(m.entries, editorEntry{
		peerID:  id,
		checked: true,
		source:  "manual",
	})
	// Move cursor to the new entry.
	m.cursor = len(m.entries) - 1
	visible := m.visibleCount()
	if m.cursor >= m.scrollOffset+visible {
		m.scrollOffset = m.cursor - visible + 1
	}
}

func (m whitelistEditorModel) Update(msg tea.Msg) (whitelistEditorModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.height = msg.Height
	case tea.KeyMsg:
		if m.confirming {
			switch msg.String() {
			case "y":
				m.confirming = false
				peers := m.selectedPeers()
				return m, tea.Batch(
					m.api.proposeWhitelist(peers),
					func() tea.Msg { return navigateBackMsg{} },
				)
			case "n", "esc":
				m.confirming = false
			}
			return m, nil
		}

		if m.adding {
			switch msg.String() {
			case "enter":
				m.addPeerID()
				if m.addError == "" {
					m.adding = false
					m.addInput.clear()
				}
			case "esc":
				m.adding = false
				m.addInput.clear()
				m.addError = ""
			default:
				old := m.addInput.text
				m.addInput.handleKey(msg)
				if m.addInput.text != old {
					m.addError = ""
				}
			}
			return m, nil
		}

		switch msg.String() {
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
				if m.cursor < m.scrollOffset {
					m.scrollOffset = m.cursor
				}
			}
		case "down", "j":
			if m.cursor < len(m.entries)-1 {
				m.cursor++
				visible := m.visibleCount()
				if m.cursor >= m.scrollOffset+visible {
					m.scrollOffset = m.cursor - visible + 1
				}
			}
		case " ":
			if m.cursor < len(m.entries) {
				e := &m.entries[m.cursor]
				// Cannot uncheck our own peer ID.
				if e.peerID == m.ourPeerID && e.checked {
					break
				}
				e.checked = !e.checked
			}
		case "a":
			m.adding = true
			m.addInput.clear()
			m.addError = ""
		case "enter":
			if m.selectedCount() > 0 {
				m.confirming = true
			}
		case "esc", "q":
			return m, navBack()
		}
	}
	return m, nil
}

func (m whitelistEditorModel) visibleCount() int {
	v := m.height - 10 // header, footer, spacing, input line
	if v < 3 {
		v = 3
	}
	return v
}

func (m whitelistEditorModel) View() string {
	var b strings.Builder

	b.WriteString(fmt.Sprintf(" %s\n\n", headerStyle.Render("Propose New Whitelist")))

	if m.confirming {
		b.WriteString(fmt.Sprintf(" %s %s\n\n",
			cursorStyle.Render(fmt.Sprintf("Propose whitelist with %d peers?", m.selectedCount())),
			dimStyle.Render("(y/n)")))
	}

	if m.adding {
		prompt := cursorStyle.Render("+ ") + "Peer ID: " + m.addInput.render()
		b.WriteString(fmt.Sprintf(" %s\n", prompt))
		if m.addError != "" {
			b.WriteString(fmt.Sprintf(" %s\n", diffRedStyle.Render("  "+m.addError)))
		}
		b.WriteString("\n")
	}

	visible := m.visibleCount()
	end := m.scrollOffset + visible
	if end > len(m.entries) {
		end = len(m.entries)
	}

	if m.scrollOffset > 0 {
		b.WriteString(dimStyle.Render("   \u25b2 more above") + "\n")
	}

	for i := m.scrollOffset; i < end; i++ {
		e := m.entries[i]
		check := "[ ]"
		if e.checked {
			check = connectedStyle.Render("[x]")
		}

		cursor := "  "
		if i == m.cursor && !m.adding {
			cursor = cursorStyle.Render("> ")
		}

		var source string
		if e.peerID == m.ourPeerID {
			source = connectedStyle.Render(" (self)")
		} else {
			switch e.source {
			case "current":
				source = dimStyle.Render(" (current)")
			case "network":
				source = dimStyle.Render(" (from network)")
			case "manual":
				source = mismatchStyle.Render(" (added)")
			}
		}

		b.WriteString(fmt.Sprintf(" %s%s %s%s\n", cursor, check, truncatePeerID(e.peerID), source))
	}

	if end < len(m.entries) {
		b.WriteString(dimStyle.Render("   \u25bc more below") + "\n")
	}

	b.WriteString(fmt.Sprintf("\n %s\n", dimStyle.Render(fmt.Sprintf(" %d selected", m.selectedCount()))))

	if m.adding {
		b.WriteString(helpStyle.Render("\n Enter: Add   Esc: Cancel   Ctrl+U: Clear"))
	} else {
		b.WriteString(helpStyle.Render("\n Space: Toggle   a: Add peer   Enter: Submit   Esc: Cancel"))
	}

	return fitToHeight(b.String(), m.height)
}
