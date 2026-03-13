package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/bisoncraft/mesh/tatanka/admin"
	tea "github.com/charmbracelet/bubbletea"
)

type diffModel struct {
	node         admin.PeerInfo
	api          *apiClient
	inBoth       []string
	onlyOurs     []string
	onlyPeers    []string
	scrollOffset int
	height       int
	confirming   bool
	statusMsg    string
}

func newDiffModel(node admin.PeerInfo, state *admin.AdminState, api *apiClient) diffModel {
	ourWl := getOurWhitelist(state)
	ourSet := make(map[string]bool, len(ourWl))
	for _, id := range ourWl {
		ourSet[id] = true
	}

	var peerWl []string
	if node.WhitelistState != nil {
		peerWl = peerIDStrings(node.WhitelistState.Current)
	}
	peerSet := make(map[string]bool, len(peerWl))
	for _, id := range peerWl {
		peerSet[id] = true
	}

	var inBoth, onlyOurs, onlyPeers []string

	for _, id := range ourWl {
		if peerSet[id] {
			inBoth = append(inBoth, id)
		} else {
			onlyOurs = append(onlyOurs, id)
		}
	}

	for _, id := range peerWl {
		if !ourSet[id] {
			onlyPeers = append(onlyPeers, id)
		}
	}

	sort.Strings(inBoth)
	sort.Strings(onlyOurs)
	sort.Strings(onlyPeers)

	return diffModel{
		node:      node,
		api:       api,
		inBoth:    inBoth,
		onlyOurs:  onlyOurs,
		onlyPeers: onlyPeers,
		height:    40, // default, updated by WindowSizeMsg
	}
}

func (m diffModel) Init() tea.Cmd {
	return nil
}

func (m diffModel) Update(msg tea.Msg) (diffModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.height = msg.Height
	case tea.KeyMsg:
		if m.confirming {
			switch msg.String() {
			case "y":
				m.confirming = false
				return m, m.api.adoptWhitelist(m.node.PeerID)
			case "n", "esc":
				m.confirming = false
			}
			return m, nil
		}
		switch msg.String() {
		case "up", "k":
			if m.scrollOffset > 0 {
				m.scrollOffset--
			}
		case "down", "j":
			if m.scrollOffset < m.maxOffset() {
				m.scrollOffset++
			}
		case "f":
			if m.node.WhitelistState != nil {
				m.confirming = true
			}
		case "esc", "q":
			return m, navBack()
		}
	}
	return m, nil
}

func (m diffModel) totalLines() int {
	total := 4 // header, blank, separator, blank
	if len(m.inBoth) > 0 {
		total += 1 + len(m.inBoth) + 1
	}
	if len(m.onlyOurs) > 0 {
		total += 1 + len(m.onlyOurs) + 1
	}
	if len(m.onlyPeers) > 0 {
		total += 1 + len(m.onlyPeers) + 1
	}
	total++ // help line
	return total
}

func (m diffModel) maxOffset() int {
	visible := m.height - 2
	if visible < 1 {
		visible = 1
	}
	maxOff := m.totalLines() - visible
	if maxOff < 0 {
		return 0
	}
	return maxOff
}

func (m diffModel) View() string {
	var lines []string

	lines = append(lines,
		headerStyle.Render(fmt.Sprintf(" Whitelist Diff \u2014 %s", m.node.PeerID)),
		"",
	)

	if m.statusMsg != "" {
		lines = append(lines, fmt.Sprintf(" %s", disconnectedStyle.Render(m.statusMsg)), "")
	}

	if m.confirming {
		lines = append(lines,
			fmt.Sprintf(" %s %s",
				cursorStyle.Render("Adopt peer's whitelist?"),
				dimStyle.Render("(y/n)")),
			"",
		)
	}

	lines = append(lines,
		dimStyle.Render(" "+strings.Repeat("\u2500", 50)),
		"",
	)

	if len(m.inBoth) > 0 {
		lines = append(lines,
			dimStyle.Render(fmt.Sprintf(" \u2713 In Both Whitelists (%d):", len(m.inBoth))))
		for _, id := range m.inBoth {
			lines = append(lines, dimStyle.Render("   "+id))
		}
		lines = append(lines, "")
	}

	if len(m.onlyOurs) > 0 {
		lines = append(lines,
			diffGreenStyle.Render(fmt.Sprintf(" + Only in Our Whitelist (%d):", len(m.onlyOurs))))
		for _, id := range m.onlyOurs {
			lines = append(lines, diffGreenStyle.Render("   "+id))
		}
		lines = append(lines, "")
	}

	if len(m.onlyPeers) > 0 {
		lines = append(lines,
			diffRedStyle.Render(fmt.Sprintf(" - Only in Peer's Whitelist (%d):", len(m.onlyPeers))))
		for _, id := range m.onlyPeers {
			lines = append(lines, diffRedStyle.Render("   "+id))
		}
		lines = append(lines, "")
	}

	helpParts := []string{"\u2191\u2193 Scroll"}
	if m.node.WhitelistState != nil {
		helpParts = append(helpParts, "f: Adopt peer's whitelist")
	}
	helpParts = append(helpParts, "Esc: Back to connections")
	lines = append(lines, helpStyle.Render(" "+strings.Join(helpParts, "   ")))

	// Apply scroll
	maxOffset := len(lines) - m.height + 2
	if maxOffset < 0 {
		maxOffset = 0
	}
	if m.scrollOffset > maxOffset {
		m.scrollOffset = maxOffset
	}

	start := m.scrollOffset
	end := start + m.height - 2
	if end > len(lines) {
		end = len(lines)
	}
	if start > len(lines) {
		start = len(lines)
	}

	return fitToHeight(strings.Join(lines[start:end], "\n"), m.height)
}
