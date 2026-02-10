package main

import (
	"fmt"
	"sort"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/bisoncraft/mesh/tatanka/admin"
)

type diffModel struct {
	node         admin.NodeInfo
	inBoth       []string
	onlyOurs     []string
	onlyPeers    []string
	scrollOffset int
	height       int
}

func newDiffModel(node admin.NodeInfo, state *admin.AdminState) diffModel {
	ourSet := make(map[string]bool)
	for _, id := range state.OurWhitelist {
		ourSet[id] = true
	}

	peerSet := make(map[string]bool)
	for _, id := range node.PeerWhitelist {
		peerSet[id] = true
	}

	var inBoth, onlyOurs, onlyPeers []string

	for _, id := range state.OurWhitelist {
		if peerSet[id] {
			inBoth = append(inBoth, id)
		} else {
			onlyOurs = append(onlyOurs, id)
		}
	}

	for _, id := range node.PeerWhitelist {
		if !ourSet[id] {
			onlyPeers = append(onlyPeers, id)
		}
	}

	sort.Strings(inBoth)
	sort.Strings(onlyOurs)
	sort.Strings(onlyPeers)

	return diffModel{
		node:      node,
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
		switch msg.String() {
		case "up", "k":
			if m.scrollOffset > 0 {
				m.scrollOffset--
			}
		case "down", "j":
			if m.scrollOffset < m.maxOffset() {
				m.scrollOffset++
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

	lines = append(lines, helpStyle.Render(" \u2191\u2193 Scroll   Esc: Back to connections"))

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
