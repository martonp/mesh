package main

import (
	"fmt"
	"strings"

	"github.com/bisoncraft/mesh/oracle"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
)

type oracleModel struct {
	data   *oracle.OracleSnapshot
	cursor int
	height int
	// sorted source names for stable ordering
	sortedSources []string
}

func newOracleModel(data *oracle.OracleSnapshot) oracleModel {
	m := oracleModel{
		data:   data,
		height: 40,
	}
	m.rebuildSortedSources()
	return m
}

func (m oracleModel) Init() tea.Cmd {
	return nil
}

func (m *oracleModel) rebuildSortedSources() {
	m.sortedSources = sortedKeys(m.data.Sources)
}

func (m oracleModel) Update(msg tea.Msg) (oracleModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.height = msg.Height

	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}
		case "down", "j":
			if m.cursor < len(m.sortedSources)-1 {
				m.cursor++
			}
		case "enter":
			if len(m.sortedSources) > 0 {
				srcName := m.sortedSources[m.cursor]
				return m, func() tea.Msg {
					return navigateToSourceDetailMsg{sourceName: srcName}
				}
			}
		case "esc", "q":
			return m, navBack()
		}
	}
	return m, nil
}

func (m oracleModel) View() string {
	var b strings.Builder

	// Header
	b.WriteString(fmt.Sprintf(" %s\n\n", headerStyle.Render("Oracle Status")))

	if len(m.data.Sources) == 0 {
		b.WriteString(" No oracle sources configured\n")
		b.WriteString(helpStyle.Render("\n Esc: Back"))
		return b.String()
	}

	// Table
	b.WriteString(m.renderSourceList(m.sortedSources))

	b.WriteString("\n")
	b.WriteString(helpStyle.Render(" \u2191\u2193 Navigate   Enter: Details   Esc: Back"))

	return fitToHeight(b.String(), m.height)
}

func (m oracleModel) renderSourceList(sorted []string) string {
	const (
		colSource = 22
		colLast   = 16
		colNext   = 16
	)

	border := tableBorderStyle.Render

	hLine := func(left, mid, right, fill string) string {
		return border(left) +
			border(strings.Repeat(fill, colSource)) +
			border(mid) +
			border(strings.Repeat(fill, colLast)) +
			border(mid) +
			border(strings.Repeat(fill, colNext)) +
			border(right)
	}

	padCell := func(s string, w int) string {
		vw := lipgloss.Width(s)
		if vw > w-1 {
			s = ansi.Truncate(s, w-2, "\u2026")
			vw = lipgloss.Width(s)
		}
		pad := w - 1 - vw
		if pad < 0 {
			pad = 0
		}
		return " " + s + strings.Repeat(" ", pad)
	}

	row := func(src, last, next string) string {
		return border("\u2502") +
			padCell(src, colSource) +
			border("\u2502") +
			padCell(last, colLast) +
			border("\u2502") +
			padCell(next, colNext) +
			border("\u2502")
	}

	var lines []string

	// Top border
	lines = append(lines, " "+hLine("\u250c", "\u252c", "\u2510", "\u2500"))

	// Header row
	lines = append(lines, " "+row("Source", "Last Fetch", "Next Fetch"))

	for i, name := range sorted {
		src := m.data.Sources[name]

		// Separator
		lines = append(lines, " "+hLine("\u251c", "\u253c", "\u2524", "\u2500"))

		lastStr := "never"
		if src.LastFetch != nil {
			lastStr = relativeTime(*src.LastFetch)
		}

		nextStr := "\u2014"
		if src.NextFetchTime != nil {
			nextStr = relativeTime(*src.NextFetchTime)
		}

		srcName := name
		if src.LastError != "" {
			srcName += " " + disconnectedStyle.Render("!")
		}
		if i == m.cursor {
			srcName = "> " + srcName
		} else {
			srcName = "  " + srcName
		}

		lines = append(lines, " "+row(srcName, lastStr, nextStr))
	}

	// Bottom border
	lines = append(lines, " "+hLine("\u2514", "\u2534", "\u2518", "\u2500"))

	return strings.Join(lines, "\n")
}
