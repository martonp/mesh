package main

import (
	"fmt"
	"strings"

	"github.com/bisoncraft/mesh/oracle"
	tea "github.com/charmbracelet/bubbletea"
)

type oracleAggregatedDetailModel struct {
	data     *oracle.OracleSnapshot
	dataType oracle.DataType
	key      string // ticker or network name
	offset   int
	height   int
}

func newOracleAggregatedDetailModel(data *oracle.OracleSnapshot, dataType oracle.DataType, key string) oracleAggregatedDetailModel {
	return oracleAggregatedDetailModel{
		data:     data,
		dataType: dataType,
		key:      key,
		height:   40,
	}
}

func (m oracleAggregatedDetailModel) Init() tea.Cmd {
	return nil
}

func (m oracleAggregatedDetailModel) Update(msg tea.Msg) (oracleAggregatedDetailModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.height = msg.Height

	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			if m.offset > 0 {
				m.offset--
			}
		case "down", "j":
			maxOffset := m.maxOffset()
			if m.offset < maxOffset {
				m.offset++
			}
		case "esc", "q":
			return m, navBack()
		}
	}
	return m, nil
}

func (m oracleAggregatedDetailModel) getContributions() map[string]*oracle.SourceContribution {
	var rate *oracle.SnapshotRate
	if m.dataType == oracle.PriceData {
		rate = m.data.Prices[m.key]
	} else {
		rate = m.data.FeeRates[m.key]
	}
	if rate == nil {
		return nil
	}
	return rate.Contributions
}

func (m oracleAggregatedDetailModel) maxOffset() int {
	contribs := m.getContributions()
	if contribs == nil {
		return 0
	}
	lines := len(contribs) * 4
	visible := m.height - 8
	if visible < 5 {
		visible = 5
	}
	maxOff := lines - visible
	if maxOff < 0 {
		return 0
	}
	return maxOff
}

func (m oracleAggregatedDetailModel) View() string {
	var b strings.Builder

	// Header
	label := m.key
	if m.dataType == oracle.PriceData {
		label += " Price"
	} else {
		label += " Fee Rate"
	}
	b.WriteString(fmt.Sprintf(" %s\n\n", headerStyle.Render("Sources: "+label)))

	contribs := m.getContributions()
	if len(contribs) == 0 {
		b.WriteString(" " + dimStyle.Render("No source data available") + "\n")
		b.WriteString(helpStyle.Render("\n Esc: Back"))
		return b.String()
	}

	// Sort by source name
	sources := sortedKeys(contribs)

	// Build content lines
	var lines []string
	for _, name := range sources {
		c := contribs[name]
		age := relativeTime(c.Stamp)
		agedWeight := dimStyle.Render(fmt.Sprintf("(weight: %.2f, %s)", c.Weight, age))
		lines = append(lines,
			fmt.Sprintf("   %s", headerStyle.Render(name)),
			fmt.Sprintf("   Value:  %s", c.Value),
			fmt.Sprintf("   %s", agedWeight),
			"",
		)
	}

	// Apply scroll offset
	visible := m.height - 8
	if visible < 5 {
		visible = 5
	}

	start := m.offset
	if start > len(lines) {
		start = len(lines)
	}
	end := start + visible
	if end > len(lines) {
		end = len(lines)
	}

	if m.offset > 0 {
		b.WriteString(dimStyle.Render("   \u25b2 more above") + "\n")
	}

	for _, line := range lines[start:end] {
		b.WriteString(line + "\n")
	}

	if end < len(lines) {
		b.WriteString(dimStyle.Render("   \u25bc more below") + "\n")
	}

	// Help
	b.WriteString(helpStyle.Render("\n \u2191\u2193 Scroll   Esc: Back"))

	return fitToHeight(b.String(), m.height)
}
