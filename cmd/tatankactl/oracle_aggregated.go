package main

import (
	"fmt"
	"strings"

	"github.com/bisoncraft/mesh/oracle"
	tea "github.com/charmbracelet/bubbletea"
)

type oracleAggregatedModel struct {
	data     *oracle.OracleSnapshot
	sections []detailSection
	focused  int
	height   int
	filter   filterState
}

func newOracleAggregatedModel(data *oracle.OracleSnapshot) oracleAggregatedModel {
	m := oracleAggregatedModel{
		data:   data,
		height: 40,
	}
	m.buildSections()
	return m
}

func (m oracleAggregatedModel) Init() tea.Cmd {
	return nil
}

func (m oracleAggregatedModel) Update(msg tea.Msg) (oracleAggregatedModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.height = msg.Height

	case tea.KeyMsg:
		if m.filter.active {
			if m.filter.handleFilterKey(msg) {
				m.buildSections()
			}
			return m, nil
		}
		switch msg.String() {
		case "up", "k":
			if len(m.sections) > 0 {
				m.sections[m.focused].cursorUp()
			}
		case "down", "j":
			if len(m.sections) > 0 {
				m.sections[m.focused].cursorDown()
			}
		case "tab":
			if len(m.sections) > 0 {
				m.focused = (m.focused + 1) % len(m.sections)
			}
		case "shift+tab":
			if len(m.sections) > 0 {
				m.focused = (m.focused - 1 + len(m.sections)) % len(m.sections)
			}
		case "enter":
			if len(m.sections) > 0 {
				sec := &m.sections[m.focused]
				key := sec.selectedKey()
				if key != "" {
					dataType := oracle.PriceData
					if sec.title == "Fee Rates" {
						dataType = oracle.FeeRateData
					}
					return m, func() tea.Msg {
						return navigateToAggregatedDetailMsg{
							dataType: dataType,
							key:      key,
						}
					}
				}
			}
		case "/":
			m.filter.startFiltering()
		case "esc", "q":
			if m.filter.handleEscOrQ() {
				m.buildSections()
			} else {
				return m, navBack()
			}
		}
	}
	return m, nil
}

func (m *oracleAggregatedModel) buildSections() {
	m.sections = nil

	if lines, keys := m.buildRateLines(m.data.Prices); len(lines) > 0 {
		m.sections = append(m.sections, detailSection{title: "Prices", lines: lines, keys: keys})
	}

	if lines, keys := m.buildRateLines(m.data.FeeRates); len(lines) > 0 {
		m.sections = append(m.sections, detailSection{title: "Fee Rates", lines: lines, keys: keys})
	}

	if m.focused >= len(m.sections) {
		m.focused = max(0, len(m.sections)-1)
	}
}

func (m oracleAggregatedModel) buildRateLines(rates map[string]*oracle.SnapshotRate) ([]string, []string) {
	var lines, keys []string
	for _, key := range sortedKeys(rates) {
		if !m.filter.matches(key) {
			continue
		}
		rate := rates[key]
		lines = append(lines, fmt.Sprintf("   %-10s %s", key, rate.Value))
		keys = append(keys, key)
	}
	return lines, keys
}

func (m oracleAggregatedModel) View() string {
	var b strings.Builder

	// Header
	b.WriteString(fmt.Sprintf(" %s\n\n", headerStyle.Render("Aggregated Data")))

	// Filter bar
	m.filter.renderFilterBar(&b)

	if len(m.sections) == 0 {
		if m.filter.input.text != "" {
			b.WriteString(fmt.Sprintf(" %s\n", dimStyle.Render("No matches for \""+m.filter.input.text+"\"")))
		} else if len(m.data.Prices) == 0 && len(m.data.FeeRates) == 0 {
			b.WriteString(" " + dimStyle.Render("No aggregated data available") + "\n")
		}
	}

	// Sections
	for i, sec := range m.sections {
		renderSection(&b, &sec, i == m.focused)
	}

	// Help
	b.WriteString(buildFilterHelp(m.sections, m.filter, "Enter: Details"))

	return fitToHeight(b.String(), m.height)
}
