package main

import (
	"fmt"
	"math"
	"strings"

	"github.com/bisoncraft/mesh/oracle"
	tea "github.com/charmbracelet/bubbletea"
)

type oracleDetailModel struct {
	data       *oracle.OracleSnapshot
	sourceName string
	sections   []detailSection
	focused    int
	height     int
	filter     filterState
}

func newOracleDetailModel(data *oracle.OracleSnapshot, sourceName string) oracleDetailModel {
	m := oracleDetailModel{
		data:       data,
		sourceName: sourceName,
		height:     40,
	}
	m.buildSections()
	return m
}

func (m *oracleDetailModel) buildSections() {
	m.sections = nil

	if lines := m.buildContribLines(m.data.Prices); len(lines) > 0 {
		m.sections = append(m.sections, detailSection{title: "Prices", lines: lines})
	}

	if lines := m.buildContribLines(m.data.FeeRates); len(lines) > 0 {
		m.sections = append(m.sections, detailSection{title: "Fee Rates", lines: lines})
	}

	src := m.data.Sources[m.sourceName]
	if src != nil && m.sourceHasQuotas(src) {
		if lines := m.buildQuotaLines(); len(lines) > 0 {
			m.sections = append(m.sections, detailSection{title: "Quotas", lines: lines})
		}
	}

	if lines := m.buildFetchLines(); len(lines) > 0 {
		m.sections = append(m.sections, detailSection{title: "Fetches (24h)", lines: lines})
	}

	if m.focused >= len(m.sections) {
		m.focused = max(0, len(m.sections)-1)
	}
}

func (m oracleDetailModel) Init() tea.Cmd {
	return nil
}

func (m oracleDetailModel) Update(msg tea.Msg) (oracleDetailModel, tea.Cmd) {
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
				m.sections[m.focused].scrollUp()
			}
		case "down", "j":
			if len(m.sections) > 0 {
				m.sections[m.focused].scrollDown()
			}
		case "tab":
			if len(m.sections) > 0 {
				m.focused = (m.focused + 1) % len(m.sections)
			}
		case "shift+tab":
			if len(m.sections) > 0 {
				m.focused = (m.focused - 1 + len(m.sections)) % len(m.sections)
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

func (m oracleDetailModel) View() string {
	var b strings.Builder

	// Header
	b.WriteString(fmt.Sprintf(" %s\n\n", headerStyle.Render("Source: "+m.sourceName)))

	src := m.data.Sources[m.sourceName]
	if src == nil {
		b.WriteString(" " + dimStyle.Render("Source not found") + "\n")
		b.WriteString(helpStyle.Render("\n Esc: Back"))
		return b.String()
	}

	// Schedule section
	b.WriteString(fmt.Sprintf(" %s\n", cursorStyle.Render("Schedule")))

	lastStr := "never"
	if src.LastFetch != nil {
		lastStr = relativeTime(*src.LastFetch)
	}
	b.WriteString(fmt.Sprintf("   Last Fetch:        %s\n", dimStyle.Render(lastStr)))

	if src.NetworkNextFetchTime != nil {
		b.WriteString(fmt.Sprintf("   Network Next:      %s\n", dimStyle.Render(relativeTime(*src.NetworkNextFetchTime))))
	}

	if src.NextFetchTime != nil {
		posStr := ""
		if len(src.OrderedNodes) > 0 {
			orderIndex := -1
			for i, nodeID := range src.OrderedNodes {
				if nodeID == m.data.NodeID {
					orderIndex = i
					break
				}
			}
			if orderIndex >= 0 {
				posStr = fmt.Sprintf(" (#%d of %d)", orderIndex+1, len(src.OrderedNodes))
			}
		}
		b.WriteString(fmt.Sprintf("   Your Next Fetch:   %s%s\n", dimStyle.Render(relativeTime(*src.NextFetchTime)), dimStyle.Render(posStr)))
	}

	hasQuotas := m.sourceHasQuotas(src)

	if hasQuotas {
		if src.NetworkSustainableRate != nil && *src.NetworkSustainableRate > 0 {
			b.WriteString(fmt.Sprintf("   Sustainable Rate:  %s\n", dimStyle.Render(fmt.Sprintf("%.4f fetches/sec", *src.NetworkSustainableRate))))
		}
		if src.NetworkSustainablePeriod != nil && *src.NetworkSustainablePeriod > 0 {
			b.WriteString(fmt.Sprintf("   Sustainable Period: %s\n", dimStyle.Render("1 fetch / "+src.NetworkSustainablePeriod.String())))
		}
	} else {
		b.WriteString(fmt.Sprintf("   %s\n", dimStyle.Render("This source has no quotas \u2014 fetch interval determined by minimum period")))
	}

	if src.MinFetchInterval != nil && *src.MinFetchInterval > 0 {
		b.WriteString(fmt.Sprintf("   Min Period:        %s\n", dimStyle.Render(src.MinFetchInterval.String())))
	}

	if src.LastError != "" {
		errAge := ""
		if src.LastErrorTime != nil {
			errAge = " (" + relativeTime(*src.LastErrorTime) + ")"
		}
		b.WriteString(fmt.Sprintf("   Last Error:        %s\n", disconnectedStyle.Render(src.LastError+errAge)))
	}
	b.WriteString("\n")

	// Fetch Order section
	if len(src.OrderedNodes) > 0 {
		b.WriteString(fmt.Sprintf(" %s\n", cursorStyle.Render("Fetch Order")))
		for i, nodeID := range src.OrderedNodes {
			label := truncatePeerID(nodeID)
			marker := "  "
			if nodeID == m.data.NodeID {
				label = "You"
				marker = "\u2190 "
			}
			b.WriteString(fmt.Sprintf("   %d. %-20s %s\n", i+1, label, dimStyle.Render(marker)))
		}
		b.WriteString("\n")
	}

	// Filter bar
	m.filter.renderFilterBar(&b)

	if len(m.sections) == 0 {
		if m.filter.input.text != "" {
			b.WriteString(fmt.Sprintf(" %s\n", dimStyle.Render("No matches for \""+m.filter.input.text+"\"")))
		} else {
			b.WriteString(" " + dimStyle.Render("No data available") + "\n")
		}
	}

	// Sections
	for i, sec := range m.sections {
		renderSection(&b, &sec, i == m.focused)
	}

	// Help
	b.WriteString(buildFilterHelp(m.sections, m.filter))

	return fitToHeight(b.String(), m.height)
}

func (m oracleDetailModel) sourceHasQuotas(src *oracle.SourceStatus) bool {
	for _, q := range src.Quotas {
		if q.FetchesLimit > 0 && q.FetchesLimit < math.MaxInt64 {
			return true
		}
	}
	return false
}

// --- Section content builders ---

func (m oracleDetailModel) buildContribLines(rates map[string]*oracle.SnapshotRate) []string {
	var lines []string
	for _, key := range sortedKeys(rates) {
		rate := rates[key]
		contrib, ok := rate.Contributions[m.sourceName]
		if !ok || !m.filter.matches(key) {
			continue
		}
		age := dimStyle.Render("(" + relativeTime(contrib.Stamp) + ")")
		lines = append(lines, fmt.Sprintf("   %-8s %s  %s",
			key, contrib.Value, age))
	}
	return lines
}

func (m oracleDetailModel) buildQuotaLines() []string {
	src := m.data.Sources[m.sourceName]
	if src == nil {
		return nil
	}

	var lines []string
	for _, nid := range sortedKeys(src.Quotas) {
		q := src.Quotas[nid]
		if q.FetchesLimit <= 0 {
			continue
		}
		label := truncatePeerID(nid)
		if nid == m.data.NodeID {
			label += " (ours)"
		}
		lines = append(lines,
			fmt.Sprintf("   %s", dimStyle.Render(label)),
			fmt.Sprintf("   Fetches:  %d / %d", q.FetchesRemaining, q.FetchesLimit),
			fmt.Sprintf("   Resets:   %s", dimStyle.Render(relativeTime(q.ResetTime))),
			"",
		)
	}
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

func (m oracleDetailModel) buildFetchLines() []string {
	src := m.data.Sources[m.sourceName]
	if src == nil || len(src.Fetches24h) == 0 {
		return nil
	}

	var lines []string
	for _, nid := range sortedKeys(src.Fetches24h) {
		count := src.Fetches24h[nid]
		label := truncatePeerID(nid)
		if nid == m.data.NodeID {
			label += " (ours)"
		}
		lines = append(lines, fmt.Sprintf("   %-24s %d", label, count))
	}
	return lines
}
