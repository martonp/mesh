package main

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

type menuModel struct {
	choices []string
	views   []viewID
	cursor  int
	height  int
}

func newMenuModel() menuModel {
	return menuModel{
		choices: []string{"Connections", "Oracle Sources", "Oracle Data"},
		views:   []viewID{viewConnections, viewOracleSources, viewOracleAggregated},
		cursor:  0,
	}
}

func (m menuModel) Init() tea.Cmd {
	return nil
}

func (m menuModel) Update(msg tea.Msg) (menuModel, tea.Cmd) {
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
			if m.cursor < len(m.choices)-1 {
				m.cursor++
			}
		case "enter":
			return m, func() tea.Msg {
				return navigateMsg{view: m.views[m.cursor]}
			}
		case "q":
			return m, tea.Quit
		}
	}
	return m, nil
}

func (m menuModel) View() string {
	var b strings.Builder

	b.WriteString(titleStyle.Render("Tatanka Admin"))
	b.WriteString("\n\n")

	for i, choice := range m.choices {
		cursor := "  "
		if i == m.cursor {
			cursor = cursorStyle.Render("> ")
		}
		b.WriteString(fmt.Sprintf("%s%s\n", cursor, choice))
	}

	b.WriteString(helpStyle.Render("\nEnter: select   q: quit"))

	return fitToHeight(menuBoxStyle.Render(b.String()), m.height)
}
