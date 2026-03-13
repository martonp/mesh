package main

import (
	"fmt"
	"math"
	"time"

	"github.com/bisoncraft/mesh/tatanka/admin"
	"github.com/charmbracelet/lipgloss"
)

// Colors
var (
	colorGreen   = lipgloss.Color("42")
	colorYellow  = lipgloss.Color("214")
	colorRed     = lipgloss.Color("196")
	colorDim     = lipgloss.Color("241")
	colorCyan    = lipgloss.Color("86")
	colorWhite   = lipgloss.Color("255")
	colorBorder  = lipgloss.Color("63")
	colorHeader  = lipgloss.Color("99")
	colorCursor  = lipgloss.Color("214")
	colorGreenFg = lipgloss.Color("46")
	colorRedFg   = lipgloss.Color("196")
)

// Styles
var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorHeader).
			MarginBottom(1)

	headerStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorWhite)

	connectedStyle = lipgloss.NewStyle().
			Foreground(colorGreen)

	mismatchStyle = lipgloss.NewStyle().
			Foreground(colorYellow)

	disconnectedStyle = lipgloss.NewStyle().
				Foreground(colorRed)

	dimStyle = lipgloss.NewStyle().
			Foreground(colorDim)

	helpStyle = lipgloss.NewStyle().
			Foreground(colorDim).
			MarginTop(1)

	cursorStyle = lipgloss.NewStyle().
			Foreground(colorCursor).
			Bold(true)

	diffGreenStyle = lipgloss.NewStyle().
			Foreground(colorGreenFg)

	diffRedStyle = lipgloss.NewStyle().
			Foreground(colorRedFg)

	diffAddedBgStyle = lipgloss.NewStyle().
				Background(lipgloss.Color("22"))

	diffRemovedBgStyle = lipgloss.NewStyle().
				Background(lipgloss.Color("52"))

	menuBoxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorBorder).
			Padding(1, 3)

	tableBorderStyle = lipgloss.NewStyle().
				Foreground(colorBorder)
)

func getStateIcon(state admin.NodeConnectionState) string {
	switch state {
	case admin.StateConnected:
		return connectedStyle.Render("●")
	case admin.StateWhitelistMismatch:
		return mismatchStyle.Render("●")
	case admin.StateDisconnected:
		return disconnectedStyle.Render("●")
	default:
		return dimStyle.Render("●")
	}
}

func getStateString(state admin.NodeConnectionState) string {
	switch state {
	case admin.StateConnected:
		return connectedStyle.Render("Connected")
	case admin.StateWhitelistMismatch:
		return mismatchStyle.Render("Whitelist Mismatch")
	case admin.StateDisconnected:
		return disconnectedStyle.Render("Disconnected")
	default:
		return string(state)
	}
}

func relativeTime(t time.Time) string {
	now := time.Now()
	d := now.Sub(t)
	if d < 0 {
		// Future time
		d = -d
		return "in " + formatDuration(d)
	}
	return formatDuration(d) + " ago"
}

func formatDuration(d time.Duration) string {
	if d < time.Second {
		return "<1s"
	}
	totalSecs := int(math.Round(d.Seconds()))
	if totalSecs < 60 {
		return fmt.Sprintf("%ds", totalSecs)
	}
	minutes := totalSecs / 60
	seconds := totalSecs % 60
	if minutes < 60 {
		if seconds == 0 {
			return fmt.Sprintf("%dm", minutes)
		}
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	hours := minutes / 60
	minutes = minutes % 60
	if hours < 24 {
		if minutes == 0 {
			return fmt.Sprintf("%dh", hours)
		}
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	days := hours / 24
	hours = hours % 24
	if hours == 0 {
		return fmt.Sprintf("%dd", days)
	}
	return fmt.Sprintf("%dd %dh", days, hours)
}

func truncatePeerID(id string) string {
	if len(id) <= 16 {
		return id
	}
	return id[:8] + ".." + id[len(id)-4:]
}
