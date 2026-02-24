package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/decred/slog"
	"github.com/jessevdk/go-flags"
	"github.com/jrick/logrotate/rotator"
	"github.com/bisoncraft/mesh/tatanka"
)

var log slog.Logger

// Config defines the configuration options for the Tatanka node.
type Config struct {
	AppDataDir    string `short:"A" long:"appdata" description:"Path to application home directory."`
	ConfigFile    string `short:"C" long:"configfile" description:"Path to configuration file."`
	DebugLevel    string `short:"d" long:"debuglevel" description:"Logging level {trace, debug, info, warn, error, critical}."`
	ListenIP      string `long:"listenip" description:"IP address to listen on."`
	ListenPort    int    `long:"listenport" description:"Port to listen on."`
	MetricsPort   int    `long:"metricsport" description:"Port to scrape metrics and fetch profiles from."`
	AdminPort     int    `long:"adminport" description:"Port to expose the admin interface on."`
	WhitelistPath string `long:"whitelistpath" description:"Path to local whitelist file."`

	// Oracle Configuration
	CMCKey           string `long:"cmckey" description:"coinmarketcap API key"`
	TatumKey         string `long:"tatumkey" description:"tatum API key"`
	BlockcypherToken string `long:"blockcyphertoken" description:"blockcypher API token"`
	CoinGeckoKey     string `long:"coingeckokey" description:"CoinGecko API key (demo or pro)"`
	CoinGeckoPlan    string `long:"coingeckoplan" description:"CoinGecko plan: 'demo' or 'pro' (required if coingeckokey is set)"`
}

// initLogRotator initializes the logging rotater to write logs to logFile and
// create roll files in the same directory.  It must be called before the
// package-global log rotater variables are used.
func initLogRotator(dir string) (*rotator.Rotator, error) {
	logDir := filepath.Join(dir, "logs")
	logFile := filepath.Join(logDir, "tatanka.log")
	err := os.MkdirAll(logDir, 0700)
	if err != nil {
		return nil, err
	}
	const maxLogRolls = 5
	return rotator.New(logFile, 32*1024, false, maxLogRolls)
}

func main() {
	// Default config values
	cfg := Config{
		AppDataDir:    defaultAppDataDir(),
		ConfigFile:    defaultConfigFile(),
		DebugLevel:    "info",
		ListenIP:      "0.0.0.0",
		ListenPort:    12345,
		MetricsPort:   12355,
		WhitelistPath: "",
		CMCKey:        "",
	}

	// Parse command-line flags (overrides file values)
	parser := flags.NewParser(&cfg, flags.Default)
	if _, err := parser.Parse(); err != nil {
		if e, ok := err.(*flags.Error); ok && e.Type == flags.ErrHelp {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "Failed to parse flags: %v\n", err)
		os.Exit(1)
	}

	// Load config file if exists (base values, overridden by flags)
	if err := loadConfigFile(parser, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config file: %v\n", err)
		os.Exit(1)
	}

	// Initialize log rotator
	logRotator, err := initLogRotator(cfg.AppDataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize log rotator: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = logRotator.Close() }()

	// Setup logger to write to both stdout and log file
	logWriter := io.MultiWriter(os.Stdout, logRotator)
	backend := slog.NewBackend(logWriter)
	log = backend.Logger("tatanka")
	level, ok := slog.LevelFromString(cfg.DebugLevel)
	if !ok {
		fmt.Fprintf(os.Stderr, "Invalid debug level: %s\n", cfg.DebugLevel)
		os.Exit(1)
	}
	log.SetLevel(level)

	log.Infof("Using app data directory: %s", cfg.AppDataDir)

	// Create Tatanka config
	tatankaCfg := &tatanka.Config{
		DataDir:       cfg.AppDataDir,
		Logger:        log,
		ListenIP:      cfg.ListenIP,
		ListenPort:    cfg.ListenPort,
		MetricsPort:   cfg.MetricsPort,
		WhitelistPath: cfg.WhitelistPath,
		AdminPort:     cfg.AdminPort,
		CMCKey:           cfg.CMCKey,
		TatumKey:         cfg.TatumKey,
		BlockcypherToken: cfg.BlockcypherToken,
		CoinGeckoKey:     cfg.CoinGeckoKey,
		CoinGeckoPlan:    cfg.CoinGeckoPlan,
	}

	// Create Tatanka node
	node, err := tatanka.NewTatankaNode(tatankaCfg)
	if err != nil {
		log.Errorf("Failed to create Tatanka node: %v", err)
		os.Exit(1)
	}

	// Setup context for shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Infof("Received shutdown signal")
		cancel()
	}()

	// Run the node until context cancels
	log.Infof("Starting Tatanka node...")
	if err := node.Run(ctx); err != nil {
		log.Errorf("Tatanka node failed: %v", err)
		os.Exit(1)
	}
}

// defaultAppDataDir returns the default application data directory.
func defaultAppDataDir() string {
	homeDir, _ := os.UserHomeDir()
	return filepath.Join(homeDir, ".tatanka")
}

// defaultConfigFile returns the default configuration file path.
func defaultConfigFile() string {
	return filepath.Join(defaultAppDataDir(), "tatanka.conf")
}

// loadConfigFile loads values from the config file into cfg.
func loadConfigFile(parser *flags.Parser, cfg *Config) error {
	filePath := cfg.ConfigFile
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil
	}

	return flags.NewIniParser(parser).ParseFile(filePath)
}
