package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"net"

	"github.com/bisoncraft/mesh/tatanka"
	"github.com/decred/slog"
	"github.com/jessevdk/go-flags"
	"github.com/jrick/logrotate/rotator"
	"github.com/libp2p/go-libp2p/core/peer"
)

var log slog.Logger

// Config defines the configuration options for the Tatanka node.
type Config struct {
	AppDataDir     string   `short:"A" long:"appdata" description:"Path to application home directory."`
	ConfigFile     string   `short:"C" long:"configfile" description:"Path to configuration file."`
	DebugLevel     string   `short:"d" long:"debuglevel" description:"Logging level {trace, debug, info, warn, error, critical}."`
	ListenPort     int      `long:"listenport" description:"Port to listen on."`
	MetricsPort    int      `long:"metricsport" description:"Port to scrape metrics and fetch profiles from."`
	AdminPort      int      `long:"adminport" description:"Port to expose the admin interface on."`
	Whitelist      string   `long:"whitelist" description:"Path to whitelist file (plain text: one peer ID per line). On first run, installs it into the data directory. On subsequent runs, validates it matches the existing whitelist unless --forcewl is set."`
	ForceWhitelist bool     `long:"forcewl" description:"Overwrite any existing whitelist in the data directory with the provided --whitelist file."`
	Bootstrap      []string `long:"bootstrap" description:"Bootstrap peer address in multiaddr format (/ip4/.../tcp/.../p2p/12D3KooW...). Can be specified multiple times."`

	// Oracle Configuration
	CMCKey           string `long:"cmckey" description:"coinmarketcap API key"`
	TatumKey         string `long:"tatumkey" description:"tatum API key"`
	BlockcypherToken string `long:"blockcyphertoken" description:"blockcypher API token"`

	// Public Address
	NATMapping bool   `long:"natmapping" description:"Automatically discover the public IP and map the listen port via UPnP. For nodes behind a consumer router. Mutually exclusive with --publicip."`
	PublicIP   string `long:"publicip" description:"Public IP address to advertise. For VPS/cloud servers or when port forwarding is configured manually. Mutually exclusive with --natmapping."`

	// Bootstrap List Publishing
	BootstrapListFile string `long:"bootstraplistfile" description:"Path to write bootstrap list JSON file for external publishing."`
	BootstrapListPort int    `long:"bootstraplistport" description:"Port to serve the bootstrap list via HTTP at /bootstrap."`
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
	// Handle "init" subcommand before parsing flags.
	if len(os.Args) > 1 && os.Args[1] == "init" {
		runInit()
		return
	}

	// Default config values
	cfg := Config{
		AppDataDir:  defaultAppDataDir(),
		ConfigFile:  defaultConfigFile(),
		DebugLevel:  "info",
		ListenPort:  12345,
		MetricsPort: 12355,
		CMCKey:      "",
	}

	// Parse command-line flags (overrides file values)
	parser := flags.NewParser(&cfg, flags.Default)
	parser.Usage = "[OPTIONS]\n\nSubcommands:\n  init\tGenerate (or load) a private key and print the peer ID"
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

	if cfg.ForceWhitelist && cfg.Whitelist == "" {
		log.Errorf("--forcewl requires --whitelist")
		os.Exit(1)
	}

	if cfg.NATMapping && cfg.PublicIP != "" {
		log.Errorf("--natmapping and --publicip are mutually exclusive")
		os.Exit(1)
	}
	if cfg.NATMapping && cfg.ListenPort == 0 {
		log.Errorf("--natmapping requires --listenport")
		os.Exit(1)
	}
	if cfg.PublicIP != "" && cfg.ListenPort == 0 {
		log.Errorf("--publicip requires --listenport")
		os.Exit(1)
	}
	if cfg.PublicIP != "" && net.ParseIP(cfg.PublicIP) == nil {
		log.Errorf("--publicip %q is not a valid IP address", cfg.PublicIP)
		os.Exit(1)
	}

	var whitelistPeers []peer.ID
	if cfg.Whitelist != "" {
		var err error
		whitelistPeers, err = loadWhitelistPeerIDsList(cfg.Whitelist)
		if err != nil {
			log.Errorf("Failed to load whitelist: %v", err)
			os.Exit(1)
		}
	}

	if cfg.Whitelist != "" {
		log.Infof("Loaded %d whitelist peer IDs from %s", len(whitelistPeers), cfg.Whitelist)
	}

	if cfg.ForceWhitelist {
		log.Infof("Whitelist force-update enabled")
	}

	if cfg.Whitelist == "" {
		log.Infof("No whitelist file provided; will load existing whitelist from data directory")
	}

	if len(whitelistPeers) == 0 && cfg.Whitelist != "" {
		log.Errorf("No peer IDs found in whitelist file %s", cfg.Whitelist)
		os.Exit(1)
	}

	// Create Tatanka config
	tatankaCfg := &tatanka.Config{
		DataDir:           cfg.AppDataDir,
		Logger:            log,
		ListenPort:        cfg.ListenPort,
		MetricsPort:       cfg.MetricsPort,
		AdminPort:         cfg.AdminPort,
		BootstrapAddrs:    cfg.Bootstrap,
		WhitelistPeers:    whitelistPeers,
		ForceWhitelist:    cfg.ForceWhitelist,
		CMCKey:            cfg.CMCKey,
		TatumKey:          cfg.TatumKey,
		BlockcypherToken:  cfg.BlockcypherToken,
		NATMapping:        cfg.NATMapping,
		PublicIP:          cfg.PublicIP,
		BootstrapListFile: cfg.BootstrapListFile,
		BootstrapListPort: cfg.BootstrapListPort,
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

func loadWhitelistPeerIDsList(path string) ([]peer.ID, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNum := 0
	ids := make([]peer.ID, 0)
	seen := make(map[peer.ID]struct{})
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if idx := strings.IndexByte(line, '#'); idx >= 0 {
			line = strings.TrimSpace(line[:idx])
		}
		if line == "" {
			continue
		}
		pid, err := peer.Decode(line)
		if err != nil {
			return nil, fmt.Errorf("invalid peer ID on line %d: %w", lineNum, err)
		}
		if _, ok := seen[pid]; ok {
			continue
		}
		seen[pid] = struct{}{}
		ids = append(ids, pid)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, errors.New("no peer IDs found")
	}
	return ids, nil
}

// runInit generates (or loads) a private key and prints the peer ID.
func runInit() {
	// Parse a minimal set of flags for init.
	type initConfig struct {
		AppDataDir string `short:"A" long:"appdata" description:"Path to application home directory."`
	}
	initCfg := &initConfig{
		AppDataDir: defaultAppDataDir(),
	}
	parser := flags.NewParser(initCfg, flags.Default)
	parser.Parse()

	pid, err := tatanka.InitTatankaNode(initCfg.AppDataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Init failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(pid.String())
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
