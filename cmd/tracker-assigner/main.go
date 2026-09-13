package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tracker-assigner/internal/config"
	"tracker-assigner/internal/service"
	"tracker-assigner/internal/storage"
	thttp "tracker-assigner/internal/transport/http"
	"tracker-assigner/internal/worker"
	"tracker-assigner/pkg/logger"
	"tracker-assigner/pkg/notifier"

	"go.uber.org/zap"
)

func validateConfig(configPath string, out io.Writer) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("configuration validation FAILED for %q: %w", configPath, err)
	}
	fmt.Fprintf(out, "✅ Configuration file %q is VALID:\n", configPath)
	fmt.Fprintf(out, " - Server: %s:%d\n", cfg.Server.Host, cfg.Server.Port)
	fmt.Fprintf(out, " - Tracker Base URL: %s (OrgID: %s, CloudOrg: %v)\n", cfg.Tracker.BaseURL, cfg.Tracker.OrgID, cfg.Tracker.IsCloudOrg)
	fmt.Fprintf(out, " - Groups (%d):\n", len(cfg.Groups))
	for gid, g := range cfg.Groups {
		fmt.Fprintf(out, "   * Group %q: Strategy=%s, MaxLoad=%d, Members=%d, ActiveStatuses=%v\n",
			gid, g.Strategy, g.MaxLoadPerUser, len(g.Assignees), g.ActiveStatuses)
	}
	fmt.Fprintf(out, " - Routing Rules (%d):\n", len(cfg.RoutingRules))
	for _, r := range cfg.RoutingRules {
		fmt.Fprintf(out, "   * Rule %q -> TargetGroup=%s (Queue: %s)\n", r.ID, r.TargetGroup, r.Queue)
	}
	fmt.Fprintf(out, " - Alerts: Telegram enabled=%v, SLA Timeout=%s\n", cfg.Alerts.Telegram.Enabled, cfg.Alerts.PendingSLATimeout)
	return nil
}

func main() {
	configPathFlag := flag.String("config", "config.yaml", "Path to config.yaml configuration file")
	debugFlag := flag.Bool("debug", false, "Enable debug logging level")
	validateFlag := flag.Bool("validate", false, "Validate configuration file and exit")
	flag.Parse()

	configPath := *configPathFlag
	if envPath := os.Getenv("CONFIG_PATH"); envPath != "" {
		configPath = envPath
	}

	// CLI Validation Mode
	if *validateFlag {
		if err := validateConfig(configPath, os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "❌ %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	debug := *debugFlag
	if os.Getenv("DEBUG") == "true" || os.Getenv("DEBUG") == "1" {
		debug = true
	}

	// 1. Initialize Zap Structured Logger
	log, err := logger.New(debug)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = log.Sync() }()

	log.Info("Starting Tracker Auto-Assigner (Open-Core)",
		zap.String("config_path", configPath),
		zap.Bool("debug", debug))

	// 2. Load Configuration
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatal("Failed to load initial configuration", zap.String("path", configPath), zap.Error(err))
	}

	// 3. Initialize SQLite Database
	db, err := storage.NewSQLite(cfg.Database.DSN)
	if err != nil {
		log.Fatal("Failed to initialize SQLite storage", zap.String("dsn", cfg.Database.DSN), zap.Error(err))
	}
	defer db.Close()
	repo := storage.NewRepository(db)

	// 4. Initialize Core Services
	trackerClient := service.NewTrackerClient(service.TrackerClientConfig{
		BaseURL:    cfg.Tracker.BaseURL,
		Token:      cfg.Tracker.Token,
		OrgID:      cfg.Tracker.OrgID,
		IsCloudOrg: cfg.Tracker.IsCloudOrg,
		Timeout:    cfg.Tracker.Timeout,
		MaxRetries: 3,
	}, nil)

	// Initialize Bug Reporter and attach Zap hook for automatic bug tickets
	bugReporter := service.NewTrackerBugReporter(trackerClient, "BUGS", 1*time.Hour, log)
	log = logger.WithTrackerHook(log, bugReporter)
	log.Info("Automated bug reporter initialized for Tracker queue BUGS (via Zap hook)")

	router := service.NewRouter(cfg)
	balancer := service.NewBalancer(repo)

	var assignerNotifier notifier.Notifier = &notifier.NopNotifier{}
	if cfg.Notifications.Telegram.Enabled {
		assignerNotifier = notifier.NewTelegramNotifier(cfg.Notifications.Telegram.BotToken, "", log)
		log.Info("Telegram notifications for assignments enabled")
	}

	assignerService := service.NewAssignerService(router, balancer, trackerClient, repo, log, cfg, assignerNotifier)

	// 5. Initialize HTTP Handlers
	webhookHandler := thttp.NewWebhookHandler(cfg.Server.SecretToken, log, assignerService)
	webhookHandler.SetErrorReporter(bugReporter)
	apiHandler := thttp.NewAPIHandler(repo, log)

	mux := http.NewServeMux()
	mux.Handle("POST /webhook", webhookHandler)
	mux.Handle("POST /api/v1/webhook", webhookHandler) // alias
	mux.Handle("GET /metrics", thttp.MetricsHandler())
	apiHandler.RegisterRoutes(mux)

	// Root health probe
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	serverAddr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	httpServer := &http.Server{
		Addr:              serverAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// 6. Context and Background Workers
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize Alert Notifier (Telegram or Nop)
	var tgNotifier notifier.Notifier = &notifier.NopNotifier{}
	if cfg.Alerts.Telegram.Enabled {
		tgNotifier = notifier.NewTelegramNotifier(cfg.Alerts.Telegram.BotToken, cfg.Alerts.Telegram.ChatID, log)
		log.Info("Telegram alert notifier enabled", zap.String("chat_id", cfg.Alerts.Telegram.ChatID))
	}

	// Pending Queue Processor Worker
	pendingWorker := worker.NewPendingProcessor(repo, router, balancer, trackerClient, 5*time.Second, log)
	pendingWorker.SetSLATimeout(cfg.Alerts.PendingSLATimeout)
	pendingWorker.SetNotifier(tgNotifier)
	pendingWorker.SetPostAssignHook(func(ctx context.Context, issueKey, candidateLogin string, payload *thttp.WebhookPayload) {
		assignerService.PostAssignActions(ctx, issueKey, candidateLogin, payload)
	})
	go pendingWorker.Start(rootCtx)

	// DLQ Retry Processor Worker
	dlqWorker := worker.NewDLQProcessor(repo, trackerClient, 10*time.Second, 30*time.Second, log)
	dlqWorker.SetNotifier(tgNotifier)
	go dlqWorker.Start(rootCtx)

	// State Sync Worker
	syncWorker := worker.NewStateSyncWorker(repo, router, trackerClient, 30*time.Minute, log)
	assignerService.SetStateSyncWorker(syncWorker)
	go syncWorker.Start(rootCtx)

	// Hot Reload Watcher
	watcher := config.NewWatcher(configPath, cfg, log)
	watcher.OnReload(func(newCfg *config.Config) {
		router.UpdateConfig(newCfg)
		webhookHandler.SetSecretToken(newCfg.Server.SecretToken)
		pendingWorker.SetSLATimeout(newCfg.Alerts.PendingSLATimeout)

		var aNotifier notifier.Notifier = &notifier.NopNotifier{}
		if newCfg.Notifications.Telegram.Enabled {
			aNotifier = notifier.NewTelegramNotifier(newCfg.Notifications.Telegram.BotToken, "", log)
		}
		assignerService.UpdateConfig(newCfg, aNotifier)

		if newCfg.Alerts.Telegram.Enabled {
			tgNotifier = notifier.NewTelegramNotifier(newCfg.Alerts.Telegram.BotToken, newCfg.Alerts.Telegram.ChatID, log)
			pendingWorker.SetNotifier(tgNotifier)
			dlqWorker.SetNotifier(tgNotifier)
		} else {
			pendingWorker.SetNotifier(&notifier.NopNotifier{})
			dlqWorker.SetNotifier(&notifier.NopNotifier{})
		}
		log.Info("Hot reload applied to router, webhook handler, assigner and alerts")
	})
	if err := watcher.Start(rootCtx); err != nil {
		log.Warn("Failed to start file watcher for hot reload (falling back to static config)", zap.Error(err))
	}

	// 7. Start HTTP Server in goroutine
	serverErrCh := make(chan error, 1)
	go func() {
		log.Info("HTTP server listening", zap.String("address", serverAddr))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
	}()

	// 8. Graceful Shutdown Signal Handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	select {
	case err := <-serverErrCh:
		log.Fatal("HTTP server failed unexpectedly", zap.Error(err))
	case sig := <-sigChan:
		log.Info("Received termination signal, starting graceful shutdown",
			zap.String("signal", sig.String()))
	}

	// Cancel worker context
	cancel()

	// Shutdown HTTP Server gracefully with 20s timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error("HTTP server shutdown encountered error", zap.Error(err))
	} else {
		log.Info("HTTP server gracefully stopped")
	}

	log.Info("Tracker Auto-Assigner successfully terminated")
}
