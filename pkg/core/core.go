package core

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"time"

	"tracker-assigner/internal/config"
	"tracker-assigner/internal/service"
	"tracker-assigner/internal/storage"
	thhttp "tracker-assigner/internal/transport/http"
	"tracker-assigner/internal/worker"
	"tracker-assigner/pkg/notifier"

	"go.uber.org/zap"
)

// Exported aliases so external extensions can access core types via public pkg/core.
type Config = config.Config
type ConfigProvider = config.ConfigProvider
type YAMLConfigProvider = config.YAMLConfigProvider
type BalancerStrategy = service.BalancerStrategy
type GroupConfig = config.GroupConfig
type AssigneeConfig = config.AssigneeConfig
type RoutingRule = config.RoutingRule
type ScheduleConfig = config.ScheduleConfig
type WorkHours = config.WorkHours
type AbsencePeriod = config.AbsencePeriod
type NotificationRule = config.NotificationRule
type StrategyType = config.StrategyType
type TrackerAPI = service.TrackerAPI
type Router = service.Router
type AssignerService = service.AssignerService
type Repository = storage.Repository
type PendingItem = storage.PendingItem
type AssigneeLoadRecord = storage.AssigneeLoadRecord
type IssueAssignment = storage.IssueAssignment
type DLQItem = storage.DLQItem
type TrackerIssue = thhttp.TrackerIssue
type WebhookPayload = thhttp.WebhookPayload
type IssueReference = thhttp.IssueReference

const (
	StrategyRoundRobin = config.StrategyRoundRobin
	StrategyLoadBased  = config.StrategyLoadBased
)

var (
	IsAssigneeWorking      = config.IsAssigneeWorking
	IsAssigneeAbsent       = config.IsAssigneeAbsent
	ErrNoAvailableAssignee = service.ErrNoAvailableAssignee
	NewYAMLConfigProvider  = config.NewYAMLConfigProvider
	WithSkills             = service.WithSkills
	GetSkills              = service.GetSkills
	WithComplexity         = service.WithComplexity
	GetComplexity          = service.GetComplexity
)

// IssueAssignmentRecord represents an assignment record.
type IssueAssignmentRecord struct {
	IssueKey   string
	GroupID    string
	AssigneeID string
	AssignedAt time.Time
}

// AssignmentInfo represents structured assignment event details with timing metrics.
type AssignmentInfo struct {
	IssueKey               string
	CandidateLogin         string
	QueueKey               string
	GroupID                string
	PendingDurationSeconds int64
	Source                 string
	AssignedAt             time.Time
}

// AssignmentCallback defines a hook invoked when an issue is assigned.
type AssignmentCallback func(ctx context.Context, info AssignmentInfo)

// CoreApp encapsulates the Open-Core components.
type CoreApp struct {
	provider        config.ConfigProvider
	cfg             *config.Config
	db              *storage.DB
	repo            storage.Repository
	router          *service.Router
	balancer        service.BalancerStrategy
	trackerClient   service.TrackerAPI
	assignerService *service.AssignerService
	webhookHandler  *thhttp.WebhookHandler
	apiHandler      *thhttp.APIHandler
	pendingWorker   *worker.PendingProcessor
	dlqWorker       *worker.DLQProcessor
	syncWorker      *worker.StateSyncWorker
	logger          *zap.Logger
}

// NewCoreApp initializes all open-core components using the given ConfigProvider.
func NewCoreApp(provider config.ConfigProvider, log *zap.Logger) (*CoreApp, error) {
	if log == nil {
		var err error
		log, err = zap.NewProduction()
		if err != nil {
			return nil, fmt.Errorf("failed to init default logger: %w", err)
		}
	}

	if provider == nil {
		return nil, fmt.Errorf("config provider cannot be nil")
	}

	cfg := provider.GetConfig()
	if cfg == nil {
		return nil, fmt.Errorf("config provider returned nil configuration")
	}

	db, err := storage.NewSQLite(cfg.Database.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	repo := storage.NewRepository(db)

	trackerClient := service.NewTrackerClient(service.TrackerClientConfig{
		BaseURL:    cfg.Tracker.BaseURL,
		Token:      cfg.Tracker.Token,
		OrgID:      cfg.Tracker.OrgID,
		IsCloudOrg: cfg.Tracker.IsCloudOrg,
		Timeout:    cfg.Tracker.Timeout,
		MaxRetries: 3,
	}, nil)

	router := service.NewRouter(cfg)
	balancer := service.NewBalancer(repo)

	var assignerNotifier notifier.Notifier = &notifier.NopNotifier{}
	if cfg.Notifications.Telegram.Enabled {
		assignerNotifier = notifier.NewTelegramNotifier(cfg.Notifications.Telegram.BotToken, "", log)
	}

	assignerService := service.NewAssignerService(router, balancer, trackerClient, repo, log, cfg, assignerNotifier)
	webhookHandler := thhttp.NewWebhookHandler(cfg.Server.SecretToken, log, assignerService)
	bugReporter := service.NewTrackerBugReporter(trackerClient, "BUGS", 1*time.Hour, log)
	webhookHandler.SetErrorReporter(bugReporter)
	apiHandler := thhttp.NewAPIHandler(repo, log)

	var tgAlertNotifier notifier.Notifier = &notifier.NopNotifier{}
	if cfg.Alerts.Telegram.Enabled {
		tgAlertNotifier = notifier.NewTelegramNotifier(cfg.Alerts.Telegram.BotToken, cfg.Alerts.Telegram.ChatID, log)
	}

	pendingWorker := worker.NewPendingProcessor(repo, router, balancer, trackerClient, 5*time.Second, log)
	pendingWorker.SetSLATimeout(cfg.Alerts.PendingSLATimeout)
	pendingWorker.SetNotifier(tgAlertNotifier)
	pendingWorker.SetPostAssignHook(func(ctx context.Context, issueKey, candidateLogin string, payload *thhttp.WebhookPayload) {
		assignerService.PostAssignActions(ctx, issueKey, candidateLogin, payload)
	})

	dlqWorker := worker.NewDLQProcessor(repo, trackerClient, 10*time.Second, 30*time.Second, log)
	dlqWorker.SetNotifier(tgAlertNotifier)

	syncWorker := worker.NewStateSyncWorker(repo, router, trackerClient, 30*time.Minute, log)
	assignerService.SetStateSyncWorker(syncWorker)

	app := &CoreApp{
		provider:        provider,
		cfg:             cfg,
		db:              db,
		repo:            repo,
		router:          router,
		balancer:        balancer,
		trackerClient:   trackerClient,
		assignerService: assignerService,
		webhookHandler:  webhookHandler,
		apiHandler:      apiHandler,
		pendingWorker:   pendingWorker,
		dlqWorker:       dlqWorker,
		syncWorker:      syncWorker,
		logger:          log,
	}

	provider.OnReload(func(newCfg *config.Config) {
		app.ApplyConfig(newCfg)
	})

	return app, nil
}

// NewCoreAppFromPath creates a CoreApp using the default YAMLConfigProvider for a file path.
func NewCoreAppFromPath(configPath string, log *zap.Logger) (*CoreApp, error) {
	provider, err := config.NewYAMLConfigProvider(configPath, log)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration from %s: %w", configPath, err)
	}
	return NewCoreApp(provider, log)
}

// SetProvider switches the active configuration provider, registers reload callbacks, and applies its config immediately.
func (a *CoreApp) SetProvider(p config.ConfigProvider) {
	if p == nil {
		return
	}
	a.provider = p
	p.OnReload(func(newCfg *config.Config) {
		a.ApplyConfig(newCfg)
	})
	a.ApplyConfig(p.GetConfig())
}

// SetBalancer replaces the active balancer strategy across all components.
func (a *CoreApp) SetBalancer(b service.BalancerStrategy) {
	a.balancer = b
	a.assignerService.SetBalancer(b)
	a.pendingWorker.SetBalancer(b)
}

// GetRepository returns the underlying repository.
func (a *CoreApp) GetRepository() storage.Repository {
	return a.repo
}

// GetConfig returns the active configuration.
func (a *CoreApp) GetConfig() *config.Config {
	return a.cfg
}

// ApplyConfig dynamically updates the active configuration on all services.
func (a *CoreApp) ApplyConfig(newCfg *config.Config) {
	if newCfg == nil {
		return
	}
	a.cfg = newCfg
	a.router.UpdateConfig(newCfg)
	a.webhookHandler.SetSecretToken(newCfg.Server.SecretToken)
	a.pendingWorker.SetSLATimeout(newCfg.Alerts.PendingSLATimeout)

	var aNotifier notifier.Notifier = &notifier.NopNotifier{}
	if newCfg.Notifications.Telegram.Enabled {
		aNotifier = notifier.NewTelegramNotifier(newCfg.Notifications.Telegram.BotToken, "", a.logger)
	}
	a.assignerService.UpdateConfig(newCfg, aNotifier)

	if newCfg.Alerts.Telegram.Enabled {
		tgNotifier := notifier.NewTelegramNotifier(newCfg.Alerts.Telegram.BotToken, newCfg.Alerts.Telegram.ChatID, a.logger)
		a.pendingWorker.SetNotifier(tgNotifier)
		a.dlqWorker.SetNotifier(tgNotifier)
	} else {
		a.pendingWorker.SetNotifier(&notifier.NopNotifier{})
		a.dlqWorker.SetNotifier(&notifier.NopNotifier{})
	}
	a.logger.Info("CoreApp configuration reloaded")
}

// Start launches background workers and configuration watcher.
func (a *CoreApp) Start(ctx context.Context) error {
	go a.pendingWorker.Start(ctx)
	go a.dlqWorker.Start(ctx)
	go a.syncWorker.Start(ctx)
	if startable, ok := a.provider.(interface{ Start(context.Context) error }); ok {
		if err := startable.Start(ctx); err != nil {
			a.logger.Warn("Config provider watcher failed to start in CoreApp", zap.Error(err))
		}
	}
	return nil
}

// RegisterRoutes registers open-core endpoints on the provided mux.
func (a *CoreApp) RegisterRoutes(mux *http.ServeMux) {
	mux.Handle("POST /webhook", a.webhookHandler)
	mux.Handle("POST /api/v1/webhook", a.webhookHandler)
	mux.Handle("GET /metrics", thhttp.MetricsHandler())
	a.apiHandler.RegisterRoutes(mux)

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
}

// RegisterAssignmentHook registers a callback that will be triggered after every issue assignment.
func (a *CoreApp) RegisterAssignmentHook(hook AssignmentCallback) {
	a.assignerService.RegisterPostAssignHook(func(ctx context.Context, issueKey, candidateLogin string, payload *thhttp.WebhookPayload) {
		queueKey := ""
		group := ""
		source := "direct"
		var delaySec int64 = 0
		now := time.Now().UTC()

		if payload != nil {
			queueKey = payload.GetQueueKey()
			group, _ = a.router.ResolveGroup(payload)
			if payload.Source != "" {
				source = payload.Source
			}

			if payload.QueueWaitDuration > 0 {
				delaySec = int64(payload.QueueWaitDuration.Seconds())
			} else if payload.Issue != nil && payload.Issue.CreatedAt != "" {
				created := parseIssueCreatedAt(payload.Issue.CreatedAt)
				if !created.IsZero() {
					dur := now.Sub(created)
					if dur > 0 {
						delaySec = int64(dur.Seconds())
					}
				}
			}
		}
		if delaySec < 0 {
			delaySec = 0
		}

		hook(ctx, AssignmentInfo{
			IssueKey:               issueKey,
			CandidateLogin:         candidateLogin,
			QueueKey:               queueKey,
			GroupID:                group,
			PendingDurationSeconds: delaySec,
			Source:                 source,
			AssignedAt:             now,
		})
	})
}

func parseIssueCreatedAt(dateStr string) time.Time {
	if dateStr == "" {
		return time.Time{}
	}
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.000-0700",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02 15:04:05",
	}
	for _, layout := range formats {
		if t, err := time.Parse(layout, dateStr); err == nil {
			return t
		}
	}
	return time.Time{}
}

// GetDB returns underlying *sql.DB for analytics or migration queries.
func (a *CoreApp) GetDB() *sql.DB {
	return a.db.DB
}

// GetServerAddr returns configured host and port string.
func (a *CoreApp) GetServerAddr() string {
	return fmt.Sprintf("%s:%d", a.cfg.Server.Host, a.cfg.Server.Port)
}

// GetTrackerOrgID returns the configured tracker organization ID.
func (a *CoreApp) GetTrackerOrgID() string {
	return a.cfg.Tracker.OrgID
}

// GetPendingCount returns current pending tickets count.
func (a *CoreApp) GetPendingCount(ctx context.Context) (int, error) {
	return a.repo.GetPendingCount(ctx)
}

// GetDLQCount returns current DLQ count.
func (a *CoreApp) GetDLQCount(ctx context.Context) (int, error) {
	return a.repo.GetDLQCount(ctx)
}

// GetTrackerClient returns the tracker API client.
func (a *CoreApp) GetTrackerClient() service.TrackerAPI {
	return a.trackerClient
}

// GetRouter returns the routing engine.
func (a *CoreApp) GetRouter() *service.Router {
	return a.router
}

// GetAssignerService returns the core assigner service.
func (a *CoreApp) GetAssignerService() *service.AssignerService {
	return a.assignerService
}

// Close gracefully closes the database.
func (a *CoreApp) Close() error {
	return a.db.Close()
}
