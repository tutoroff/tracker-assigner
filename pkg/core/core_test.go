package core_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"tracker-assigner/internal/config"
	"tracker-assigner/internal/service"
	"tracker-assigner/pkg/core"
	"tracker-assigner/pkg/logger"
)

func TestCoreAppLifecycle(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	cfgPath := filepath.Join(tempDir, "config.yaml")

	cfgContent := `
server:
  host: "127.0.0.1"
  port: 9090
  secret_token: "test-secret"
tracker:
  base_url: "https://api.tracker.yandex.net"
  token: "fake-token"
  org_id: "org-123"
  is_cloud_org: false
  timeout: 5s
  robot_login: "robot"
database:
  dsn: "` + filepath.ToSlash(dbPath) + `"
routing_rules:
  - id: "rule-support"
    queue: "SUP"
    target_group: "l1"
groups:
  l1:
    strategy: "round_robin"
    max_load_per_user: 5
    assignees:
      - login: "user1"
`
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	log, err := logger.New(true)
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}

	provider, err := config.NewYAMLConfigProvider(cfgPath, log)
	if err != nil {
		t.Fatalf("failed to create YAMLConfigProvider: %v", err)
	}

	app, err := core.NewCoreApp(provider, log)
	if err != nil {
		t.Fatalf("failed to create CoreApp: %v", err)
	}
	defer app.Close()

	if app.GetTrackerOrgID() != "org-123" {
		t.Errorf("expected org-123, got %s", app.GetTrackerOrgID())
	}

	if app.GetServerAddr() != "127.0.0.1:9090" {
		t.Errorf("expected 127.0.0.1:9090, got %s", app.GetServerAddr())
	}

	if db := app.GetDB(); db == nil {
		t.Error("expected non-nil *sql.DB")
	}

	mux := http.NewServeMux()
	app.RegisterRoutes(mux)

	// Test healthz endpoint
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("healthz returned status %d, expected 200", w.Code)
	}

	// Test starting workers with cancelable context
	ctx, cancel := context.WithCancel(context.Background())
	if err := app.Start(ctx); err != nil {
		t.Errorf("failed to start workers: %v", err)
	}
	cancel()
	time.Sleep(50 * time.Millisecond)

	// Test hook registration
	hookCalled := false
	app.RegisterAssignmentHook(func(ctx context.Context, info core.AssignmentInfo) {
		hookCalled = true
	})
	_ = hookCalled
}

type mockProvider struct {
	cfg       *config.Config
	callbacks []func(*config.Config)
}

func (m *mockProvider) GetConfig() *config.Config {
	return m.cfg
}

func (m *mockProvider) OnReload(cb func(*config.Config)) {
	m.callbacks = append(m.callbacks, cb)
}

func TestCoreApp_SetProvider(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test2.db")
	cfgPath := filepath.Join(tempDir, "config.yaml")

	cfgContent := `
server:
  host: "127.0.0.1"
  port: 8081
tracker:
  base_url: "https://api.tracker.yandex.net"
  token: "fake-token"
  org_id: "org-init"
database:
  dsn: "` + filepath.ToSlash(dbPath) + `"
`
	_ = os.WriteFile(cfgPath, []byte(cfgContent), 0644)

	log, _ := logger.New(true)
	initProv, err := config.NewYAMLConfigProvider(cfgPath, log)
	if err != nil {
		t.Fatalf("failed init YAML provider: %v", err)
	}

	app, err := core.NewCoreApp(initProv, log)
	if err != nil {
		t.Fatalf("failed create app: %v", err)
	}
	defer app.Close()

	if app.GetConfig().Tracker.OrgID != "org-init" {
		t.Errorf("expected org-init, got %s", app.GetConfig().Tracker.OrgID)
	}

	// Now swap to new mock provider
	newCfg := *app.GetConfig()
	newCfg.Tracker.OrgID = "org-swapped"
	mockProv := &mockProvider{cfg: &newCfg}

	app.SetProvider(mockProv)

	if app.GetConfig().Tracker.OrgID != "org-swapped" {
		t.Errorf("expected org-swapped after SetProvider, got %s", app.GetConfig().Tracker.OrgID)
	}
}

func TestCoreApp_NewCoreApp_Validation(t *testing.T) {
	log, _ := logger.New(true)

	// 1. Nil provider
	_, err := core.NewCoreApp(nil, log)
	if err == nil || err.Error() != "config provider cannot be nil" {
		t.Errorf("expected 'config provider cannot be nil', got: %v", err)
	}

	// 2. Provider returns nil config
	nilCfgProv := &mockProvider{cfg: nil}
	_, err = core.NewCoreApp(nilCfgProv, log)
	if err == nil || err.Error() != "config provider returned nil configuration" {
		t.Errorf("expected 'config provider returned nil configuration', got: %v", err)
	}

	// 3. Logger nil fallback (should not panic)
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "logger_test.db")
	cfg := &config.Config{
		Database: config.DatabaseConfig{DSN: filepath.ToSlash(dbPath)},
		Tracker:  config.TrackerConfig{BaseURL: "https://api.tracker.yandex.net"},
	}
	app, err := core.NewCoreApp(&mockProvider{cfg: cfg}, nil)
	if err != nil {
		t.Fatalf("expected nil error with fallback logger, got %v", err)
	}
	_ = app.Close()

	// 4. Invalid database DSN
	badCfg := &config.Config{
		Database: config.DatabaseConfig{DSN: "http://invalid:::path???/db"},
	}
	_, err = core.NewCoreApp(&mockProvider{cfg: badCfg}, log)
	if err == nil {
		t.Errorf("expected error with invalid sqlite DSN, got nil")
	}
}

func TestCoreApp_NewCoreAppFromPath(t *testing.T) {
	log, _ := logger.New(true)

	// 1. Invalid path
	_, err := core.NewCoreAppFromPath("/nonexistent/file/path/config.yaml", log)
	if err == nil {
		t.Error("expected error for nonexistent config file, got nil")
	}

	// 2. Valid path
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "valid_from_path.db")
	cfgPath := filepath.Join(tempDir, "config.yaml")
	cfgContent := `
server:
  host: "127.0.0.1"
  port: 9091
tracker:
  base_url: "https://api.tracker.yandex.net"
  token: "tok"
  org_id: "org-path"
database:
  dsn: "` + filepath.ToSlash(dbPath) + `"
`
	_ = os.WriteFile(cfgPath, []byte(cfgContent), 0644)

	app, err := core.NewCoreAppFromPath(cfgPath, log)
	if err != nil {
		t.Fatalf("unexpected error for valid path: %v", err)
	}
	defer app.Close()

	if app.GetTrackerOrgID() != "org-path" {
		t.Errorf("expected org-path, got %s", app.GetTrackerOrgID())
	}
}

func TestCoreApp_CountsAndBalancer(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "counts.db")
	log, _ := logger.New(true)
	cfg := &config.Config{
		Database: config.DatabaseConfig{DSN: filepath.ToSlash(dbPath)},
		Tracker:  config.TrackerConfig{BaseURL: "https://api.tracker.yandex.net"},
	}
	app, err := core.NewCoreApp(&mockProvider{cfg: cfg}, log)
	if err != nil {
		t.Fatalf("failed create app: %v", err)
	}
	defer app.Close()

	ctx := context.Background()
	pending, err := app.GetPendingCount(ctx)
	if err != nil || pending != 0 {
		t.Errorf("expected 0 pending, got %d, err: %v", pending, err)
	}

	dlq, err := app.GetDLQCount(ctx)
	if err != nil || dlq != 0 {
		t.Errorf("expected 0 dlq, got %d, err: %v", dlq, err)
	}

	repo := app.GetRepository()
	if repo == nil {
		t.Error("expected non-nil repository")
	}

	// Set custom balancer
	app.SetBalancer(service.NewBalancer(repo))
}

func TestCoreApp_ApplyConfig_AlertsAndNotifications(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "apply_cfg.db")
	log, _ := logger.New(true)
	cfg := &config.Config{
		Database: config.DatabaseConfig{DSN: filepath.ToSlash(dbPath)},
		Tracker:  config.TrackerConfig{BaseURL: "https://api.tracker.yandex.net"},
	}
	app, err := core.NewCoreApp(&mockProvider{cfg: cfg}, log)
	if err != nil {
		t.Fatalf("failed create app: %v", err)
	}
	defer app.Close()

	// 1. Apply nil config (no-op)
	app.ApplyConfig(nil)

	// 2. Apply config with Telegram alerts and notifications enabled
	newCfg := *cfg
	newCfg.Alerts.Telegram.Enabled = true
	newCfg.Alerts.Telegram.BotToken = "alert-token"
	newCfg.Alerts.Telegram.ChatID = "-1001"
	newCfg.Notifications.Telegram.Enabled = true
	newCfg.Notifications.Telegram.BotToken = "notif-token"
	newCfg.Alerts.PendingSLATimeout = 30 * time.Minute

	app.ApplyConfig(&newCfg)
	if app.GetConfig().Alerts.PendingSLATimeout != 30*time.Minute {
		t.Errorf("expected 30m SLA timeout after ApplyConfig, got %v", app.GetConfig().Alerts.PendingSLATimeout)
	}

	// 3. Apply config with Telegram alerts disabled
	newCfg2 := newCfg
	newCfg2.Alerts.Telegram.Enabled = false
	newCfg2.Notifications.Telegram.Enabled = false
	app.ApplyConfig(&newCfg2)
}

func TestCoreApp_RegisterAssignmentHook_Execution(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "hook.db")
	log, _ := logger.New(true)

	trackerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"1","key":"SUP-10"}`))
	}))
	defer trackerServer.Close()

	cfg := &config.Config{
		Database: config.DatabaseConfig{DSN: filepath.ToSlash(dbPath)},
		Tracker: config.TrackerConfig{
			BaseURL: trackerServer.URL,
			Token:   "mock-token",
			OrgID:   "mock-org",
		},
		RoutingRules: []config.RoutingRule{
			{ID: "r1", Queue: "SUP", TargetGroup: "support"},
		},
		Groups: map[string]config.GroupConfig{
			"support": {
				Strategy:       config.StrategyRoundRobin,
				MaxLoadPerUser: 5,
				Schedule: config.ScheduleConfig{
					Timezone:  "UTC",
					WorkDays:  []int{1, 2, 3, 4, 5, 6, 7},
					WorkHours: config.WorkHours{Start: "00:00", End: "23:59"},
				},
				Assignees: []config.AssigneeConfig{{Login: "alice"}},
			},
		},
	}

	app, err := core.NewCoreApp(&mockProvider{cfg: cfg}, log)
	if err != nil {
		t.Fatalf("failed create app: %v", err)
	}
	defer app.Close()

	var recordedInfo core.AssignmentInfo
	var hookCalled bool
	app.RegisterAssignmentHook(func(ctx context.Context, info core.AssignmentInfo) {
		hookCalled = true
		recordedInfo = info
	})

	// Register routes and send mock webhook through mux
	mux := http.NewServeMux()
	app.RegisterRoutes(mux)

	// Test webhook payload with CreatedAt format
	payloadJSON := `{
		"issue": {
			"key": "SUP-10",
			"queue": {"key": "SUP"},
			"createdAt": "2026-09-10T12:00:00.000+0000"
		}
	}`

	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(payloadJSON))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected webhook HTTP 200, got %d", w.Code)
	}

	if !hookCalled {
		t.Errorf("expected post assignment hook to be called")
	}
	if recordedInfo.IssueKey != "SUP-10" || recordedInfo.CandidateLogin != "alice" {
		t.Errorf("unexpected hook info: %+v", recordedInfo)
	}
	if recordedInfo.GroupID != "support" || recordedInfo.QueueKey != "SUP" {
		t.Errorf("unexpected group/queue in hook info: %+v", recordedInfo)
	}
}

