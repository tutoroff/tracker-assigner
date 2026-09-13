package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/zap"
	"tracker-assigner/internal/config"
)

func TestYAMLConfigProvider(t *testing.T) {
	tempDir := t.TempDir()
	cfgPath := filepath.Join(tempDir, "config.yaml")

	cfgContent := `
server:
  host: "127.0.0.1"
  port: 8080
tracker:
  base_url: "https://api.tracker.yandex.net"
  token: "test-token"
  org_id: "org-1"
database:
  dsn: "test.db"
groups:
  support:
    strategy: "round_robin"
    max_load_per_user: 5
    assignees:
      - login: "user1"
`
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	provider, err := config.NewYAMLConfigProvider(cfgPath, zap.NewNop())
	if err != nil {
		t.Fatalf("failed to create YAMLConfigProvider: %v", err)
	}

	cfg := provider.GetConfig()
	if cfg == nil {
		t.Fatal("expected non-nil config from GetConfig")
	}
	if cfg.Server.Port != 8080 {
		t.Errorf("expected port 8080, got %d", cfg.Server.Port)
	}

	if provider.Get() != cfg {
		t.Errorf("expected Get() to match GetConfig()")
	}

	reloaded := false
	provider.OnReload(func(newCfg *config.Config) {
		reloaded = true
	})

	// Test Reload
	if err := provider.Reload(); err != nil {
		t.Fatalf("reload failed: %v", err)
	}
	if !reloaded {
		t.Errorf("expected reload callback to be executed")
	}
}
