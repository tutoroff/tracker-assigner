package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateConfig_Valid(t *testing.T) {
	tempDir := t.TempDir()
	cfgPath := filepath.Join(tempDir, "config.yaml")
	dbPath := filepath.Join(tempDir, "test.db")

	cfgContent := `
server:
  host: "127.0.0.1"
  port: 8080
tracker:
  base_url: "https://api.tracker.yandex.net"
  token: "secret"
  org_id: "org-1"
database:
  dsn: "` + filepath.ToSlash(dbPath) + `"
groups:
  support:
    strategy: "round_robin"
    max_load_per_user: 3
    assignees:
      - login: "alice"
routing_rules:
  - id: "r1"
    queue: "SUP"
    target_group: "support"
alerts:
  telegram:
    enabled: false
  pending_sla_timeout: 1h
`
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	var buf bytes.Buffer
	err := validateConfig(cfgPath, &buf)
	if err != nil {
		t.Fatalf("expected validateConfig to succeed, got: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "is VALID") {
		t.Errorf("expected 'is VALID' in output, got: %s", output)
	}
	if !strings.Contains(output, "127.0.0.1:8080") {
		t.Errorf("expected server host:port in output, got: %s", output)
	}
	if !strings.Contains(output, "support") {
		t.Errorf("expected group 'support' in output, got: %s", output)
	}
	if !strings.Contains(output, "r1") {
		t.Errorf("expected rule 'r1' in output, got: %s", output)
	}
}

func TestValidateConfig_InvalidYAML(t *testing.T) {
	tempDir := t.TempDir()
	cfgPath := filepath.Join(tempDir, "bad_config.yaml")
	if err := os.WriteFile(cfgPath, []byte(`bad: yaml: [syntax`), 0644); err != nil {
		t.Fatalf("failed to write bad config: %v", err)
	}

	var buf bytes.Buffer
	err := validateConfig(cfgPath, &buf)
	if err == nil {
		t.Fatal("expected validateConfig to fail on bad YAML, got nil")
	}
}

func TestValidateConfig_NonexistentFile(t *testing.T) {
	var buf bytes.Buffer
	err := validateConfig(filepath.Join(t.TempDir(), "missing.yaml"), &buf)
	if err == nil {
		t.Fatal("expected validateConfig to fail on missing file, got nil")
	}
}

func TestCLI_ValidationFlag_Exec(t *testing.T) {
	tempDir := t.TempDir()
	cfgPath := filepath.Join(tempDir, "cli_valid.yaml")
	dbPath := filepath.Join(tempDir, "cli.db")

	cfgContent := `
server:
  host: "0.0.0.0"
  port: 8080
tracker:
  base_url: "https://api.tracker.yandex.net"
  token: "test"
  org_id: "org-test"
database:
  dsn: "` + filepath.ToSlash(dbPath) + `"
`
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cmd := exec.Command("go", "run", ".", "-validate", "-config", cfgPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("expected CLI validation to succeed, error: %v, output: %s", err, string(out))
	}
	if !strings.Contains(string(out), "is VALID") {
		t.Errorf("expected 'is VALID' in stdout, got: %s", string(out))
	}
}

func TestCLI_ValidationFlag_Exec_Invalid(t *testing.T) {
	tempDir := t.TempDir()
	cfgPath := filepath.Join(tempDir, "cli_invalid.yaml")
	if err := os.WriteFile(cfgPath, []byte(`bad: [yaml: broken`), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cmd := exec.Command("go", "run", ".", "-validate", "-config", cfgPath)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected CLI validation to fail on invalid YAML, got success with output: %s", string(out))
	}
	if !strings.Contains(string(out), "FAILED") {
		t.Errorf("expected 'FAILED' in stderr output, got: %s", string(out))
	}
}

func TestCLI_ValidationFlag_Exec_Missing(t *testing.T) {
	missingPath := filepath.Join(t.TempDir(), "nonexistent.yaml")
	cmd := exec.Command("go", "run", ".", "-validate", "-config", missingPath)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected CLI validation to fail on missing config, got success with output: %s", string(out))
	}
	if !strings.Contains(string(out), "FAILED") {
		t.Errorf("expected 'FAILED' in output, got: %s", string(out))
	}
}
