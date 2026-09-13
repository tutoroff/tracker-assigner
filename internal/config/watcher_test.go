package config

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestWatcher_ReloadValidAndInvalid(t *testing.T) {
	logger := zap.NewNop()

	tmpFile, err := os.CreateTemp("", "watcher-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	_, _ = tmpFile.WriteString(sampleConfigYAML)
	_ = tmpFile.Close()

	initialCfg, err := Load(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to load initial: %v", err)
	}

	watcher := NewWatcher(tmpFile.Name(), initialCfg, logger)

	var callbackCount int32
	watcher.OnReload(func(newCfg *Config) {
		atomic.AddInt32(&callbackCount, 1)
	})

	// 1. Valid update (change port to 9999)
	updatedYAML := sampleConfigYAML + "\n# comment"
	_ = os.WriteFile(tmpFile.Name(), []byte(updatedYAML), 0644)

	if err := watcher.Reload(); err != nil {
		t.Fatalf("expected successful reload, got: %v", err)
	}
	if atomic.LoadInt32(&callbackCount) != 1 {
		t.Errorf("expected 1 callback call, got %d", atomic.LoadInt32(&callbackCount))
	}

	// 2. Invalid update (broken yaml)
	_ = os.WriteFile(tmpFile.Name(), []byte("invalid: [broken"), 0644)

	err = watcher.Reload()
	if err == nil {
		t.Fatalf("expected error on invalid config, got nil")
	}

	// Previous config must be retained
	if watcher.Get().Server.Port != 9090 {
		t.Errorf("expected previous config port 9090 retained, got %d", watcher.Get().Server.Port)
	}
	// Callback should NOT have been called for invalid reload
	if atomic.LoadInt32(&callbackCount) != 1 {
		t.Errorf("expected callback count to remain 1, got %d", atomic.LoadInt32(&callbackCount))
	}
}

func TestWatcher_LiveFSNotify(t *testing.T) {
	logger := zap.NewNop()

	tmpFile, err := os.CreateTemp("", "fsnotify-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	_, _ = tmpFile.WriteString(sampleConfigYAML)
	_ = tmpFile.Close()

	initialCfg, _ := Load(tmpFile.Name())
	watcher := NewWatcher(tmpFile.Name(), initialCfg, logger)

	var reloaded atomic.Bool
	watcher.OnReload(func(newCfg *Config) {
		reloaded.Store(true)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := watcher.Start(ctx); err != nil {
		t.Fatalf("failed to start watcher: %v", err)
	}

	// Give watcher a moment to register
	time.Sleep(50 * time.Millisecond)

	// Modify file
	_ = os.WriteFile(tmpFile.Name(), []byte(sampleConfigYAML+"\n# live edit"), 0644)

	// Wait up to 1 second for debounce & reload
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if reloaded.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !reloaded.Load() {
		t.Errorf("expected watcher to detect file modification via fsnotify and reload")
	}
}
