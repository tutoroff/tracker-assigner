package config

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"
)

// ReloadCallback is triggered when configuration is successfully reloaded.
type ReloadCallback func(newCfg *Config)

// Watcher monitors config.yaml changes and updates configuration in-memory without restarting.
type Watcher struct {
	configPath string
	mu         sync.RWMutex
	current    *Config
	callbacks  []ReloadCallback
	logger     *zap.Logger
}

// NewWatcher creates a new configuration Watcher.
func NewWatcher(configPath string, initialCfg *Config, logger *zap.Logger) *Watcher {
	return &Watcher{
		configPath: configPath,
		current:    initialCfg,
		logger:     logger,
	}
}

// OnReload registers a callback to be notified when config changes.
func (w *Watcher) OnReload(cb ReloadCallback) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.callbacks = append(w.callbacks, cb)
}

// Get returns the current active configuration thread-safely.
func (w *Watcher) Get() *Config {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.current
}

// GetConfig returns the current active configuration thread-safely (satisfies ConfigProvider).
func (w *Watcher) GetConfig() *Config {
	return w.Get()
}

// Reload forces an immediate reload from disk and notifies callbacks if valid.
func (w *Watcher) Reload() error {
	newCfg, err := Load(w.configPath)
	if err != nil {
		w.logger.Error("Hot Reload: failed to load or validate new configuration, retaining previous config",
			zap.String("path", w.configPath),
			zap.Error(err))
		return err
	}

	w.mu.Lock()
	w.current = newCfg
	callbacks := make([]ReloadCallback, len(w.callbacks))
	copy(callbacks, w.callbacks)
	w.mu.Unlock()

	w.logger.Info("Hot Reload: configuration successfully reloaded from file",
		zap.String("path", w.configPath))

	for _, cb := range callbacks {
		cb(newCfg)
	}

	return nil
}

// Start begins watching the config file directory with fsnotify until ctx is cancelled.
func (w *Watcher) Start(ctx context.Context) error {
	absPath, err := filepath.Abs(w.configPath)
	if err != nil {
		return fmt.Errorf("failed to resolve config file path: %w", err)
	}
	dir := filepath.Dir(absPath)
	filename := filepath.Base(absPath)

	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create fsnotify watcher: %w", err)
	}

	if err := fsWatcher.Add(dir); err != nil {
		_ = fsWatcher.Close()
		return fmt.Errorf("failed to watch config directory %s: %w", dir, err)
	}

	w.logger.Info("Config watcher started",
		zap.String("watching_dir", dir),
		zap.String("target_file", filename))

	go func() {
		defer fsWatcher.Close()

		var (
			debounceTimer *time.Timer
			timerCh       <-chan time.Time
		)

		for {
			select {
			case <-ctx.Done():
				w.logger.Info("Stopping config watcher")
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				return

			case event, ok := <-fsWatcher.Events:
				if !ok {
					return
				}
				// Check if the modified file is our config file
				if filepath.Base(event.Name) == filename {
					if event.Op&(fsnotify.Write|fsnotify.Create) != 0 {
						if debounceTimer != nil {
							debounceTimer.Stop()
						}
						debounceTimer = time.NewTimer(150 * time.Millisecond)
						timerCh = debounceTimer.C
					}
				}

			case <-timerCh:
				w.logger.Info("Detected change in config file, executing Hot Reload...")
				_ = w.Reload()

			case err, ok := <-fsWatcher.Errors:
				if !ok {
					return
				}
				w.logger.Error("Error from file system watcher", zap.Error(err))
			}
		}
	}()

	return nil
}
