package config

import (
	"context"
	"sync"

	"go.uber.org/zap"
)

// ConfigProvider defines an interface for obtaining the active configuration and subscribing to updates.
type ConfigProvider interface {
	GetConfig() *Config
	OnReload(cb func(newCfg *Config))
}

// YAMLConfigProvider loads and monitors configuration from a YAML file.
type YAMLConfigProvider struct {
	path      string
	watcher   *Watcher
	mu        sync.RWMutex
	current   *Config
	callbacks []func(newCfg *Config)
	logger    *zap.Logger
}

// NewYAMLConfigProvider creates a new YAMLConfigProvider for the given file path.
func NewYAMLConfigProvider(path string, logger *zap.Logger) (*YAMLConfigProvider, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	cfg, err := Load(path)
	if err != nil {
		return nil, err
	}
	watcher := NewWatcher(path, cfg, logger)
	p := &YAMLConfigProvider{
		path:    path,
		watcher: watcher,
		current: cfg,
		logger:  logger,
	}
	watcher.OnReload(func(newCfg *Config) {
		p.mu.Lock()
		p.current = newCfg
		cbs := make([]func(newCfg *Config), len(p.callbacks))
		copy(cbs, p.callbacks)
		p.mu.Unlock()

		for _, cb := range cbs {
			cb(newCfg)
		}
	})
	return p, nil
}

// GetConfig returns the current active configuration thread-safely.
func (p *YAMLConfigProvider) GetConfig() *Config {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.current
}

// Get returns the current active configuration thread-safely (alias for GetConfig).
func (p *YAMLConfigProvider) Get() *Config {
	return p.GetConfig()
}

// OnReload registers a callback to be notified when configuration is reloaded.
func (p *YAMLConfigProvider) OnReload(cb func(newCfg *Config)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.callbacks = append(p.callbacks, cb)
}

// Start begins watching the config file directory for changes.
func (p *YAMLConfigProvider) Start(ctx context.Context) error {
	return p.watcher.Start(ctx)
}

// Reload forces an immediate reload from disk.
func (p *YAMLConfigProvider) Reload() error {
	return p.watcher.Reload()
}
