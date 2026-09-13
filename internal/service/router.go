package service

import (
	"errors"
	"fmt"
	"sync"

	"tracker-assigner/internal/config"
	thhttp "tracker-assigner/internal/transport/http"
)

var (
	// ErrNoMatchingRule is returned when no routing rule matches the webhook payload.
	ErrNoMatchingRule = errors.New("no routing rule matched the ticket")
	// ErrGroupNotFound is returned when the target group is not defined in the configuration.
	ErrGroupNotFound = errors.New("target group not found in configuration")
)

// Router matches incoming webhook tickets against configured routing rules.
type Router struct {
	mu  sync.RWMutex
	cfg *config.Config
}

// NewRouter creates a new Router with the provided configuration.
func NewRouter(cfg *config.Config) *Router {
	return &Router{
		cfg: cfg,
	}
}

// UpdateConfig updates the router configuration (supporting Hot Reload).
func (r *Router) UpdateConfig(cfg *config.Config) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cfg = cfg
}

// ResolveGroup evaluates the payload against routing rules and returns the target group ID.
func (r *Router) ResolveGroup(payload *thhttp.WebhookPayload) (string, error) {
	if payload == nil {
		return "", errors.New("payload is nil")
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	queue := payload.GetQueueKey()
	components := payload.GetComponents()
	tags := payload.GetTags()

	for _, rule := range r.cfg.RoutingRules {
		if rule.Matches(queue, components, tags) {
			// Verify target group exists in config
			if _, exists := r.cfg.Groups[rule.TargetGroup]; !exists {
				return "", fmt.Errorf("%w: %q (rule %s)", ErrGroupNotFound, rule.TargetGroup, rule.ID)
			}
			return rule.TargetGroup, nil
		}
	}

	return "", fmt.Errorf("%w for queue=%q components=%v tags=%v", ErrNoMatchingRule, queue, components, tags)
}

// GetGroup returns a copy of the group configuration for the given group ID.
func (r *Router) GetGroup(groupID string) (config.GroupConfig, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	group, exists := r.cfg.Groups[groupID]
	if !exists {
		return config.GroupConfig{}, fmt.Errorf("%w: %s", ErrGroupNotFound, groupID)
	}
	return group, nil
}

// GetAllGroups returns a copy of all configured groups.
func (r *Router) GetAllGroups() map[string]config.GroupConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string]config.GroupConfig, len(r.cfg.Groups))
	for k, v := range r.cfg.Groups {
		result[k] = v
	}
	return result
}
