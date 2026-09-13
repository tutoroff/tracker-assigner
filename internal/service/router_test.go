package service

import (
	"errors"
	"testing"

	"tracker-assigner/internal/config"
	thhttp "tracker-assigner/internal/transport/http"
)

func createTestConfig() *config.Config {
	return &config.Config{
		RoutingRules: []config.RoutingRule{
			{
				ID:          "rule-finance",
				Queue:       "SUPPORT",
				Components:  []string{"billing"},
				TargetGroup: "finance-team",
			},
			{
				ID:          "rule-vip",
				Queue:       "SUPPORT",
				Tags:        []string{"vip"},
				TargetGroup: "vip-team",
			},
			{
				ID:          "rule-default",
				Queue:       "SUPPORT",
				TargetGroup: "general-team",
			},
		},
		Groups: map[string]config.GroupConfig{
			"finance-team": {Strategy: config.StrategyLoadBased},
			"vip-team":     {Strategy: config.StrategyRoundRobin},
			"general-team": {Strategy: config.StrategyRoundRobin},
		},
	}
}

func TestRouter_ResolveGroup(t *testing.T) {
	cfg := createTestConfig()
	router := NewRouter(cfg)

	t.Run("Match by component", func(t *testing.T) {
		payload := &thhttp.WebhookPayload{
			Issue: &thhttp.TrackerIssue{
				Key:   "SUPPORT-1",
				Queue: &thhttp.IssueReference{Key: "SUPPORT"},
				Components: []thhttp.NamedItem{
					{Name: "billing"},
				},
			},
		}
		group, err := router.ResolveGroup(payload)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if group != "finance-team" {
			t.Errorf("expected finance-team, got %s", group)
		}
	})

	t.Run("Match by tag", func(t *testing.T) {
		payload := &thhttp.WebhookPayload{
			Issue: &thhttp.TrackerIssue{
				Key:   "SUPPORT-2",
				Queue: &thhttp.IssueReference{Key: "SUPPORT"},
				Tags:  []string{"vip"},
			},
		}
		group, err := router.ResolveGroup(payload)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if group != "vip-team" {
			t.Errorf("expected vip-team, got %s", group)
		}
	})

	t.Run("Fallback to default rule", func(t *testing.T) {
		payload := &thhttp.WebhookPayload{
			Issue: &thhttp.TrackerIssue{
				Key:   "SUPPORT-3",
				Queue: &thhttp.IssueReference{Key: "SUPPORT"},
			},
		}
		group, err := router.ResolveGroup(payload)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if group != "general-team" {
			t.Errorf("expected general-team, got %s", group)
		}
	})

	t.Run("No matching rule for other queue", func(t *testing.T) {
		payload := &thhttp.WebhookPayload{
			Issue: &thhttp.TrackerIssue{
				Key:   "OTHER-1",
				Queue: &thhttp.IssueReference{Key: "OTHER"},
			},
		}
		_, err := router.ResolveGroup(payload)
		if !errors.Is(err, ErrNoMatchingRule) {
			t.Errorf("expected ErrNoMatchingRule, got %v", err)
		}
	})

	t.Run("UpdateConfig changes routing dynamically", func(t *testing.T) {
		newCfg := createTestConfig()
		newCfg.RoutingRules = append([]config.RoutingRule{
			{
				ID:          "rule-security",
				Queue:       "SUPPORT",
				Tags:        []string{"security"},
				TargetGroup: "vip-team",
			},
		}, newCfg.RoutingRules...)

		router.UpdateConfig(newCfg)

		payload := &thhttp.WebhookPayload{
			Issue: &thhttp.TrackerIssue{
				Key:   "SUPPORT-4",
				Queue: &thhttp.IssueReference{Key: "SUPPORT"},
				Tags:  []string{"security"},
			},
		}
		group, err := router.ResolveGroup(payload)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if group != "vip-team" {
			t.Errorf("expected vip-team after update, got %s", group)
		}
	})
}

func TestRouter_GetGroupAndAllGroups(t *testing.T) {
	cfg := createTestConfig()
	router := NewRouter(cfg)

	all := router.GetAllGroups()
	if len(all) != 3 {
		t.Errorf("expected 3 groups, got %d", len(all))
	}

	g, err := router.GetGroup("finance-team")
	if err != nil || g.Strategy != config.StrategyLoadBased {
		t.Errorf("expected finance-team with load_based strategy, got %v, err=%v", g, err)
	}

	_, err = router.GetGroup("nonexistent")
	if !errors.Is(err, ErrGroupNotFound) {
		t.Errorf("expected ErrGroupNotFound for nonexistent group, got %v", err)
	}
}
