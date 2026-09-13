package service

import (
	"context"
	"testing"

	"tracker-assigner/internal/config"
	"tracker-assigner/internal/storage"
	thhttp "tracker-assigner/internal/transport/http"

	"go.uber.org/zap"
)

func TestAssignerService_ManualIntervention(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		RoutingRules: []config.RoutingRule{
			{ID: "r1", Queue: "HELP", TargetGroup: "support"},
		},
		Groups: map[string]config.GroupConfig{
			"support": {
				Assignees: []config.AssigneeConfig{
					{Login: "alice"},
					{Login: "bob"},
				},
			},
		},
	}
	router := NewRouter(cfg)
	service := NewAssignerService(router, nil, &mockTrackerAPI{}, repo, logger, nil, nil)

	// Pre-conditions:
	// Alice has load 2, Bob has load 0
	_ = repo.SetAssigneeLoad(context.Background(), "support", "alice", 2)
	_ = repo.SetAssigneeLoad(context.Background(), "support", "bob", 0)

	// A ticket HELP-77 was in pending queue
	_ = repo.EnqueuePending(context.Background(), storage.PendingItem{
		IssueKey: "HELP-77",
		QueueKey: "HELP",
		GroupID:  "support",
		Payload:  `{}`,
	})

	// Webhook arrives: Human reassigns HELP-77 from Alice to Bob
	payload := &thhttp.WebhookPayload{
		Issue: &thhttp.TrackerIssue{
			Key:   "HELP-77",
			Queue: &thhttp.IssueReference{Key: "HELP"},
		},
		Changes: map[string]thhttp.FieldChange{
			"assignee": {
				From: map[string]any{"login": "alice"},
				To:   map[string]any{"login": "bob"},
			},
		},
	}

	err := service.ProcessWebhook(context.Background(), payload, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 1. Pending ticket HELP-77 should be removed
	pendingCount, _ := repo.GetPendingCount(context.Background())
	if pendingCount != 0 {
		t.Errorf("expected pending queue empty, got %d", pendingCount)
	}

	// 2. Alice load should be decremented to 1
	aliceLoad, _ := repo.GetAssigneeLoad(context.Background(), "support", "alice")
	if aliceLoad != 1 {
		t.Errorf("expected Alice load 1, got %d", aliceLoad)
	}

	// 3. Bob load should be incremented to 1
	bobLoad, _ := repo.GetAssigneeLoad(context.Background(), "support", "bob")
	if bobLoad != 1 {
		t.Errorf("expected Bob load 1, got %d", bobLoad)
	}
}

func TestAssignerService_ManualIntervention_TrackedIssueNoDoubleDecrement(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		RoutingRules: []config.RoutingRule{
			{ID: "r1", Queue: "HELP", TargetGroup: "support"},
		},
		Groups: map[string]config.GroupConfig{
			"support": {
				Assignees: []config.AssigneeConfig{
					{Login: "alice"},
					{Login: "bob"},
				},
			},
		},
	}
	router := NewRouter(cfg)
	service := NewAssignerService(router, nil, &mockTrackerAPI{}, repo, logger, nil, nil)

	ctx := context.Background()

	// Assign HELP-100 to alice via IncrementLoadForIssue (tracked in SQLite)
	_, _ = repo.IncrementLoadForIssue(ctx, "support", "alice", "HELP-100")
	// Also assign HELP-101 to alice so she has load 2
	_, _ = repo.IncrementLoadForIssue(ctx, "support", "alice", "HELP-101")

	aliceLoad, _ := repo.GetAssigneeLoad(ctx, "support", "alice")
	if aliceLoad != 2 {
		t.Fatalf("expected alice initial load 2, got %d", aliceLoad)
	}

	// Operator manually reassigns HELP-100 from alice to bob
	payload := &thhttp.WebhookPayload{
		Issue: &thhttp.TrackerIssue{
			Key:   "HELP-100",
			Queue: &thhttp.IssueReference{Key: "HELP"},
		},
		Changes: map[string]thhttp.FieldChange{
			"assignee": {
				From: map[string]any{"login": "alice"},
				To:   map[string]any{"login": "bob"},
			},
		},
	}

	err := service.ProcessWebhook(ctx, payload, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Alice load MUST be decremented by EXACTLY 1 (from 2 to 1), NOT double-decremented to 0!
	aliceLoadAfter, _ := repo.GetAssigneeLoad(ctx, "support", "alice")
	if aliceLoadAfter != 1 {
		t.Errorf("CRITICAL BUG: expected alice load to be 1 after reassignment, got %d (possible double decrement!)", aliceLoadAfter)
	}

	// Bob load must be 1
	bobLoadAfter, _ := repo.GetAssigneeLoad(ctx, "support", "bob")
	if bobLoadAfter != 1 {
		t.Errorf("expected bob load 1, got %d", bobLoadAfter)
	}

	// Verify HELP-100 is now tracked under bob
	asgn, err := repo.GetIssueAssignment(ctx, "HELP-100")
	if err != nil || asgn == nil || asgn.AssigneeID != "bob" {
		t.Errorf("expected assignment tracked for bob, got %v, err=%v", asgn, err)
	}
}

func TestAssignerService_ManualIntervention_Unassign(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		RoutingRules: []config.RoutingRule{
			{ID: "r1", Queue: "HELP", TargetGroup: "support"},
		},
		Groups: map[string]config.GroupConfig{
			"support": {
				Assignees: []config.AssigneeConfig{
					{Login: "alice"},
				},
			},
		},
	}
	router := NewRouter(cfg)
	service := NewAssignerService(router, nil, &mockTrackerAPI{}, repo, logger, nil, nil)
	ctx := context.Background()

	// Assign HELP-200 to alice
	_, _ = repo.IncrementLoadForIssue(ctx, "support", "alice", "HELP-200")
	load, _ := repo.GetAssigneeLoad(ctx, "support", "alice")
	if load != 1 {
		t.Fatalf("expected alice load 1, got %d", load)
	}

	// Webhook: operator clears assignee (To is nil/empty)
	payload := &thhttp.WebhookPayload{
		Issue: &thhttp.TrackerIssue{
			Key:   "HELP-200",
			Queue: &thhttp.IssueReference{Key: "HELP"},
		},
		Changes: map[string]thhttp.FieldChange{
			"assignee": {
				From: map[string]any{"login": "alice"},
				To:   nil,
			},
		},
	}

	err := service.ProcessWebhook(ctx, payload, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Alice load must be decremented to 0
	loadAfter, _ := repo.GetAssigneeLoad(ctx, "support", "alice")
	if loadAfter != 0 {
		t.Errorf("expected alice load 0, got %d", loadAfter)
	}

	// Assignment must be deleted from issue_assignments
	asgn, _ := repo.GetIssueAssignment(ctx, "HELP-200")
	if asgn != nil {
		t.Errorf("expected issue assignment deleted, but found %v", asgn)
	}
}

func TestAssignerService_IssueCreated_ByRobotTokenUser(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		Tracker: config.TrackerConfig{
			RobotLogin: "tut0roff",
			RobotUID:   "8000000000000002",
		},
		RoutingRules: []config.RoutingRule{
			{ID: "r1", Queue: "MARKETING", TargetGroup: "marketing_team"},
		},
		Groups: map[string]config.GroupConfig{
			"marketing_team": {
				Strategy:       config.StrategyRoundRobin,
				MaxLoadPerUser: 10,
				Schedule: config.ScheduleConfig{
					Timezone:  "UTC",
					WorkDays:  []int{1, 2, 3, 4, 5, 6, 7},
					WorkHours: config.WorkHours{Start: "00:00", End: "23:59"},
				},
				Assignees: []config.AssigneeConfig{
					{Login: "tut0roff"},
				},
			},
		},
	}

	router := NewRouter(cfg)
	balancer := NewBalancer(repo)
	tracker := &mockTrackerAPI{}
	service := NewAssignerService(router, balancer, tracker, repo, logger, cfg, nil)

	// An issue created by tut0roff (the human whose token the robot runs under)
	payload := &thhttp.WebhookPayload{
		Event: "issue_created",
		Issue: &thhttp.TrackerIssue{
			Key:   "MARKETING-99",
			Queue: &thhttp.IssueReference{Key: "MARKETING"},
			CreatedBy: &thhttp.IssueReference{
				Login: "tut0roff",
				ID:    "8000000000000002",
			},
			UpdatedBy: &thhttp.IssueReference{
				Login: "tut0roff",
				ID:    "8000000000000002",
			},
		},
	}

	err := service.ProcessWebhook(context.Background(), payload, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should NOT be ignored because event is issue_created!
	load, _ := repo.GetAssigneeLoad(context.Background(), "marketing_team", "tut0roff")
	if load != 1 {
		t.Errorf("expected ticket to be assigned and load to be 1, got %d", load)
	}
}

func TestAssignerService_AlreadyAssignedIssue_WebhookWithoutAssignee(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		RoutingRules: []config.RoutingRule{
			{ID: "r1", Queue: "MARKETING", TargetGroup: "marketing_team"},
		},
		Groups: map[string]config.GroupConfig{
			"marketing_team": {
				Strategy:       config.StrategyRoundRobin,
				MaxLoadPerUser: 10,
				Schedule: config.ScheduleConfig{
					Timezone:  "UTC",
					WorkDays:  []int{1, 2, 3, 4, 5, 6, 7},
					WorkHours: config.WorkHours{Start: "00:00", End: "23:59"},
				},
				Assignees: []config.AssigneeConfig{
					{Login: "tut0roff"},
				},
			},
		},
	}

	router := NewRouter(cfg)
	balancer := NewBalancer(repo)
	tracker := &mockTrackerAPI{}
	service := NewAssignerService(router, balancer, tracker, repo, logger, cfg, nil)
	ctx := context.Background()

	// Initial assignment
	_, _ = repo.IncrementLoadForIssue(ctx, "marketing_team", "tut0roff", "MARKETING-55")

	// Now a webhook arrives 10 seconds later (e.g. comment added) where assignee field is omitted in the body
	payload := &thhttp.WebhookPayload{
		Event: "issue_updated",
		Issue: &thhttp.TrackerIssue{
			Key:   "MARKETING-55",
			Queue: &thhttp.IssueReference{Key: "MARKETING"},
			// Notice: Assignee is nil!
		},
	}

	err := service.ProcessWebhook(ctx, payload, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Load should remain 1, NOT increment to 2
	load, _ := repo.GetAssigneeLoad(ctx, "marketing_team", "tut0roff")
	if load != 1 {
		t.Errorf("expected load to remain 1, got %d", load)
	}
}
