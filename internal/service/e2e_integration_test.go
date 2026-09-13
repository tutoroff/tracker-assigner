package service

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"tracker-assigner/internal/config"
	"tracker-assigner/internal/storage"
	thhttp "tracker-assigner/internal/transport/http"

	"go.uber.org/zap"
)

type recordedAssignment struct {
	IssueKey string
	Assignee string
}

type controllableTrackerAPI struct {
	mockTrackerAPI
	fail500     atomic.Bool
	assignments []recordedAssignment
}

func (c *controllableTrackerAPI) AssignIssue(ctx context.Context, issueKey, assignee string) error {
	if c.fail500.Load() {
		return &APIError{StatusCode: 500, Message: "Internal Server Error"}
	}
	c.assignments = append(c.assignments, recordedAssignment{IssueKey: issueKey, Assignee: assignee})
	return nil
}

func TestE2E_FullLifecycle(t *testing.T) {
	logger := zap.NewNop()
	db, err := storage.NewSQLite("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		RoutingRules: []config.RoutingRule{
			{ID: "r-support", Queue: "SUP", TargetGroup: "support"},
		},
		Groups: map[string]config.GroupConfig{
			"support": {
				Strategy:       config.StrategyRoundRobin,
				MaxLoadPerUser: 1, // each can only hold 1 ticket!
				Schedule: config.ScheduleConfig{
					Timezone:  "UTC",
					WorkDays:  []int{1, 2, 3, 4, 5, 6, 7},
					WorkHours: config.WorkHours{Start: "00:00", End: "23:59"},
				},
				Assignees: []config.AssigneeConfig{
					{Login: "alice"},
					{Login: "bob"},
				},
			},
		},
	}

	router := NewRouter(cfg)
	balancer := NewBalancer(repo)
	tracker := &controllableTrackerAPI{}
	assigner := NewAssignerService(router, balancer, tracker, repo, logger, nil, nil)

	ctx := context.Background()

	// Step 1: First ticket arrives (SUP-1) -> Should assign to Alice
	p1 := &thhttp.WebhookPayload{
		Issue: &thhttp.TrackerIssue{
			Key:   "SUP-1",
			Queue: &thhttp.IssueReference{Key: "SUP"},
		},
	}
	if err := assigner.ProcessWebhook(ctx, p1, []byte(`{"key":"SUP-1"}`)); err != nil {
		t.Fatalf("p1 failed: %v", err)
	}

	if len(tracker.assignments) != 1 || tracker.assignments[0].Assignee != "alice" {
		t.Fatalf("expected SUP-1 assigned to alice, got %v", tracker.assignments)
	}

	// Step 2: Second ticket arrives (SUP-2) -> Should assign to Bob (Round-Robin)
	p2 := &thhttp.WebhookPayload{
		Issue: &thhttp.TrackerIssue{
			Key:   "SUP-2",
			Queue: &thhttp.IssueReference{Key: "SUP"},
		},
	}
	if err := assigner.ProcessWebhook(ctx, p2, []byte(`{"key":"SUP-2"}`)); err != nil {
		t.Fatalf("p2 failed: %v", err)
	}

	if len(tracker.assignments) != 2 || tracker.assignments[1].Assignee != "bob" {
		t.Fatalf("expected SUP-2 assigned to bob, got %v", tracker.assignments)
	}

	// Step 3: Both Alice and Bob are at max load (1). Third ticket arrives (SUP-3)
	p3 := &thhttp.WebhookPayload{
		Issue: &thhttp.TrackerIssue{
			Key:   "SUP-3",
			Queue: &thhttp.IssueReference{Key: "SUP"},
		},
	}
	if err := assigner.ProcessWebhook(ctx, p3, []byte(`{"key":"SUP-3"}`)); err != nil {
		t.Fatalf("p3 failed: %v", err)
	}

	// Tracker should NOT have received a third assignment
	if len(tracker.assignments) != 2 {
		t.Fatalf("expected tracker assignments still 2, got %d", len(tracker.assignments))
	}

	// SUP-3 must be in SQLite Pending Queue
	pendingCount, _ := repo.GetPendingCount(ctx)
	if pendingCount != 1 {
		t.Fatalf("expected 1 item in pending queue, got %d", pendingCount)
	}

	// Step 4: Alice closes SUP-1 in Tracker -> webhook arrives
	pClose := &thhttp.WebhookPayload{
		Issue: &thhttp.TrackerIssue{
			Key:      "SUP-1",
			Status:   &thhttp.IssueReference{Key: "closed"},
			Assignee: &thhttp.IssueReference{Login: "alice"},
		},
	}
	if err := assigner.ProcessWebhook(ctx, pClose, []byte(`{}`)); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	aliceLoad, _ := repo.GetAssigneeLoad(ctx, "support", "alice")
	if aliceLoad != 0 {
		t.Fatalf("expected Alice load 0 after close, got %d", aliceLoad)
	}

	// Step 5: Process pending queue -> SUP-3 should now be assigned to Alice!
	pendingItem, _ := repo.GetNextPending(ctx, "support")
	if pendingItem == nil || pendingItem.IssueKey != "SUP-3" {
		t.Fatalf("expected SUP-3 in pending queue")
	}

	cand, err := balancer.PickAssignee(ctx, "support", cfg.Groups["support"], time.Now())
	if err != nil || cand.Login != "alice" {
		t.Fatalf("expected candidate alice, got %v (err: %v)", cand, err)
	}

	if err := tracker.AssignIssue(ctx, pendingItem.IssueKey, cand.Login); err != nil {
		t.Fatalf("failed assigning pending: %v", err)
	}
	_ = repo.IncrementLoad(ctx, "support", cand.Login)
	_ = repo.MarkPendingAssigned(ctx, pendingItem.ID)

	pendingCount, _ = repo.GetPendingCount(ctx)
	if pendingCount != 0 {
		t.Errorf("expected 0 pending items, got %d", pendingCount)
	}

	if len(tracker.assignments) != 3 || tracker.assignments[2].Assignee != "alice" {
		t.Fatalf("expected SUP-3 assigned to alice, got %v", tracker.assignments)
	}

	// Step 6: Test DLQ resilience on 5xx error
	tracker.fail500.Store(true) // simulate Tracker downtime
	p4 := &thhttp.WebhookPayload{
		Issue: &thhttp.TrackerIssue{
			Key:   "SUP-4",
			Queue: &thhttp.IssueReference{Key: "SUP"},
		},
	}
	// Alice is at 1, but let's decrement Bob's load so Bob is candidate
	_ = repo.DecrementLoad(ctx, "support", "bob")

	if err := assigner.ProcessWebhook(ctx, p4, []byte(`{"key":"SUP-4"}`)); err != nil {
		t.Fatalf("expected nil error on handled 5xx DLQ, got %v", err)
	}

	dlqCount, _ := repo.GetDLQCount(ctx)
	if dlqCount != 1 {
		t.Fatalf("expected 1 item in DLQ, got %d", dlqCount)
	}
}
