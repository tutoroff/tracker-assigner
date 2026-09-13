package service

import (
	"context"
	"encoding/json"
	"testing"

	"tracker-assigner/internal/config"
	"tracker-assigner/internal/storage"
	thhttp "tracker-assigner/internal/transport/http"

	"go.uber.org/zap"
)

type mockTrackerAPI struct {
	assignedKey      string
	assignedAssignee string
	assignErr        error
}

func (m *mockTrackerAPI) AssignIssue(ctx context.Context, issueKey, assignee string) error {
	m.assignedKey = issueKey
	m.assignedAssignee = assignee
	return m.assignErr
}
func (m *mockTrackerAPI) ClearAssignee(ctx context.Context, issueKey string) error {
	return nil
}
func (m *mockTrackerAPI) TransitionIssue(ctx context.Context, issueKey, transitionID string) error {
	return nil
}
func (m *mockTrackerAPI) GetTransitions(ctx context.Context, issueKey string) ([]TrackerTransition, error) {
	return nil, nil
}
func (m *mockTrackerAPI) GetIssue(ctx context.Context, issueKey string) (*thhttp.TrackerIssue, error) {
	return nil, nil
}
func (m *mockTrackerAPI) GetUser(ctx context.Context, user string) (*TrackerUser, error) {
	return &TrackerUser{Login: user, Dismissed: false}, nil
}
func (m *mockTrackerAPI) SearchIssues(ctx context.Context, tql string, page, perPage int) ([]thhttp.TrackerIssue, error) {
	return nil, nil
}
func (m *mockTrackerAPI) AddComment(ctx context.Context, issueKey, comment string) error {
	return nil
}

func TestAssignerService_AssignSuccess(t *testing.T) {
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
				Strategy:       config.StrategyRoundRobin,
				MaxLoadPerUser: 5,
				Schedule: config.ScheduleConfig{
					Timezone:  "UTC",
					WorkDays:  []int{1, 2, 3, 4, 5, 6, 7},
					WorkHours: config.WorkHours{Start: "00:00", End: "23:59"},
				},
				Assignees: []config.AssigneeConfig{
					{Login: "alice"},
				},
			},
		},
	}

	router := NewRouter(cfg)
	balancer := NewBalancer(repo)
	tracker := &mockTrackerAPI{}
	service := NewAssignerService(router, balancer, tracker, repo, logger, nil, nil)

	payload := &thhttp.WebhookPayload{
		Issue: &thhttp.TrackerIssue{
			Key:   "HELP-10",
			Queue: &thhttp.IssueReference{Key: "HELP"},
		},
	}

	err := service.ProcessWebhook(context.Background(), payload, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tracker.assignedKey != "HELP-10" || tracker.assignedAssignee != "alice" {
		t.Errorf("expected assignment of HELP-10 to alice, got %s -> %s", tracker.assignedKey, tracker.assignedAssignee)
	}

	load, _ := repo.GetAssigneeLoad(context.Background(), "support", "alice")
	if load != 1 {
		t.Errorf("expected load 1 after assignment, got %d", load)
	}
}

func TestAssignerService_PendingQueue_WhenBusy(t *testing.T) {
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
				Strategy:       config.StrategyRoundRobin,
				MaxLoadPerUser: 1, // capacity 1
				Schedule: config.ScheduleConfig{
					Timezone:  "UTC",
					WorkDays:  []int{1, 2, 3, 4, 5, 6, 7},
					WorkHours: config.WorkHours{Start: "00:00", End: "23:59"},
				},
				Assignees: []config.AssigneeConfig{
					{Login: "alice"},
				},
			},
		},
	}

	// Pre-fill Alice's load to 1 (at capacity)
	_ = repo.IncrementLoad(context.Background(), "support", "alice")

	router := NewRouter(cfg)
	balancer := NewBalancer(repo)
	tracker := &mockTrackerAPI{}
	service := NewAssignerService(router, balancer, tracker, repo, logger, nil, nil)

	payload := &thhttp.WebhookPayload{
		Issue: &thhttp.TrackerIssue{
			Key:   "HELP-99",
			Queue: &thhttp.IssueReference{Key: "HELP"},
		},
	}

	rawBody := []byte(`{"issue":{"key":"HELP-99"}}`)
	err := service.ProcessWebhook(context.Background(), payload, rawBody)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should NOT call Tracker
	if tracker.assignedKey != "" {
		t.Errorf("tracker was called, but should have been skipped")
	}

	// Must be in Pending Queue
	count, err := repo.GetPendingCount(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("expected 1 item in pending queue, got %d (err: %v)", count, err)
	}

	item, err := repo.GetNextPending(context.Background(), "support")
	if err != nil || item == nil {
		t.Fatalf("expected pending item, got nil")
	}
	if item.IssueKey != "HELP-99" {
		t.Errorf("expected issue key HELP-99 in pending queue, got %s", item.IssueKey)
	}
}

func TestAssignerService_DLQ_On5xxError(t *testing.T) {
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
				Strategy:       config.StrategyRoundRobin,
				MaxLoadPerUser: 5,
				Schedule: config.ScheduleConfig{
					Timezone:  "UTC",
					WorkDays:  []int{1, 2, 3, 4, 5, 6, 7},
					WorkHours: config.WorkHours{Start: "00:00", End: "23:59"},
				},
				Assignees: []config.AssigneeConfig{
					{Login: "alice"},
				},
			},
		},
	}

	router := NewRouter(cfg)
	balancer := NewBalancer(repo)
	tracker := &mockTrackerAPI{
		assignErr: &APIError{StatusCode: 502, Message: "Bad Gateway"},
	}
	service := NewAssignerService(router, balancer, tracker, repo, logger, nil, nil)

	payload := &thhttp.WebhookPayload{
		Issue: &thhttp.TrackerIssue{
			Key:   "HELP-500",
			Queue: &thhttp.IssueReference{Key: "HELP"},
		},
	}

	err := service.ProcessWebhook(context.Background(), payload, []byte(`{}`))
	if err != nil {
		t.Fatalf("expected nil error (handled via DLQ), got: %v", err)
	}

	// Must be in DLQ
	count, err := repo.GetDLQCount(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("expected 1 item in DLQ, got %d", count)
	}

	items, _ := repo.GetDLQItems(context.Background(), 10, 0)
	if len(items) != 1 || items[0].IssueKey != "HELP-500" {
		t.Errorf("expected DLQ item for HELP-500, got %v", items)
	}
}

func TestAssignerService_CloseTicket_DecrementsLoad(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	_ = repo.IncrementLoad(context.Background(), "support", "alice")
	_ = repo.IncrementLoad(context.Background(), "support", "alice")

	tracker := &mockTrackerAPI{}
	service := NewAssignerService(nil, nil, tracker, repo, logger, nil, nil)

	payload := &thhttp.WebhookPayload{
		Issue: &thhttp.TrackerIssue{
			Key:      "HELP-10",
			Status:   &thhttp.IssueReference{Key: "closed"},
			Assignee: &thhttp.IssueReference{Login: "alice"},
		},
	}

	err := service.ProcessWebhook(context.Background(), payload, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	load, _ := repo.GetAssigneeLoad(context.Background(), "support", "alice")
	if load != 1 {
		t.Errorf("expected load to decrement to 1, got %d", load)
	}
}

func TestAssignerService_EnqueuePending_SavesMLContext(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		RoutingRules: []config.RoutingRule{
			{Queue: "SEC", TargetGroup: "security_team"},
		},
		Groups: map[string]config.GroupConfig{
			"security_team": {
				MaxLoadPerUser: 1,
				Schedule: config.ScheduleConfig{
					Timezone: "UTC",
					WorkDays: []int{}, // off-shift -> ErrNoAvailableAssignee
				},
				Assignees: []config.AssigneeConfig{
					{Login: "alice"},
				},
			},
		},
	}
	router := NewRouter(cfg)
	tracker := &mockTrackerAPI{}
	balancer := NewBalancer(repo)
	service := NewAssignerService(router, balancer, tracker, repo, logger, cfg, nil)

	ctx := WithComplexity(WithSkills(context.Background(), []string{"security", "backend"}), 5)
	rawBody := []byte(`{"issue":{"key":"SEC-100","queue":{"key":"SEC"}}}`)
	payload := &thhttp.WebhookPayload{
		Issue: &thhttp.TrackerIssue{
			Key:   "SEC-100",
			Queue: &thhttp.IssueReference{Key: "SEC"},
		},
	}

	err := service.ProcessWebhook(ctx, payload, rawBody)
	if err != nil {
		t.Fatalf("expected nil error on enqueue to pending queue, got: %v", err)
	}

	item, err := repo.GetNextPending(context.Background(), "security_team")
	if err != nil || item == nil {
		t.Fatalf("expected pending item in repository, got item=%v, err=%v", item, err)
	}

	var payloadMap map[string]any
	if err := json.Unmarshal([]byte(item.Payload), &payloadMap); err != nil {
		t.Fatalf("failed to decode pending item payload: %v", err)
	}

	rawSkills, ok := payloadMap["_ml_skills"].([]any)
	if !ok || len(rawSkills) != 2 || rawSkills[0] != "security" || rawSkills[1] != "backend" {
		t.Errorf("expected _ml_skills in pending item payload, got: %v", payloadMap["_ml_skills"])
	}

	rawComplexity, ok := payloadMap["_ml_complexity"].(float64)
	if !ok || int(rawComplexity) != 5 {
		t.Errorf("expected _ml_complexity = 5 in pending item payload, got: %v", payloadMap["_ml_complexity"])
	}
}

