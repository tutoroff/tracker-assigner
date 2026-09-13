package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"tracker-assigner/internal/config"
	"tracker-assigner/internal/service"
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
func (m *mockTrackerAPI) GetTransitions(ctx context.Context, issueKey string) ([]service.TrackerTransition, error) {
	return nil, nil
}
func (m *mockTrackerAPI) GetIssue(ctx context.Context, issueKey string) (*thhttp.TrackerIssue, error) {
	return nil, nil
}
func (m *mockTrackerAPI) GetUser(ctx context.Context, user string) (*service.TrackerUser, error) {
	return &service.TrackerUser{Login: user, Dismissed: false}, nil
}
func (m *mockTrackerAPI) SearchIssues(ctx context.Context, tql string, page, perPage int) ([]thhttp.TrackerIssue, error) {
	return nil, nil
}
func (m *mockTrackerAPI) AddComment(ctx context.Context, issueKey, comment string) error {
	return nil
}

func TestPendingProcessor_ProcessOnce(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		Groups: map[string]config.GroupConfig{
			"support": {
				Strategy:       config.StrategyRoundRobin,
				MaxLoadPerUser: 1, // max load 1
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

	router := service.NewRouter(cfg)
	balancer := service.NewBalancer(repo)
	tracker := &mockTrackerAPI{}

	// Initially Alice is at max capacity (1)
	_ = repo.IncrementLoad(context.Background(), "support", "alice")

	// Enqueue pending ticket
	_ = repo.EnqueuePending(context.Background(), storage.PendingItem{
		IssueKey: "DEV-101",
		QueueKey: "DEV",
		GroupID:  "support",
		Payload:  `{}`,
	})

	processor := NewPendingProcessor(repo, router, balancer, tracker, 1*time.Second, logger)

	// First pass: Alice is still busy -> 0 assigned
	assigned, err := processor.ProcessOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if assigned != 0 {
		t.Errorf("expected 0 assigned, got %d", assigned)
	}

	// Now Alice finishes a ticket -> decrement load to 0
	_ = repo.DecrementLoad(context.Background(), "support", "alice")

	// Second pass: Alice is now available -> DEV-101 should be assigned!
	assigned, err = processor.ProcessOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if assigned != 1 {
		t.Errorf("expected 1 assigned, got %d", assigned)
	}

	if tracker.assignedKey != "DEV-101" || tracker.assignedAssignee != "alice" {
		t.Errorf("expected DEV-101 assigned to alice, got %s to %s", tracker.assignedKey, tracker.assignedAssignee)
	}

	// Pending queue should now be empty
	count, _ := repo.GetPendingCount(context.Background())
	if count != 0 {
		t.Errorf("expected pending queue empty, got %d", count)
	}

	// Alice load should now be 1 again
	load, _ := repo.GetAssigneeLoad(context.Background(), "support", "alice")
	if load != 1 {
		t.Errorf("expected load 1 after assigning pending ticket, got %d", load)
	}
}

type mockNotifier struct {
	notifiedCount int
	lastTitle     string
	lastMessage   string
}

func (m *mockNotifier) Notify(ctx context.Context, title, message string) error {
	m.notifiedCount++
	m.lastTitle = title
	m.lastMessage = message
	return nil
}

func (m *mockNotifier) NotifyToChat(ctx context.Context, chatID, title, message string) error {
	return m.Notify(ctx, title, message)
}

func TestPendingProcessor_SLABreachAlert(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		Groups: map[string]config.GroupConfig{
			"support": {
				Strategy:       config.StrategyLoadBased,
				MaxLoadPerUser: 1,
				Assignees: []config.AssigneeConfig{
					{Login: "alice"},
				},
			},
		},
	}
	router := service.NewRouter(cfg)
	balancer := service.NewBalancer(repo)
	tracker := &mockTrackerAPI{}

	// Fill load so Alice cannot be assigned
	_ = repo.SetAssigneeLoad(context.Background(), "support", "alice", 1)

	// Enqueue pending item created 3 hours ago
	pastTime := time.Now().Add(-3 * time.Hour)
	item := storage.PendingItem{
		IssueKey:  "SLA-1",
		GroupID:   "support",
		Payload:   `{}`,
		CreatedAt: pastTime,
	}
	_ = repo.EnqueuePending(context.Background(), item)

	notifierMock := &mockNotifier{}
	processor := NewPendingProcessor(repo, router, balancer, tracker, 1*time.Second, logger)
	processor.SetSLATimeout(2 * time.Hour)
	processor.SetNotifier(notifierMock)

	assigned, err := processor.ProcessOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if assigned != 0 {
		t.Errorf("expected 0 assigned, got %d", assigned)
	}

	if notifierMock.notifiedCount != 1 {
		t.Errorf("expected 1 notification for SLA breach, got %d", notifierMock.notifiedCount)
	}

	// Calling ProcessOnce again immediately should be throttled (not spamming notifier)
	_, _ = processor.ProcessOnce(context.Background())
	if notifierMock.notifiedCount != 1 {
		t.Errorf("expected still 1 notification due to throttling, got %d", notifierMock.notifiedCount)
	}
}

func TestPendingProcessor_PostAssignHook(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
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
					{Login: "bob"},
				},
			},
		},
	}

	router := service.NewRouter(cfg)
	balancer := service.NewBalancer(repo)
	tracker := &mockTrackerAPI{}

	_ = repo.EnqueuePending(context.Background(), storage.PendingItem{
		IssueKey: "DEV-200",
		QueueKey: "DEV",
		GroupID:  "support",
		Payload:  `{"issue":{"key":"DEV-200"}}`,
	})

	processor := NewPendingProcessor(repo, router, balancer, tracker, 1*time.Second, logger)

	var hookCalled bool
	var hookKey, hookLogin string
	processor.SetPostAssignHook(func(ctx context.Context, issueKey, candidateLogin string, payload *thhttp.WebhookPayload) {
		hookCalled = true
		hookKey = issueKey
		hookLogin = candidateLogin
	})

	assigned, err := processor.ProcessOnce(context.Background())
	if err != nil || assigned != 1 {
		t.Fatalf("expected 1 assigned, got %d, err: %v", assigned, err)
	}

	if !hookCalled {
		t.Errorf("expected PostAssignHook to be called")
	}
	if hookKey != "DEV-200" || hookLogin != "bob" {
		t.Errorf("expected hook called for DEV-200 and bob, got %s and %s", hookKey, hookLogin)
	}
}

func TestPendingProcessor_Lifecycle_StartAndShutdown(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		Groups: map[string]config.GroupConfig{
			"support": {
				Strategy:       config.StrategyRoundRobin,
				MaxLoadPerUser: 5,
				Assignees:      []config.AssigneeConfig{{Login: "alice"}},
			},
		},
	}
	router := service.NewRouter(cfg)
	balancer := service.NewBalancer(repo)
	tracker := &mockTrackerAPI{}

	processor := NewPendingProcessor(repo, router, balancer, tracker, 20*time.Millisecond, logger)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		processor.Start(ctx)
		close(done)
	}()

	// Trigger manual processing
	processor.Trigger()
	processor.Trigger() // test channel full / coalescing

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Clean exit
	case <-time.After(1 * time.Second):
		t.Fatal("PendingProcessor did not stop after context cancel")
	}
}

func TestPendingProcessor_UnknownGroup_DiscardsItem(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		Groups: map[string]config.GroupConfig{},
	}
	router := service.NewRouter(cfg)
	balancer := service.NewBalancer(repo)
	tracker := &mockTrackerAPI{}

	// Enqueue item with unknown group
	_ = repo.EnqueuePending(context.Background(), storage.PendingItem{
		IssueKey: "UNK-1",
		GroupID:  "nonexistent_group",
		Payload:  `{}`,
	})

	processor := NewPendingProcessor(repo, router, balancer, tracker, 1*time.Second, logger)

	assigned, err := processor.ProcessOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if assigned != 0 {
		t.Errorf("expected 0 assigned, got %d", assigned)
	}

	// Should have discarded / marked assigned so it doesn't loop forever
	count, _ := repo.GetPendingCount(context.Background())
	if count != 0 {
		t.Errorf("expected pending queue empty after unknown group item handled, got %d", count)
	}
}

func TestPendingProcessor_Tracker5xx_MovesToDLQ(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		Groups: map[string]config.GroupConfig{
			"support": {
				Strategy:       config.StrategyRoundRobin,
				MaxLoadPerUser: 5,
				Schedule: config.ScheduleConfig{
					Timezone:  "UTC",
					WorkDays:  []int{1, 2, 3, 4, 5, 6, 7},
					WorkHours: config.WorkHours{Start: "00:00", End: "23:59"},
				},
				Assignees: []config.AssigneeConfig{{Login: "alice"}},
			},
		},
	}
	router := service.NewRouter(cfg)
	balancer := service.NewBalancer(repo)
	tracker := &mockTrackerAPI{
		assignErr: &service.APIError{StatusCode: 502, Message: "Bad Gateway"},
	}

	_ = repo.EnqueuePending(context.Background(), storage.PendingItem{
		IssueKey: "DEV-502",
		GroupID:  "support",
		Payload:  `{}`,
	})

	processor := NewPendingProcessor(repo, router, balancer, tracker, 1*time.Second, logger)

	assigned, err := processor.ProcessOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if assigned != 0 {
		t.Errorf("expected 0 assigned on 502, got %d", assigned)
	}

	// Pending queue should be empty (ticket removed from pending)
	pendingCount, _ := repo.GetPendingCount(context.Background())
	if pendingCount != 0 {
		t.Errorf("expected pending queue empty, got %d", pendingCount)
	}

	// Dead letter queue should have 1 item
	dlqCount, _ := repo.GetDLQCount(context.Background())
	if dlqCount != 1 {
		t.Fatalf("expected 1 item in DLQ, got %d", dlqCount)
	}

	dlqItems, _ := repo.GetDLQItems(context.Background(), 10, 0)
	if len(dlqItems) != 1 || dlqItems[0].IssueKey != "DEV-502" || dlqItems[0].Action != "assign" {
		t.Errorf("unexpected DLQ item: %+v", dlqItems)
	}
}

func TestPendingProcessor_Tracker404_DiscardsTicket(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		Groups: map[string]config.GroupConfig{
			"support": {
				Strategy:       config.StrategyRoundRobin,
				MaxLoadPerUser: 5,
				Schedule: config.ScheduleConfig{
					Timezone:  "UTC",
					WorkDays:  []int{1, 2, 3, 4, 5, 6, 7},
					WorkHours: config.WorkHours{Start: "00:00", End: "23:59"},
				},
				Assignees: []config.AssigneeConfig{{Login: "alice"}},
			},
		},
	}
	router := service.NewRouter(cfg)
	balancer := service.NewBalancer(repo)
	tracker := &mockTrackerAPI{
		assignErr: errors.New("404 Not Found: Issue was deleted"),
	}

	_ = repo.EnqueuePending(context.Background(), storage.PendingItem{
		IssueKey: "DEV-404",
		GroupID:  "support",
		Payload:  `{}`,
	})

	processor := NewPendingProcessor(repo, router, balancer, tracker, 1*time.Second, logger)

	assigned, err := processor.ProcessOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if assigned != 0 {
		t.Errorf("expected 0 assigned on 404, got %d", assigned)
	}

	pendingCount, _ := repo.GetPendingCount(context.Background())
	if pendingCount != 0 {
		t.Errorf("expected pending queue empty after 404 discard, got %d", pendingCount)
	}

	dlqCount, _ := repo.GetDLQCount(context.Background())
	if dlqCount != 0 {
		t.Errorf("expected 0 DLQ items on 404, got %d", dlqCount)
	}
}

func TestPendingProcessor_ContextCanceled(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	router := service.NewRouter(&config.Config{})
	balancer := service.NewBalancer(repo)
	tracker := &mockTrackerAPI{}

	processor := NewPendingProcessor(repo, router, balancer, tracker, 1*time.Second, logger)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := processor.ProcessOnce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}

func TestPendingProcessor_SetBalancer(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	router := service.NewRouter(&config.Config{})
	b1 := service.NewBalancer(repo)
	b2 := service.NewBalancer(repo)

	processor := NewPendingProcessor(repo, router, b1, &mockTrackerAPI{}, 0, logger)
	processor.SetBalancer(b2)
	// Verified without panic
}

func TestPendingProcessor_Tracker400_MovesToDLQ(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		Groups: map[string]config.GroupConfig{
			"support": {
				Strategy:       config.StrategyRoundRobin,
				MaxLoadPerUser: 5,
				Schedule: config.ScheduleConfig{
					Timezone:  "UTC",
					WorkDays:  []int{1, 2, 3, 4, 5, 6, 7},
					WorkHours: config.WorkHours{Start: "00:00", End: "23:59"},
				},
				Assignees: []config.AssigneeConfig{{Login: "alice"}},
			},
		},
	}
	router := service.NewRouter(cfg)
	balancer := service.NewBalancer(repo)
	tracker := &mockTrackerAPI{
		assignErr: &service.APIError{StatusCode: 400, Message: "Bad Request: user has no license in tracker"},
	}

	_ = repo.EnqueuePending(context.Background(), storage.PendingItem{
		IssueKey: "DEV-400",
		GroupID:  "support",
		Payload:  `{}`,
	})

	processor := NewPendingProcessor(repo, router, balancer, tracker, 1*time.Second, logger)

	// Single pass should finish promptly (no infinite loop) and move ticket to DLQ
	assigned, err := processor.ProcessOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if assigned != 0 {
		t.Errorf("expected 0 assigned on 400, got %d", assigned)
	}

	// Pending queue should be unblocked and empty
	pendingCount, _ := repo.GetPendingCount(context.Background())
	if pendingCount != 0 {
		t.Errorf("expected pending queue empty after 400 escalation, got %d", pendingCount)
	}

	// DLQ should have 1 item
	dlqCount, _ := repo.GetDLQCount(context.Background())
	if dlqCount != 1 {
		t.Fatalf("expected 1 item in DLQ, got %d", dlqCount)
	}
}

func TestPendingProcessor_Tracker429_PausesPass(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		Groups: map[string]config.GroupConfig{
			"support": {
				Strategy:       config.StrategyRoundRobin,
				MaxLoadPerUser: 5,
				Schedule: config.ScheduleConfig{
					Timezone:  "UTC",
					WorkDays:  []int{1, 2, 3, 4, 5, 6, 7},
					WorkHours: config.WorkHours{Start: "00:00", End: "23:59"},
				},
				Assignees: []config.AssigneeConfig{{Login: "alice"}},
			},
		},
	}
	router := service.NewRouter(cfg)
	balancer := service.NewBalancer(repo)
	tracker := &mockTrackerAPI{
		assignErr: &service.APIError{StatusCode: 429, Message: "Too Many Requests", RetryAfter: 5 * time.Second},
	}

	_ = repo.EnqueuePending(context.Background(), storage.PendingItem{
		IssueKey: "DEV-429",
		GroupID:  "support",
		Payload:  `{}`,
	})

	processor := NewPendingProcessor(repo, router, balancer, tracker, 1*time.Second, logger)

	assigned, err := processor.ProcessOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if assigned != 0 {
		t.Errorf("expected 0 assigned on 429, got %d", assigned)
	}

	// On 429, ticket remains in pending queue to be retried on next tick
	pendingCount, _ := repo.GetPendingCount(context.Background())
	if pendingCount != 1 {
		t.Errorf("expected pending count 1 on 429 rate limit, got %d", pendingCount)
	}

	// Should NOT be moved to DLQ on rate limit
	dlqCount, _ := repo.GetDLQCount(context.Background())
	if dlqCount != 0 {
		t.Errorf("expected 0 items in DLQ on 429, got %d", dlqCount)
	}
}

type mockContextCapturingBalancer struct {
	capturedCtx context.Context
}

func (m *mockContextCapturingBalancer) PickAssignee(ctx context.Context, groupID string, group config.GroupConfig, now time.Time) (*config.AssigneeConfig, error) {
	m.capturedCtx = ctx
	return &config.AssigneeConfig{Login: "expert_bob"}, nil
}

func TestPendingProcessor_RestoresMLContext(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		Groups: map[string]config.GroupConfig{
			"support": {
				Assignees: []config.AssigneeConfig{
					{Login: "expert_bob"},
				},
			},
		},
	}
	router := service.NewRouter(cfg)
	capturingBalancer := &mockContextCapturingBalancer{}
	tracker := &mockTrackerAPI{}

	// Enqueue pending item with ML skills and complexity embedded in Payload
	_ = repo.EnqueuePending(context.Background(), storage.PendingItem{
		IssueKey: "TASK-ML-99",
		GroupID:  "support",
		Payload:  `{"_ml_skills":["database","devops"],"_ml_complexity":4,"issue":{"key":"TASK-ML-99"}}`,
	})

	processor := NewPendingProcessor(repo, router, capturingBalancer, tracker, 1*time.Second, logger)

	var hookCtx context.Context
	processor.SetPostAssignHook(func(ctx context.Context, issueKey, candidateLogin string, payload *thhttp.WebhookPayload) {
		hookCtx = ctx
	})

	assigned, err := processor.ProcessOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if assigned != 1 {
		t.Fatalf("expected 1 assigned, got %d", assigned)
	}

	// Verify balancer received restored skills and complexity
	capturedSkills := service.GetSkills(capturingBalancer.capturedCtx)
	if len(capturedSkills) != 2 || capturedSkills[0] != "database" || capturedSkills[1] != "devops" {
		t.Errorf("expected balancer to receive restored skills ['database', 'devops'], got %v", capturedSkills)
	}

	capturedComplexity := service.GetComplexity(capturingBalancer.capturedCtx)
	if capturedComplexity != 4 {
		t.Errorf("expected balancer to receive restored complexity 4, got %d", capturedComplexity)
	}

	// Verify postAssignHook also received restored skills and complexity
	hookSkills := service.GetSkills(hookCtx)
	if len(hookSkills) != 2 || hookSkills[0] != "database" {
		t.Errorf("expected hook to receive restored skills, got %v", hookSkills)
	}
}


