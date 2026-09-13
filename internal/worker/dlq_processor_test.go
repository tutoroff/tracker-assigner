package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"tracker-assigner/internal/storage"

	"go.uber.org/zap"
)

func TestDLQProcessor_Success(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	tracker := &mockTrackerAPI{}

	// Enqueue DLQ item ready for immediate retry
	item := storage.DLQItem{
		IssueKey:    "DEV-500",
		Action:      "assign",
		Payload:     `{"assignee":"bob","group":"support"}`,
		MaxRetries:  3,
		NextRetryAt: time.Now().Add(-1 * time.Minute),
	}
	_ = repo.EnqueueDLQ(context.Background(), item)

	processor := NewDLQProcessor(repo, tracker, 1*time.Second, 1*time.Second, logger)

	resolved, err := processor.ProcessOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resolved != 1 {
		t.Errorf("expected 1 resolved item, got %d", resolved)
	}

	if tracker.assignedKey != "DEV-500" || tracker.assignedAssignee != "bob" {
		t.Errorf("expected DEV-500 assigned to bob, got %s -> %s", tracker.assignedKey, tracker.assignedAssignee)
	}

	// Active DLQ count should now be 0
	count, _ := repo.GetDLQCount(context.Background())
	if count != 0 {
		t.Errorf("expected 0 pending DLQ items, got %d", count)
	}

	// Local load for bob should be incremented to 1
	load, _ := repo.GetAssigneeLoad(context.Background(), "support", "bob")
	if load != 1 {
		t.Errorf("expected load 1 after resolved DLQ assignment, got %d", load)
	}
}

func TestDLQProcessor_FailureBackoff(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	tracker := &mockTrackerAPI{
		assignErr: errors.New("503 Service Unavailable"),
	}

	item := storage.DLQItem{
		IssueKey:    "DEV-503",
		Action:      "assign",
		Payload:     `{"assignee":"bob","group":"support"}`,
		MaxRetries:  3,
		NextRetryAt: time.Now().Add(-1 * time.Minute),
	}
	_ = repo.EnqueueDLQ(context.Background(), item)

	processor := NewDLQProcessor(repo, tracker, 1*time.Second, 10*time.Second, logger)

	resolved, err := processor.ProcessOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resolved != 0 {
		t.Errorf("expected 0 resolved items on failure, got %d", resolved)
	}

	// Item should still be pending, but with retry_count = 1 and next_retry_at in future
	items, err := repo.GetDLQItemsForRetry(context.Background(), 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 0 {
		t.Errorf("expected item to be delayed in future, but was returned for retry immediately")
	}

	all, _ := repo.GetDLQItems(context.Background(), 1, 0)
	if len(all) == 1 {
		if all[0].RetryCount != 1 {
			t.Errorf("expected retry_count 1, got %d", all[0].RetryCount)
		}
		if all[0].ErrorMessage != "503 Service Unavailable" {
			t.Errorf("expected recorded error message, got %s", all[0].ErrorMessage)
		}
	}
}

func TestDLQProcessor_FatalErrorNotification(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	tracker := &mockTrackerAPI{
		assignErr: errors.New("403 Forbidden: User not permitted"),
	}

	// MaxRetries = 1, current retry = 0. Failure will bring retry to 1 >= MaxRetries -> failed
	item := storage.DLQItem{
		IssueKey:    "DEV-FAIL",
		Action:      "assign",
		Payload:     `{"assignee":"bob","group":"support"}`,
		MaxRetries:  1,
		NextRetryAt: time.Now().Add(-1 * time.Minute),
	}
	_ = repo.EnqueueDLQ(context.Background(), item)

	notifierMock := &mockNotifier{}
	processor := NewDLQProcessor(repo, tracker, 1*time.Second, 10*time.Second, logger)
	processor.SetNotifier(notifierMock)

	resolved, err := processor.ProcessOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resolved != 0 {
		t.Errorf("expected 0 resolved, got %d", resolved)
	}

	if notifierMock.notifiedCount != 1 {
		t.Errorf("expected 1 notification on fatal DLQ exhaustion, got %d", notifierMock.notifiedCount)
	}
}

func TestDLQProcessor_Lifecycle_StartAndShutdown(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	processor := NewDLQProcessor(repo, &mockTrackerAPI{}, 20*time.Millisecond, 1*time.Second, logger)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		processor.Start(ctx)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Clean exit
	case <-time.After(1 * time.Second):
		t.Fatal("DLQProcessor did not stop after context cancel")
	}
}

func TestDLQProcessor_UnsupportedAction(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	item := storage.DLQItem{
		IssueKey:    "DEV-UNKNOWN",
		Action:      "delete_ticket",
		Payload:     `{}`,
		MaxRetries:  3,
		NextRetryAt: time.Now().Add(-1 * time.Minute),
	}
	_ = repo.EnqueueDLQ(context.Background(), item)

	processor := NewDLQProcessor(repo, &mockTrackerAPI{}, 1*time.Second, 1*time.Second, logger)

	resolved, err := processor.ProcessOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resolved != 0 {
		t.Errorf("expected 0 resolved for unsupported action, got %d", resolved)
	}

	all, _ := repo.GetDLQItems(context.Background(), 10, 0)
	if len(all) != 1 || all[0].ErrorMessage != "unsupported DLQ action: delete_ticket" {
		t.Errorf("expected recorded error for unsupported action, got %+v", all)
	}
}

func TestDLQProcessor_CorruptedPayload(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	// 1. Invalid JSON
	item1 := storage.DLQItem{
		IssueKey:    "DEV-CORRUPT",
		Action:      "assign",
		Payload:     `{invalid json`,
		MaxRetries:  3,
		NextRetryAt: time.Now().Add(-1 * time.Minute),
	}
	_ = repo.EnqueueDLQ(context.Background(), item1)

	// 2. Missing assignee
	item2 := storage.DLQItem{
		IssueKey:    "DEV-NO-ASSIGNEE",
		Action:      "assign",
		Payload:     `{"assignee":"","group":"support"}`,
		MaxRetries:  3,
		NextRetryAt: time.Now().Add(-1 * time.Minute),
	}
	_ = repo.EnqueueDLQ(context.Background(), item2)

	processor := NewDLQProcessor(repo, &mockTrackerAPI{}, 1*time.Second, 1*time.Second, logger)

	resolved, err := processor.ProcessOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resolved != 0 {
		t.Errorf("expected 0 resolved, got %d", resolved)
	}
}

func TestDLQProcessor_ProcessAll_And_Cleanup(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	tracker := &mockTrackerAPI{}
	processor := NewDLQProcessor(repo, tracker, 1*time.Second, 1*time.Second, logger)

	// Insert item and process it successfully
	item := storage.DLQItem{
		IssueKey:    "DEV-RESOLVE",
		Action:      "assign",
		Payload:     `{"assignee":"alice","group":"support"}`,
		MaxRetries:  3,
		NextRetryAt: time.Now().Add(-1 * time.Minute),
	}
	_ = repo.EnqueueDLQ(context.Background(), item)

	// Manually call processAll
	processor.processAll(context.Background())

	count, _ := repo.GetDLQCount(context.Background())
	if count != 0 {
		t.Errorf("expected 0 active DLQ items after processAll, got %d", count)
	}
}

func TestDLQProcessor_ContextCanceled(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	processor := NewDLQProcessor(repo, &mockTrackerAPI{}, 1*time.Second, 1*time.Second, logger)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := processor.ProcessOnce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}

func TestDLQProcessor_Defaults(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	p := NewDLQProcessor(repo, &mockTrackerAPI{}, 0, 0, logger)
	if p.interval != 10*time.Second {
		t.Errorf("expected default interval 10s, got %v", p.interval)
	}
	if p.baseBackoff != 30*time.Second {
		t.Errorf("expected default baseBackoff 30s, got %v", p.baseBackoff)
	}
}

