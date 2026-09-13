package worker

import (
	"context"
	"testing"
	"time"

	"tracker-assigner/internal/config"
	"tracker-assigner/internal/service"
	"tracker-assigner/internal/storage"
	thhttp "tracker-assigner/internal/transport/http"

	"go.uber.org/zap"
)

type searchMockTrackerAPI struct {
	mockTrackerAPI
	searchedQueries []string
	searchResult    map[string][]thhttp.TrackerIssue
}

func (m *searchMockTrackerAPI) SearchIssues(ctx context.Context, tql string, page, perPage int) ([]thhttp.TrackerIssue, error) {
	m.searchedQueries = append(m.searchedQueries, tql)
	if issues, ok := m.searchResult[tql]; ok {
		return issues, nil
	}
	return nil, nil
}

func TestStateSyncWorker_ReconcilesLoad(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		Groups: map[string]config.GroupConfig{
			"support": {
				Assignees: []config.AssigneeConfig{
					{Login: "alice"},
					{Login: "bob"},
				},
			},
		},
	}
	router := service.NewRouter(cfg)

	// Pre-fill local load: Alice has 1, Bob has 5
	_ = repo.SetAssigneeLoad(context.Background(), "support", "alice", 1)
	_ = repo.SetAssigneeLoad(context.Background(), "support", "bob", 5)

	// Tracker reality: Alice actually has 4 issues, Bob actually has 2 (in a single batch query)
	tracker := &searchMockTrackerAPI{
		searchResult: map[string][]thhttp.TrackerIssue{
			`Assignee: "alice", "bob" and Resolution: empty()`: {
				{Key: "DEV-1", Assignee: &thhttp.IssueReference{Login: "alice"}},
				{Key: "DEV-2", Assignee: &thhttp.IssueReference{Login: "alice"}},
				{Key: "DEV-3", Assignee: &thhttp.IssueReference{Login: "alice"}},
				{Key: "DEV-4", Assignee: &thhttp.IssueReference{Login: "alice"}},
				{Key: "DEV-5", Assignee: &thhttp.IssueReference{Login: "bob"}},
				{Key: "DEV-6", Assignee: &thhttp.IssueReference{Login: "bob"}},
			},
		},
	}

	worker := NewStateSyncWorker(repo, router, tracker, 1*time.Minute, logger)

	err := worker.SyncOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify only 1 batch query was executed
	if len(tracker.searchedQueries) != 1 {
		t.Errorf("expected exactly 1 batch query, got %d", len(tracker.searchedQueries))
	}

	// Verify reconciled loads in SQLite
	aliceLoad, _ := repo.GetAssigneeLoad(context.Background(), "support", "alice")
	if aliceLoad != 4 {
		t.Errorf("expected Alice load reconciled to 4, got %d", aliceLoad)
	}

	bobLoad, _ := repo.GetAssigneeLoad(context.Background(), "support", "bob")
	if bobLoad != 2 {
		t.Errorf("expected Bob load reconciled to 2, got %d", bobLoad)
	}
}

func TestStateSyncWorker_StatusFilter(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		Groups: map[string]config.GroupConfig{
			"support": {
				ActiveStatuses:  []string{"open", "inProgress"},
				IgnoredStatuses: []string{"needInfo"},
				Assignees: []config.AssigneeConfig{
					{Login: "alice"},
				},
			},
		},
	}
	router := service.NewRouter(cfg)

	tracker := &searchMockTrackerAPI{
		searchResult: map[string][]thhttp.TrackerIssue{},
	}

	worker := NewStateSyncWorker(repo, router, tracker, 1*time.Minute, logger)
	_ = worker.SyncOnce(context.Background())

	if len(tracker.searchedQueries) != 1 {
		t.Fatalf("expected 1 query, got %d", len(tracker.searchedQueries))
	}
	expectedTQL := `Assignee: "alice" and Resolution: empty() and Status: "open", "inProgress" and Status: !"needInfo"`
	if tracker.searchedQueries[0] != expectedTQL {
		t.Errorf("expected TQL %q, got %q", expectedTQL, tracker.searchedQueries[0])
	}
}

func TestStateSyncWorker_Lifecycle(t *testing.T) {
	logger := zap.NewNop()
	db, _ := storage.NewSQLite("file::memory:?cache=shared")
	defer db.Close()
	repo := storage.NewRepository(db)
	router := service.NewRouter(&config.Config{})
	tracker := &searchMockTrackerAPI{}

	worker := NewStateSyncWorker(repo, router, tracker, 20*time.Millisecond, logger)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		worker.Start(ctx)
		close(done)
	}()

	worker.TriggerSync()
	worker.TriggerSync()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Clean exit
	case <-time.After(1 * time.Second):
		t.Fatal("StateSyncWorker did not stop after cancel")
	}
}

