package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"tracker-assigner/internal/config"
	"tracker-assigner/internal/storage"
	thhttp "tracker-assigner/internal/transport/http"

	"go.uber.org/zap"
)

// mockSyncTrigger tracks calls to TriggerSync
type mockSyncTrigger struct {
	triggered int32
}

func (m *mockSyncTrigger) TriggerSync() {
	atomic.AddInt32(&m.triggered, 1)
}

// TestTASK22_WebhookStormAndRobotLoopProtection tests:
// 1) Robot self-events are ignored to prevent loops.
// 2) Rapid duplicate webhooks for the same issue within dedup window are dropped.
func TestTASK22_WebhookStormAndRobotLoopProtection(t *testing.T) {
	logger := zap.NewNop()
	db, err := storage.NewSQLite("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		Tracker: config.TrackerConfig{
			RobotLogin:    "tut0roff",
			RobotUID:      "8000000000000002",
			RobotCloudUID: "ajefr4bv6e90bheosaqr",
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
					{Login: "tut0roff", UID: "8000000000000002", CloudUID: "ajefr4bv6e90bheosaqr"},
				},
			},
		},
	}

	router := NewRouter(cfg)
	balancer := NewBalancer(repo)

	var assignCallCount int32
	tracker := &trackerClientMockRecorder{
		onAssign: func(key, assignee string) error {
			atomic.AddInt32(&assignCallCount, 1)
			return nil
		},
	}

	service := NewAssignerService(router, balancer, tracker, repo, logger, cfg, nil)

	// Sub-test 1: Robot triggered webhook is ignored
	robotPayload := &thhttp.WebhookPayload{
		Event: "issue_updated",
		Issue: &thhttp.TrackerIssue{
			Key:   "MARKETING-15",
			Queue: &thhttp.IssueReference{Key: "MARKETING"},
			UpdatedBy: &thhttp.IssueReference{
				ID:       "8000000000000002",
				CloudUID: "ajefr4bv6e90bheosaqr",
				Login:    "tut0roff",
			},
		},
	}
	err = service.ProcessWebhook(context.Background(), robotPayload, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if atomic.LoadInt32(&assignCallCount) != 0 {
		t.Errorf("expected 0 assignments for robot webhook, got %d", atomic.LoadInt32(&assignCallCount))
	}

	// Sub-test 2: Webhook storm: 5 rapid webhooks for the same issue
	normalPayload := &thhttp.WebhookPayload{
		Event: "issue_created",
		Issue: &thhttp.TrackerIssue{
			Key:   "MARKETING-15",
			Queue: &thhttp.IssueReference{Key: "MARKETING"},
			CreatedBy: &thhttp.IssueReference{
				Login: "client_user",
			},
		},
	}

	for i := 0; i < 5; i++ {
		_ = service.ProcessWebhook(context.Background(), normalPayload, []byte(`{}`))
	}

	// Exactly 1 assignment should have occurred!
	if count := atomic.LoadInt32(&assignCallCount); count != 1 {
		t.Errorf("expected exactly 1 assignment during webhook storm, got %d", count)
	}

	// Load should be exactly 1, NOT 5 or 11
	load, _ := repo.GetAssigneeLoad(context.Background(), "marketing_team", "tut0roff")
	if load != 1 {
		t.Errorf("expected assignee load to be 1, got %d", load)
	}
}

// TestTASK23_PhantomWIPLimitProtection tests:
// 1) Repeated assignment calls never cause load counter to exceed real active tickets.
// 2) When all candidates are busy, StateSync is triggered to verify load.
func TestTASK23_PhantomWIPLimitProtection(t *testing.T) {
	logger := zap.NewNop()
	db, err := storage.NewSQLite("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		RoutingRules: []config.RoutingRule{
			{ID: "r1", Queue: "MARKETING", TargetGroup: "marketing_team"},
		},
		Groups: map[string]config.GroupConfig{
			"marketing_team": {
				Strategy:       config.StrategyRoundRobin,
				MaxLoadPerUser: 1, // limit is 1
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
	tracker := &trackerClientMockRecorder{}
	service := NewAssignerService(router, balancer, tracker, repo, logger, cfg, nil)
	syncTrigger := &mockSyncTrigger{}
	service.SetStateSyncWorker(syncTrigger)

	// Assign first ticket MARKETING-1
	p1 := &thhttp.WebhookPayload{
		Issue: &thhttp.TrackerIssue{
			Key:   "MARKETING-1",
			Queue: &thhttp.IssueReference{Key: "MARKETING"},
		},
	}
	_ = service.ProcessWebhook(context.Background(), p1, []byte(`{}`))

	load, _ := repo.GetAssigneeLoad(context.Background(), "marketing_team", "tut0roff")
	if load != 1 {
		t.Fatalf("expected load 1, got %d", load)
	}

	// Try assigning second ticket MARKETING-2: tut0roff is at capacity (1/1)
	p2 := &thhttp.WebhookPayload{
		Issue: &thhttp.TrackerIssue{
			Key:   "MARKETING-2",
			Queue: &thhttp.IssueReference{Key: "MARKETING"},
		},
	}
	_ = service.ProcessWebhook(context.Background(), p2, []byte(`{}`))

	// Should trigger StateSyncWorker to verify if load is real
	if atomic.LoadInt32(&syncTrigger.triggered) == 0 {
		t.Errorf("expected StateSyncWorker to be triggered on overload, got 0")
	}

	// MARKETING-2 should be in pending queue
	count, _ := repo.GetPendingCount(context.Background())
	if count != 1 {
		t.Errorf("expected 1 pending item, got %d", count)
	}
}

// TestTASK24_CyrillicUTF8Encoding tests:
// 1) TrackerClient sets Content-Type: application/json; charset=utf-8
// 2) Cyrillic comment text is passed and received with correct UTF-8 encoding.
func TestTASK24_CyrillicUTF8Encoding(t *testing.T) {
	var receivedContentType string
	var receivedCommentText string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedContentType = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)

		var m map[string]string
		_ = json.Unmarshal(body, &m)
		receivedCommentText = m["text"]

		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"123"}`))
	}))
	defer ts.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL: ts.URL,
		Token:   "dummy",
	}, ts.Client())

	cyrillicComment := "Задача взята в работу пользователем @tut0roff"
	err := client.AddComment(context.Background(), "MARKETING-15", cyrillicComment)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify Content-Type with charset=utf-8
	if !strings.Contains(receivedContentType, "application/json") || !strings.Contains(receivedContentType, "charset=utf-8") {
		t.Errorf("expected Content-Type to contain 'application/json' and 'charset=utf-8', got %q", receivedContentType)
	}

	// Verify Cyrillic text was received uncorrupted (no \ufffd)
	if receivedCommentText != cyrillicComment {
		t.Errorf("expected comment text %q, got %q", cyrillicComment, receivedCommentText)
	}
	if strings.Contains(receivedCommentText, "\ufffd") {
		t.Errorf("comment text contains Unicode replacement character \\ufffd")
	}
}

// TestTASK25_CloudOrgUserIdentification tests matching and canonical resolution
// for Yandex Cloud Organization (cloudUid and numeric id).
func TestTASK25_CloudOrgUserIdentification(t *testing.T) {
	logger := zap.NewNop()
	db, err := storage.NewSQLite("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		RoutingRules: []config.RoutingRule{
			{ID: "r1", Queue: "MARKETING", TargetGroup: "marketing_team"},
		},
		Groups: map[string]config.GroupConfig{
			"marketing_team": {
				Strategy:       config.StrategyRoundRobin,
				MaxLoadPerUser: 5,
				Schedule: config.ScheduleConfig{
					Timezone:  "UTC",
					WorkDays:  []int{1, 2, 3, 4, 5, 6, 7},
					WorkHours: config.WorkHours{Start: "00:00", End: "23:59"},
				},
				Assignees: []config.AssigneeConfig{
					{
						Login:    "tut0roff",
						UID:      "8000000000000002",
						CloudUID: "ajefr4bv6e90bheosaqr",
					},
				},
			},
		},
	}

	// 1. Check AssigneeConfig.Matches
	member := cfg.Groups["marketing_team"].Assignees[0]
	if !member.Matches("tut0roff") {
		t.Errorf("expected match by login")
	}
	if !member.Matches("8000000000000002") {
		t.Errorf("expected match by numeric UID")
	}
	if !member.Matches("ajefr4bv6e90bheosaqr") {
		t.Errorf("expected match by cloudUid")
	}

	// 2. Webhook payload with only cloudUid in assignee should decrement load properly
	router := NewRouter(cfg)
	tracker := &trackerClientMockRecorder{}
	service := NewAssignerService(router, nil, tracker, repo, logger, cfg, nil)

	// Simulate existing load for tut0roff
	_ = repo.SetAssigneeLoad(context.Background(), "marketing_team", "tut0roff", 3)

	// Webhook comes with closed status and only cloudUid in assignee
	payload := &thhttp.WebhookPayload{
		Issue: &thhttp.TrackerIssue{
			Key:    "MARKETING-17",
			Status: &thhttp.IssueReference{Key: "closed"},
			Assignee: &thhttp.IssueReference{
				CloudUID: "ajefr4bv6e90bheosaqr",
				ID:       "8000000000000002",
			},
		},
	}

	err = service.ProcessWebhook(context.Background(), payload, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	load, _ := repo.GetAssigneeLoad(context.Background(), "marketing_team", "tut0roff")
	if load != 2 {
		t.Errorf("expected load to decrement from 3 to 2 using CloudUID, got %d", load)
	}
}

// TestTASK26_AssigneeRemovalOnPauseAndNeedInfo tests that translating to needInfo/paused
// triggers ClearAssignee and decrements load.
func TestTASK26_AssigneeRemovalOnPauseAndNeedInfo(t *testing.T) {
	logger := zap.NewNop()
	db, err := storage.NewSQLite("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()
	repo := storage.NewRepository(db)

	cfg := &config.Config{
		RoutingRules: []config.RoutingRule{
			{ID: "r1", Queue: "MARKETING", TargetGroup: "marketing_team"},
		},
		Groups: map[string]config.GroupConfig{
			"marketing_team": {
				Assignees: []config.AssigneeConfig{
					{Login: "tut0roff"},
				},
			},
		},
	}

	var clearedKey string
	tracker := &trackerClientMockRecorder{
		onClear: func(key string) error {
			clearedKey = key
			return nil
		},
	}

	// No remove_assignee_statuses configured -> fallback to default list containing needInfo and paused
	service := NewAssignerService(nil, nil, tracker, repo, logger, cfg, nil)

	// Pre-fill assignment and load
	_, _ = repo.IncrementLoadForIssue(context.Background(), "marketing_team", "tut0roff", "MARKETING-17")

	load, _ := repo.GetAssigneeLoad(context.Background(), "marketing_team", "tut0roff")
	if load != 1 {
		t.Fatalf("expected load 1, got %d", load)
	}

	// Send webhook transitioning to needInfo
	payload := &thhttp.WebhookPayload{
		Issue: &thhttp.TrackerIssue{
			Key:      "MARKETING-17",
			Status:   &thhttp.IssueReference{Key: "needInfo"},
			Assignee: &thhttp.IssueReference{Login: "tut0roff"},
		},
	}

	err = service.ProcessWebhook(context.Background(), payload, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if clearedKey != "MARKETING-17" {
		t.Errorf("expected ClearAssignee for MARKETING-17, got %s", clearedKey)
	}

	load, _ = repo.GetAssigneeLoad(context.Background(), "marketing_team", "tut0roff")
	if load != 0 {
		t.Errorf("expected load to decrement to 0 upon needInfo, got %d", load)
	}
}

// TestTASK27_TrackerTransitionMappingAndDynamicResolution tests:
// 1) Configured TransitionMap translates in_progress to start_progress.
// 2) Dynamic resolution resolves start_progress via GET /transitions when direct call returns 404.
func TestTASK27_TrackerTransitionMappingAndDynamicResolution(t *testing.T) {
	var executedTransition string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/transitions") && r.Method == http.MethodGet {
			// Returns available transitions
			transitions := []TrackerTransition{
				{
					ID:      "start_progress",
					Display: "В работу",
					To:      &thhttp.IssueReference{Key: "inProgress"},
				},
				{
					ID:      "close",
					Display: "Закрыть",
					To:      &thhttp.IssueReference{Key: "closed"},
				},
			}
			data, _ := json.Marshal(transitions)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
			return
		}

		if strings.Contains(r.URL.Path, "/transitions/in_progress/_execute") {
			// Tracker doesn't have "in_progress" transition -> returns 404
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"transition not found"}`))
			return
		}

		if strings.Contains(r.URL.Path, "/transitions/start_progress/_execute") {
			executedTransition = "start_progress"
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"ok"}`))
			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL: ts.URL,
		Token:   "dummy",
	}, ts.Client())

	// Call TransitionIssue with "in_progress" (not a transition ID, but target status)
	// Client should dynamically query transitions and execute "start_progress"
	err := client.TransitionIssue(context.Background(), "MARKETING-15", "in_progress")
	if err != nil {
		t.Fatalf("expected dynamic resolution to succeed, got %v", err)
	}

	if executedTransition != "start_progress" {
		t.Errorf("expected executed transition 'start_progress', got %q", executedTransition)
	}
}

// Helper mock
type trackerClientMockRecorder struct {
	onAssign func(key, assignee string) error
	onClear  func(key string) error
	assigns  map[string]string
}

func (m *trackerClientMockRecorder) AssignIssue(ctx context.Context, issueKey, assignee string) error {
	if m.assigns == nil {
		m.assigns = make(map[string]string)
	}
	m.assigns[issueKey] = assignee
	if m.onAssign != nil {
		return m.onAssign(issueKey, assignee)
	}
	return nil
}
func (m *trackerClientMockRecorder) ClearAssignee(ctx context.Context, issueKey string) error {
	if m.onClear != nil {
		return m.onClear(issueKey)
	}
	return nil
}
func (m *trackerClientMockRecorder) TransitionIssue(ctx context.Context, issueKey, transitionID string) error {
	return nil
}
func (m *trackerClientMockRecorder) GetTransitions(ctx context.Context, issueKey string) ([]TrackerTransition, error) {
	return nil, nil
}
func (m *trackerClientMockRecorder) GetIssue(ctx context.Context, issueKey string) (*thhttp.TrackerIssue, error) {
	return nil, nil
}
func (m *trackerClientMockRecorder) GetUser(ctx context.Context, user string) (*TrackerUser, error) {
	return &TrackerUser{Login: user}, nil
}
func (m *trackerClientMockRecorder) SearchIssues(ctx context.Context, tql string, page, perPage int) ([]thhttp.TrackerIssue, error) {
	return nil, nil
}
func (m *trackerClientMockRecorder) AddComment(ctx context.Context, issueKey, comment string) error {
	return nil
}
