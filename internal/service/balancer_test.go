package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"tracker-assigner/internal/config"
	"tracker-assigner/internal/storage"
)

// mockRepo implements storage.Repository for balancer tests.
type mockRepo struct {
	loads map[string]storage.AssigneeLoadRecord
}

func (m *mockRepo) IncrementLoad(ctx context.Context, groupID, assigneeID string) error {
	return nil
}
func (m *mockRepo) IncrementLoadForIssue(ctx context.Context, groupID, assigneeID, issueKey string) (bool, error) {
	return true, nil
}
func (m *mockRepo) DecrementLoad(ctx context.Context, groupID, assigneeID string) error {
	return nil
}
func (m *mockRepo) DecrementLoadForIssue(ctx context.Context, issueKey string) (string, error) {
	return "", nil
}
func (m *mockRepo) DecrementAssigneeGlobalLoad(ctx context.Context, assigneeID string) error {
	return nil
}
func (m *mockRepo) SetAssigneeLoad(ctx context.Context, groupID, assigneeID string, load int) error {
	return nil
}
func (m *mockRepo) GetIssueAssignment(ctx context.Context, issueKey string) (*storage.IssueAssignment, error) {
	return nil, nil
}
func (m *mockRepo) GetAssigneesLoad(ctx context.Context, groupID string) (map[string]storage.AssigneeLoadRecord, error) {
	return m.loads, nil
}
func (m *mockRepo) SyncAssigneeIssues(ctx context.Context, groupID, assigneeID string, issueKeys []string) error {
	return nil
}
func (m *mockRepo) GetAssigneeLoad(ctx context.Context, groupID, assigneeID string) (int, error) {
	if rec, ok := m.loads[assigneeID]; ok {
		return rec.CurrentLoad, nil
	}
	return 0, nil
}
func (m *mockRepo) EnqueuePending(ctx context.Context, item storage.PendingItem) error { return nil }
func (m *mockRepo) GetNextPending(ctx context.Context, groupID string) (*storage.PendingItem, error) {
	return nil, nil
}
func (m *mockRepo) MarkPendingAssigned(ctx context.Context, id int64) error { return nil }
func (m *mockRepo) RemovePendingByIssueKey(ctx context.Context, issueKey string) (bool, error) {
	return false, nil
}
func (m *mockRepo) GetPendingCount(ctx context.Context) (int, error) { return 0, nil }
func (m *mockRepo) GetPendingItems(ctx context.Context, limit, offset int) ([]storage.PendingItem, error) {
	return nil, nil
}
func (m *mockRepo) EnqueueDLQ(ctx context.Context, item storage.DLQItem) error { return nil }
func (m *mockRepo) GetDLQItemsForRetry(ctx context.Context, limit int) ([]storage.DLQItem, error) {
	return nil, nil
}
func (m *mockRepo) UpdateDLQRetry(ctx context.Context, id int64, retryCount int, nextRetryAt time.Time, errMsg string) error {
	return nil
}
func (m *mockRepo) MarkDLQResolved(ctx context.Context, id int64) error { return nil }
func (m *mockRepo) GetDLQCount(ctx context.Context) (int, error)        { return 0, nil }
func (m *mockRepo) GetDLQItems(ctx context.Context, limit, offset int) ([]storage.DLQItem, error) {
	return nil, nil
}
func (m *mockRepo) CleanupOldDLQ(ctx context.Context, olderThan time.Duration) (int64, error) {
	return 0, nil
}

func TestBalancer_LoadBased(t *testing.T) {
	now := time.Date(2026, time.September, 7, 12, 0, 0, 0, time.UTC) // Monday 12:00
	group := config.GroupConfig{
		Strategy:       config.StrategyLoadBased,
		MaxLoadPerUser: 5,
		Schedule: config.ScheduleConfig{
			Timezone:  "UTC",
			WorkDays:  []int{1, 2, 3, 4, 5},
			WorkHours: config.WorkHours{Start: "09:00", End: "18:00"},
		},
		Assignees: []config.AssigneeConfig{
			{Login: "alice"},
			{Login: "bob"},
			{Login: "charlie"},
		},
	}

	repo := &mockRepo{
		loads: map[string]storage.AssigneeLoadRecord{
			"alice":   {AssigneeID: "alice", CurrentLoad: 3},
			"bob":     {AssigneeID: "bob", CurrentLoad: 1},
			"charlie": {AssigneeID: "charlie", CurrentLoad: 4},
		},
	}

	balancer := NewBalancer(repo)
	picked, err := balancer.PickAssignee(context.Background(), "group1", group, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if picked.Login != "bob" {
		t.Errorf("expected bob (lowest load 1), got %s", picked.Login)
	}
}

func TestBalancer_RoundRobin(t *testing.T) {
	now := time.Date(2026, time.September, 7, 12, 0, 0, 0, time.UTC) // Monday 12:00
	group := config.GroupConfig{
		Strategy:       config.StrategyRoundRobin,
		MaxLoadPerUser: 5,
		Schedule: config.ScheduleConfig{
			Timezone:  "UTC",
			WorkDays:  []int{1, 2, 3, 4, 5},
			WorkHours: config.WorkHours{Start: "09:00", End: "18:00"},
		},
		Assignees: []config.AssigneeConfig{
			{Login: "alice"},
			{Login: "bob"},
			{Login: "charlie"},
		},
	}

	t1 := now.Add(-10 * time.Minute)
	t2 := now.Add(-30 * time.Minute)

	repo := &mockRepo{
		loads: map[string]storage.AssigneeLoadRecord{
			"alice":   {AssigneeID: "alice", CurrentLoad: 1, LastAssignedAt: &t1},
			"bob":     {AssigneeID: "bob", CurrentLoad: 1, LastAssignedAt: &t2}, // oldest assignment
			"charlie": {AssigneeID: "charlie", CurrentLoad: 1, LastAssignedAt: &now},
		},
	}

	balancer := NewBalancer(repo)
	picked, err := balancer.PickAssignee(context.Background(), "group1", group, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if picked.Login != "bob" {
		t.Errorf("expected bob (oldest assigned), got %s", picked.Login)
	}
}

func TestBalancer_RoundRobin_NeverAssignedTakesPriority(t *testing.T) {
	now := time.Date(2026, time.September, 7, 12, 0, 0, 0, time.UTC)
	group := config.GroupConfig{
		Strategy:       config.StrategyRoundRobin,
		MaxLoadPerUser: 5,
		Schedule: config.ScheduleConfig{
			Timezone:  "UTC",
			WorkDays:  []int{1, 2, 3, 4, 5},
			WorkHours: config.WorkHours{Start: "00:00", End: "23:59"},
		},
		Assignees: []config.AssigneeConfig{
			{Login: "alice"},
			{Login: "bob"},
		},
	}

	assignedTime := now.Add(-1 * time.Hour)
	repo := &mockRepo{
		loads: map[string]storage.AssigneeLoadRecord{
			"alice": {AssigneeID: "alice", CurrentLoad: 0, LastAssignedAt: &assignedTime},
			"bob":   {AssigneeID: "bob", CurrentLoad: 0, LastAssignedAt: nil}, // never assigned
		},
	}

	balancer := NewBalancer(repo)
	picked, err := balancer.PickAssignee(context.Background(), "group1", group, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if picked.Login != "bob" {
		t.Errorf("expected bob (never assigned), got %s", picked.Login)
	}
}

func TestBalancer_AllBusyOrOffShift(t *testing.T) {
	now := time.Date(2026, time.September, 7, 12, 0, 0, 0, time.UTC)
	group := config.GroupConfig{
		Strategy:       config.StrategyLoadBased,
		MaxLoadPerUser: 2,
		Schedule: config.ScheduleConfig{
			Timezone:  "UTC",
			WorkDays:  []int{1, 2, 3, 4, 5},
			WorkHours: config.WorkHours{Start: "09:00", End: "18:00"},
		},
		Assignees: []config.AssigneeConfig{
			{Login: "alice", MaxLoad: 2},
			{Login: "bob", Schedule: &config.ScheduleConfig{
				Timezone:  "UTC",
				WorkDays:  []int{6, 7}, // weekend only
				WorkHours: config.WorkHours{Start: "09:00", End: "18:00"},
			}},
		},
	}

	repo := &mockRepo{
		loads: map[string]storage.AssigneeLoadRecord{
			"alice": {AssigneeID: "alice", CurrentLoad: 2}, // full
			"bob":   {AssigneeID: "bob", CurrentLoad: 0},   // off shift
		},
	}

	balancer := NewBalancer(repo)
	_, err := balancer.PickAssignee(context.Background(), "group1", group, now)
	if !errors.Is(err, ErrNoAvailableAssignee) {
		t.Errorf("expected ErrNoAvailableAssignee, got %v", err)
	}
}

func TestBalancer_AbsentAssignee(t *testing.T) {
	now := time.Date(2026, time.September, 11, 12, 0, 0, 0, time.UTC)
	group := config.GroupConfig{
		Strategy:       config.StrategyLoadBased,
		MaxLoadPerUser: 5,
		Schedule: config.ScheduleConfig{
			Timezone:  "UTC",
			WorkDays:  []int{1, 2, 3, 4, 5},
			WorkHours: config.WorkHours{Start: "09:00", End: "18:00"},
		},
		Assignees: []config.AssigneeConfig{
			{
				Login: "alice",
				Absences: []config.AbsencePeriod{
					{Start: "2026-09-10", End: "2026-09-15", Reason: "vacation"},
				},
			},
			{
				Login: "bob",
			},
		},
	}

	repo := &mockRepo{
		loads: map[string]storage.AssigneeLoadRecord{
			"alice": {AssigneeID: "alice", CurrentLoad: 0},
			"bob":   {AssigneeID: "bob", CurrentLoad: 1},
		},
	}

	balancer := NewBalancer(repo)
	// Alice is on vacation, so Bob must be picked even though Bob has higher load
	cand, err := balancer.PickAssignee(context.Background(), "group1", group, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cand.Login != "bob" {
		t.Errorf("expected bob to be picked because alice is absent, got %s", cand.Login)
	}
}

func TestBalancer_CloudUIDLoad(t *testing.T) {
	now := time.Date(2026, time.September, 11, 12, 0, 0, 0, time.UTC)
	group := config.GroupConfig{
		Strategy:       config.StrategyLoadBased,
		MaxLoadPerUser: 2,
		Schedule: config.ScheduleConfig{
			Timezone:  "UTC",
			WorkDays:  []int{1, 2, 3, 4, 5},
			WorkHours: config.WorkHours{Start: "09:00", End: "18:00"},
		},
		Assignees: []config.AssigneeConfig{
			{
				Login:    "tut0roff",
				CloudUID: "ajefr4bv6e90bheosaqr",
			},
		},
	}

	// Load is stored under cloudUid
	repo := &mockRepo{
		loads: map[string]storage.AssigneeLoadRecord{
			"ajefr4bv6e90bheosaqr": {AssigneeID: "ajefr4bv6e90bheosaqr", CurrentLoad: 2},
		},
	}

	balancer := NewBalancer(repo)
	// Should recognize that load is 2 >= maxLoad (2), so no available assignee
	_, err := balancer.PickAssignee(context.Background(), "group1", group, now)
	if !errors.Is(err, ErrNoAvailableAssignee) {
		t.Errorf("expected ErrNoAvailableAssignee due to CloudUID load limit, got %v", err)
	}
}
