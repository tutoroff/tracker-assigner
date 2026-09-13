package storage

import (
	"context"
	"testing"
	"time"
)

func setupTestRepo(t *testing.T) (*SQLRepository, func()) {
	t.Helper()
	db, err := NewSQLite("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	repo := NewRepository(db)
	return repo, func() { db.Close() }
}

func TestRepository_AssigneeLoad(t *testing.T) {
	repo, teardown := setupTestRepo(t)
	defer teardown()

	ctx := context.Background()
	groupID := "support"
	user := "alice"

	// Initial load should be 0
	load, err := repo.GetAssigneeLoad(ctx, groupID, user)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if load != 0 {
		t.Errorf("expected 0 initial load, got %d", load)
	}

	// Increment
	if err := repo.IncrementLoad(ctx, groupID, user); err != nil {
		t.Fatalf("failed to increment load: %v", err)
	}
	load, _ = repo.GetAssigneeLoad(ctx, groupID, user)
	if load != 1 {
		t.Errorf("expected 1 after increment, got %d", load)
	}

	// Increment again
	if err := repo.IncrementLoad(ctx, groupID, user); err != nil {
		t.Fatalf("failed to increment load: %v", err)
	}
	load, _ = repo.GetAssigneeLoad(ctx, groupID, user)
	if load != 2 {
		t.Errorf("expected 2 after second increment, got %d", load)
	}

	// Decrement
	if err := repo.DecrementLoad(ctx, groupID, user); err != nil {
		t.Fatalf("failed to decrement load: %v", err)
	}
	load, _ = repo.GetAssigneeLoad(ctx, groupID, user)
	if load != 1 {
		t.Errorf("expected 1 after decrement, got %d", load)
	}

	// Decrement twice (should not go below 0)
	_ = repo.DecrementLoad(ctx, groupID, user)
	_ = repo.DecrementLoad(ctx, groupID, user)
	load, _ = repo.GetAssigneeLoad(ctx, groupID, user)
	if load != 0 {
		t.Errorf("expected load to floor at 0, got %d", load)
	}

	// Set load explicitly
	if err := repo.SetAssigneeLoad(ctx, groupID, user, 7); err != nil {
		t.Fatalf("failed to set load: %v", err)
	}
	load, _ = repo.GetAssigneeLoad(ctx, groupID, user)
	if load != 7 {
		t.Errorf("expected 7 after explicit set, got %d", load)
	}

	// Map of loads
	records, err := repo.GetAssigneesLoad(ctx, groupID)
	if err != nil {
		t.Fatalf("failed to get group loads: %v", err)
	}
	if records[user].CurrentLoad != 7 {
		t.Errorf("expected Alice load 7 in map, got %d", records[user].CurrentLoad)
	}
}

func TestRepository_PendingQueue(t *testing.T) {
	repo, teardown := setupTestRepo(t)
	defer teardown()

	ctx := context.Background()

	// Enqueue 2 items
	item1 := PendingItem{
		IssueKey: "DEV-1",
		QueueKey: "DEV",
		GroupID:  "group-a",
		Payload:  `{"id":"1"}`,
	}
	item2 := PendingItem{
		IssueKey: "DEV-2",
		QueueKey: "DEV",
		GroupID:  "group-b",
		Payload:  `{"id":"2"}`,
	}

	if err := repo.EnqueuePending(ctx, item1); err != nil {
		t.Fatalf("failed to enqueue item1: %v", err)
	}
	if err := repo.EnqueuePending(ctx, item2); err != nil {
		t.Fatalf("failed to enqueue item2: %v", err)
	}

	count, err := repo.GetPendingCount(ctx)
	if err != nil || count != 2 {
		t.Fatalf("expected count 2, got %d (err: %v)", count, err)
	}

	// Dequeue for group-a
	nextA, err := repo.GetNextPending(ctx, "group-a")
	if err != nil || nextA == nil {
		t.Fatalf("expected item for group-a, got nil (err: %v)", err)
	}
	if nextA.IssueKey != "DEV-1" {
		t.Errorf("expected DEV-1, got %s", nextA.IssueKey)
	}

	// Mark assigned
	if err := repo.MarkPendingAssigned(ctx, nextA.ID); err != nil {
		t.Fatalf("failed to mark assigned: %v", err)
	}

	count, _ = repo.GetPendingCount(ctx)
	if count != 1 {
		t.Errorf("expected count 1, got %d", count)
	}

	// Remove by issue key (manual intervention simulation)
	removed, err := repo.RemovePendingByIssueKey(ctx, "DEV-2")
	if err != nil || !removed {
		t.Fatalf("expected DEV-2 to be removed, got %v (err: %v)", removed, err)
	}

	count, _ = repo.GetPendingCount(ctx)
	if count != 0 {
		t.Errorf("expected count 0 after removals, got %d", count)
	}
}

func TestRepository_DLQ(t *testing.T) {
	repo, teardown := setupTestRepo(t)
	defer teardown()

	ctx := context.Background()

	// Enqueue DLQ item ready for immediate retry
	item := DLQItem{
		IssueKey:     "BUG-99",
		Action:       "assign",
		Payload:      `{"assignee":"bob"}`,
		ErrorMessage: "500 Internal Server Error",
		MaxRetries:   3,
		NextRetryAt:  time.Now().Add(-1 * time.Second), // past
	}

	if err := repo.EnqueueDLQ(ctx, item); err != nil {
		t.Fatalf("failed to enqueue DLQ: %v", err)
	}

	count, err := repo.GetDLQCount(ctx)
	if err != nil || count != 1 {
		t.Fatalf("expected DLQ count 1, got %d", count)
	}

	// Retrieve for retry
	items, err := repo.GetDLQItemsForRetry(ctx, 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("expected 1 retryable item, got %d (err: %v)", len(items), err)
	}
	if items[0].IssueKey != "BUG-99" {
		t.Errorf("expected BUG-99, got %s", items[0].IssueKey)
	}

	// Update retry
	nextRetry := time.Now().Add(5 * time.Minute)
	if err := repo.UpdateDLQRetry(ctx, items[0].ID, 1, nextRetry, "another 500 error"); err != nil {
		t.Fatalf("failed to update DLQ retry: %v", err)
	}

	// Since nextRetry is 5 min in future, GetDLQItemsForRetry should return 0 items now
	futureItems, err := repo.GetDLQItemsForRetry(ctx, 10)
	if err != nil || len(futureItems) != 0 {
		t.Fatalf("expected 0 retryable items now, got %d", len(futureItems))
	}

	// Mark resolved
	if err := repo.MarkDLQResolved(ctx, items[0].ID); err != nil {
		t.Fatalf("failed to mark DLQ resolved: %v", err)
	}

	count, _ = repo.GetDLQCount(ctx)
	if count != 0 {
		t.Errorf("expected 0 pending DLQ items, got %d", count)
	}
}

func TestRepository_IssueAssignments(t *testing.T) {
	repo, teardown := setupTestRepo(t)
	defer teardown()

	ctx := context.Background()
	groupID := "marketing_team"
	user1 := "tut0roff"
	user2 := "alex"
	issueKey := "MARKETING-15"

	// 1. Initial assignment increments load
	inc, err := repo.IncrementLoadForIssue(ctx, groupID, user1, issueKey)
	if err != nil || !inc {
		t.Fatalf("expected assignment increment true, got %v, err: %v", inc, err)
	}
	load, _ := repo.GetAssigneeLoad(ctx, groupID, user1)
	if load != 1 {
		t.Fatalf("expected load 1 for user1, got %d", load)
	}

	// 2. Duplicate assignment for SAME issue does NOT double-increment (Webhook Storm protection)
	for i := 0; i < 5; i++ {
		inc, err := repo.IncrementLoadForIssue(ctx, groupID, user1, issueKey)
		if err != nil {
			t.Fatalf("unexpected error on repeated assign: %v", err)
		}
		if inc {
			t.Errorf("iteration %d: expected inc=false for duplicate assignment, got true", i)
		}
	}
	load, _ = repo.GetAssigneeLoad(ctx, groupID, user1)
	if load != 1 {
		t.Errorf("expected load to remain 1 after repeated assignments, got %d", load)
	}

	// 3. Reassign to another user decrements user1 and increments user2
	inc, err = repo.IncrementLoadForIssue(ctx, groupID, user2, issueKey)
	if err != nil || !inc {
		t.Fatalf("expected reassign increment true, got %v, err: %v", inc, err)
	}
	load1, _ := repo.GetAssigneeLoad(ctx, groupID, user1)
	load2, _ := repo.GetAssigneeLoad(ctx, groupID, user2)
	if load1 != 0 {
		t.Errorf("expected user1 load to decrement to 0, got %d", load1)
	}
	if load2 != 1 {
		t.Errorf("expected user2 load to increment to 1, got %d", load2)
	}

	// 4. Check GetIssueAssignment
	item, err := repo.GetIssueAssignment(ctx, issueKey)
	if err != nil || item == nil || item.AssigneeID != user2 {
		t.Fatalf("expected assignment for %s, got %v, err: %v", user2, item, err)
	}

	// 5. DecrementLoadForIssue removes assignment and decrements load
	removedAssignee, err := repo.DecrementLoadForIssue(ctx, issueKey)
	if err != nil || removedAssignee != user2 {
		t.Fatalf("expected removed assignee %s, got %s, err: %v", user2, removedAssignee, err)
	}
	load2, _ = repo.GetAssigneeLoad(ctx, groupID, user2)
	if load2 != 0 {
		t.Errorf("expected user2 load to decrement to 0, got %d", load2)
	}

	// Verify GetIssueAssignment returns nil now
	itemAfter, err := repo.GetIssueAssignment(ctx, issueKey)
	if err != nil || itemAfter != nil {
		t.Errorf("expected nil after decrement, got %v, err: %v", itemAfter, err)
	}

	// 6. Subsequent decrement for already removed issue is a safe no-op
	removedAgain, err := repo.DecrementLoadForIssue(ctx, issueKey)
	if err != nil || removedAgain != "" {
		t.Errorf("expected empty string on double decrement, got %s, err: %v", removedAgain, err)
	}

	// 7. DecrementAssigneeGlobalLoad
	_ = repo.SetAssigneeLoad(ctx, groupID, user1, 5)
	if err := repo.DecrementAssigneeGlobalLoad(ctx, user1); err != nil {
		t.Fatalf("failed global decrement: %v", err)
	}
	load1, _ = repo.GetAssigneeLoad(ctx, groupID, user1)
	if load1 != 4 {
		t.Errorf("expected global load decremented to 4, got %d", load1)
	}
}

func TestRepository_SyncAssigneeIssues(t *testing.T) {
	repo, teardown := setupTestRepo(t)
	defer teardown()

	ctx := context.Background()
	groupID := "qa_team"
	assigneeID := "charlie"

	// 1. Initial sync - empty to 2 issues
	initialIssues := []string{"QA-1", "QA-2"}
	err := repo.SyncAssigneeIssues(ctx, groupID, assigneeID, initialIssues)
	if err != nil {
		t.Fatalf("failed to sync assignee issues: %v", err)
	}

	// Verify load
	load, err := repo.GetAssigneeLoad(ctx, groupID, assigneeID)
	if err != nil || load != 2 {
		t.Fatalf("expected load to be 2, got %d (err: %v)", load, err)
	}

	// Verify assignments
	for _, key := range initialIssues {
		assignment, err := repo.GetIssueAssignment(ctx, key)
		if err != nil || assignment == nil {
			t.Errorf("expected issue %s to be assigned to %s, got err %v", key, assigneeID, err)
		} else if assignment.AssigneeID != assigneeID {
			t.Errorf("expected issue %s to be assigned to %s, got %s", key, assigneeID, assignment.AssigneeID)
		}
	}

	// 2. Sync again - preserving one, deleting one, adding one
	updatedIssues := []string{"QA-2", "QA-3"}
	err = repo.SyncAssigneeIssues(ctx, groupID, assigneeID, updatedIssues)
	if err != nil {
		t.Fatalf("failed to sync updated assignee issues: %v", err)
	}

	// Verify load remains 2
	load, err = repo.GetAssigneeLoad(ctx, groupID, assigneeID)
	if err != nil || load != 2 {
		t.Fatalf("expected load to be 2, got %d (err: %v)", load, err)
	}

	// Verify old issue was deleted
	assignment, _ := repo.GetIssueAssignment(ctx, "QA-1")
	if assignment != nil {
		t.Errorf("expected QA-1 to be deleted, but it is still assigned to %s", assignment.AssigneeID)
	}

	// Verify preserved issue remains
	assignment, err = repo.GetIssueAssignment(ctx, "QA-2")
	if err != nil || assignment == nil || assignment.AssigneeID != assigneeID {
		t.Errorf("expected QA-2 to be preserved and assigned to %s", assigneeID)
	}

	// Verify new issue was added
	assignment, err = repo.GetIssueAssignment(ctx, "QA-3")
	if err != nil || assignment == nil || assignment.AssigneeID != assigneeID {
		t.Errorf("expected QA-3 to be added and assigned to %s", assigneeID)
	}

	// 3. Sync to empty - remove all
	err = repo.SyncAssigneeIssues(ctx, groupID, assigneeID, []string{})
	if err != nil {
		t.Fatalf("failed to sync empty assignee issues: %v", err)
	}

	// Verify load is 0
	load, err = repo.GetAssigneeLoad(ctx, groupID, assigneeID)
	if err != nil || load != 0 {
		t.Fatalf("expected load to be 0, got %d (err: %v)", load, err)
	}

	// Verify all issues removed
	for _, key := range updatedIssues {
		assignment, _ := repo.GetIssueAssignment(ctx, key)
		if assignment != nil {
			t.Errorf("expected %s to be deleted, but it is still assigned", key)
		}
	}
}
