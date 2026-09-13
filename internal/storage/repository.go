package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// AssigneeLoadRecord represents assignee load in SQLite.
type AssigneeLoadRecord struct {
	AssigneeID     string     `json:"assignee_id"`
	GroupID        string     `json:"group_id"`
	CurrentLoad    int        `json:"current_load"`
	LastAssignedAt *time.Time `json:"last_assigned_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// PendingItem represents a ticket in the pending queue.
type PendingItem struct {
	ID        int64     `json:"id"`
	IssueKey  string    `json:"issue_key"`
	QueueKey  string    `json:"queue_key"`
	GroupID   string    `json:"group_id"`
	Payload   string    `json:"payload"`
	Status    string    `json:"status"`
	Attempts  int       `json:"attempts"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// DLQItem represents an entry in the dead letter queue.
type DLQItem struct {
	ID           int64     `json:"id"`
	IssueKey     string    `json:"issue_key"`
	Action       string    `json:"action"`
	Payload      string    `json:"payload"`
	ErrorMessage string    `json:"error_message"`
	Status       string    `json:"status"`
	RetryCount   int       `json:"retry_count"`
	MaxRetries   int       `json:"max_retries"`
	NextRetryAt  time.Time `json:"next_retry_at"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// IssueAssignment represents an issue currently assigned to an employee in a group.
type IssueAssignment struct {
	IssueKey   string    `json:"issue_key"`
	GroupID    string    `json:"group_id"`
	AssigneeID string    `json:"assignee_id"`
	AssignedAt time.Time `json:"assigned_at"`
}

// Repository defines data access methods for loads and queues.
type Repository interface {
	// Assignee load operations
	IncrementLoad(ctx context.Context, groupID, assigneeID string) error
	IncrementLoadForIssue(ctx context.Context, groupID, assigneeID, issueKey string) (bool, error)
	DecrementLoad(ctx context.Context, groupID, assigneeID string) error
	DecrementLoadForIssue(ctx context.Context, issueKey string) (string, error)
	DecrementAssigneeGlobalLoad(ctx context.Context, assigneeID string) error
	SetAssigneeLoad(ctx context.Context, groupID, assigneeID string, load int) error
	GetAssigneesLoad(ctx context.Context, groupID string) (map[string]AssigneeLoadRecord, error)
	GetAssigneeLoad(ctx context.Context, groupID, assigneeID string) (int, error)
	GetIssueAssignment(ctx context.Context, issueKey string) (*IssueAssignment, error)
	SyncAssigneeIssues(ctx context.Context, groupID, assigneeID string, issueKeys []string) error

	// Pending queue operations
	EnqueuePending(ctx context.Context, item PendingItem) error
	GetNextPending(ctx context.Context, groupID string) (*PendingItem, error)
	MarkPendingAssigned(ctx context.Context, id int64) error
	RemovePendingByIssueKey(ctx context.Context, issueKey string) (bool, error)
	GetPendingCount(ctx context.Context) (int, error)
	GetPendingItems(ctx context.Context, limit, offset int) ([]PendingItem, error)

	// Dead letter queue operations
	EnqueueDLQ(ctx context.Context, item DLQItem) error
	GetDLQItemsForRetry(ctx context.Context, limit int) ([]DLQItem, error)
	UpdateDLQRetry(ctx context.Context, id int64, retryCount int, nextRetryAt time.Time, errMsg string) error
	MarkDLQResolved(ctx context.Context, id int64) error
	GetDLQCount(ctx context.Context) (int, error)
	GetDLQItems(ctx context.Context, limit, offset int) ([]DLQItem, error)
	CleanupOldDLQ(ctx context.Context, olderThan time.Duration) (int64, error)
}

// SQLRepository implements Repository using SQLite.
type SQLRepository struct {
	db *DB
}

// NewRepository creates a new SQLRepository.
func NewRepository(db *DB) *SQLRepository {
	return &SQLRepository{db: db}
}

// IncrementLoad atomically increments load and updates last_assigned_at.
func (r *SQLRepository) IncrementLoad(ctx context.Context, groupID, assigneeID string) error {
	query := `
	INSERT INTO assignee_load (assignee_id, group_id, current_load, last_assigned_at, updated_at)
	VALUES (?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	ON CONFLICT(assignee_id, group_id) DO UPDATE SET
		current_load = current_load + 1,
		last_assigned_at = CURRENT_TIMESTAMP,
		updated_at = CURRENT_TIMESTAMP;
	`
	_, err := r.db.ExecContext(ctx, query, assigneeID, groupID)
	if err != nil {
		return fmt.Errorf("failed to increment load for %s in %s: %w", assigneeID, groupID, err)
	}
	return nil
}

// DecrementLoad atomically decrements load (ensuring it never drops below 0).
func (r *SQLRepository) DecrementLoad(ctx context.Context, groupID, assigneeID string) error {
	query := `
	UPDATE assignee_load
	SET current_load = MAX(0, current_load - 1),
	    updated_at = CURRENT_TIMESTAMP
	WHERE assignee_id = ? AND group_id = ?;
	`
	_, err := r.db.ExecContext(ctx, query, assigneeID, groupID)
	if err != nil {
		return fmt.Errorf("failed to decrement load for %s in %s: %w", assigneeID, groupID, err)
	}
	return nil
}

// DecrementAssigneeGlobalLoad decrements load across whichever group has positive load for this assignee.
func (r *SQLRepository) DecrementAssigneeGlobalLoad(ctx context.Context, assigneeID string) error {
	query := `
	UPDATE assignee_load
	SET current_load = MAX(0, current_load - 1),
	    updated_at = CURRENT_TIMESTAMP
	WHERE rowid = (
		SELECT rowid FROM assignee_load
		WHERE assignee_id = ? AND current_load > 0
		ORDER BY current_load DESC
		LIMIT 1
	);
	`
	_, err := r.db.ExecContext(ctx, query, assigneeID)
	if err != nil {
		return fmt.Errorf("failed to decrement global load for %s: %w", assigneeID, err)
	}
	return nil
}

// SetAssigneeLoad sets current load explicitly (used for TQL state synchronization).
func (r *SQLRepository) SetAssigneeLoad(ctx context.Context, groupID, assigneeID string, load int) error {
	if load < 0 {
		load = 0
	}
	query := `
	INSERT INTO assignee_load (assignee_id, group_id, current_load, updated_at)
	VALUES (?, ?, ?, CURRENT_TIMESTAMP)
	ON CONFLICT(assignee_id, group_id) DO UPDATE SET
		current_load = excluded.current_load,
		updated_at = CURRENT_TIMESTAMP;
	`
	_, err := r.db.ExecContext(ctx, query, assigneeID, groupID, load)
	if err != nil {
		return fmt.Errorf("failed to set load for %s in %s: %w", assigneeID, groupID, err)
	}
	return nil
}

// GetAssigneesLoad returns map of assignee records for a group.
func (r *SQLRepository) GetAssigneesLoad(ctx context.Context, groupID string) (map[string]AssigneeLoadRecord, error) {
	query := `
	SELECT assignee_id, group_id, current_load, last_assigned_at, updated_at
	FROM assignee_load
	WHERE group_id = ?;
	`
	rows, err := r.db.QueryContext(ctx, query, groupID)
	if err != nil {
		return nil, fmt.Errorf("failed to query assignees load for group %s: %w", groupID, err)
	}
	defer rows.Close()

	result := make(map[string]AssigneeLoadRecord)
	for rows.Next() {
		var rec AssigneeLoadRecord
		var lastAssigned sql.NullTime
		if err := rows.Scan(&rec.AssigneeID, &rec.GroupID, &rec.CurrentLoad, &lastAssigned, &rec.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan assignee load row: %w", err)
		}
		if lastAssigned.Valid {
			rec.LastAssignedAt = &lastAssigned.Time
		}
		result[rec.AssigneeID] = rec
	}
	return result, rows.Err()
}

// GetAssigneeLoad returns the current load for a single assignee in a group.
func (r *SQLRepository) GetAssigneeLoad(ctx context.Context, groupID, assigneeID string) (int, error) {
	query := `SELECT current_load FROM assignee_load WHERE group_id = ? AND assignee_id = ?;`
	var load int
	err := r.db.QueryRowContext(ctx, query, groupID, assigneeID).Scan(&load)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get load for %s: %w", assigneeID, err)
	}
	return load, nil
}

// EnqueuePending adds a ticket to the pending queue.
func (r *SQLRepository) EnqueuePending(ctx context.Context, item PendingItem) error {
	createdAtVal := time.Now().UTC().Format("2006-01-02 15:04:05")
	if !item.CreatedAt.IsZero() {
		createdAtVal = item.CreatedAt.UTC().Format("2006-01-02 15:04:05")
	}

	query := `
	INSERT INTO pending_queue (issue_key, queue_key, group_id, payload, status, attempts, created_at, updated_at)
	VALUES (?, ?, ?, ?, 'pending', 0, ?, CURRENT_TIMESTAMP)
	ON CONFLICT(issue_key) DO UPDATE SET
		status = 'pending',
		attempts = attempts + 1,
		updated_at = CURRENT_TIMESTAMP;
	`
	_, err := r.db.ExecContext(ctx, query, item.IssueKey, item.QueueKey, item.GroupID, item.Payload, createdAtVal)
	if err != nil {
		return fmt.Errorf("failed to enqueue pending item %s: %w", item.IssueKey, err)
	}
	return nil
}

// GetNextPending retrieves the oldest pending ticket for a given group, or any group if groupID is empty.
func (r *SQLRepository) GetNextPending(ctx context.Context, groupID string) (*PendingItem, error) {
	var query string
	var args []any
	if groupID != "" {
		query = `
		SELECT id, issue_key, queue_key, group_id, payload, status, attempts, created_at, updated_at
		FROM pending_queue
		WHERE status = 'pending' AND group_id = ?
		ORDER BY created_at ASC
		LIMIT 1;
		`
		args = append(args, groupID)
	} else {
		query = `
		SELECT id, issue_key, queue_key, group_id, payload, status, attempts, created_at, updated_at
		FROM pending_queue
		WHERE status = 'pending'
		ORDER BY created_at ASC
		LIMIT 1;
		`
	}

	var item PendingItem
	err := r.db.QueryRowContext(ctx, query, args...).Scan(
		&item.ID,
		&item.IssueKey,
		&item.QueueKey,
		&item.GroupID,
		&item.Payload,
		&item.Status,
		&item.Attempts,
		&item.CreatedAt,
		&item.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get next pending item: %w", err)
	}
	return &item, nil
}

// MarkPendingAssigned removes or marks the item as assigned.
func (r *SQLRepository) MarkPendingAssigned(ctx context.Context, id int64) error {
	query := `DELETE FROM pending_queue WHERE id = ?;`
	_, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete pending item %d: %w", id, err)
	}
	return nil
}

// RemovePendingByIssueKey removes a pending ticket when manual intervention happens.
func (r *SQLRepository) RemovePendingByIssueKey(ctx context.Context, issueKey string) (bool, error) {
	query := `DELETE FROM pending_queue WHERE issue_key = ?;`
	res, err := r.db.ExecContext(ctx, query, issueKey)
	if err != nil {
		return false, fmt.Errorf("failed to delete pending issue %s: %w", issueKey, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

// GetPendingCount returns total pending items count.
func (r *SQLRepository) GetPendingCount(ctx context.Context) (int, error) {
	query := `SELECT COUNT(*) FROM pending_queue WHERE status = 'pending';`
	var count int
	if err := r.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count pending items: %w", err)
	}
	return count, nil
}

// GetPendingItems lists pending items with pagination.
func (r *SQLRepository) GetPendingItems(ctx context.Context, limit, offset int) ([]PendingItem, error) {
	if limit <= 0 {
		limit = 50
	}
	query := `
	SELECT id, issue_key, queue_key, group_id, payload, status, attempts, created_at, updated_at
	FROM pending_queue
	ORDER BY created_at DESC
	LIMIT ? OFFSET ?;
	`
	rows, err := r.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending items: %w", err)
	}
	defer rows.Close()

	var items []PendingItem
	for rows.Next() {
		var item PendingItem
		if err := rows.Scan(
			&item.ID,
			&item.IssueKey,
			&item.QueueKey,
			&item.GroupID,
			&item.Payload,
			&item.Status,
			&item.Attempts,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan pending item: %w", err)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// EnqueueDLQ adds a failed action to DLQ.
func (r *SQLRepository) EnqueueDLQ(ctx context.Context, item DLQItem) error {
	maxRetries := item.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}
	nextRetry := item.NextRetryAt
	if nextRetry.IsZero() {
		nextRetry = time.Now().Add(1 * time.Minute)
	}
	nextRetryStr := nextRetry.UTC().Format("2006-01-02 15:04:05")

	query := `
	INSERT INTO dead_letter_queue (issue_key, action, payload, error_message, status, retry_count, max_retries, next_retry_at, created_at, updated_at)
	VALUES (?, ?, ?, ?, 'pending', 0, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
	`
	_, err := r.db.ExecContext(ctx, query, item.IssueKey, item.Action, item.Payload, item.ErrorMessage, maxRetries, nextRetryStr)
	if err != nil {
		return fmt.Errorf("failed to enqueue dlq item for issue %s: %w", item.IssueKey, err)
	}
	return nil
}

// GetDLQItemsForRetry retrieves items ready for retry.
func (r *SQLRepository) GetDLQItemsForRetry(ctx context.Context, limit int) ([]DLQItem, error) {
	if limit <= 0 {
		limit = 10
	}
	nowStr := time.Now().UTC().Format("2006-01-02 15:04:05")
	query := `
	SELECT id, issue_key, action, payload, error_message, status, retry_count, max_retries, next_retry_at, created_at, updated_at
	FROM dead_letter_queue
	WHERE status = 'pending' AND next_retry_at <= ? AND retry_count < max_retries
	ORDER BY next_retry_at ASC
	LIMIT ?;
	`
	rows, err := r.db.QueryContext(ctx, query, nowStr, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get dlq items for retry: %w", err)
	}
	defer rows.Close()

	var items []DLQItem
	for rows.Next() {
		var item DLQItem
		var errMsg sql.NullString
		var nextRetryStr string
		if err := rows.Scan(
			&item.ID,
			&item.IssueKey,
			&item.Action,
			&item.Payload,
			&errMsg,
			&item.Status,
			&item.RetryCount,
			&item.MaxRetries,
			&nextRetryStr,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan dlq item: %w", err)
		}
		if parsedTime, err := time.Parse("2006-01-02 15:04:05", nextRetryStr); err == nil {
			item.NextRetryAt = parsedTime
		} else if parsedTime, err := time.Parse(time.RFC3339, nextRetryStr); err == nil {
			item.NextRetryAt = parsedTime
		}
		if errMsg.Valid {
			item.ErrorMessage = errMsg.String
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// UpdateDLQRetry updates retry count, error message and next retry timestamp.
func (r *SQLRepository) UpdateDLQRetry(ctx context.Context, id int64, retryCount int, nextRetryAt time.Time, errMsg string) error {
	status := "pending"
	nextRetryStr := nextRetryAt.UTC().Format("2006-01-02 15:04:05")
	query := `
	UPDATE dead_letter_queue
	SET retry_count = ?,
	    next_retry_at = ?,
	    error_message = ?,
	    status = CASE WHEN ? >= max_retries THEN 'failed' ELSE ? END,
	    updated_at = CURRENT_TIMESTAMP
	WHERE id = ?;
	`
	_, err := r.db.ExecContext(ctx, query, retryCount, nextRetryStr, errMsg, retryCount, status, id)
	if err != nil {
		return fmt.Errorf("failed to update dlq item %d: %w", id, err)
	}
	return nil
}

// MarkDLQResolved marks a DLQ item as resolved.
func (r *SQLRepository) MarkDLQResolved(ctx context.Context, id int64) error {
	query := `UPDATE dead_letter_queue SET status = 'resolved', updated_at = CURRENT_TIMESTAMP WHERE id = ?;`
	_, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to mark dlq item %d resolved: %w", id, err)
	}
	return nil
}

// GetDLQCount returns count of active/pending DLQ items.
func (r *SQLRepository) GetDLQCount(ctx context.Context) (int, error) {
	query := `SELECT COUNT(*) FROM dead_letter_queue WHERE status = 'pending';`
	var count int
	if err := r.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count dlq items: %w", err)
	}
	return count, nil
}

// GetDLQItems lists DLQ items with pagination.
func (r *SQLRepository) GetDLQItems(ctx context.Context, limit, offset int) ([]DLQItem, error) {
	if limit <= 0 {
		limit = 50
	}
	query := `
	SELECT id, issue_key, action, payload, error_message, status, retry_count, max_retries, next_retry_at, created_at, updated_at
	FROM dead_letter_queue
	ORDER BY created_at DESC
	LIMIT ? OFFSET ?;
	`
	rows, err := r.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to query dlq items: %w", err)
	}
	defer rows.Close()

	var items []DLQItem
	for rows.Next() {
		var item DLQItem
		var errMsg sql.NullString
		if err := rows.Scan(
			&item.ID,
			&item.IssueKey,
			&item.Action,
			&item.Payload,
			&errMsg,
			&item.Status,
			&item.RetryCount,
			&item.MaxRetries,
			&item.NextRetryAt,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan dlq item: %w", err)
		}
		if errMsg.Valid {
			item.ErrorMessage = errMsg.String
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// CleanupOldDLQ deletes resolved or failed DLQ records older than the specified duration.
func (r *SQLRepository) CleanupOldDLQ(ctx context.Context, olderThan time.Duration) (int64, error) {
	threshold := time.Now().Add(-olderThan).UTC().Format("2006-01-02 15:04:05")
	query := `DELETE FROM dead_letter_queue WHERE status IN ('resolved', 'failed') AND updated_at < ?;`
	res, err := r.db.ExecContext(ctx, query, threshold)
	if err != nil {
		return 0, fmt.Errorf("failed to clean up old dlq items: %w", err)
	}
	return res.RowsAffected()
}

// IncrementLoadForIssue records an issue assignment and increments assignee load idempotently.
// If the issue is already assigned to assigneeID, it does NOT double-increment load (returns false, nil).
// If the issue was previously assigned to another assignee, it decrements the old assignee's load and increments the new one.
func (r *SQLRepository) IncrementLoadForIssue(ctx context.Context, groupID, assigneeID, issueKey string) (bool, error) {
	if issueKey == "" {
		return false, r.IncrementLoad(ctx, groupID, assigneeID)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var existingAssignee, existingGroup string
	queryCheck := `SELECT assignee_id, group_id FROM issue_assignments WHERE issue_key = ?;`
	err = tx.QueryRowContext(ctx, queryCheck, issueKey).Scan(&existingAssignee, &existingGroup)
	if err == nil {
		// Assignment already exists
		if existingAssignee == assigneeID {
			// Already assigned to this exact user -> idempotent no-op
			return false, tx.Commit()
		}

		// Reassigned from existingAssignee to assigneeID
		queryDec := `
		UPDATE assignee_load
		SET current_load = MAX(0, current_load - 1),
		    updated_at = CURRENT_TIMESTAMP
		WHERE assignee_id = ? AND group_id = ?;
		`
		if _, err := tx.ExecContext(ctx, queryDec, existingAssignee, existingGroup); err != nil {
			return false, fmt.Errorf("failed to decrement previous assignee load: %w", err)
		}

		queryUpdate := `
		UPDATE issue_assignments
		SET assignee_id = ?, group_id = ?, assigned_at = CURRENT_TIMESTAMP
		WHERE issue_key = ?;
		`
		if _, err := tx.ExecContext(ctx, queryUpdate, assigneeID, groupID, issueKey); err != nil {
			return false, fmt.Errorf("failed to update issue assignment: %w", err)
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		// New assignment
		queryInsert := `
		INSERT INTO issue_assignments (issue_key, group_id, assignee_id, assigned_at)
		VALUES (?, ?, ?, CURRENT_TIMESTAMP);
		`
		if _, err := tx.ExecContext(ctx, queryInsert, issueKey, groupID, assigneeID); err != nil {
			return false, fmt.Errorf("failed to insert issue assignment: %w", err)
		}
	} else {
		return false, fmt.Errorf("failed to check existing issue assignment: %w", err)
	}

	// Increment new assignee's load
	queryInc := `
	INSERT INTO assignee_load (assignee_id, group_id, current_load, last_assigned_at, updated_at)
	VALUES (?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	ON CONFLICT(assignee_id, group_id) DO UPDATE SET
		current_load = current_load + 1,
		last_assigned_at = CURRENT_TIMESTAMP,
		updated_at = CURRENT_TIMESTAMP;
	`
	if _, err := tx.ExecContext(ctx, queryInc, assigneeID, groupID); err != nil {
		return false, fmt.Errorf("failed to increment load for %s: %w", assigneeID, err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return true, nil
}

// DecrementLoadForIssue removes an issue assignment and decrements the assignee's load.
func (r *SQLRepository) DecrementLoadForIssue(ctx context.Context, issueKey string) (string, error) {
	if issueKey == "" {
		return "", nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var assigneeID, groupID string
	queryFind := `SELECT assignee_id, group_id FROM issue_assignments WHERE issue_key = ?;`
	err = tx.QueryRowContext(ctx, queryFind, issueKey).Scan(&assigneeID, &groupID)
	if errors.Is(err, sql.ErrNoRows) {
		// Not tracked in issue_assignments
		_ = tx.Commit()
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("failed to query issue assignment: %w", err)
	}

	// Delete from issue_assignments
	queryDel := `DELETE FROM issue_assignments WHERE issue_key = ?;`
	if _, err := tx.ExecContext(ctx, queryDel, issueKey); err != nil {
		return "", fmt.Errorf("failed to delete issue assignment: %w", err)
	}

	// Decrement load
	queryDec := `
	UPDATE assignee_load
	SET current_load = MAX(0, current_load - 1),
	    updated_at = CURRENT_TIMESTAMP
	WHERE assignee_id = ? AND group_id = ?;
	`
	if _, err := tx.ExecContext(ctx, queryDec, assigneeID, groupID); err != nil {
		return "", fmt.Errorf("failed to decrement assignee load: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("failed to commit decrement transaction: %w", err)
	}

	return assigneeID, nil
}

// GetIssueAssignment returns the assignment record for a given issueKey.
func (r *SQLRepository) GetIssueAssignment(ctx context.Context, issueKey string) (*IssueAssignment, error) {
	query := `SELECT issue_key, group_id, assignee_id, assigned_at FROM issue_assignments WHERE issue_key = ?;`
	var item IssueAssignment
	err := r.db.QueryRowContext(ctx, query, issueKey).Scan(&item.IssueKey, &item.GroupID, &item.AssigneeID, &item.AssignedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get issue assignment for %s: %w", issueKey, err)
	}
	return &item, nil
}

// SyncAssigneeIssues synchronizes the issue_assignments table for a specific assignee in a group and updates their load.
func (r *SQLRepository) SyncAssigneeIssues(ctx context.Context, groupID, assigneeID string, issueKeys []string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// 1. Get existing issues
	existing := make(map[string]bool)
	rows, err := tx.QueryContext(ctx, "SELECT issue_key FROM issue_assignments WHERE group_id = ? AND assignee_id = ?", groupID, assigneeID)
	if err != nil {
		return fmt.Errorf("failed to get existing assignments: %w", err)
	}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err == nil {
			existing[key] = true
		}
	}
	rows.Close()

	// 2. Identify new and removed issues
	currentMap := make(map[string]bool)
	for _, key := range issueKeys {
		currentMap[key] = true
	}

	var toDelete []string
	var toInsert []string
	for key := range existing {
		if !currentMap[key] {
			toDelete = append(toDelete, key)
		}
	}
	for _, key := range issueKeys {
		if !existing[key] {
			toInsert = append(toInsert, key)
		}
	}

	// 3. Delete removed
	for _, key := range toDelete {
		if _, err := tx.ExecContext(ctx, "DELETE FROM issue_assignments WHERE issue_key = ?", key); err != nil {
			return fmt.Errorf("failed to delete removed assignment %s: %w", key, err)
		}
	}

	// 4. Insert new
	for _, key := range toInsert {
		if _, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO issue_assignments (issue_key, group_id, assignee_id, assigned_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)", key, groupID, assigneeID); err != nil {
			return fmt.Errorf("failed to insert new assignment %s: %w", key, err)
		}
	}

	// 5. Update load correctly
	loadCount := len(issueKeys)
	queryLoad := `
	INSERT INTO assignee_load (assignee_id, group_id, current_load, updated_at)
	VALUES (?, ?, ?, CURRENT_TIMESTAMP)
	ON CONFLICT(assignee_id, group_id) DO UPDATE SET
		current_load = ?,
		updated_at = CURRENT_TIMESTAMP;
	`
	if _, err := tx.ExecContext(ctx, queryLoad, assigneeID, groupID, loadCount, loadCount); err != nil {
		return fmt.Errorf("failed to set load for %s: %w", assigneeID, err)
	}

	return tx.Commit()
}
