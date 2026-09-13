-- Migration: 0001_init.sql
-- Description: Create initial schema for assignee_load, pending_queue, and dead_letter_queue

CREATE TABLE IF NOT EXISTS assignee_load (
    assignee_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    current_load INTEGER NOT NULL DEFAULT 0,
    last_assigned_at DATETIME,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (assignee_id, group_id)
);

CREATE INDEX IF NOT EXISTS idx_assignee_load_group_load 
ON assignee_load(group_id, current_load);

CREATE TABLE IF NOT EXISTS pending_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_key TEXT NOT NULL UNIQUE,
    queue_key TEXT NOT NULL,
    group_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pending_queue_status_created 
ON pending_queue(status, created_at);

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_key TEXT NOT NULL,
    action TEXT NOT NULL,
    payload TEXT NOT NULL,
    error_message TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 5,
    next_retry_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dlq_status_next_retry 
ON dead_letter_queue(status, next_retry_at);

CREATE TABLE IF NOT EXISTS issue_assignments (
    issue_key TEXT PRIMARY KEY,
    group_id TEXT NOT NULL,
    assignee_id TEXT NOT NULL,
    assigned_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_issue_assignments_assignee 
ON issue_assignments(assignee_id, group_id);
