package storage

import (
	"context"
	"testing"
)

func TestNewSQLite_InMemory(t *testing.T) {
	db, err := NewSQLite("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("failed to initialize in-memory sqlite: %v", err)
	}
	defer db.Close()

	// Verify tables exist
	tables := []string{"assignee_load", "pending_queue", "dead_letter_queue", "issue_assignments"}
	for _, tbl := range tables {
		var name string
		query := "SELECT name FROM sqlite_master WHERE type='table' AND name=?;"
		err := db.QueryRowContext(context.Background(), query, tbl).Scan(&name)
		if err != nil {
			t.Errorf("table %s was not created: %v", tbl, err)
		}
		if name != tbl {
			t.Errorf("expected table name %s, got %s", tbl, name)
		}
	}
}
