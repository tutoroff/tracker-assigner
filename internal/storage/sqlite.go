package storage

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed migrations/0001_init.sql
var initMigrationSQL string

// DB wraps sql.DB with SQLite configuration and helper methods.
type DB struct {
	*sql.DB
}

// NewSQLite opens a SQLite database, configures pragmas and applies initial migrations.
func NewSQLite(dsn string) (*DB, error) {
	if dsn == "" {
		dsn = "file:assigner.db?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// SQLite connection pool configuration
	db.SetMaxOpenConns(1) // Avoid database locked errors with SQLite single-file write
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping sqlite database: %w", err)
	}

	// Apply PRAGMAs
	pragmas := []string{
		"PRAGMA busy_timeout = 5000;",
		"PRAGMA foreign_keys = ON;",
		"PRAGMA synchronous = NORMAL;",
		"PRAGMA cache_size = -20000;",
		"PRAGMA temp_store = MEMORY;",
	}
	for _, pragma := range pragmas {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to exec pragma %q: %w", pragma, err)
		}
	}

	// Run initial schema migration
	if err := runMigrations(ctx, db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	return &DB{DB: db}, nil
}

func runMigrations(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, initMigrationSQL); err != nil {
		return fmt.Errorf("migration execution failed: %w", err)
	}

	return tx.Commit()
}
