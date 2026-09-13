package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"tracker-assigner/internal/storage"

	"go.uber.org/zap"
)

func TestAPIHandler_Endpoints(t *testing.T) {
	logger := zap.NewNop()
	db, err := storage.NewSQLite("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	defer db.Close()
	repo := storage.NewRepository(db)

	api := NewAPIHandler(repo, logger)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)
	mux.Handle("GET /metrics", MetricsHandler())

	// Seed data
	_ = repo.IncrementLoad(context.Background(), "support", "alice")
	_ = repo.EnqueuePending(context.Background(), storage.PendingItem{
		IssueKey: "DEV-10",
		QueueKey: "DEV",
		GroupID:  "support",
		Payload:  `{}`,
	})
	_ = repo.EnqueueDLQ(context.Background(), storage.DLQItem{
		IssueKey:     "DEV-20",
		Action:       "assign",
		Payload:      `{}`,
		ErrorMessage: "500 error",
	})

	t.Run("GET /api/v1/status", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rr.Code)
		}

		var resp map[string]any
		_ = json.Unmarshal(rr.Body.Bytes(), &resp)

		if resp["status"] != "healthy" {
			t.Errorf("expected status healthy, got %v", resp["status"])
		}
		if int(resp["pending_queue_count"].(float64)) != 1 {
			t.Errorf("expected pending_queue_count 1, got %v", resp["pending_queue_count"])
		}
		if int(resp["dlq_count"].(float64)) != 1 {
			t.Errorf("expected dlq_count 1, got %v", resp["dlq_count"])
		}
	})

	t.Run("GET /api/v1/pending", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/pending?limit=10&offset=0", nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rr.Code)
		}

		var resp struct {
			Total int                   `json:"total"`
			Items []storage.PendingItem `json:"items"`
		}
		_ = json.Unmarshal(rr.Body.Bytes(), &resp)

		if resp.Total != 1 || len(resp.Items) != 1 {
			t.Errorf("expected 1 item, got %d (total: %d)", len(resp.Items), resp.Total)
		}
		if resp.Items[0].IssueKey != "DEV-10" {
			t.Errorf("expected DEV-10, got %s", resp.Items[0].IssueKey)
		}
	})

	t.Run("GET /api/v1/dlq", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/dlq", nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rr.Code)
		}

		var resp struct {
			Total int               `json:"total"`
			Items []storage.DLQItem `json:"items"`
		}
		_ = json.Unmarshal(rr.Body.Bytes(), &resp)

		if resp.Total != 1 || len(resp.Items) != 1 {
			t.Errorf("expected 1 DLQ item, got %d", len(resp.Items))
		}
	})

	t.Run("GET /api/v1/loads", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/loads?group=support", nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rr.Code)
		}

		var resp struct {
			Group string                                `json:"group"`
			Loads map[string]storage.AssigneeLoadRecord `json:"loads"`
		}
		_ = json.Unmarshal(rr.Body.Bytes(), &resp)

		if resp.Group != "support" {
			t.Errorf("expected group support, got %s", resp.Group)
		}
		if resp.Loads["alice"].CurrentLoad != 1 {
			t.Errorf("expected Alice load 1, got %d", resp.Loads["alice"].CurrentLoad)
		}
	})

	t.Run("GET /metrics", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rr.Code)
		}

		metricsOutput := rr.Body.String()
		if len(metricsOutput) == 0 {
			t.Fatalf("expected non-empty metrics output")
		}
	})
}
