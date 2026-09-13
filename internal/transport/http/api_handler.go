package http

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"tracker-assigner/internal/storage"

	"go.uber.org/zap"
)

// APIHandler provides internal REST API endpoints for queue and system observability.
type APIHandler struct {
	repo   storage.Repository
	logger *zap.Logger
}

// NewAPIHandler creates a new APIHandler.
func NewAPIHandler(repo storage.Repository, logger *zap.Logger) *APIHandler {
	return &APIHandler{
		repo:   repo,
		logger: logger,
	}
}

// RegisterRoutes registers REST API endpoints on the given ServeMux.
func (h *APIHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/status", h.handleStatus)
	mux.HandleFunc("GET /api/v1/pending", h.handlePending)
	mux.HandleFunc("GET /api/v1/dlq", h.handleDLQ)
	mux.HandleFunc("GET /api/v1/loads", h.handleLoads)
}

func (h *APIHandler) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	pendingCount, err := h.repo.GetPendingCount(ctx)
	if err != nil {
		h.logger.Error("Failed to get pending count for status API", zap.Error(err))
		http.Error(w, `{"error":"failed to get pending count"}`, http.StatusInternalServerError)
		return
	}

	dlqCount, err := h.repo.GetDLQCount(ctx)
	if err != nil {
		h.logger.Error("Failed to get DLQ count for status API", zap.Error(err))
		http.Error(w, `{"error":"failed to get dlq count"}`, http.StatusInternalServerError)
		return
	}

	// Update Prometheus metrics dynamically on status poll
	MetricPendingQueueSize.Set(float64(pendingCount))
	MetricDLQSize.Set(float64(dlqCount))

	resp := map[string]any{
		"status":              "healthy",
		"pending_queue_count": pendingCount,
		"dlq_count":           dlqCount,
		"timestamp":           time.Now().UTC().Format(time.RFC3339),
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *APIHandler) handlePending(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit, offset := parsePagination(r)

	total, err := h.repo.GetPendingCount(ctx)
	if err != nil {
		h.logger.Error("Failed to count pending items", zap.Error(err))
		http.Error(w, `{"error":"database error"}`, http.StatusInternalServerError)
		return
	}

	items, err := h.repo.GetPendingItems(ctx, limit, offset)
	if err != nil {
		h.logger.Error("Failed to query pending items", zap.Error(err))
		http.Error(w, `{"error":"database error"}`, http.StatusInternalServerError)
		return
	}

	if items == nil {
		items = []storage.PendingItem{}
	}

	resp := map[string]any{
		"total":  total,
		"limit":  limit,
		"offset": offset,
		"items":  items,
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *APIHandler) handleDLQ(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit, offset := parsePagination(r)

	total, err := h.repo.GetDLQCount(ctx)
	if err != nil {
		h.logger.Error("Failed to count DLQ items", zap.Error(err))
		http.Error(w, `{"error":"database error"}`, http.StatusInternalServerError)
		return
	}

	items, err := h.repo.GetDLQItems(ctx, limit, offset)
	if err != nil {
		h.logger.Error("Failed to query DLQ items", zap.Error(err))
		http.Error(w, `{"error":"database error"}`, http.StatusInternalServerError)
		return
	}

	if items == nil {
		items = []storage.DLQItem{}
	}

	resp := map[string]any{
		"total":  total,
		"limit":  limit,
		"offset": offset,
		"items":  items,
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *APIHandler) handleLoads(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	groupID := r.URL.Query().Get("group")
	if groupID == "" {
		http.Error(w, `{"error":"query param 'group' is required"}`, http.StatusBadRequest)
		return
	}

	loads, err := h.repo.GetAssigneesLoad(ctx, groupID)
	if err != nil {
		h.logger.Error("Failed to fetch assignees load for API", zap.String("group", groupID), zap.Error(err))
		http.Error(w, `{"error":"database error"}`, http.StatusInternalServerError)
		return
	}

	resp := map[string]any{
		"group": groupID,
		"loads": loads,
	}

	writeJSON(w, http.StatusOK, resp)
}

func parsePagination(r *http.Request) (limit, offset int) {
	limit = 50
	offset = 0

	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		if l, err := strconv.Atoi(lStr); err == nil && l > 0 && l <= 1000 {
			limit = l
		}
	}
	if oStr := r.URL.Query().Get("offset"); oStr != "" {
		if o, err := strconv.Atoi(oStr); err == nil && o >= 0 {
			offset = o
		}
	}
	return
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
