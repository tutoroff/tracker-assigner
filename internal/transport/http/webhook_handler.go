package http

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
)

// IssueReference represents a basic reference (e.g. Queue, Status, User).
type IssueReference struct {
	ID       string `json:"id,omitempty"`
	Key      string `json:"key,omitempty"`
	Display  string `json:"display,omitempty"`
	Login    string `json:"login,omitempty"`
	CloudUID string `json:"cloudUid,omitempty"`
	Name     string `json:"name,omitempty"`
}

// NamedItem represents an item with name or id (e.g. Component, Tag).
type NamedItem struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
	Key  string `json:"key,omitempty"`
}

// TrackerIssue represents the issue structure in a Tracker webhook.
type TrackerIssue struct {
	ID          string          `json:"id,omitempty"`
	Key         string          `json:"key,omitempty"`
	Summary     string          `json:"summary,omitempty"`
	Description string          `json:"description,omitempty"`
	Queue       *IssueReference `json:"queue,omitempty"`
	Status      *IssueReference `json:"status,omitempty"`
	Assignee    *IssueReference `json:"assignee,omitempty"`
	CreatedBy   *IssueReference `json:"createdBy,omitempty"`
	UpdatedBy   *IssueReference `json:"updatedBy,omitempty"`
	Components  any             `json:"components,omitempty"`
	Tags        any             `json:"tags,omitempty"`
	CreatedAt   string          `json:"createdAt,omitempty"`
	UpdatedAt   string          `json:"updatedAt,omitempty"`
}

// FieldChange represents before and after values for a changed field.
type FieldChange struct {
	From any `json:"from,omitempty"`
	To   any `json:"to,omitempty"`
}

// WebhookPayload represents the standard Tracker webhook body.
type WebhookPayload struct {
	Event     string                 `json:"event,omitempty"`
	Events    []string               `json:"events,omitempty"`
	Issue     *TrackerIssue          `json:"issue,omitempty"`
	Changes   map[string]FieldChange `json:"changes,omitempty"`
	OrgID     string                 `json:"orgId,omitempty"`
	UserID    string                 `json:"userId,omitempty"`
	UpdatedBy *IssueReference        `json:"updatedBy,omitempty"`

	// Fallback top-level fields when payload is flat
	Key         string          `json:"key,omitempty"`
	Summary     string          `json:"summary,omitempty"`
	Description string          `json:"description,omitempty"`
	Queue       *IssueReference `json:"queue,omitempty"`
	Status      *IssueReference `json:"status,omitempty"`
	Assignee    *IssueReference `json:"assignee,omitempty"`
	Components  any             `json:"components,omitempty"`
	Tags        any             `json:"tags,omitempty"`

	// Internal routing & analytics metadata
	QueueWaitDuration time.Duration `json:"-"`
	Source            string        `json:"-"`
}

// GetSummary returns issue summary/title safely.
func (p *WebhookPayload) GetSummary() string {
	if p.Issue != nil && p.Issue.Summary != "" {
		return p.Issue.Summary
	}
	return p.Summary
}

// GetDescription returns issue description safely.
func (p *WebhookPayload) GetDescription() string {
	if p.Issue != nil && p.Issue.Description != "" {
		return p.Issue.Description
	}
	return p.Description
}

// GetIssueKey returns the issue key safely.
func (p *WebhookPayload) GetIssueKey() string {
	key := ""
	if p.Issue != nil && p.Issue.Key != "" {
		key = p.Issue.Key
	} else {
		key = p.Key
	}
	if strings.HasPrefix(key, "<{") || strings.HasPrefix(key, "{{") {
		return ""
	}
	return key
}

// GetQueueKey returns the queue key safely.
func (p *WebhookPayload) GetQueueKey() string {
	queue := ""
	if p.Issue != nil && p.Issue.Queue != nil {
		if p.Issue.Queue.Key != "" {
			queue = p.Issue.Queue.Key
		} else {
			queue = p.Issue.Queue.ID
		}
	} else if p.Queue != nil {
		if p.Queue.Key != "" {
			queue = p.Queue.Key
		} else {
			queue = p.Queue.ID
		}
	}
	if strings.HasPrefix(queue, "<{") || strings.HasPrefix(queue, "{{") {
		return ""
	}
	return queue
}

// GetAssigneeRef returns the IssueReference for the assignee if present.
func (p *WebhookPayload) GetAssigneeRef() *IssueReference {
	if p.Issue != nil && p.Issue.Assignee != nil {
		return p.Issue.Assignee
	}
	if p.Assignee != nil {
		return p.Assignee
	}
	return nil
}

// GetAssigneeLogin returns assignee's login, CloudUID, or ID if present.
func (p *WebhookPayload) GetAssigneeLogin() string {
	ref := p.GetAssigneeRef()
	if ref == nil {
		return ""
	}
	if ref.Login != "" {
		return ref.Login
	}
	if ref.CloudUID != "" {
		return ref.CloudUID
	}
	return ref.ID
}

// GetUpdatedByRef returns the IssueReference of the user who modified the issue.
func (p *WebhookPayload) GetUpdatedByRef() *IssueReference {
	if p.Issue != nil && p.Issue.UpdatedBy != nil {
		return p.Issue.UpdatedBy
	}
	if p.UpdatedBy != nil {
		return p.UpdatedBy
	}
	return nil
}

// IsActorRobot returns true if the event was triggered by the configured robot.
func (p *WebhookPayload) IsActorRobot(robotLogin, robotUID, robotCloudUID string) bool {
	if robotLogin == "" && robotUID == "" && robotCloudUID == "" {
		return false
	}
	ref := p.GetUpdatedByRef()
	if ref != nil {
		if robotLogin != "" && strings.EqualFold(ref.Login, robotLogin) {
			return true
		}
		if robotUID != "" && (strings.EqualFold(ref.ID, robotUID) || strings.EqualFold(ref.Key, robotUID)) {
			return true
		}
		if robotCloudUID != "" && strings.EqualFold(ref.CloudUID, robotCloudUID) {
			return true
		}
	}
	if p.UserID != "" {
		if robotUID != "" && strings.EqualFold(p.UserID, robotUID) {
			return true
		}
		if robotLogin != "" && strings.EqualFold(p.UserID, robotLogin) {
			return true
		}
		if robotCloudUID != "" && strings.EqualFold(p.UserID, robotCloudUID) {
			return true
		}
	}
	return false
}

// GetStatusKey returns status key safely.
func (p *WebhookPayload) GetStatusKey() string {
	if p.Issue != nil && p.Issue.Status != nil {
		if p.Issue.Status.Key != "" {
			return strings.ToLower(p.Issue.Status.Key)
		}
		return strings.ToLower(p.Issue.Status.ID)
	}
	if p.Status != nil {
		if p.Status.Key != "" {
			return strings.ToLower(p.Status.Key)
		}
		return strings.ToLower(p.Status.ID)
	}
	return ""
}

// GetComponents returns slice of component names.
func (p *WebhookPayload) GetComponents() []string {
	if p.Issue != nil && p.Issue.Components != nil {
		return extractStringList(p.Issue.Components)
	}
	return extractStringList(p.Components)
}

// GetTags returns tags slice.
func (p *WebhookPayload) GetTags() []string {
	if p.Issue != nil && p.Issue.Tags != nil {
		return extractStringList(p.Issue.Tags)
	}
	return extractStringList(p.Tags)
}

func extractStringList(val any) []string {
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case string:
		if v == "" || strings.HasPrefix(v, "<{") || strings.HasPrefix(v, "{{") {
			return nil
		}
		parts := strings.Split(v, ",")
		var res []string
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				res = append(res, trimmed)
			}
		}
		return res
	case []string:
		return v
	case []any:
		var res []string
		for _, item := range v {
			if s, ok := item.(string); ok && s != "" {
				res = append(res, s)
			} else if m, ok := item.(map[string]any); ok {
				if name, ok := m["name"].(string); ok && name != "" {
					res = append(res, name)
				} else if key, ok := m["key"].(string); ok && key != "" {
					res = append(res, key)
				}
			}
		}
		return res
	case []NamedItem:
		var res []string
		for _, item := range v {
			if item.Name != "" {
				res = append(res, item.Name)
			} else if item.Key != "" {
				res = append(res, item.Key)
			}
		}
		return res
	}
	return nil
}

// IsClosed checks if the issue transitioned to a closed/resolved state.
func (p *WebhookPayload) IsClosed() bool {
	status := p.GetStatusKey()
	switch status {
	case "closed", "resolved", "done", "fixed":
		return true
	}
	if change, ok := p.Changes["status"]; ok {
		if toMap, ok := change.To.(map[string]any); ok {
			if key, ok := toMap["key"].(string); ok {
				k := strings.ToLower(key)
				return k == "closed" || k == "resolved" || k == "done" || k == "fixed"
			}
		}
	}
	return false
}

// WebhookProcessor processes parsed webhook events.
type WebhookProcessor interface {
	ProcessWebhook(ctx context.Context, payload *WebhookPayload, rawBody []byte) error
}

// ErrorReporter reports critical server errors to external tracking systems.
type ErrorReporter interface {
	ReportError(ctx context.Context, endpoint string, err error, requestBody []byte) (string, error)
}

// WebhookHandler handles incoming webhooks from Tracker.
type WebhookHandler struct {
	secretToken   string
	logger        *zap.Logger
	processor     WebhookProcessor
	errorReporter ErrorReporter
}

// NewWebhookHandler creates a new handler.
func NewWebhookHandler(secretToken string, logger *zap.Logger, processor WebhookProcessor) *WebhookHandler {
	return &WebhookHandler{
		secretToken: secretToken,
		logger:      logger,
		processor:   processor,
	}
}

// SetSecretToken updates the secret token (for Hot Reload).
func (h *WebhookHandler) SetSecretToken(token string) {
	h.secretToken = token
}

// SetErrorReporter sets an optional error reporter for 500 errors.
func (h *WebhookHandler) SetErrorReporter(reporter ErrorReporter) {
	h.errorReporter = reporter
}

// ServeHTTP handles the HTTP request for incoming webhooks.
func (h *WebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Validate Secret Token if configured
	if h.secretToken != "" {
		tokenHeader := r.Header.Get("X-Secret-Token")
		if tokenHeader == "" {
			tokenHeader = r.Header.Get("Secret-Token")
		}
		if tokenHeader == "" {
			// Also check Authorization header Bearer format
			auth := r.Header.Get("Authorization")
			if strings.HasPrefix(auth, "Bearer ") {
				tokenHeader = strings.TrimPrefix(auth, "Bearer ")
			}
		}

		if tokenHeader != h.secretToken {
			h.logger.Warn("Unauthorized webhook request: secret token mismatch or missing",
				zap.String("remote_addr", r.RemoteAddr))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized: invalid secret token"}`))
			return
		}
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Error("Failed to read webhook request body", zap.Error(err))
		http.Error(w, `{"error":"failed to read body"}`, http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var payload WebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		h.logger.Error("Failed to parse webhook JSON", zap.Error(err), zap.ByteString("body", body))
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}

	h.logger.Info("Received tracker webhook",
		zap.String("issue_key", payload.GetIssueKey()),
		zap.String("queue", payload.GetQueueKey()),
		zap.String("status", payload.GetStatusKey()),
		zap.String("event", payload.Event))

	if h.processor != nil {
		if err := h.processor.ProcessWebhook(r.Context(), &payload, body); err != nil {
			h.logger.Error("Error processing webhook in service",
				zap.String("issue_key", payload.GetIssueKey()),
				zap.Error(err))
			if h.errorReporter != nil {
				go func() {
					_, _ = h.errorReporter.ReportError(context.Background(), "/webhook", err, body)
				}()
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"status":"error","message":"failed to process webhook"}`))
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
