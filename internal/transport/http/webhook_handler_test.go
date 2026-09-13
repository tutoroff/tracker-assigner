package http

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"
)

type mockProcessor struct {
	processedPayload *WebhookPayload
	called           bool
	err              error
}

func (m *mockProcessor) ProcessWebhook(ctx context.Context, payload *WebhookPayload, rawBody []byte) error {
	m.called = true
	m.processedPayload = payload
	return m.err
}

func TestWebhookHandler_Authentication(t *testing.T) {
	logger := zap.NewNop()
	secret := "my-secret-token"
	handler := NewWebhookHandler(secret, logger, nil)

	t.Run("Valid Token via X-Secret-Token", func(t *testing.T) {
		body := `{"issue":{"key":"DEV-100","queue":{"key":"DEV"}}}`
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewBufferString(body))
		req.Header.Set("X-Secret-Token", secret)
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rr.Code)
		}
	})

	t.Run("Valid Token via Bearer Authorization", func(t *testing.T) {
		body := `{"issue":{"key":"DEV-100","queue":{"key":"DEV"}}}`
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer "+secret)
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rr.Code)
		}
	})

	t.Run("Invalid Token", func(t *testing.T) {
		body := `{"issue":{"key":"DEV-100"}}`
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewBufferString(body))
		req.Header.Set("X-Secret-Token", "wrong-secret")
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("expected status 401, got %d", rr.Code)
		}
	})

	t.Run("Missing Token", func(t *testing.T) {
		body := `{"issue":{"key":"DEV-100"}}`
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewBufferString(body))
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("expected status 401, got %d", rr.Code)
		}
	})

	t.Run("Method Not Allowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/webhook", nil)
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected status 405, got %d", rr.Code)
		}
	})

	t.Run("Malformed JSON", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewBufferString("not-json"))
		req.Header.Set("X-Secret-Token", secret)
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected status 400, got %d", rr.Code)
		}
	})
}

func TestWebhookHandler_ProcessorDelegation(t *testing.T) {
	logger := zap.NewNop()
	proc := &mockProcessor{}
	handler := NewWebhookHandler("", logger, proc)

	body := `{"issue":{"key":"HELP-42","queue":{"key":"HELP"},"status":{"key":"open"},"tags":["vip"],"components":[{"name":"billing"}]}}`
	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewBufferString(body))
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	if !proc.called {
		t.Fatalf("expected processor to be called")
	}
	if proc.processedPayload.GetIssueKey() != "HELP-42" {
		t.Errorf("expected issue key HELP-42, got %s", proc.processedPayload.GetIssueKey())
	}
	if proc.processedPayload.GetQueueKey() != "HELP" {
		t.Errorf("expected queue key HELP, got %s", proc.processedPayload.GetQueueKey())
	}
	if len(proc.processedPayload.GetTags()) != 1 || proc.processedPayload.GetTags()[0] != "vip" {
		t.Errorf("expected tag vip, got %v", proc.processedPayload.GetTags())
	}
	if len(proc.processedPayload.GetComponents()) != 1 || proc.processedPayload.GetComponents()[0] != "billing" {
		t.Errorf("expected component billing, got %v", proc.processedPayload.GetComponents())
	}
}

func TestWebhookHandler_HelpersAndRobotDetection(t *testing.T) {
	// 1. IsActorRobot tests
	pRobot := &WebhookPayload{
		Issue: &TrackerIssue{
			UpdatedBy: &IssueReference{
				Login:    "robot-user",
				ID:       "10001",
				CloudUID: "cloud-uid-123",
			},
		},
	}
	if !pRobot.IsActorRobot("robot-user", "", "") {
		t.Errorf("expected robot detection by login")
	}
	if !pRobot.IsActorRobot("", "10001", "") {
		t.Errorf("expected robot detection by ID")
	}
	if !pRobot.IsActorRobot("", "", "cloud-uid-123") {
		t.Errorf("expected robot detection by cloudUID")
	}
	if pRobot.IsActorRobot("other-user", "999", "other-cloud") {
		t.Errorf("did not expect robot detection for different user")
	}

	// Top level UpdatedBy and UserID
	pRobotFlat := &WebhookPayload{
		UserID: "10001",
	}
	if !pRobotFlat.IsActorRobot("", "10001", "") {
		t.Errorf("expected robot detection by top-level UserID")
	}

	// 2. Assignee helpers
	pAssignee := &WebhookPayload{
		Issue: &TrackerIssue{
			Assignee: &IssueReference{
				Login:    "dev_user",
				CloudUID: "cuid_456",
				ID:       "20002",
			},
		},
	}
	if ref := pAssignee.GetAssigneeRef(); ref == nil || ref.Login != "dev_user" {
		t.Errorf("expected assignee ref with dev_user")
	}
	if login := pAssignee.GetAssigneeLogin(); login != "dev_user" {
		t.Errorf("expected assignee login dev_user, got %s", login)
	}

	// Fallback to CloudUID when login is empty
	pCloudAssignee := &WebhookPayload{
		Assignee: &IssueReference{
			CloudUID: "cuid_789",
			ID:       "30003",
		},
	}
	if login := pCloudAssignee.GetAssigneeLogin(); login != "cuid_789" {
		t.Errorf("expected fallback to cloudUID cuid_789, got %s", login)
	}

	// 3. IsClosed tests
	pClosed1 := &WebhookPayload{
		Issue: &TrackerIssue{
			Status: &IssueReference{Key: "closed"},
		},
	}
	if !pClosed1.IsClosed() {
		t.Errorf("expected closed status to be recognized")
	}

	pClosed2 := &WebhookPayload{
		Changes: map[string]FieldChange{
			"status": {
				To: map[string]any{"key": "resolved"},
			},
		},
	}
	if !pClosed2.IsClosed() {
		t.Errorf("expected resolved transition to be recognized as closed")
	}

	pOpen := &WebhookPayload{
		Issue: &TrackerIssue{
			Status: &IssueReference{Key: "open"},
		},
	}
	if pOpen.IsClosed() {
		t.Errorf("did not expect open status to be recognized as closed")
	}

	// 4. SetSecretToken test
	handler := NewWebhookHandler("old-secret", zap.NewNop(), nil)
	handler.SetSecretToken("new-secret")
	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewBufferString(`{}`))
	req.Header.Set("X-Secret-Token", "new-secret")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected status 200 with new secret, got %d", rr.Code)
	}
}
