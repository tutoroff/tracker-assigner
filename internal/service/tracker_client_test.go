package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestTrackerClient_AssignIssue_Success(t *testing.T) {
	var requestedAuth string
	var requestedOrgID string
	var requestedMethod string
	var requestedPath string
	var reqBody map[string]string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedAuth = r.Header.Get("Authorization")
		requestedOrgID = r.Header.Get("X-Org-ID")
		requestedMethod = r.Method
		requestedPath = r.URL.Path

		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &reqBody)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"1","key":"DEV-101"}`))
	}))
	defer server.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL:    server.URL,
		Token:      "test-token",
		OrgID:      "123456",
		IsCloudOrg: false,
		Timeout:    2 * time.Second,
		MaxRetries: 1,
	}, server.Client())

	ctx := context.Background()
	err := client.AssignIssue(ctx, "DEV-101", "alice")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if requestedMethod != http.MethodPatch {
		t.Errorf("expected PATCH, got %s", requestedMethod)
	}
	if requestedPath != "/v2/issues/DEV-101" {
		t.Errorf("expected /v2/issues/DEV-101, got %s", requestedPath)
	}
	if requestedAuth != "OAuth test-token" {
		t.Errorf("expected 'OAuth test-token', got %s", requestedAuth)
	}
	if requestedOrgID != "123456" {
		t.Errorf("expected '123456', got %s", requestedOrgID)
	}
	if reqBody["assignee"] != "alice" {
		t.Errorf("expected assignee 'alice', got %s", reqBody["assignee"])
	}
}

func TestTrackerClient_CloudOrgHeader(t *testing.T) {
	var cloudOrgHeader string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cloudOrgHeader = r.Header.Get("X-Cloud-Org-ID")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL:    server.URL,
		Token:      "test-token",
		OrgID:      "cloud-123",
		IsCloudOrg: true,
	}, server.Client())

	_ = client.AssignIssue(context.Background(), "DEV-1", "bob")
	if cloudOrgHeader != "cloud-123" {
		t.Errorf("expected X-Cloud-Org-ID 'cloud-123', got %s", cloudOrgHeader)
	}
}

func TestTrackerClient_ServerError_RetryAndDetection(t *testing.T) {
	var attempts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal server error"}`))
	}))
	defer server.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL:    server.URL,
		Token:      "test-token",
		OrgID:      "123",
		MaxRetries: 2,
	}, server.Client())

	err := client.AssignIssue(context.Background(), "DEV-200", "charlie")
	if err == nil {
		t.Fatalf("expected error on 500, got nil")
	}

	if atomic.LoadInt32(&attempts) != 3 { // 1 initial + 2 retries
		t.Errorf("expected 3 attempts, got %d", atomic.LoadInt32(&attempts))
	}

	if !Is5xx(err) {
		t.Errorf("expected Is5xx to be true for 500 error, got false")
	}
}

func TestTrackerClient_ClientError_NoRetry(t *testing.T) {
	var attempts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid field"}`))
	}))
	defer server.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL:    server.URL,
		Token:      "test-token",
		MaxRetries: 3,
	}, server.Client())

	err := client.AssignIssue(context.Background(), "DEV-300", "david")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	if atomic.LoadInt32(&attempts) != 1 {
		t.Errorf("expected 1 attempt (no retries for 4xx), got %d", atomic.LoadInt32(&attempts))
	}
	if Is5xx(err) {
		t.Errorf("expected Is5xx to be false for 400 error")
	}
}

func TestTrackerClient_ClearAssignee(t *testing.T) {
	var requestedMethod string
	var requestedPath string
	var rawBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedMethod = r.Method
		requestedPath = r.URL.Path
		rawBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"1","key":"DEV-400","assignee":null}`))
	}))
	defer server.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL: server.URL,
		Token:   "test-token",
	}, server.Client())

	err := client.ClearAssignee(context.Background(), "DEV-400")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if requestedMethod != http.MethodPatch {
		t.Errorf("expected PATCH, got %s", requestedMethod)
	}
	if requestedPath != "/v2/issues/DEV-400" {
		t.Errorf("expected /v2/issues/DEV-400, got %s", requestedPath)
	}
	if string(rawBody) != `{"assignee":null}` {
		t.Errorf("expected body '{\"assignee\":null}', got %s", string(rawBody))
	}
}

func TestTrackerClient_RateLimit_RetryAfter_Seconds(t *testing.T) {
	var attempts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := atomic.AddInt32(&attempts, 1)
		if att == 1 {
			w.Header().Set("Retry-After", "0.1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limit exceeded"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"1","key":"DEV-500"}`))
	}))
	defer server.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL:    server.URL,
		Token:      "test-token",
		MaxRetries: 2,
	}, server.Client())

	start := time.Now()
	err := client.AssignIssue(context.Background(), "DEV-500", "elena")
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected successful assignment after retry, got: %v", err)
	}
	if atomic.LoadInt32(&attempts) != 2 {
		t.Errorf("expected 2 attempts, got %d", atomic.LoadInt32(&attempts))
	}
	if elapsed < 80*time.Millisecond {
		t.Errorf("expected at least 80ms backoff sleep based on Retry-After, took %v", elapsed)
	}
}

func TestTrackerClient_RateLimit_RetryAfter_HttpDate(t *testing.T) {
	var attempts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := atomic.AddInt32(&attempts, 1)
		if att == 1 {
			retryTime := time.Now().Add(200 * time.Millisecond).UTC().Format(http.TimeFormat)
			w.Header().Set("Retry-After", retryTime)
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limit exceeded"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"2","key":"DEV-501"}`))
	}))
	defer server.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL:    server.URL,
		Token:      "test-token",
		MaxRetries: 1,
	}, server.Client())

	err := client.AssignIssue(context.Background(), "DEV-501", "ivan")
	if err != nil {
		t.Fatalf("expected success on retry, got: %v", err)
	}
	if atomic.LoadInt32(&attempts) != 2 {
		t.Errorf("expected 2 attempts, got %d", atomic.LoadInt32(&attempts))
	}
}

func TestTrackerClient_RateLimit_ExhaustedRetries(t *testing.T) {
	var attempts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"slow down"}`))
	}))
	defer server.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL:    server.URL,
		Token:      "test-token",
		MaxRetries: 1,
	}, server.Client())

	err := client.AssignIssue(context.Background(), "DEV-502", "olga")
	if err == nil {
		t.Fatal("expected error after retries exhausted, got nil")
	}

	if atomic.LoadInt32(&attempts) != 2 {
		t.Errorf("expected 2 attempts (1 initial + 1 retry), got %d", atomic.LoadInt32(&attempts))
	}

	if !Is429(err) {
		t.Errorf("expected Is429(err) to be true, got false for: %v", err)
	}

	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected error to be *APIError, got %T", err)
	}
	if !apiErr.IsRateLimit() {
		t.Errorf("expected apiErr.IsRateLimit() to be true")
	}
	if apiErr.StatusCode != http.StatusTooManyRequests {
		t.Errorf("expected status 429, got %d", apiErr.StatusCode)
	}
	if apiErr.RetryAfter != 1*time.Second {
		t.Errorf("expected RetryAfter 1s, got %v", apiErr.RetryAfter)
	}
}

func TestTrackerClient_RateLimit_MissingHeader_ExponentialFallback(t *testing.T) {
	var attempts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := atomic.AddInt32(&attempts, 1)
		if att == 1 {
			// 429 without Retry-After header
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limit"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"3","key":"DEV-503"}`))
	}))
	defer server.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL:    server.URL,
		Token:      "test-token",
		MaxRetries: 1,
	}, server.Client())

	err := client.AssignIssue(context.Background(), "DEV-503", "sergey")
	if err != nil {
		t.Fatalf("expected success with exponential fallback, got %v", err)
	}
	if atomic.LoadInt32(&attempts) != 2 {
		t.Errorf("expected 2 attempts, got %d", atomic.LoadInt32(&attempts))
	}
}

func TestTrackerClient_RateLimit_ContextCanceled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "10")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"rate limit"}`))
	}))
	defer server.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL:    server.URL,
		Token:      "test-token",
		MaxRetries: 2,
	}, server.Client())

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := client.AssignIssue(ctx, "DEV-504", "viktor")
	if err == nil {
		t.Fatal("expected error due to canceled context, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Errorf("expected context deadline exceeded, got: %v", err)
	}
}

func TestTrackerClient_RateLimit_RetryAfter_QuotedAndDurationFormat(t *testing.T) {
	var attempts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := atomic.AddInt32(&attempts, 1)
		if att == 1 {
			// Quoted duration string
			w.Header().Set("Retry-After", `"100ms"`)
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limit exceeded"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"99","key":"DEV-999"}`))
	}))
	defer server.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL:    server.URL,
		Token:      "test-token",
		MaxRetries: 2,
	}, server.Client())

	start := time.Now()
	err := client.AssignIssue(context.Background(), "DEV-999", "alex")
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected success on retry, got: %v", err)
	}
	if atomic.LoadInt32(&attempts) != 2 {
		t.Errorf("expected 2 attempts, got %d", atomic.LoadInt32(&attempts))
	}
	if elapsed < 80*time.Millisecond {
		t.Errorf("expected at least 80ms backoff sleep based on Retry-After 100ms, took %v", elapsed)
	}
}

func TestTrackerClient_RateLimit_NilSafety(t *testing.T) {
	if Is429(nil) {
		t.Error("expected Is429(nil) to be false")
	}
	if Is5xx(nil) {
		t.Error("expected Is5xx(nil) to be false")
	}

	var nilErr *APIError
	if nilErr.IsRateLimit() {
		t.Error("expected (*APIError)(nil).IsRateLimit() to be false")
	}
	if nilErr.IsServerError() {
		t.Error("expected (*APIError)(nil).IsServerError() to be false")
	}

	// Verify parseRetryAfter directly with various edge cases
	cases := []struct {
		input    string
		expected time.Duration
	}{
		{"", 0},
		{"0", 0},
		{"-5", 0},
		{"1", 1 * time.Second},
		{`"2"`, 2 * time.Second},
		{"500ms", 500 * time.Millisecond},
		{`"250ms"`, 250 * time.Millisecond},
		{"invalid-garbage", 0},
	}
	for _, tc := range cases {
		dur := parseRetryAfter(tc.input)
		if dur != tc.expected {
			t.Errorf("parseRetryAfter(%q) = %v, expected %v", tc.input, dur, tc.expected)
		}
	}
}

func TestTrackerClient_GetUser(t *testing.T) {
	var reqPath string
	var reqMethod string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqPath = r.URL.Path
		reqMethod = r.Method
		if r.URL.Path == "/v2/users/alice" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"self": "https://api.tracker.yandex.net/v2/users/1120000000000001",
				"uid": 1120000000000001,
				"login": "alice",
				"trackerUid": 1001,
				"cloudUid": "c-alice-uid",
				"firstName": "Alice",
				"lastName": "Smith",
				"display": "Alice Smith",
				"email": "alice@example.com",
				"dismissed": false,
				"hasLicense": true
			}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"errorMessages":["User not found"]}`))
	}))
	defer server.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL: server.URL,
		Token:   "test-token",
	}, server.Client())

	// 1. Success case
	user, err := client.GetUser(context.Background(), "alice")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if reqMethod != http.MethodGet || reqPath != "/v2/users/alice" {
		t.Errorf("expected GET /v2/users/alice, got %s %s", reqMethod, reqPath)
	}
	if user.Login != "alice" || user.UID != 1120000000000001 || user.CloudUID != "c-alice-uid" || !user.HasLicense {
		t.Errorf("unexpected user profile data: %+v", user)
	}

	// 2. Not found case
	notFoundUser, err := client.GetUser(context.Background(), "unknown")
	if err == nil {
		t.Fatal("expected error on 404, got nil")
	}
	if notFoundUser != nil {
		t.Errorf("expected nil user on error, got %+v", notFoundUser)
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
		t.Errorf("expected APIError with status 404, got %v", err)
	}
}

func TestTrackerUser_GetFullName(t *testing.T) {
	tests := []struct {
		name     string
		user     TrackerUser
		expected string
	}{
		{
			name:     "display takes precedence",
			user:     TrackerUser{Display: "Display Name", FirstName: "First", LastName: "Last", Login: "user1"},
			expected: "Display Name",
		},
		{
			name:     "first and last name when display is empty",
			user:     TrackerUser{FirstName: "Ivan", LastName: "Ivanov", Login: "iivanov"},
			expected: "Ivan Ivanov",
		},
		{
			name:     "first name only",
			user:     TrackerUser{FirstName: "Petr", Login: "ppetr"},
			expected: "Petr",
		},
		{
			name:     "last name only",
			user:     TrackerUser{LastName: "Sidorov", Login: "sidorov"},
			expected: "Sidorov",
		},
		{
			name:     "fallback to login when all names empty",
			user:     TrackerUser{Login: "robot-assigner"},
			expected: "robot-assigner",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.user.GetFullName()
			if got != tc.expected {
				t.Errorf("GetFullName() = %q, want %q", got, tc.expected)
			}
		})
	}
}

func TestTrackerClient_GetIssue(t *testing.T) {
	var reqPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqPath = r.URL.Path
		if r.URL.Path == "/v2/issues/DEV-101" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"id": "issue-101",
				"key": "DEV-101",
				"summary": "Fix connection timeout",
				"description": "Reproduced on staging",
				"queue": {"key": "DEV", "display": "Development"}
			}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL: server.URL,
		Token:   "test-token",
	}, server.Client())

	issue, err := client.GetIssue(context.Background(), "DEV-101")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reqPath != "/v2/issues/DEV-101" {
		t.Errorf("expected path /v2/issues/DEV-101, got %s", reqPath)
	}
	if issue.Key != "DEV-101" || issue.Summary != "Fix connection timeout" || issue.Queue.Key != "DEV" {
		t.Errorf("unexpected issue data: %+v", issue)
	}

	// Error case
	_, err = client.GetIssue(context.Background(), "DEV-999")
	if err == nil {
		t.Fatal("expected error on 404, got nil")
	}
}

func TestTrackerClient_SearchIssues(t *testing.T) {
	var reqPath string
	var reqBody map[string]string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqPath = r.URL.RequestURI()
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &reqBody)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[
			{"key":"DEV-1","summary":"Task 1"},
			{"key":"DEV-2","summary":"Task 2"}
		]`))
	}))
	defer server.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL: server.URL,
		Token:   "test-token",
	}, server.Client())

	// 1. Explicit pagination
	issues, err := client.SearchIssues(context.Background(), `Queue: DEV`, 2, 20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reqPath != "/v2/issues/_search?page=2&perPage=20" {
		t.Errorf("expected URI /v2/issues/_search?page=2&perPage=20, got %s", reqPath)
	}
	if reqBody["query"] != "Queue: DEV" {
		t.Errorf("expected query 'Queue: DEV', got %s", reqBody["query"])
	}
	if len(issues) != 2 || issues[0].Key != "DEV-1" || issues[1].Key != "DEV-2" {
		t.Errorf("unexpected search results: %+v", issues)
	}

	// 2. Default pagination (page <= 0 -> 1, perPage <= 0 -> 50)
	_, err = client.SearchIssues(context.Background(), `Queue: DEV`, 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reqPath != "/v2/issues/_search?page=1&perPage=50" {
		t.Errorf("expected URI /v2/issues/_search?page=1&perPage=50, got %s", reqPath)
	}
}

func TestTrackerClient_AddComment(t *testing.T) {
	var reqPath string
	var reqBody map[string]string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &reqBody)

		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"comment-1","text":"Assigned automatically"}`))
	}))
	defer server.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL: server.URL,
		Token:   "test-token",
	}, server.Client())

	err := client.AddComment(context.Background(), "DEV-500", "Assigned automatically")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reqPath != "/v2/issues/DEV-500/comments" {
		t.Errorf("expected path /v2/issues/DEV-500/comments, got %s", reqPath)
	}
	if reqBody["text"] != "Assigned automatically" {
		t.Errorf("expected comment text, got %s", reqBody["text"])
	}
}

func TestTrackerClient_CreateIssue(t *testing.T) {
	var reqPath string
	var reqBody map[string]interface{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqPath = r.URL.Path
		reqBody = make(map[string]interface{})
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &reqBody)

		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"bug-1","key":"BUGS-1","summary":"Test crash"}`))
	}))
	defer server.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL: server.URL,
		Token:   "test-token",
	}, server.Client())

	// 1. With issueType and tags
	issue, err := client.CreateIssue(context.Background(), "BUGS", "Test crash", "Stack trace details", "bug", []string{"urgent", "crash"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reqPath != "/v2/issues/" {
		t.Errorf("expected /v2/issues/, got %s", reqPath)
	}
	if reqBody["queue"] != "BUGS" || reqBody["summary"] != "Test crash" || reqBody["type"] != "bug" {
		t.Errorf("unexpected payload fields: %+v", reqBody)
	}
	if issue.Key != "BUGS-1" {
		t.Errorf("expected key BUGS-1, got %s", issue.Key)
	}

	// 2. Without issueType and tags
	_, err = client.CreateIssue(context.Background(), "BUGS", "Simple task", "Description", "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := reqBody["type"]; ok {
		t.Errorf("expected 'type' to be omitted when empty, got %v", reqBody["type"])
	}
	if _, ok := reqBody["tags"]; ok {
		t.Errorf("expected 'tags' to be omitted when nil/empty, got %v", reqBody["tags"])
	}
}

func TestTrackerClient_GetTransitions(t *testing.T) {
	var reqPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[
			{"id":"in_progress","display":"In Progress","to":{"key":"inProgress","id":"2"}},
			{"id":"close","display":"Closed","to":{"key":"closed","id":"3"}}
		]`))
	}))
	defer server.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL: server.URL,
		Token:   "test-token",
	}, server.Client())

	transitions, err := client.GetTransitions(context.Background(), "DEV-10")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reqPath != "/v2/issues/DEV-10/transitions" {
		t.Errorf("expected /v2/issues/DEV-10/transitions, got %s", reqPath)
	}
	if len(transitions) != 2 || transitions[0].ID != "in_progress" || transitions[1].To.Key != "closed" {
		t.Errorf("unexpected transitions: %+v", transitions)
	}
}

func TestTrackerClient_TransitionIssue_DirectAndFallback(t *testing.T) {
	t.Run("direct transition success", func(t *testing.T) {
		var executedPath string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			executedPath = r.URL.Path
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer server.Close()

		client := NewTrackerClient(TrackerClientConfig{
			BaseURL: server.URL,
			Token:   "test-token",
		}, server.Client())

		err := client.TransitionIssue(context.Background(), "DEV-20", "in_progress")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if executedPath != "/v2/issues/DEV-20/transitions/in_progress/_execute" {
			t.Errorf("expected direct execute path, got %s", executedPath)
		}
	})

	t.Run("fallback matching transition id on 404", func(t *testing.T) {
		var executedPaths []string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			executedPaths = append(executedPaths, r.URL.Path)
			switch r.URL.Path {
			case "/v2/issues/DEV-21/transitions/in-progress/_execute":
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error":"transition not found"}`))
			case "/v2/issues/DEV-21/transitions":
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[
					{"id":"in_progress","display":"Start Work","to":{"key":"inProgress","id":"2"}}
				]`))
			case "/v2/issues/DEV-21/transitions/in_progress/_execute":
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"status":"inProgress"}`))
			default:
				w.WriteHeader(http.StatusBadRequest)
			}
		}))
		defer server.Close()

		client := NewTrackerClient(TrackerClientConfig{
			BaseURL: server.URL,
			Token:   "test-token",
		}, server.Client())

		err := client.TransitionIssue(context.Background(), "DEV-21", "in-progress")
		if err != nil {
			t.Fatalf("expected fallback to succeed, got %v", err)
		}
		if len(executedPaths) != 3 {
			t.Errorf("expected 3 requests (failed execute, get transitions, dynamic execute), got %v", executedPaths)
		}
	})

	t.Run("fallback matching target status key or id", func(t *testing.T) {
		var successfulExecution string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/v2/issues/DEV-22/transitions/need_info/_execute":
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"bad request"}`))
			case "/v2/issues/DEV-22/transitions":
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[
					{"id":"ask_user","display":"Ask Info","to":{"key":"need_info","id":"4"}}
				]`))
			case "/v2/issues/DEV-22/transitions/ask_user/_execute":
				successfulExecution = r.URL.Path
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()

		client := NewTrackerClient(TrackerClientConfig{
			BaseURL: server.URL,
			Token:   "test-token",
		}, server.Client())

		err := client.TransitionIssue(context.Background(), "DEV-22", "need_info")
		if err != nil {
			t.Fatalf("expected successful fallback, got %v", err)
		}
		if successfulExecution != "/v2/issues/DEV-22/transitions/ask_user/_execute" {
			t.Errorf("expected /v2/issues/DEV-22/transitions/ask_user/_execute, got %s", successfulExecution)
		}
	})

	t.Run("fallback matching display name", func(t *testing.T) {
		var executedFallback string
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/v2/issues/DEV-23/transitions/close_ticket/_execute":
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error":"not found"}`))
			case "/v2/issues/DEV-23/transitions":
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[
					{"id":"t-99","display":"Close Ticket","to":{"key":"resolved","id":"5"}}
				]`))
			case "/v2/issues/DEV-23/transitions/t-99/_execute":
				executedFallback = r.URL.Path
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{}`))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()

		client := NewTrackerClient(TrackerClientConfig{
			BaseURL: server.URL,
			Token:   "test-token",
		}, server.Client())

		err := client.TransitionIssue(context.Background(), "DEV-23", "close_ticket")
		if err != nil {
			t.Fatalf("expected fallback by display name, got %v", err)
		}
		if executedFallback != "/v2/issues/DEV-23/transitions/t-99/_execute" {
			t.Errorf("expected /v2/issues/DEV-23/transitions/t-99/_execute, got %s", executedFallback)
		}
	})

	t.Run("fallback fails when no matching transitions exist", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/transitions") {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[
					{"id":"other","display":"Other Action"}
				]`))
				return
			}
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`))
		}))
		defer server.Close()

		client := NewTrackerClient(TrackerClientConfig{
			BaseURL: server.URL,
			Token:   "test-token",
		}, server.Client())

		err := client.TransitionIssue(context.Background(), "DEV-24", "nonexistent")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
			t.Errorf("expected original 404 error returned, got %v", err)
		}
	})
}

func TestTrackerClient_DefaultsAndAPIError(t *testing.T) {
	// Test NewTrackerClient defaults
	c := NewTrackerClient(TrackerClientConfig{
		MaxRetries: -1,
	}, nil)
	if c.baseURL != "https://api.tracker.yandex.net" {
		t.Errorf("expected default baseURL, got %s", c.baseURL)
	}
	if c.timeout != 15*time.Second {
		t.Errorf("expected default timeout 15s, got %v", c.timeout)
	}
	if c.maxRetries != 2 {
		t.Errorf("expected default maxRetries 2, got %d", c.maxRetries)
	}
	if c.httpClient == nil {
		t.Errorf("expected default httpClient, got nil")
	}

	// Test APIError.Error()
	err := &APIError{
		StatusCode: 404,
		Message:    "Issue not found",
		Endpoint:   "/v2/issues/DEV-404",
	}
	expectedStr := "yandex tracker API error 404 on /v2/issues/DEV-404: Issue not found"
	if err.Error() != expectedStr {
		t.Errorf("APIError.Error() = %q, want %q", err.Error(), expectedStr)
	}
}

func TestTrackerClient_TransitionIssue_NormalizedWithSpacesAndHyphens(t *testing.T) {
	var executedFallback string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/issues/DEV-25/transitions/Close Ticket/_execute",
			"/v2/issues/DEV-25/transitions/Close%20Ticket/_execute":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`))
		case "/v2/issues/DEV-25/transitions":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[
				{"id":"t-close","display":"Close Ticket","to":{"key":"closed","id":"3"}}
			]`))
		case "/v2/issues/DEV-25/transitions/t-close/_execute":
			executedFallback = r.URL.Path
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL: server.URL,
		Token:   "test-token",
	}, server.Client())

	// Call with space in transition ID matching display name
	err := client.TransitionIssue(context.Background(), "DEV-25", "Close Ticket")
	if err != nil {
		t.Fatalf("expected transition with spaces to succeed via fallback, got: %v", err)
	}
	if executedFallback != "/v2/issues/DEV-25/transitions/t-close/_execute" {
		t.Errorf("expected executed path /v2/issues/DEV-25/transitions/t-close/_execute, got %s", executedFallback)
	}
}

func TestTrackerClient_Execute_ContextCanceled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL:    server.URL,
		Token:      "test-token",
		MaxRetries: 2,
	}, server.Client())

	// 1. Pre-canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.AssignIssue(ctx, "DEV-100", "alice")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled for pre-canceled context, got: %v", err)
	}

	// 2. Client with MaxRetries = 0 on pre-canceled context
	clientNoRetries := NewTrackerClient(TrackerClientConfig{
		BaseURL:    server.URL,
		Token:      "test-token",
		MaxRetries: 0,
	}, server.Client())

	err2 := clientNoRetries.AssignIssue(ctx, "DEV-100", "alice")
	if !errors.Is(err2, context.Canceled) {
		t.Errorf("expected context.Canceled for 0 retries on canceled context, got: %v", err2)
	}
}


