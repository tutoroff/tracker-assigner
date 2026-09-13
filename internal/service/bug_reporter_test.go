package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	thhttp "tracker-assigner/internal/transport/http"

	"go.uber.org/zap"
)

func TestComputeFingerprint(t *testing.T) {
	fp1 := ComputeFingerprint("/webhook", errors.New("database locked"))
	fp2 := ComputeFingerprint("/webhook", errors.New("database locked"))
	fp3 := ComputeFingerprint("/webhook", errors.New("something else"))

	if fp1 == "" {
		t.Fatalf("expected non-empty fingerprint")
	}
	if fp1 != fp2 {
		t.Fatalf("fingerprints should match for identical error, got %s vs %s", fp1, fp2)
	}
	if fp1 == fp3 {
		t.Fatalf("fingerprints should differ for different errors")
	}
	if ComputeFingerprint("/webhook", nil) != "" {
		t.Fatalf("expected empty fingerprint for nil error")
	}
}

func TestTrackerBugReporter_ReportError_NewBug(t *testing.T) {
	var searchCalls int32
	var createCalls int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "_search") {
			atomic.AddInt32(&searchCalls, 1)
			_, _ = w.Write([]byte(`[]`))
			return
		}
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/v2/issues") {
			atomic.AddInt32(&createCalls, 1)
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["queue"] != "BUGS" {
				t.Errorf("expected queue BUGS, got %v", body["queue"])
			}
			_, _ = w.Write([]byte(`{"key":"BUGS-101","summary":"test"}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL: ts.URL,
		Token:   "dummy",
		OrgID:   "123",
	}, nil)

	reporter := NewTrackerBugReporter(client, "BUGS", 1*time.Hour, zap.NewNop())

	key, err := reporter.ReportError(context.Background(), "/webhook", errors.New("unexpected nil pointer"), []byte(`{"event":"test"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key != "BUGS-101" {
		t.Fatalf("expected issue key BUGS-101, got %s", key)
	}

	if atomic.LoadInt32(&searchCalls) != 1 {
		t.Errorf("expected 1 search call, got %d", searchCalls)
	}
	if atomic.LoadInt32(&createCalls) != 1 {
		t.Errorf("expected 1 create call, got %d", createCalls)
	}
}

func TestTrackerBugReporter_Deduplication_InMemory(t *testing.T) {
	var createCalls int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "_search") {
			_, _ = w.Write([]byte(`[]`))
			return
		}
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/v2/issues") {
			atomic.AddInt32(&createCalls, 1)
			_, _ = w.Write([]byte(`{"key":"BUGS-202"}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL: ts.URL,
		Token:   "dummy",
		OrgID:   "123",
	}, nil)

	reporter := NewTrackerBugReporter(client, "BUGS", 1*time.Hour, zap.NewNop())

	errTest := errors.New("connection reset by peer")

	// Call 1: Creates bug
	key1, err := reporter.ReportError(context.Background(), "/api/webhook", errTest, nil)
	if err != nil || key1 != "BUGS-202" {
		t.Fatalf("call 1 failed: key=%s, err=%v", key1, err)
	}

	// Call 2: Identical error -> Suppressed by in-memory deduplication!
	key2, err2 := reporter.ReportError(context.Background(), "/api/webhook", errTest, nil)
	if err2 != nil {
		t.Fatalf("call 2 unexpected error: %v", err2)
	}
	if key2 != "" {
		t.Errorf("expected empty key on in-memory deduplicated skip, got %s", key2)
	}

	if atomic.LoadInt32(&createCalls) != 1 {
		t.Errorf("expected only 1 create call due to deduplication, got %d", createCalls)
	}
}

func TestTrackerBugReporter_Deduplication_RemoteTracker(t *testing.T) {
	var createCalls int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "_search") {
			// Tracker search returns existing open bug
			existing := []thhttp.TrackerIssue{
				{Key: "BUGS-555", Summary: "Existing bug"},
			}
			data, _ := json.Marshal(existing)
			_, _ = w.Write(data)
			return
		}
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/v2/issues") {
			atomic.AddInt32(&createCalls, 1)
			_, _ = w.Write([]byte(`{"key":"BUGS-NEW"}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL: ts.URL,
		Token:   "dummy",
		OrgID:   "123",
	}, nil)

	reporter := NewTrackerBugReporter(client, "BUGS", 1*time.Hour, zap.NewNop())

	key, err := reporter.ReportError(context.Background(), "/webhook", errors.New("already reported bug"), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key != "BUGS-555" {
		t.Fatalf("expected existing key BUGS-555 returned, got %s", key)
	}
	if atomic.LoadInt32(&createCalls) != 0 {
		t.Errorf("expected 0 create calls because bug already exists in Tracker, got %d", createCalls)
	}
}

func TestTrackerBugReporter_TQL_And_Tags(t *testing.T) {
	var capturedTQL string
	var capturedTags []string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "_search") {
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			capturedTQL = body["query"]
			_, _ = w.Write([]byte(`[]`))
			return
		}
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/v2/issues") {
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if rawTags, ok := body["tags"].([]any); ok {
				for _, rt := range rawTags {
					capturedTags = append(capturedTags, rt.(string))
				}
			}
			_, _ = w.Write([]byte(`{"key":"BUGS-999"}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	client := NewTrackerClient(TrackerClientConfig{
		BaseURL: ts.URL,
		Token:   "dummy",
		OrgID:   "123",
	}, nil)

	reporter := NewTrackerBugReporter(client, "BUGS", 1*time.Hour, zap.NewNop())
	errTest := errors.New("panic in webhook processor")
	fp := ComputeFingerprint("/webhook", errTest)

	key, err := reporter.ReportError(context.Background(), "/webhook", errTest, []byte(`{"test":true}`))
	if err != nil || key != "BUGS-999" {
		t.Fatalf("expected BUGS-999, got key=%s, err=%v", key, err)
	}

	if !strings.Contains(capturedTQL, "Resolution: empty()") {
		t.Errorf("expected TQL to check Resolution: empty(), got: %s", capturedTQL)
	}
	if !strings.Contains(capturedTQL, "fp-"+fp) {
		t.Errorf("expected TQL to search for tag fp-%s, got: %s", fp, capturedTQL)
	}

	foundTag := false
	for _, tag := range capturedTags {
		if tag == "fp-"+fp {
			foundTag = true
			break
		}
	}
	if !foundTag {
		t.Errorf("expected created issue to include tag fp-%s, got tags: %v", fp, capturedTags)
	}
}
