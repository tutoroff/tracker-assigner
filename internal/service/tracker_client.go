package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	thhttp "tracker-assigner/internal/transport/http"
)

// APIError represents an error returned by the Yandex Tracker API.
type APIError struct {
	StatusCode int
	Message    string
	Endpoint   string
	RetryAfter time.Duration
}

func (e *APIError) Error() string {
	return fmt.Sprintf("yandex tracker API error %d on %s: %s", e.StatusCode, e.Endpoint, e.Message)
}

// IsServerError returns true if HTTP status is 5xx.
func (e *APIError) IsServerError() bool {
	if e == nil {
		return false
	}
	return e.StatusCode >= 500 && e.StatusCode < 600
}

// IsRateLimit returns true if HTTP status is 429.
func (e *APIError) IsRateLimit() bool {
	if e == nil {
		return false
	}
	return e.StatusCode == http.StatusTooManyRequests
}

// parseRetryAfter parses the Retry-After header as seconds or HTTP date.
func parseRetryAfter(headerVal string) time.Duration {
	headerVal = strings.Trim(strings.TrimSpace(headerVal), "\"")
	if headerVal == "" {
		return 0
	}
	// Try parsing as integer seconds
	if sec, err := strconv.Atoi(headerVal); err == nil {
		if sec > 0 {
			return time.Duration(sec) * time.Second
		}
		return 0
	}
	// Try parsing as float seconds (e.g. "0.5")
	if sec, err := strconv.ParseFloat(headerVal, 64); err == nil {
		if sec > 0 {
			return time.Duration(sec * float64(time.Second))
		}
		return 0
	}
	// Try parsing as duration string (e.g. "2s", "500ms")
	if d, err := time.ParseDuration(headerVal); err == nil {
		if d > 0 {
			return d
		}
		return 0
	}
	// Try parsing as HTTP date format (RFC 1123, RFC 850, ANSI C)
	if t, err := http.ParseTime(headerVal); err == nil {
		delay := time.Until(t)
		if delay > 0 {
			return delay
		}
	}
	return 0
}

// TrackerClientConfig configures TrackerClient.
type TrackerClientConfig struct {
	BaseURL    string
	Token      string
	OrgID      string
	IsCloudOrg bool
	Timeout    time.Duration
	MaxRetries int
}

// TrackerClient is an HTTP client for interacting with Yandex Tracker REST API.
type TrackerClient struct {
	baseURL    string
	token      string
	orgID      string
	isCloudOrg bool
	timeout    time.Duration
	maxRetries int
	httpClient *http.Client
}

// NewTrackerClient creates and validates a TrackerClient.
func NewTrackerClient(cfg TrackerClientConfig, client *http.Client) *TrackerClient {
	baseURL := strings.TrimRight(cfg.BaseURL, "/")
	if baseURL == "" {
		baseURL = "https://api.tracker.yandex.net"
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	retries := cfg.MaxRetries
	if retries < 0 {
		retries = 2
	}
	if client == nil {
		client = &http.Client{
			Timeout: timeout,
		}
	}

	return &TrackerClient{
		baseURL:    baseURL,
		token:      cfg.Token,
		orgID:      cfg.OrgID,
		isCloudOrg: cfg.IsCloudOrg,
		timeout:    timeout,
		maxRetries: retries,
		httpClient: client,
	}
}

// Execute performs an HTTP request to Yandex Tracker with auth headers and retries.
func (c *TrackerClient) Execute(ctx context.Context, method, path string, body any, out any) error {
	var bodyBytes []byte
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to marshal request body: %w", err)
		}
		bodyBytes = data
	}

	fullURL := c.baseURL + path

	var lastErr error
	var nextBackoff time.Duration
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if attempt > 0 {
			backoff := nextBackoff
			if backoff <= 0 {
				backoff = time.Duration(1<<attempt) * 100 * time.Millisecond
			}
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		nextBackoff = 0

		var reqBody io.Reader
		if bodyBytes != nil {
			reqBody = bytes.NewReader(bodyBytes)
		}

		req, err := http.NewRequestWithContext(ctx, method, fullURL, reqBody)
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("Authorization", "OAuth "+c.token)
		if c.orgID != "" {
			if c.isCloudOrg {
				req.Header.Set("X-Cloud-Org-ID", c.orgID)
			} else {
				req.Header.Set("X-Org-ID", c.orgID)
			}
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		req.Header.Set("Accept", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = fmt.Errorf("network error on %s %s: %w", method, fullURL, err)
			continue
		}

		respBody, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("failed to read response body: %w", readErr)
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			apiErr := &APIError{
				StatusCode: resp.StatusCode,
				Message:    string(respBody),
				Endpoint:   path,
			}
			if apiErr.IsRateLimit() {
				apiErr.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
				nextBackoff = apiErr.RetryAfter
			}
			lastErr = apiErr

			// Only retry on 5xx and 429
			if apiErr.IsServerError() || apiErr.IsRateLimit() {
				continue
			}
			// 4xx client errors should not be retried
			return apiErr
		}

		if out != nil && len(respBody) > 0 {
			if err := json.Unmarshal(respBody, out); err != nil {
				return fmt.Errorf("failed to decode response JSON: %w", err)
			}
		}

		return nil
	}

	return lastErr
}

// normalizeTransitionString cleans transition identifiers for loose matching.
func normalizeTransitionString(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "_", "")
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, " ", "")
	return s
}

// AssignIssue assigns an issue to a given user login or UID in Yandex Tracker.
func (c *TrackerClient) AssignIssue(ctx context.Context, issueKey, assignee string) error {
	path := fmt.Sprintf("/v2/issues/%s", issueKey)
	reqPayload := map[string]string{
		"assignee": assignee,
	}
	return c.Execute(ctx, http.MethodPatch, path, reqPayload, nil)
}

// ClearAssignee removes the assignee from an issue.
func (c *TrackerClient) ClearAssignee(ctx context.Context, issueKey string) error {
	path := fmt.Sprintf("/v2/issues/%s", issueKey)
	reqPayload := map[string]interface{}{
		"assignee": nil,
	}
	return c.Execute(ctx, http.MethodPatch, path, reqPayload, nil)
}

// TrackerTransition represents an available transition for an issue in Yandex Tracker.
type TrackerTransition struct {
	ID      string                 `json:"id"`
	Display string                 `json:"display,omitempty"`
	To      *thhttp.IssueReference `json:"to,omitempty"`
}

// GetTransitions fetches all available transitions for an issue.
func (c *TrackerClient) GetTransitions(ctx context.Context, issueKey string) ([]TrackerTransition, error) {
	path := fmt.Sprintf("/v2/issues/%s/transitions", issueKey)
	var transitions []TrackerTransition
	if err := c.Execute(ctx, http.MethodGet, path, nil, &transitions); err != nil {
		return nil, err
	}
	return transitions, nil
}

// TransitionIssue transitions an issue to a given state or transition ID.
// If direct transition fails with 400 or 404, it dynamically queries available transitions
// and attempts to match by transition ID, target status key, or display name.
func (c *TrackerClient) TransitionIssue(ctx context.Context, issueKey, transitionID string) error {
	path := fmt.Sprintf("/v2/issues/%s/transitions/%s/_execute", issueKey, transitionID)
	err := c.Execute(ctx, http.MethodPost, path, map[string]interface{}{}, nil)
	if err == nil {
		return nil
	}

	// If error is 404 or 400, try resolving transition dynamically
	var apiErr *APIError
	if errors.As(err, &apiErr) && (apiErr.StatusCode == http.StatusNotFound || apiErr.StatusCode == http.StatusBadRequest) {
		transitions, getErr := c.GetTransitions(ctx, issueKey)
		if getErr == nil && len(transitions) > 0 {
			targetNorm := normalizeTransitionString(transitionID)
			for _, t := range transitions {
				tidNorm := normalizeTransitionString(t.ID)
				if tidNorm == targetNorm {
					return c.Execute(ctx, http.MethodPost, fmt.Sprintf("/v2/issues/%s/transitions/%s/_execute", issueKey, t.ID), map[string]interface{}{}, nil)
				}
				if t.To != nil {
					toKeyNorm := normalizeTransitionString(t.To.Key)
					if toKeyNorm == targetNorm {
						return c.Execute(ctx, http.MethodPost, fmt.Sprintf("/v2/issues/%s/transitions/%s/_execute", issueKey, t.ID), map[string]interface{}{}, nil)
					}
					toIdNorm := normalizeTransitionString(t.To.ID)
					if toIdNorm == targetNorm {
						return c.Execute(ctx, http.MethodPost, fmt.Sprintf("/v2/issues/%s/transitions/%s/_execute", issueKey, t.ID), map[string]interface{}{}, nil)
					}
				}
				dispNorm := normalizeTransitionString(t.Display)
				if dispNorm == targetNorm {
					return c.Execute(ctx, http.MethodPost, fmt.Sprintf("/v2/issues/%s/transitions/%s/_execute", issueKey, t.ID), map[string]interface{}{}, nil)
				}
			}
		}
	}

	return err
}

// TrackerUser represents a user account profile in Yandex Tracker.
type TrackerUser struct {
	Self       string `json:"self"`
	UID        int64  `json:"uid"`
	Login      string `json:"login"`
	TrackerUID int64  `json:"trackerUid"`
	CloudUID   string `json:"cloudUid"`
	FirstName  string `json:"firstName,omitempty"`
	LastName   string `json:"lastName,omitempty"`
	Display    string `json:"display"`
	Email      string `json:"email"`
	Dismissed  bool   `json:"dismissed"`
	HasLicense bool   `json:"hasLicense"`
}

// GetFullName returns the user's display name or "First Last" or Login.
func (u *TrackerUser) GetFullName() string {
	if u.Display != "" {
		return u.Display
	}
	if u.FirstName != "" || u.LastName != "" {
		return strings.TrimSpace(fmt.Sprintf("%s %s", u.FirstName, u.LastName))
	}
	return u.Login
}

// GetUser retrieves user profile from Yandex Tracker by login or UID.
func (c *TrackerClient) GetUser(ctx context.Context, user string) (*TrackerUser, error) {
	path := fmt.Sprintf("/v2/users/%s", user)
	var u TrackerUser
	if err := c.Execute(ctx, http.MethodGet, path, nil, &u); err != nil {
		return nil, err
	}
	return &u, nil
}

// GetIssue retrieves an issue from Yandex Tracker by its ID or Key.
func (c *TrackerClient) GetIssue(ctx context.Context, issueKey string) (*thhttp.TrackerIssue, error) {
	path := fmt.Sprintf("/v2/issues/%s", issueKey)
	var issue thhttp.TrackerIssue
	if err := c.Execute(ctx, http.MethodGet, path, nil, &issue); err != nil {
		return nil, err
	}
	return &issue, nil
}

// SearchIssues performs a TQL query in Tracker (e.g. for state sync).
func (c *TrackerClient) SearchIssues(ctx context.Context, tql string, page, perPage int) ([]thhttp.TrackerIssue, error) {
	if page <= 0 {
		page = 1
	}
	if perPage <= 0 {
		perPage = 50
	}
	path := fmt.Sprintf("/v2/issues/_search?page=%d&perPage=%d", page, perPage)
	reqPayload := map[string]string{
		"query": tql,
	}

	var issues []thhttp.TrackerIssue
	if err := c.Execute(ctx, http.MethodPost, path, reqPayload, &issues); err != nil {
		return nil, err
	}
	return issues, nil
}

// AddComment posts a comment to an issue.
func (c *TrackerClient) AddComment(ctx context.Context, issueKey, comment string) error {
	path := fmt.Sprintf("/v2/issues/%s/comments", issueKey)
	reqPayload := map[string]string{
		"text": comment,
	}
	return c.Execute(ctx, http.MethodPost, path, reqPayload, nil)
}

// UpdateIssue updates fields on an issue in Yandex Tracker (e.g. tags, components, description).
func (c *TrackerClient) UpdateIssue(ctx context.Context, issueKey string, fields map[string]any) error {
	path := fmt.Sprintf("/v2/issues/%s", issueKey)
	return c.Execute(ctx, http.MethodPatch, path, fields, nil)
}

// CreateIssue creates a new issue in the specified queue.
func (c *TrackerClient) CreateIssue(ctx context.Context, queue, summary, description, issueType string, tags []string) (*thhttp.TrackerIssue, error) {
	path := "/v2/issues/"
	reqPayload := map[string]any{
		"queue":       queue,
		"summary":     summary,
		"description": description,
	}
	if issueType != "" {
		reqPayload["type"] = issueType
	}
	if len(tags) > 0 {
		reqPayload["tags"] = tags
	}

	var created thhttp.TrackerIssue
	if err := c.Execute(ctx, http.MethodPost, path, reqPayload, &created); err != nil {
		return nil, err
	}
	return &created, nil
}

// Is5xx checks if an error is a server error (for DLQ decision).
func Is5xx(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.IsServerError()
	}
	return false
}

// Is429 checks if an error is a rate limit error (429 Too Many Requests).
func Is429(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.IsRateLimit()
	}
	return false
}
