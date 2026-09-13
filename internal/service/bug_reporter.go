package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
)

// BugReporter defines the interface for reporting critical server errors.
type BugReporter interface {
	ReportError(ctx context.Context, endpoint string, err error, requestBody []byte) (string, error)
}

// NopBugReporter does nothing.
type NopBugReporter struct{}

func (n *NopBugReporter) ReportError(ctx context.Context, endpoint string, err error, requestBody []byte) (string, error) {
	return "", nil
}

// TrackerBugReporter automatically creates bug tickets in Yandex Tracker with deduplication.
type TrackerBugReporter struct {
	trackerClient *TrackerClient
	queue         string
	logger        *zap.Logger
	cacheMu       sync.Mutex
	recentBugs    map[string]time.Time
	cacheTTL      time.Duration
}

// NewTrackerBugReporter creates a new TrackerBugReporter.
func NewTrackerBugReporter(trackerClient *TrackerClient, queue string, cacheTTL time.Duration, logger *zap.Logger) *TrackerBugReporter {
	if queue == "" {
		queue = "BUGS"
	}
	if cacheTTL <= 0 {
		cacheTTL = 1 * time.Hour
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &TrackerBugReporter{
		trackerClient: trackerClient,
		queue:         queue,
		logger:        logger,
		recentBugs:    make(map[string]time.Time),
		cacheTTL:      cacheTTL,
	}
}

// ComputeFingerprint generates a SHA-256 hash for error deduplication.
func ComputeFingerprint(endpoint string, err error) string {
	if err == nil {
		return ""
	}
	raw := fmt.Sprintf("%s:%s", endpoint, err.Error())
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:12])
}

// ReportError sends a server error to Yandex Tracker queue BUGS with deduplication.
func (r *TrackerBugReporter) ReportError(ctx context.Context, endpoint string, err error, requestBody []byte) (string, error) {
	if err == nil || r.trackerClient == nil {
		return "", nil
	}

	fingerprint := ComputeFingerprint(endpoint, err)

	// 1. In-memory deduplication check
	r.cacheMu.Lock()
	now := time.Now().UTC()
	// Clean expired entries
	for fp, ts := range r.recentBugs {
		if now.Sub(ts) > r.cacheTTL {
			delete(r.recentBugs, fp)
		}
	}
	if lastReported, exists := r.recentBugs[fingerprint]; exists && now.Sub(lastReported) < r.cacheTTL {
		r.cacheMu.Unlock()
		r.logger.Debug("Server error suppressed by in-memory deduplication cache",
			zap.String("fingerprint", fingerprint),
			zap.String("endpoint", endpoint))
		return "", nil
	}
	r.recentBugs[fingerprint] = now
	r.cacheMu.Unlock()

	// 2. Remote check in Yandex Tracker to prevent duplicate open issues
	fpTag := "fp-" + fingerprint
	tql := fmt.Sprintf(`Queue: %s and (Resolution: empty() or Status: open) and (Tags: "%s" or Description: "%s")`, r.queue, fpTag, fingerprint)
	existing, searchErr := r.trackerClient.SearchIssues(ctx, tql, 1, 1)
	if searchErr == nil && len(existing) > 0 {
		issueKey := existing[0].Key
		r.logger.Info("Open bug already exists in Tracker, skipping issue creation",
			zap.String("existing_key", issueKey),
			zap.String("fingerprint", fingerprint))
		return issueKey, nil
	}

	// 3. Format bug title and description
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown-server"
	}

	shortErr := err.Error()
	if len(shortErr) > 80 {
		shortErr = shortErr[:77] + "..."
	}
	summary := fmt.Sprintf("[Auto-Bug] %s: %s", endpoint, shortErr)

	bodySnippet := string(requestBody)
	if len(bodySnippet) > 500 {
		bodySnippet = bodySnippet[:500] + " ... [TRUNCATED]"
	}
	if bodySnippet == "" {
		bodySnippet = "(empty body)"
	}

	description := fmt.Sprintf(`### 🚨 Автоматическая фиксация серверной ошибки (HTTP 500)

**Параметры инцидента:**
- **Сервер/Хост:** `+"`%s`"+`
- **Эндпоинт:** `+"`%s`"+`
- **Время:** `+"`%s`"+`
- **Хеш ошибки (Fingerprint):** `+"`%s`"+`

#### Текст ошибки:
`+"```\n%s\n```"+`

#### Тело запроса (Request Payload):
`+"```json\n%s\n```"+`

_Создано автоматически подсистемой мониторинга Tracker Assigner._`,
		hostname,
		endpoint,
		now.Format(time.RFC3339),
		fingerprint,
		err.Error(),
		bodySnippet,
	)

	tags := []string{"auto-bug", "server-500", "production", fpTag}

	// 4. Create issue in Yandex Tracker
	created, createErr := r.trackerClient.CreateIssue(ctx, r.queue, summary, description, "bug", tags)
	if createErr != nil {
		r.logger.Error("Failed to create auto-bug in Yandex Tracker",
			zap.String("queue", r.queue),
			zap.String("fingerprint", fingerprint),
			zap.Error(createErr))
		return "", createErr
	}

	r.logger.Info("Auto-bug successfully registered in Tracker",
		zap.String("issue_key", created.Key),
		zap.String("queue", r.queue),
		zap.String("fingerprint", fingerprint))

	return created.Key, nil
}
