package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"tracker-assigner/internal/service"
	"tracker-assigner/internal/storage"
	"tracker-assigner/pkg/notifier"

	"go.uber.org/zap"
)

// DLQProcessor handles retries of failed API calls from Dead Letter Queue.
type DLQProcessor struct {
	repo          storage.Repository
	trackerClient service.TrackerAPI
	interval      time.Duration
	baseBackoff   time.Duration
	batchSize     int
	notifier      notifier.Notifier
	logger        *zap.Logger
	mu            sync.Mutex
}

// NewDLQProcessor creates a new DLQProcessor.
func NewDLQProcessor(
	repo storage.Repository,
	trackerClient service.TrackerAPI,
	interval time.Duration,
	baseBackoff time.Duration,
	logger *zap.Logger,
) *DLQProcessor {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	if baseBackoff <= 0 {
		baseBackoff = 30 * time.Second
	}
	return &DLQProcessor{
		repo:          repo,
		trackerClient: trackerClient,
		interval:      interval,
		baseBackoff:   baseBackoff,
		batchSize:     10,
		logger:        logger,
	}
}

// SetNotifier sets the alert notification client for fatal errors.
func (d *DLQProcessor) SetNotifier(n notifier.Notifier) {
	d.notifier = n
}

// Start runs the DLQ retry loop.
func (d *DLQProcessor) Start(ctx context.Context) {
	d.logger.Info("Starting Dead Letter Queue processor",
		zap.Duration("interval", d.interval),
		zap.Duration("base_backoff", d.baseBackoff))

	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.logger.Info("Stopping DLQ processor")
			return
		case <-ticker.C:
			d.processAll(ctx)
		}
	}
}

// ProcessOnce attempts to process currently eligible DLQ items (exported for tests).
func (d *DLQProcessor) ProcessOnce(ctx context.Context) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	items, err := d.repo.GetDLQItemsForRetry(ctx, d.batchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to get dlq items for retry: %w", err)
	}

	successCount := 0

	for _, item := range items {
		select {
		case <-ctx.Done():
			return successCount, ctx.Err()
		default:
		}

		err := d.retryItem(ctx, item)
		if err == nil {
			// Resolved
			if err := d.repo.MarkDLQResolved(ctx, item.ID); err != nil {
				d.logger.Error("Failed to mark DLQ item resolved", zap.Int64("id", item.ID), zap.Error(err))
			} else {
				successCount++
				d.logger.Info("DLQ item successfully processed and resolved",
					zap.String("issue_key", item.IssueKey),
					zap.String("action", item.Action))
			}
		} else {
			// Retry failed: calculate exponential backoff
			newRetryCount := item.RetryCount + 1
			multiplier := math.Pow(2, float64(item.RetryCount))
			backoffDuration := time.Duration(multiplier) * d.baseBackoff
			nextRetry := time.Now().Add(backoffDuration)

			d.logger.Warn("DLQ item retry failed, scheduling backoff",
				zap.String("issue_key", item.IssueKey),
				zap.Int("new_retry_count", newRetryCount),
				zap.Int("max_retries", item.MaxRetries),
				zap.Duration("backoff", backoffDuration),
				zap.Error(err))

			if err := d.repo.UpdateDLQRetry(ctx, item.ID, newRetryCount, nextRetry, err.Error()); err != nil {
				d.logger.Error("Failed to update DLQ retry status", zap.Int64("id", item.ID), zap.Error(err))
			}

			if newRetryCount >= item.MaxRetries {
				d.logger.Error("DLQ item permanently failed after reaching max retries",
					zap.String("issue_key", item.IssueKey),
					zap.Int("retries", newRetryCount),
					zap.Error(err))

				if d.notifier != nil {
					msg := fmt.Sprintf("Тикет <b>%s</b> исчерпал лимит повторов (%d/%d) в Dead Letter Queue!\nДействие: <code>%s</code>\nОшибка: <code>%s</code>",
						item.IssueKey, newRetryCount, item.MaxRetries, item.Action, err.Error())
					_ = d.notifier.Notify(ctx, "DLQ Fatal Error: Сбой назначения тикета", msg)
				}
			}
		}
	}

	return successCount, nil
}

func (d *DLQProcessor) retryItem(ctx context.Context, item storage.DLQItem) error {
	switch item.Action {
	case "assign":
		var p struct {
			Assignee string `json:"assignee"`
			Group    string `json:"group"`
		}
		if err := json.Unmarshal([]byte(item.Payload), &p); err != nil {
			return fmt.Errorf("invalid DLQ payload for assign action: %w", err)
		}
		if p.Assignee == "" {
			return errors.New("missing assignee in DLQ assign payload")
		}

		if err := d.trackerClient.AssignIssue(ctx, item.IssueKey, p.Assignee); err != nil {
			return err
		}

		// On successful assignment, update local load idempotently
		if p.Group != "" {
			_, _ = d.repo.IncrementLoadForIssue(ctx, p.Group, p.Assignee, item.IssueKey)
		}
		return nil

	default:
		return fmt.Errorf("unsupported DLQ action: %s", item.Action)
	}
}

func (d *DLQProcessor) processAll(ctx context.Context) {
	count, err := d.ProcessOnce(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		d.logger.Error("Error in DLQ processor pass", zap.Error(err))
	} else if count > 0 {
		d.logger.Info("DLQ processor completed pass", zap.Int("resolved_count", count))
	}

	// Clean up records resolved or failed older than 14 days
	cleaned, err := d.repo.CleanupOldDLQ(ctx, 14*24*time.Hour)
	if err == nil && cleaned > 0 {
		d.logger.Info("Cleaned up expired DLQ records", zap.Int64("count", cleaned))
	}
}
