package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"tracker-assigner/internal/service"
	"tracker-assigner/internal/storage"
	thhttp "tracker-assigner/internal/transport/http"
	"tracker-assigner/pkg/notifier"

	"go.uber.org/zap"
)

// PostAssignHook represents a callback executed after successful assignment of a pending ticket.
type PostAssignHook func(ctx context.Context, issueKey, candidateLogin string, payload *thhttp.WebhookPayload)

// PendingProcessor periodically scans Pending Queue and assigns tickets when assignees become available.
type PendingProcessor struct {
	repo           storage.Repository
	router         *service.Router
	balancer       service.BalancerStrategy
	trackerClient  service.TrackerAPI
	interval       time.Duration
	slaTimeout     time.Duration
	notifier       notifier.Notifier
	postAssignHook PostAssignHook
	triggerChan    chan struct{}
	logger         *zap.Logger
	slaAlerted     map[string]time.Time
	mu             sync.Mutex
}

// NewPendingProcessor creates a new PendingProcessor.
func NewPendingProcessor(
	repo storage.Repository,
	router *service.Router,
	balancer service.BalancerStrategy,
	trackerClient service.TrackerAPI,
	interval time.Duration,
	logger *zap.Logger,
) *PendingProcessor {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &PendingProcessor{
		repo:          repo,
		router:        router,
		balancer:      balancer,
		trackerClient: trackerClient,
		interval:      interval,
		triggerChan:   make(chan struct{}, 10),
		slaAlerted:    make(map[string]time.Time),
		logger:        logger,
	}
}

// SetBalancer replaces the active balancer strategy.
func (p *PendingProcessor) SetBalancer(b service.BalancerStrategy) {
	p.balancer = b
}

// SetPostAssignHook sets the post assignment hook (e.g. for comments, transitions, and notifications).
func (p *PendingProcessor) SetPostAssignHook(h PostAssignHook) {
	p.postAssignHook = h
}

// Trigger requests an immediate scan of the pending queue.
func (p *PendingProcessor) Trigger() {
	select {
	case p.triggerChan <- struct{}{}:
	default:
		// Channel full, another run already queued
	}
}

// SetSLATimeout sets the duration threshold for triggering an SLA alert.
func (p *PendingProcessor) SetSLATimeout(d time.Duration) {
	p.slaTimeout = d
}

// SetNotifier sets the alert notification client.
func (p *PendingProcessor) SetNotifier(n notifier.Notifier) {
	p.notifier = n
}

// Start runs the processor background loop until the context is canceled.
func (p *PendingProcessor) Start(ctx context.Context) {
	p.logger.Info("Starting Pending Queue processor", zap.Duration("interval", p.interval))
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Stopping Pending Queue processor")
			return
		case <-ticker.C:
			p.processAll(ctx)
		case <-p.triggerChan:
			p.processAll(ctx)
		}
	}
}

// ProcessOnce runs a single processing pass over the pending queue (exported for tests).
func (p *PendingProcessor) ProcessOnce(ctx context.Context) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	assignedCount := 0

	for {
		select {
		case <-ctx.Done():
			return assignedCount, ctx.Err()
		default:
		}

		// Fetch the next pending ticket (oldest across all groups)
		item, err := p.repo.GetNextPending(ctx, "")
		if err != nil {
			return assignedCount, fmt.Errorf("failed to fetch next pending ticket: %w", err)
		}
		if item == nil {
			// No more pending tickets
			break
		}

		// Get group configuration
		groupCfg, err := p.router.GetGroup(item.GroupID)
		if err != nil {
			p.logger.Error("Pending ticket refers to unknown group",
				zap.String("issue_key", item.IssueKey),
				zap.String("group", item.GroupID),
				zap.Error(err))
			// Skip or mark failed to prevent infinite loop
			_ = p.repo.MarkPendingAssigned(ctx, item.ID)
			continue
		}

		// Restore ML context from item.Payload if present
		itemCtx := ctx
		if item.Payload != "" {
			var payloadMap map[string]any
			if err := json.Unmarshal([]byte(item.Payload), &payloadMap); err == nil {
				if rawSkills, ok := payloadMap["_ml_skills"].([]any); ok {
					var skills []string
					for _, s := range rawSkills {
						if str, ok := s.(string); ok {
							skills = append(skills, str)
						}
					}
					if len(skills) > 0 {
						itemCtx = service.WithSkills(itemCtx, skills)
					}
				}
				if rawComplexity, ok := payloadMap["_ml_complexity"].(float64); ok && int(rawComplexity) > 0 {
					itemCtx = service.WithComplexity(itemCtx, int(rawComplexity))
				}
			}
		}

		// Try to pick an available candidate
		candidate, err := p.balancer.PickAssignee(itemCtx, item.GroupID, groupCfg, time.Now())
		if err != nil {
			if errors.Is(err, service.ErrNoAvailableAssignee) {
				// Check if pending ticket exceeded SLA threshold
				if p.slaTimeout > 0 && time.Since(item.CreatedAt) > p.slaTimeout {
					lastAlert, alreadyAlerted := p.slaAlerted[item.IssueKey]
					if !alreadyAlerted || time.Since(lastAlert) >= 1*time.Hour {
						p.slaAlerted[item.IssueKey] = time.Now()
						p.logger.Warn("Pending ticket SLA timeout exceeded",
							zap.String("issue_key", item.IssueKey),
							zap.String("group", item.GroupID),
							zap.Duration("waiting_for", time.Since(item.CreatedAt)),
							zap.Duration("sla_threshold", p.slaTimeout))

						if p.notifier != nil {
							msg := fmt.Sprintf("Тикет <b>%s</b> ожидает назначения в группе <code>%s</code> уже %s (порог SLA: %s). Все сотрудники заняты или оффлайн.",
								item.IssueKey, item.GroupID, time.Since(item.CreatedAt).Round(time.Minute), p.slaTimeout)
							_ = p.notifier.Notify(ctx, "SLA Breach: Тикет завис в очереди ожидания", msg)
						}
					}
				}

				p.logger.Debug("No candidate available for pending ticket yet",
					zap.String("issue_key", item.IssueKey),
					zap.String("group", item.GroupID))
				break
			}
			p.logger.Error("Error checking candidate for pending ticket",
				zap.String("issue_key", item.IssueKey),
				zap.Error(err))
			break
		}

		// Candidate found: Assign in Tracker
		p.logger.Info("Pending ticket assignee found, assigning in Tracker",
			zap.String("issue_key", item.IssueKey),
			zap.String("assignee", candidate.Login))

		if err := p.trackerClient.AssignIssue(ctx, item.IssueKey, candidate.Login); err != nil {
			p.logger.Error("Failed to assign pending ticket in Tracker",
				zap.String("issue_key", item.IssueKey),
				zap.String("assignee", candidate.Login),
				zap.Error(err))

			if strings.Contains(err.Error(), "404") {
				p.logger.Warn("Pending ticket not found in Tracker (404), discarding from pending queue",
					zap.String("issue_key", item.IssueKey))
				_ = p.repo.MarkPendingAssigned(ctx, item.ID)
			} else if service.Is429(err) {
				p.logger.Warn("Tracker rate limit exceeded during pending assignment, pausing pass",
					zap.String("issue_key", item.IssueKey),
					zap.Error(err))
				break
			} else {
				// Move to DLQ and remove from pending queue to prevent infinite starvation loop
				dlqPayload, _ := json.Marshal(map[string]string{
					"assignee": candidate.Login,
					"group":    item.GroupID,
				})
				_ = p.repo.EnqueueDLQ(ctx, storage.DLQItem{
					IssueKey:     item.IssueKey,
					Action:       "assign",
					Payload:      string(dlqPayload),
					ErrorMessage: err.Error(),
					MaxRetries:   5,
				})
				_ = p.repo.MarkPendingAssigned(ctx, item.ID)
			}
			continue
		}

		// Assignment succeeded in Tracker
		if _, err := p.repo.IncrementLoadForIssue(ctx, item.GroupID, candidate.Login, item.IssueKey); err != nil {
			p.logger.Error("Failed to increment load for assigned pending ticket",
				zap.String("assignee", candidate.Login),
				zap.Error(err))
		}

		if err := p.repo.MarkPendingAssigned(ctx, item.ID); err != nil {
			p.logger.Error("Failed to mark pending ticket assigned",
				zap.Int64("id", item.ID),
				zap.Error(err))
		}

		delete(p.slaAlerted, item.IssueKey)

		// Execute post-assign hook (transition status, add comment, send Telegram notification)
		if p.postAssignHook != nil {
			var wp *thhttp.WebhookPayload
			if item.Payload != "" {
				var parsed thhttp.WebhookPayload
				if err := json.Unmarshal([]byte(item.Payload), &parsed); err == nil {
					wp = &parsed
				}
			}
			if wp == nil {
				wp = &thhttp.WebhookPayload{
					Issue: &thhttp.TrackerIssue{
						Key:   item.IssueKey,
						Queue: &thhttp.IssueReference{Key: item.QueueKey},
					},
				}
			}
			wp.QueueWaitDuration = time.Since(item.CreatedAt)
			wp.Source = "pending_queue"
			p.postAssignHook(itemCtx, item.IssueKey, candidate.Login, wp)
		}

		assignedCount++
	}

	return assignedCount, nil
}

func (p *PendingProcessor) processAll(ctx context.Context) {
	count, err := p.ProcessOnce(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		p.logger.Error("Error in pending processor pass", zap.Error(err))
	} else if count > 0 {
		p.logger.Info("Pending processor successfully assigned tickets", zap.Int("count", count))
	}
}
