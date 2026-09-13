package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"tracker-assigner/internal/service"
	"tracker-assigner/internal/storage"
	thhttp "tracker-assigner/internal/transport/http"

	"go.uber.org/zap"
)

// StateSyncWorker periodically queries Yandex Tracker via TQL to reconcile local SQLite load counters.
type StateSyncWorker struct {
	repo          storage.Repository
	router        *service.Router
	trackerClient service.TrackerAPI
	interval      time.Duration
	triggerChan   chan struct{}
	logger        *zap.Logger
	mu            sync.Mutex
}

// NewStateSyncWorker creates a new StateSyncWorker.
func NewStateSyncWorker(
	repo storage.Repository,
	router *service.Router,
	trackerClient service.TrackerAPI,
	interval time.Duration,
	logger *zap.Logger,
) *StateSyncWorker {
	if interval <= 0 {
		interval = 30 * time.Minute
	}
	return &StateSyncWorker{
		repo:          repo,
		router:        router,
		trackerClient: trackerClient,
		interval:      interval,
		triggerChan:   make(chan struct{}, 5),
		logger:        logger,
	}
}

// TriggerSync requests an immediate non-blocking reconciliation pass.
func (w *StateSyncWorker) TriggerSync() {
	select {
	case w.triggerChan <- struct{}{}:
	default:
	}
}

// Start begins the periodic reconciliation loop until ctx is canceled.
func (w *StateSyncWorker) Start(ctx context.Context) {
	w.logger.Info("Starting StateSyncWorker", zap.Duration("interval", w.interval))
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("Stopping StateSyncWorker")
			return
		case <-ticker.C:
			if err := w.SyncOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				w.logger.Error("Error during state sync pass", zap.Error(err))
			}
		case <-w.triggerChan:
			if err := w.SyncOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				w.logger.Error("Error during triggered state sync pass", zap.Error(err))
			}
		}
	}
}

// SyncOnce performs a single reconciliation pass against Yandex Tracker (exported for tests).
func (w *StateSyncWorker) SyncOnce(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	groups := w.router.GetAllGroups()
	if len(groups) == 0 {
		return nil
	}

	for groupID, groupCfg := range groups {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if len(groupCfg.Assignees) == 0 {
			continue
		}

		// Build batch query for all assignees in this group
		var assigneeTerms []string
		for _, a := range groupCfg.Assignees {
			if a.Login != "" {
				assigneeTerms = append(assigneeTerms, fmt.Sprintf("%q", a.Login))
			}
			if a.UID != "" {
				assigneeTerms = append(assigneeTerms, fmt.Sprintf("%q", a.UID))
			}
			if a.Login == "" && a.UID == "" && a.CloudUID != "" {
				assigneeTerms = append(assigneeTerms, fmt.Sprintf("%q", a.CloudUID))
			}
		}
		if len(assigneeTerms) == 0 {
			continue
		}

		tql := fmt.Sprintf(`Assignee: %s and Resolution: empty()`, strings.Join(assigneeTerms, ", "))

		// Filter active statuses if configured
		if len(groupCfg.ActiveStatuses) > 0 {
			var statusTerms []string
			for _, st := range groupCfg.ActiveStatuses {
				if st != "" {
					statusTerms = append(statusTerms, fmt.Sprintf("%q", st))
				}
			}
			if len(statusTerms) > 0 {
				tql += fmt.Sprintf(` and Status: %s`, strings.Join(statusTerms, ", "))
			}
		}

		// Exclude ignored statuses if configured
		for _, ign := range groupCfg.IgnoredStatuses {
			if ign != "" {
				tql += fmt.Sprintf(` and Status: !%q`, ign)
			}
		}

		issues, err := w.fetchTrackerIssues(ctx, tql)
		if err != nil {
			w.logger.Error("Failed to query Tracker for group load in batch",
				zap.String("group", groupID),
				zap.String("tql", tql),
				zap.Error(err))
			continue
		}

		// Extract issues per assignee
		assigneeIssues := make(map[string][]string, len(groupCfg.Assignees))
		for _, a := range groupCfg.Assignees {
			assigneeIssues[a.Login] = []string{}
		}

		for _, issue := range issues {
			if issue.Assignee == nil {
				continue
			}
			for _, a := range groupCfg.Assignees {
				if a.Matches(issue.Assignee.Login) || a.Matches(issue.Assignee.CloudUID) || a.Matches(issue.Assignee.ID) {
					assigneeIssues[a.Login] = append(assigneeIssues[a.Login], issue.Key)
					break
				}
			}
		}

		// Reconcile each assignee's load and issue assignments in SQLite
		for _, a := range groupCfg.Assignees {
			issueKeys := assigneeIssues[a.Login]
			count := len(issueKeys)
			currentLocal, err := w.repo.GetAssigneeLoad(ctx, groupID, a.Login)
			if err != nil {
				w.logger.Error("Failed to fetch local load for assignee",
					zap.String("assignee", a.Login),
					zap.Error(err))
				continue
			}

			if currentLocal != count {
				w.logger.Info("Reconciled load mismatch with Tracker (batch)",
					zap.String("group", groupID),
					zap.String("assignee", a.Login),
					zap.Int("old_local_load", currentLocal),
					zap.Int("new_tracker_load", count))
			}

			// Always sync issue assignments to ensure missing/extra records are fixed
			if err := w.repo.SyncAssigneeIssues(ctx, groupID, a.Login, issueKeys); err != nil {
				w.logger.Error("Failed to update reconciled load in SQLite",
					zap.String("assignee", a.Login),
					zap.Error(err))
			}
		}
	}

	return nil
}

func (w *StateSyncWorker) fetchTrackerIssues(ctx context.Context, tql string) ([]thhttp.TrackerIssue, error) {
	var allIssues []thhttp.TrackerIssue
	page := 1
	perPage := 50

	for {
		issues, err := w.trackerClient.SearchIssues(ctx, tql, page, perPage)
		if err != nil {
			return nil, err
		}

		allIssues = append(allIssues, issues...)
		if len(issues) < perPage {
			break
		}
		page++
	}

	return allIssues, nil
}
