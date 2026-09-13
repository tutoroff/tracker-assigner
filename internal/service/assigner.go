package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"strings"
	"sync"
	"time"

	"tracker-assigner/internal/config"
	"tracker-assigner/internal/storage"
	thhttp "tracker-assigner/internal/transport/http"

	"go.uber.org/zap"
)

// TrackerAPI defines operations needed from Yandex Tracker API.
type TrackerAPI interface {
	AssignIssue(ctx context.Context, issueKey, assignee string) error
	ClearAssignee(ctx context.Context, issueKey string) error
	TransitionIssue(ctx context.Context, issueKey, transitionID string) error
	GetTransitions(ctx context.Context, issueKey string) ([]TrackerTransition, error)
	GetIssue(ctx context.Context, issueKey string) (*thhttp.TrackerIssue, error)
	GetUser(ctx context.Context, user string) (*TrackerUser, error)
	SearchIssues(ctx context.Context, tql string, page, perPage int) ([]thhttp.TrackerIssue, error)
	AddComment(ctx context.Context, issueKey, comment string) error
}

// StateSyncTrigger defines an interface to trigger an immediate state sync pass.
type StateSyncTrigger interface {
	TriggerSync()
}

// AssignerService coordinates routing, balancing, Tracker assignment, and pending/DLQ queues.
type AssignerService struct {
	router           *Router
	balancer         BalancerStrategy
	trackerClient    TrackerAPI
	repo             storage.Repository
	logger           *zap.Logger
	cfg              *config.Config
	notifier         Notifier
	stateSyncWorker  StateSyncTrigger
	inFlightMu       sync.Mutex
	inFlight         map[string]bool
	recentlyAssigned map[string]time.Time
	userNamesMu      sync.RWMutex
	userNames        map[string]string
	postAssignHooks  []func(ctx context.Context, issueKey, candidateLogin string, payload *thhttp.WebhookPayload)
}

// Notifier defines interface for sending system alerts.
type Notifier interface {
	Notify(ctx context.Context, title, message string) error
	NotifyToChat(ctx context.Context, chatID, title, message string) error
}

type contextKey string

const (
	skillsContextKey     contextKey = "ml_skills"
	complexityContextKey contextKey = "ml_complexity"
)

// WithSkills stores predicted or required skills into context.
func WithSkills(ctx context.Context, skills []string) context.Context {
	return context.WithValue(ctx, skillsContextKey, skills)
}

// GetSkills retrieves predicted or required skills from context.
func GetSkills(ctx context.Context) []string {
	if ctx == nil {
		return nil
	}
	if val, ok := ctx.Value(skillsContextKey).([]string); ok {
		return val
	}
	return nil
}

// WithComplexity stores issue complexity (1-5) into context.
func WithComplexity(ctx context.Context, complexity int) context.Context {
	return context.WithValue(ctx, complexityContextKey, complexity)
}

// GetComplexity retrieves complexity from context, default 1.
func GetComplexity(ctx context.Context) int {
	if ctx == nil {
		return 1
	}
	if val, ok := ctx.Value(complexityContextKey).(int); ok && val > 0 {
		return val
	}
	return 1
}

// NewAssignerService creates an AssignerService.
func NewAssignerService(
	router *Router,
	balancer BalancerStrategy,
	trackerClient TrackerAPI,
	repo storage.Repository,
	logger *zap.Logger,
	cfg *config.Config,
	notifier Notifier,
) *AssignerService {
	return &AssignerService{
		router:           router,
		balancer:         balancer,
		trackerClient:    trackerClient,
		repo:             repo,
		logger:           logger,
		cfg:              cfg,
		notifier:         notifier,
		inFlight:         make(map[string]bool),
		recentlyAssigned: make(map[string]time.Time),
		userNames:        make(map[string]string),
	}
}

// SetBalancer replaces the current balancer strategy.
func (s *AssignerService) SetBalancer(b BalancerStrategy) {
	s.balancer = b
}

func (s *AssignerService) getAssigneeFullName(ctx context.Context, login string) string {
	if login == "" {
		return ""
	}

	// 1. Check if configured in static config
	if s.cfg != nil {
		if _, a := s.cfg.FindAssignee(login); a != nil && a.Name != "" {
			return a.Name
		}
	}

	// 2. Check in-memory cache
	s.userNamesMu.RLock()
	cached, ok := s.userNames[login]
	s.userNamesMu.RUnlock()
	if ok {
		return cached
	}

	// 3. Query Tracker API
	if s.trackerClient != nil {
		u, err := s.trackerClient.GetUser(ctx, login)
		if err == nil && u != nil {
			name := u.GetFullName()
			if name != "" {
				s.userNamesMu.Lock()
				s.userNames[login] = name
				s.userNamesMu.Unlock()
				return name
			}
		}
	}

	return ""
}

// SetStateSyncWorker sets the worker for triggering sync passes.
func (s *AssignerService) SetStateSyncWorker(w StateSyncTrigger) {
	s.stateSyncWorker = w
}

// UpdateConfig updates the configuration and notifier dynamically.
func (s *AssignerService) UpdateConfig(cfg *config.Config, notifier Notifier) {
	s.cfg = cfg
	s.notifier = notifier
}

// ProcessWebhook handles the incoming webhook payload.
func (s *AssignerService) ProcessWebhook(ctx context.Context, payload *thhttp.WebhookPayload, rawBody []byte) error {
	issueKey := payload.GetIssueKey()
	if issueKey == "" {
		s.logger.Debug("Ignoring webhook without issue key")
		return nil
	}

	// 0. Ignore webhooks triggered by the service robot itself (TASK-22)
	// Important: issue_created events must NEVER be ignored as self-echoes,
	// because creating a new issue is not an auto-assignment echo.
	if s.cfg != nil && payload.Event != "issue_created" {
		if payload.IsActorRobot(s.cfg.Tracker.RobotLogin, s.cfg.Tracker.RobotUID, s.cfg.Tracker.RobotCloudUID) {
			s.logger.Info("Skipping webhook triggered by service robot itself",
				zap.String("issue_key", issueKey),
				zap.String("event", payload.Event))
			return nil
		}
	}

	// Lock & deduplicate in-flight and recently assigned events (TASK-22)
	s.inFlightMu.Lock()
	if s.inFlight[issueKey] {
		s.inFlightMu.Unlock()
		s.logger.Warn("Dropping concurrent duplicate webhook for issue already in flight",
			zap.String("issue_key", issueKey))
		return nil
	}

	// Prune recentlyAssigned entries older than 1 minute to prevent unbounded memory growth
	now := time.Now()
	for k, t := range s.recentlyAssigned {
		if now.Sub(t) > 1*time.Minute {
			delete(s.recentlyAssigned, k)
		}
	}

	// Check if this event contains a human-driven manual assignee change
	_, hasAssigneeChange := payload.Changes["assignee"]
	isManualAssigneeChange := hasAssigneeChange && (s.cfg == nil || !payload.IsActorRobot(s.cfg.Tracker.RobotLogin, s.cfg.Tracker.RobotUID, s.cfg.Tracker.RobotCloudUID))

	if lastAssigned, ok := s.recentlyAssigned[issueKey]; ok {
		// If recently assigned (< 5 seconds) and not a closure/pause status change or manual intervention, drop echo webhook
		if time.Since(lastAssigned) < 5*time.Second && !s.shouldRemoveAssignee(payload) && !isManualAssigneeChange {
			s.inFlightMu.Unlock()
			s.logger.Info("Dropping duplicate webhook for recently assigned issue (dedup window)",
				zap.String("issue_key", issueKey),
				zap.Duration("elapsed", time.Since(lastAssigned)))
			return nil
		}
	}
	s.inFlight[issueKey] = true
	s.inFlightMu.Unlock()

	defer func() {
		s.inFlightMu.Lock()
		delete(s.inFlight, issueKey)
		s.inFlightMu.Unlock()
	}()

	queueKey := payload.GetQueueKey()
	assigneeLogin := payload.GetAssigneeLogin()

	// 1. Handle ticket closure or pause: decrement load and remove assignee (TASK-23, TASK-26)
	if s.shouldRemoveAssignee(payload) {
		s.logger.Info("Ticket transitioned to a state requiring assignee removal",
			zap.String("issue_key", issueKey),
			zap.String("assignee", assigneeLogin))

		removedUser, err := s.repo.DecrementLoadForIssue(ctx, issueKey)
		if err != nil {
			s.logger.Error("Failed to decrement tracked load for issue in SQLite", zap.Error(err))
		} else if removedUser != "" {
			s.logger.Info("Decremented load for tracked issue",
				zap.String("issue_key", issueKey), zap.String("assignee", removedUser))
		} else if assigneeLogin != "" {
			// Fallback if not tracked in issue_assignments
			canonical := s.resolveCanonicalAssignee(assigneeLogin)
			if err := s.repo.DecrementAssigneeGlobalLoad(ctx, canonical); err != nil {
				s.logger.Error("Failed to decrement global load on ticket removal",
					zap.String("assignee", canonical),
					zap.Error(err))
			}
		}

		// Clear assignee in tracker
		if err := s.trackerClient.ClearAssignee(ctx, issueKey); err != nil {
			s.logger.Error("Failed to clear assignee in Tracker",
				zap.String("issue_key", issueKey), zap.Error(err))
		}

		// Also ensure it is removed from pending queue if closed while pending
		_, _ = s.repo.RemovePendingByIssueKey(ctx, issueKey)
		return nil
	}

	// 2. Handle manual assignment intervention (TASK-14, TASK-25)
	if change, ok := payload.Changes["assignee"]; ok {
		fromLogin := s.resolveCanonicalAssignee(extractLogin(change.From))
		toLogin := s.resolveCanonicalAssignee(extractLogin(change.To))

		s.logger.Info("Detected manual assignee change in Tracker",
			zap.String("issue_key", issueKey),
			zap.String("from", fromLogin),
			zap.String("to", toLogin))

		if toLogin == "" {
			// Manual unassignment in Tracker (cleared assignee)
			removedUser, err := s.repo.DecrementLoadForIssue(ctx, issueKey)
			if err != nil {
				s.logger.Error("Failed to decrement tracked load for unassigned issue", zap.Error(err))
			} else if removedUser == "" && fromLogin != "" {
				_ = s.repo.DecrementAssigneeGlobalLoad(ctx, fromLogin)
			}
		} else {
			// Reassigned or assigned to toLogin
			targetGroup, _ := s.router.ResolveGroup(payload)
			if targetGroup == "" && s.cfg != nil {
				if gName, _ := s.cfg.FindAssignee(toLogin); gName != "" {
					targetGroup = gName
				}
			}
			if targetGroup == "" {
				for gid := range s.router.GetAllGroups() {
					targetGroup = gid
					break
				}
			}
			if targetGroup != "" {
				// Check if this issue is already tracked in SQLite
				assignment, _ := s.repo.GetIssueAssignment(ctx, issueKey)
				// IncrementLoadForIssue atomically decrements previous assignee if tracked
				_, _ = s.repo.IncrementLoadForIssue(ctx, targetGroup, toLogin, issueKey)

				// If not previously tracked in SQLite, decrement old user load manually
				if assignment == nil && fromLogin != "" && fromLogin != toLogin {
					_ = s.repo.DecrementAssigneeGlobalLoad(ctx, fromLogin)
				}
			}
		}

		// Remove from pending queue if it was waiting
		_, _ = s.repo.RemovePendingByIssueKey(ctx, issueKey)
		return nil
	}

	if assigneeLogin != "" {
		// Ticket already has an assignee
		canonical := s.resolveCanonicalAssignee(assigneeLogin)
		removed, _ := s.repo.RemovePendingByIssueKey(ctx, issueKey)
		if removed {
			s.logger.Info("Pending ticket was manually assigned in Tracker, removed from pending queue",
				zap.String("issue_key", issueKey),
				zap.String("manual_assignee", canonical))
		}

		targetGroup, _ := s.router.ResolveGroup(payload)
		if targetGroup == "" && s.cfg != nil {
			if gName, _ := s.cfg.FindAssignee(canonical); gName != "" {
				targetGroup = gName
			}
		}
		if targetGroup != "" {
			_, _ = s.repo.IncrementLoadForIssue(ctx, targetGroup, canonical, issueKey)
		}
		return nil
	}

	// If assignee is not specified in the webhook body, check if the issue is already assigned in SQLite
	existingAssignment, err := s.repo.GetIssueAssignment(ctx, issueKey)
	if err == nil && existingAssignment != nil {
		s.logger.Debug("Ticket is already tracked as assigned in SQLite, skipping auto-assign",
			zap.String("issue_key", issueKey),
			zap.String("assignee", existingAssignment.AssigneeID))
		return nil
	}

	// 3. Ticket is open and has no assignee: route it
	targetGroup, err := s.router.ResolveGroup(payload)
	if err != nil {
		if errors.Is(err, ErrNoMatchingRule) {
			s.logger.Info("Ticket does not match any routing rules, skipping auto-assign",
				zap.String("issue_key", issueKey),
				zap.String("queue", queueKey))
			return nil
		}
		return fmt.Errorf("routing error: %w", err)
	}

	groupCfg, err := s.router.GetGroup(targetGroup)
	if err != nil {
		return fmt.Errorf("failed to get group config: %w", err)
	}

	// 4. Balance and assign
	return s.AssignOrEnqueue(ctx, issueKey, queueKey, targetGroup, groupCfg, rawBody, payload)
}

// AssignOrEnqueue attempts to assign the issue to an available candidate, or enqueues to Pending Queue.
func (s *AssignerService) AssignOrEnqueue(
	ctx context.Context,
	issueKey, queueKey, targetGroup string,
	groupCfg config.GroupConfig,
	rawBody []byte,
	payload *thhttp.WebhookPayload,
) error {
	candidate, err := s.balancer.PickAssignee(ctx, targetGroup, groupCfg, time.Now())
	if err != nil {
		if errors.Is(err, ErrNoAvailableAssignee) {
			// Trigger quick reconciliation pass in case of phantom overload
			if s.stateSyncWorker != nil {
				s.stateSyncWorker.TriggerSync()
			}

			// All assignees are busy or off-shift -> Enqueue to Pending Queue
			s.logger.Warn("All assignees busy or off shift. Enqueueing ticket to Pending Queue",
				zap.String("issue_key", issueKey),
				zap.String("group", targetGroup))

			payloadStr := string(rawBody)
			skills := GetSkills(ctx)
			complexity := GetComplexity(ctx)
			if len(skills) > 0 || complexity > 1 {
				var payloadMap map[string]any
				if err := json.Unmarshal(rawBody, &payloadMap); err == nil {
					if len(skills) > 0 {
						payloadMap["_ml_skills"] = skills
					}
					if complexity > 1 {
						payloadMap["_ml_complexity"] = complexity
					}
					if enrichedBytes, err := json.Marshal(payloadMap); err == nil {
						payloadStr = string(enrichedBytes)
					}
				} else {
					fallbackMap := map[string]any{
						"raw":            string(rawBody),
						"_ml_skills":     skills,
						"_ml_complexity": complexity,
					}
					if fbBytes, err := json.Marshal(fallbackMap); err == nil {
						payloadStr = string(fbBytes)
					}
				}
			}

			pendingItem := storage.PendingItem{
				IssueKey: issueKey,
				QueueKey: queueKey,
				GroupID:  targetGroup,
				Payload:  payloadStr,
				Status:   "pending",
			}
			if err := s.repo.EnqueuePending(ctx, pendingItem); err != nil {
				return fmt.Errorf("failed to enqueue to pending queue: %w", err)
			}
			return nil
		}
		return fmt.Errorf("balancer error: %w", err)
	}

	// Candidate found: assign in Yandex Tracker
	s.logger.Info("Assigning ticket to candidate",
		zap.String("issue_key", issueKey),
		zap.String("assignee", candidate.Login),
		zap.String("group", targetGroup))

	if err := s.trackerClient.AssignIssue(ctx, issueKey, candidate.Login); err != nil {
		s.logger.Error("Failed to assign ticket in Tracker",
			zap.String("issue_key", issueKey),
			zap.String("assignee", candidate.Login),
			zap.Error(err))

		// If 5xx server error, push to DLQ for exponential retry
		if Is5xx(err) {
			s.logger.Warn("Tracker returned 5xx, sending assignment to DLQ",
				zap.String("issue_key", issueKey))

			dlqPayload, _ := json.Marshal(map[string]string{
				"assignee": candidate.Login,
				"group":    targetGroup,
			})
			dlqItem := storage.DLQItem{
				IssueKey:     issueKey,
				Action:       "assign",
				Payload:      string(dlqPayload),
				ErrorMessage: err.Error(),
				MaxRetries:   5,
			}
			_ = s.repo.EnqueueDLQ(ctx, dlqItem)
			return nil
		}
		return fmt.Errorf("failed to assign in tracker: %w", err)
	}

	// Record assignment in recent assignments cache for deduplication (TASK-22)
	s.inFlightMu.Lock()
	s.recentlyAssigned[issueKey] = time.Now()
	s.inFlightMu.Unlock()

	// Update local load in SQLite idempotently (TASK-23)
	incremented, err := s.repo.IncrementLoadForIssue(ctx, targetGroup, candidate.Login, issueKey)
	if err != nil {
		s.logger.Error("Failed to increment local load in SQLite",
			zap.String("assignee", candidate.Login),
			zap.Error(err))
	} else if !incremented {
		s.logger.Warn("Issue was already recorded as assigned to this candidate, load not double-incremented",
			zap.String("issue_key", issueKey),
			zap.String("assignee", candidate.Login))
	}

	// Remove from pending queue if it was pending
	_, _ = s.repo.RemovePendingByIssueKey(ctx, issueKey)

	s.logger.Info("Ticket successfully assigned and load updated",
		zap.String("issue_key", issueKey),
		zap.String("assignee", candidate.Login))

	if payload != nil {
		if payload.Source == "" {
			payload.Source = "direct"
		}
		s.postAssignActions(ctx, issueKey, candidate.Login, payload)
	}

	return nil
}

// RegisterPostAssignHook registers a callback to be notified whenever an issue is assigned.
func (s *AssignerService) RegisterPostAssignHook(hook func(ctx context.Context, issueKey, candidateLogin string, payload *thhttp.WebhookPayload)) {
	s.inFlightMu.Lock()
	defer s.inFlightMu.Unlock()
	s.postAssignHooks = append(s.postAssignHooks, hook)
}

// PostAssignActions executes transitions, comments, notifications, and registered hooks after an issue is assigned.
func (s *AssignerService) PostAssignActions(ctx context.Context, issueKey string, candidateLogin string, payload *thhttp.WebhookPayload) {
	s.postAssignActions(ctx, issueKey, candidateLogin, payload)
}

func (s *AssignerService) postAssignActions(ctx context.Context, issueKey string, candidateLogin string, payload *thhttp.WebhookPayload) {
	// Trigger any registered post-assign hooks (e.g. analytics collector)
	s.inFlightMu.Lock()
	hooks := make([]func(ctx context.Context, issueKey, candidateLogin string, payload *thhttp.WebhookPayload), len(s.postAssignHooks))
	copy(hooks, s.postAssignHooks)
	s.inFlightMu.Unlock()

	for _, hook := range hooks {
		hook(ctx, issueKey, candidateLogin, payload)
	}

	if s.cfg == nil {
		return
	}

	// 1. Transition (TASK-27)
	transition := s.cfg.Tracker.Workflow.AssignTransition
	if s.cfg.Tracker.Workflow.TransitionMap != nil {
		if mapped, ok := s.cfg.Tracker.Workflow.TransitionMap[transition]; ok && mapped != "" {
			transition = mapped
		}
	}
	if transition != "" {
		if err := s.trackerClient.TransitionIssue(ctx, issueKey, transition); err != nil {
			s.logger.Error("Failed to transition issue", zap.String("issue_key", issueKey), zap.Error(err))
		}
	}

	// 2. Comment (TASK-24)
	commentTpl := "Задача взята в работу пользователем @{{login}}"
	if s.cfg.Tracker.Workflow.AssignCommentTemplate != "" {
		commentTpl = s.cfg.Tracker.Workflow.AssignCommentTemplate
	}
	comment := strings.ReplaceAll(commentTpl, "{{login}}", candidateLogin)
	if err := s.trackerClient.AddComment(ctx, issueKey, comment); err != nil {
		s.logger.Error("Failed to add comment", zap.String("issue_key", issueKey), zap.Error(err))
	}

	// 3. Notification
	if s.cfg.Notifications.Telegram.Enabled && s.notifier != nil {
		var components []string
		var tags []string
		var summary string

		if payload != nil {
			components = payload.GetComponents()
			tags = payload.GetTags()
			summary = payload.GetSummary()
		}

		// If summary is empty, try fetching full issue from Tracker API
		if summary == "" && s.trackerClient != nil {
			if issue, err := s.trackerClient.GetIssue(ctx, issueKey); err == nil && issue != nil {
				summary = issue.Summary
				if len(components) == 0 && issue.Components != nil {
					if compList := extractStringListFromVal(issue.Components); len(compList) > 0 {
						components = compList
					}
				}
				if len(tags) == 0 && issue.Tags != nil {
					if tagList := extractStringListFromVal(issue.Tags); len(tagList) > 0 {
						tags = tagList
					}
				}
			}
		}

		// Resolve Assignee full name
		assigneeFullName := s.getAssigneeFullName(ctx, candidateLogin)

		webBaseURL := "https://tracker.yandex.ru"
		if s.cfg.Tracker.WebURL != "" {
			webBaseURL = strings.TrimRight(s.cfg.Tracker.WebURL, "/")
		}

		var assigneeText string
		if assigneeFullName != "" {
			assigneeText = fmt.Sprintf("%s (@%s)", html.EscapeString(assigneeFullName), html.EscapeString(candidateLogin))
		} else {
			assigneeText = fmt.Sprintf("@%s", html.EscapeString(candidateLogin))
		}

		summaryText := html.EscapeString(summary)
		if summaryText == "" {
			summaryText = "Без названия"
		}

		msg := fmt.Sprintf(
			"🎯 <b>Задача назначена</b>\n\n"+
				"📋 <b>Тикет:</b> <a href=\"%s/%s\">%s</a> — %s\n"+
				"👤 <b>Исполнитель:</b> %s",
			webBaseURL, issueKey, issueKey, summaryText,
			assigneeText,
		)

		var matchedAny bool
		var defaultRules []config.NotificationRule

		for _, rule := range s.cfg.Notifications.Telegram.Rules {
			if rule.Default || (len(rule.Components) == 0 && len(rule.Tags) == 0) {
				defaultRules = append(defaultRules, rule)
				continue
			}

			match := false

			// check component
			for _, rc := range rule.Components {
				for _, pc := range components {
					if strings.EqualFold(rc, pc) {
						match = true
						break
					}
				}
				if match {
					break
				}
			}

			// check tags
			if !match {
				for _, rt := range rule.Tags {
					for _, pt := range tags {
						if strings.EqualFold(rt, pt) {
							match = true
							break
						}
					}
					if match {
						break
					}
				}
			}

			if match {
				matchedAny = true
				if err := s.notifier.NotifyToChat(ctx, rule.ChatID, "", msg); err != nil {
					s.logger.Error("Failed to send telegram notification", zap.Error(err))
				} else {
					s.logger.Info("Sent assignment telegram notification",
						zap.String("chat_id", rule.ChatID),
						zap.String("issue_key", issueKey),
						zap.String("assignee", candidateLogin))
				}
			}
		}

		if !matchedAny {
			for _, rule := range defaultRules {
				if err := s.notifier.NotifyToChat(ctx, rule.ChatID, "", msg); err != nil {
					s.logger.Error("Failed to send default telegram notification", zap.Error(err))
				} else {
					s.logger.Info("Sent default assignment telegram notification",
						zap.String("chat_id", rule.ChatID),
						zap.String("issue_key", issueKey),
						zap.String("assignee", candidateLogin))
				}
			}
		}
	}
}

func extractStringListFromVal(val any) []string {
	if val == nil {
		return nil
	}
	if s, ok := val.(string); ok {
		return strings.Split(s, ",")
	}
	if arr, ok := val.([]string); ok {
		return arr
	}
	if arr, ok := val.([]any); ok {
		var res []string
		for _, item := range arr {
			if s, ok := item.(string); ok {
				res = append(res, s)
			}
		}
		return res
	}
	return nil
}

func extractLogin(val any) string {
	if val == nil {
		return ""
	}
	if str, ok := val.(string); ok {
		return str
	}
	if m, ok := val.(map[string]any); ok {
		if login, ok := m["login"].(string); ok && login != "" {
			return login
		}
		if cloudUID, ok := m["cloudUid"].(string); ok && cloudUID != "" {
			return cloudUID
		}
		if id, ok := m["id"].(string); ok && id != "" {
			return id
		}
	}
	return ""
}

func normalizeStatus(s string) string {
	return strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(s), "_", ""), "-", ""))
}

func (s *AssignerService) shouldRemoveAssignee(payload *thhttp.WebhookPayload) bool {
	status := payload.GetStatusKey()

	defaultStatuses := []string{
		"closed", "resolved", "done", "fixed",
		"needinfo", "paused", "waiting", "waitingforuser",
	}

	checkMatch := func(target string) bool {
		targetNorm := normalizeStatus(target)
		if targetNorm == "" {
			return false
		}
		if s.cfg != nil && len(s.cfg.Tracker.Workflow.RemoveAssigneeStatuses) > 0 {
			for _, configured := range s.cfg.Tracker.Workflow.RemoveAssigneeStatuses {
				if normalizeStatus(configured) == targetNorm {
					return true
				}
			}
		}
		// Also check default fallback statuses (TASK-26)
		for _, def := range defaultStatuses {
			if def == targetNorm {
				return true
			}
		}
		return false
	}

	if checkMatch(status) {
		return true
	}
	if payload.IsClosed() {
		return true
	}

	if change, ok := payload.Changes["status"]; ok {
		if toMap, ok := change.To.(map[string]any); ok {
			if key, ok := toMap["key"].(string); ok && checkMatch(key) {
				return true
			}
			if id, ok := toMap["id"].(string); ok && checkMatch(id) {
				return true
			}
			if display, ok := toMap["display"].(string); ok && checkMatch(display) {
				return true
			}
		} else if toStr, ok := change.To.(string); ok && checkMatch(toStr) {
			return true
		}
	}

	return false
}

// resolveCanonicalAssignee resolves an identifier (login, uid, or cloud_uid) to the configured primary Login (TASK-25).
func (s *AssignerService) resolveCanonicalAssignee(identifier string) string {
	if identifier == "" {
		return ""
	}
	if s.cfg != nil {
		if _, a := s.cfg.FindAssignee(identifier); a != nil {
			return a.Login
		}
	}
	return identifier
}
