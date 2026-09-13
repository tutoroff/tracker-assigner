package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// StrategyType defines balancing strategy.
type StrategyType string

const (
	StrategyRoundRobin StrategyType = "round_robin"
	StrategyLoadBased  StrategyType = "load_based"
)

// ServerConfig holds HTTP server configuration.
type ServerConfig struct {
	Host        string `yaml:"host"`
	Port        int    `yaml:"port"`
	SecretToken string `yaml:"secret_token"`
}

// TrackerWorkflowConfig holds workflow settings for Yandex Tracker.
type TrackerWorkflowConfig struct {
	AssignTransition       string            `yaml:"assign_transition"`
	TransitionMap          map[string]string `yaml:"transition_map,omitempty"`
	RemoveAssigneeStatuses []string          `yaml:"remove_assignee_statuses"`
	AssignCommentTemplate  string            `yaml:"assign_comment_template"`
}

// TrackerConfig holds Yandex Tracker API configuration.
type TrackerConfig struct {
	BaseURL       string                `yaml:"base_url"`
	WebURL        string                `yaml:"web_url,omitempty"`
	Token         string                `yaml:"token"`
	OrgID         string                `yaml:"org_id"`
	IsCloudOrg    bool                  `yaml:"is_cloud_org"`
	Timeout       time.Duration         `yaml:"timeout"`
	RobotLogin    string                `yaml:"robot_login,omitempty"`
	RobotUID      string                `yaml:"robot_uid,omitempty"`
	RobotCloudUID string                `yaml:"robot_cloud_uid,omitempty"`
	Workflow      TrackerWorkflowConfig `yaml:"workflow"`
}

// DatabaseConfig holds SQLite database configuration.
type DatabaseConfig struct {
	DSN string `yaml:"dsn"`
}

// RoutingRule defines a rule matching a ticket to a group.
type RoutingRule struct {
	ID          string   `yaml:"id"`
	Queue       string   `yaml:"queue"`
	Components  []string `yaml:"components"`
	Tags        []string `yaml:"tags"`
	TargetGroup string   `yaml:"target_group"`
}

// Matches checks if the rule matches the given queue, components, and tags.
func (r *RoutingRule) Matches(queue string, components, tags []string) bool {
	// Queue matching is required if rule.Queue is specified
	if r.Queue != "" && !strings.EqualFold(r.Queue, queue) {
		return false
	}

	// If rule specifies components, at least one must match
	if len(r.Components) > 0 {
		matched := false
		for _, rc := range r.Components {
			for _, c := range components {
				if strings.EqualFold(rc, c) {
					matched = true
					break
				}
			}
			if matched {
				break
			}
		}
		if !matched {
			return false
		}
	}

	// If rule specifies tags, all rule tags or at least one?
	// Usually tags in rule are required tags (subset match)
	if len(r.Tags) > 0 {
		matchedCount := 0
		for _, rt := range r.Tags {
			for _, t := range tags {
				if strings.EqualFold(rt, t) {
					matchedCount++
					break
				}
			}
		}
		if matchedCount < len(r.Tags) {
			return false
		}
	}

	return true
}

// WorkHours defines working hours in HH:MM format.
type WorkHours struct {
	Start string `yaml:"start"`
	End   string `yaml:"end"`
}

// ScheduleConfig defines weekly work schedule and timezone.
type ScheduleConfig struct {
	Timezone  string    `yaml:"timezone"`
	WorkDays  []int     `yaml:"work_days"` // 1=Monday, 7=Sunday
	WorkHours WorkHours `yaml:"work_hours"`
}

// AbsencePeriod defines a vacation, sick leave, or absence range.
type AbsencePeriod struct {
	Start  string `yaml:"start"`  // "YYYY-MM-DD" or RFC3339
	End    string `yaml:"end"`    // "YYYY-MM-DD" or RFC3339
	Reason string `yaml:"reason"` // e.g. "vacation", "sick_leave"
}

// AssigneeConfig defines an individual team member.
type AssigneeConfig struct {
	Login    string          `yaml:"login"`
	Name     string          `yaml:"name,omitempty"`      // Full name e.g. "Кирилл Бобров"
	UID      string          `yaml:"uid,omitempty"`       // e.g. "8000000000000002"
	CloudUID string          `yaml:"cloud_uid,omitempty"` // e.g. "ajefr4bv6e90bheosaqr"
	MaxLoad  int             `yaml:"max_load,omitempty"`  // Override group max_load if > 0
	Schedule *ScheduleConfig `yaml:"schedule,omitempty"`  // Override group schedule if set
	Absences []AbsencePeriod `yaml:"absences,omitempty"`
}

// Matches checks if the assignee matches the given identifier (login, uid, or cloud_uid).
func (a *AssigneeConfig) Matches(identifier string) bool {
	if identifier == "" {
		return false
	}
	if strings.EqualFold(a.Login, identifier) {
		return true
	}
	if a.UID != "" && strings.EqualFold(a.UID, identifier) {
		return true
	}
	if a.CloudUID != "" && strings.EqualFold(a.CloudUID, identifier) {
		return true
	}
	return false
}

// GroupConfig defines a team group for dispatching tickets.
type GroupConfig struct {
	Strategy        StrategyType     `yaml:"strategy"` // round_robin or load_based
	MaxLoadPerUser  int              `yaml:"max_load_per_user"`
	Schedule        ScheduleConfig   `yaml:"schedule"`
	ActiveStatuses  []string         `yaml:"active_statuses,omitempty"`  // Statuses that count towards active load (e.g. "open", "inProgress")
	IgnoredStatuses []string         `yaml:"ignored_statuses,omitempty"` // Statuses ignored for load (e.g. "needInfo", "waiting")
	Assignees       []AssigneeConfig `yaml:"assignees"`
}

// TelegramConfig defines settings for Telegram alerts.
type TelegramConfig struct {
	Enabled  bool   `yaml:"enabled"`
	BotToken string `yaml:"bot_token"`
	ChatID   string `yaml:"chat_id"`
}

// AlertsConfig defines alerting settings for DLQ and SLA escalations.
type AlertsConfig struct {
	Telegram          TelegramConfig `yaml:"telegram"`
	PendingSLATimeout time.Duration  `yaml:"pending_sla_timeout"` // e.g. 2h
}

// NotificationRule defines rules for telegram notifications.
type NotificationRule struct {
	Components []string `yaml:"components"`
	Tags       []string `yaml:"tags"`
	ChatID     string   `yaml:"chat_id"`
	Default    bool     `yaml:"default"`
}

// TelegramNotificationsConfig defines telegram notification rules.
type TelegramNotificationsConfig struct {
	Enabled  bool               `yaml:"enabled"`
	BotToken string             `yaml:"bot_token"`
	Rules    []NotificationRule `yaml:"rules"`
}

// NotificationsConfig defines application notification settings.
type NotificationsConfig struct {
	Telegram TelegramNotificationsConfig `yaml:"telegram"`
}

// Config represents the complete application configuration.
type Config struct {
	Server        ServerConfig           `yaml:"server"`
	Tracker       TrackerConfig          `yaml:"tracker"`
	Database      DatabaseConfig         `yaml:"database"`
	RoutingRules  []RoutingRule          `yaml:"routing_rules"`
	Groups        map[string]GroupConfig `yaml:"groups"`
	Alerts        AlertsConfig           `yaml:"alerts"`
	Notifications NotificationsConfig    `yaml:"notifications"`
}

// Load reads and parses configuration from a YAML file, with environment variable overrides.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse yaml config: %w", err)
	}

	cfg.applyDefaultsAndEnv()

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation error: %w", err)
	}

	return &cfg, nil
}

// applyDefaultsAndEnv fills defaults and applies environment variables.
func (c *Config) applyDefaultsAndEnv() {
	if c.Server.Host == "" {
		c.Server.Host = "0.0.0.0"
	}
	if c.Server.Port == 0 {
		c.Server.Port = 8080
	}
	if envPort := os.Getenv("SERVER_PORT"); envPort != "" {
		if p, err := strconv.Atoi(envPort); err == nil {
			c.Server.Port = p
		}
	}
	if envSecret := os.Getenv("WEBHOOK_SECRET"); envSecret != "" {
		c.Server.SecretToken = envSecret
	}

	if c.Tracker.BaseURL == "" {
		c.Tracker.BaseURL = "https://api.tracker.yandex.net"
	}
	if envToken := os.Getenv("TRACKER_TOKEN"); envToken != "" {
		c.Tracker.Token = envToken
	}
	if envOrg := os.Getenv("TRACKER_ORG_ID"); envOrg != "" {
		c.Tracker.OrgID = envOrg
	}
	if envCloud := os.Getenv("TRACKER_IS_CLOUD"); envCloud != "" {
		c.Tracker.IsCloudOrg = strings.ToLower(envCloud) == "true" || envCloud == "1"
	}
	if c.Tracker.Timeout <= 0 {
		c.Tracker.Timeout = 15 * time.Second
	}
	if envRobotLogin := os.Getenv("TRACKER_ROBOT_LOGIN"); envRobotLogin != "" {
		c.Tracker.RobotLogin = envRobotLogin
	}
	if envRobotUID := os.Getenv("TRACKER_ROBOT_UID"); envRobotUID != "" {
		c.Tracker.RobotUID = envRobotUID
	}
	if envRobotCloudUID := os.Getenv("TRACKER_ROBOT_CLOUD_UID"); envRobotCloudUID != "" {
		c.Tracker.RobotCloudUID = envRobotCloudUID
	}

	if c.Database.DSN == "" {
		c.Database.DSN = "assigner.db"
	}
	if envDSN := os.Getenv("DATABASE_DSN"); envDSN != "" {
		c.Database.DSN = envDSN
	}

	// Set default strategy for groups
	for k, g := range c.Groups {
		if g.Strategy == "" {
			g.Strategy = StrategyRoundRobin
		}
		if g.MaxLoadPerUser <= 0 {
			g.MaxLoadPerUser = 10
		}
		if g.Schedule.Timezone == "" {
			g.Schedule.Timezone = "UTC"
		}
		if len(g.Schedule.WorkDays) == 0 {
			g.Schedule.WorkDays = []int{1, 2, 3, 4, 5} // Mon-Fri
		}
		if g.Schedule.WorkHours.Start == "" {
			g.Schedule.WorkHours.Start = "00:00"
		}
		if g.Schedule.WorkHours.End == "" {
			g.Schedule.WorkHours.End = "23:59"
		}
		c.Groups[k] = g
	}

	// Default Alerts configuration
	if c.Alerts.PendingSLATimeout <= 0 {
		c.Alerts.PendingSLATimeout = 2 * time.Hour
	}
	if envTgToken := os.Getenv("TELEGRAM_BOT_TOKEN"); envTgToken != "" {
		c.Alerts.Telegram.BotToken = envTgToken
		c.Alerts.Telegram.Enabled = true
	}
	if envTgChat := os.Getenv("TELEGRAM_CHAT_ID"); envTgChat != "" {
		c.Alerts.Telegram.ChatID = envTgChat
	}
}

// FindAssignee finds an assignee by login, uid, or cloud_uid across all groups.
func (c *Config) FindAssignee(identifier string) (string, *AssigneeConfig) {
	if identifier == "" {
		return "", nil
	}
	for gName, group := range c.Groups {
		for _, member := range group.Assignees {
			if member.Matches(identifier) {
				return gName, &member
			}
		}
	}
	return "", nil
}

// Validate checks configuration logic and consistency.
func (c *Config) Validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d", c.Server.Port)
	}

	for _, rule := range c.RoutingRules {
		if rule.ID == "" {
			return fmt.Errorf("routing rule must have an id")
		}
		if rule.TargetGroup == "" {
			return fmt.Errorf("routing rule %q must have target_group", rule.ID)
		}
		if _, ok := c.Groups[rule.TargetGroup]; !ok {
			return fmt.Errorf("routing rule %q references non-existent group %q", rule.ID, rule.TargetGroup)
		}
	}

	for groupName, group := range c.Groups {
		if group.Strategy != StrategyRoundRobin && group.Strategy != StrategyLoadBased {
			return fmt.Errorf("group %q has unknown strategy %q (must be %q or %q)",
				groupName, group.Strategy, StrategyRoundRobin, StrategyLoadBased)
		}
		if _, err := time.LoadLocation(group.Schedule.Timezone); err != nil {
			return fmt.Errorf("group %q has invalid timezone %q: %w", groupName, group.Schedule.Timezone, err)
		}
		if err := validateHours(group.Schedule.WorkHours); err != nil {
			return fmt.Errorf("group %q has invalid work hours: %w", groupName, err)
		}
		for _, member := range group.Assignees {
			if member.Login == "" {
				return fmt.Errorf("group %q contains assignee with empty login", groupName)
			}
			if member.Schedule != nil {
				if _, err := time.LoadLocation(member.Schedule.Timezone); err != nil {
					return fmt.Errorf("assignee %q in group %q has invalid timezone: %w", member.Login, groupName, err)
				}
				if err := validateHours(member.Schedule.WorkHours); err != nil {
					return fmt.Errorf("assignee %q in group %q has invalid work hours: %w", member.Login, groupName, err)
				}
			}
			for _, abs := range member.Absences {
				if err := validateAbsence(abs); err != nil {
					return fmt.Errorf("assignee %q in group %q has invalid absence: %w", member.Login, groupName, err)
				}
			}
		}
	}

	return nil
}

func validateHours(wh WorkHours) error {
	if wh.Start == "" && wh.End == "" {
		return nil
	}
	startParts := strings.Split(wh.Start, ":")
	endParts := strings.Split(wh.End, ":")
	if len(startParts) != 2 || len(endParts) != 2 {
		return fmt.Errorf("work hours must be in HH:MM format, got start=%q, end=%q", wh.Start, wh.End)
	}
	sH, err1 := strconv.Atoi(startParts[0])
	sM, err2 := strconv.Atoi(startParts[1])
	eH, err3 := strconv.Atoi(endParts[0])
	eM, err4 := strconv.Atoi(endParts[1])
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil ||
		sH < 0 || sH > 23 || sM < 0 || sM > 59 || eH < 0 || eH > 23 || eM < 0 || eM > 59 {
		return fmt.Errorf("invalid hour or minute value in start=%q, end=%q", wh.Start, wh.End)
	}
	return nil
}

func validateAbsence(p AbsencePeriod) error {
	layouts := []string{"2006-01-02", time.RFC3339}
	validStart := p.Start == ""
	for _, l := range layouts {
		if _, err := time.Parse(l, p.Start); err == nil {
			validStart = true
			break
		}
	}
	if !validStart {
		return fmt.Errorf("invalid absence start date %q (expected YYYY-MM-DD or RFC3339)", p.Start)
	}

	validEnd := p.End == ""
	for _, l := range layouts {
		if _, err := time.Parse(l, p.End); err == nil {
			validEnd = true
			break
		}
	}
	if !validEnd {
		return fmt.Errorf("invalid absence end date %q (expected YYYY-MM-DD or RFC3339)", p.End)
	}
	return nil
}

// IsAssigneeAbsent checks if an assignee is currently in an active absence period.
func IsAssigneeAbsent(now time.Time, assignee AssigneeConfig) bool {
	for _, period := range assignee.Absences {
		if isTimeInAbsence(now, period.Start, period.End) {
			return true
		}
	}
	return false
}

func isTimeInAbsence(now time.Time, startStr, endStr string) bool {
	if startStr == "" && endStr == "" {
		return false
	}
	layouts := []string{"2006-01-02", time.RFC3339}
	var startTime, endTime time.Time

	for _, layout := range layouts {
		if startTime.IsZero() && startStr != "" {
			if t, e := time.Parse(layout, startStr); e == nil {
				startTime = t
			}
		}
		if endTime.IsZero() && endStr != "" {
			if t, e := time.Parse(layout, endStr); e == nil {
				if layout == "2006-01-02" {
					t = t.Add(24*time.Hour - time.Nanosecond)
				}
				endTime = t
			}
		}
	}

	if !startTime.IsZero() && now.Before(startTime) {
		return false
	}
	if !endTime.IsZero() && now.After(endTime) {
		return false
	}
	return !startTime.IsZero() || !endTime.IsZero()
}

// IsAssigneeWorking checks if an assignee is currently working at the given time.
func IsAssigneeWorking(t time.Time, group GroupConfig, assignee AssigneeConfig) bool {
	if IsAssigneeAbsent(t, assignee) {
		return false
	}

	sched := group.Schedule
	if assignee.Schedule != nil {
		sched = *assignee.Schedule
	}

	loc, err := time.LoadLocation(sched.Timezone)
	if err != nil {
		loc = time.UTC
	}
	localTime := t.In(loc)

	// In Go: Sunday=0, Monday=1, ..., Saturday=6
	// In our config: 1=Mon, ..., 7=Sun
	weekday := int(localTime.Weekday())
	if weekday == 0 {
		weekday = 7 // Sunday is 7
	}

	dayMatched := false
	for _, d := range sched.WorkDays {
		if d == weekday {
			dayMatched = true
			break
		}
	}
	if !dayMatched {
		return false
	}

	// Check hours
	currentMinutes := localTime.Hour()*60 + localTime.Minute()

	sParts := strings.Split(sched.WorkHours.Start, ":")
	eParts := strings.Split(sched.WorkHours.End, ":")
	if len(sParts) != 2 || len(eParts) != 2 {
		return true // default allow if invalid
	}
	sH, _ := strconv.Atoi(sParts[0])
	sM, _ := strconv.Atoi(sParts[1])
	eH, _ := strconv.Atoi(eParts[0])
	eM, _ := strconv.Atoi(eParts[1])

	startMinutes := sH*60 + sM
	endMinutes := eH*60 + eM

	if startMinutes <= endMinutes {
		return currentMinutes >= startMinutes && currentMinutes <= endMinutes
	}
	// Overnight shift (e.g. 22:00 to 06:00)
	return currentMinutes >= startMinutes || currentMinutes <= endMinutes
}
