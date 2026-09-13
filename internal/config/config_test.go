package config

import (
	"os"
	"testing"
	"time"
)

const sampleConfigYAML = `
server:
  host: "127.0.0.1"
  port: 9090
  secret_token: "super-secret"

tracker:
  base_url: "https://api.tracker.yandex.net"
  token: "oauth-sample"
  org_id: "org-101"
  is_cloud_org: false
  timeout: 10s

database:
  dsn: "file::memory:?cache=shared"

routing_rules:
  - id: "rule-billing"
    queue: "HELP"
    components: ["billing", "payments"]
    tags: ["urgent"]
    target_group: "finance"
  - id: "rule-default"
    queue: "HELP"
    target_group: "support"

groups:
  finance:
    strategy: "load_based"
    max_load_per_user: 5
    schedule:
      timezone: "Europe/Moscow"
      work_days: [1, 2, 3, 4, 5]
      work_hours:
        start: "09:00"
        end: "18:00"
    assignees:
      - login: "alice"
        max_load: 8
      - login: "bob"
  support:
    strategy: "round_robin"
    max_load_per_user: 10
    schedule:
      timezone: "UTC"
      work_days: [1, 2, 3, 4, 5, 6, 7]
      work_hours:
        start: "00:00"
        end: "23:59"
    assignees:
      - login: "charlie"
`

func TestConfig_LoadAndValidate(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(sampleConfigYAML); err != nil {
		t.Fatalf("failed to write sample yaml: %v", err)
	}
	_ = tmpFile.Close()

	cfg, err := Load(tmpFile.Name())
	if err != nil {
		t.Fatalf("expected valid config, got error: %v", err)
	}

	if cfg.Server.Port != 9090 {
		t.Errorf("expected port 9090, got %d", cfg.Server.Port)
	}
	if cfg.Server.SecretToken != "super-secret" {
		t.Errorf("expected secret super-secret, got %s", cfg.Server.SecretToken)
	}
	if len(cfg.RoutingRules) != 2 {
		t.Errorf("expected 2 routing rules, got %d", len(cfg.RoutingRules))
	}
	if len(cfg.Groups) != 2 {
		t.Errorf("expected 2 groups, got %d", len(cfg.Groups))
	}
}

func TestRoutingRule_Matches(t *testing.T) {
	rule := RoutingRule{
		ID:          "test-rule",
		Queue:       "HELP",
		Components:  []string{"Billing", "Payments"},
		Tags:        []string{"urgent"},
		TargetGroup: "finance",
	}

	// Match queue + component + tag
	if !rule.Matches("HELP", []string{"billing"}, []string{"urgent", "external"}) {
		t.Errorf("expected rule to match")
	}

	// Different queue
	if rule.Matches("OTHER", []string{"billing"}, []string{"urgent"}) {
		t.Errorf("expected rule not to match wrong queue")
	}

	// Missing component
	if rule.Matches("HELP", []string{"docs"}, []string{"urgent"}) {
		t.Errorf("expected rule not to match without component")
	}

	// Missing tag
	if rule.Matches("HELP", []string{"billing"}, []string{"low"}) {
		t.Errorf("expected rule not to match without required tag")
	}
}

func TestIsAssigneeWorking(t *testing.T) {
	group := GroupConfig{
		Schedule: ScheduleConfig{
			Timezone: "UTC",
			WorkDays: []int{1, 2, 3, 4, 5}, // Mon-Fri
			WorkHours: WorkHours{
				Start: "09:00",
				End:   "18:00",
			},
		},
	}
	assignee := AssigneeConfig{
		Login: "alice",
	}

	// Monday at 10:00 UTC -> Working
	monWork := time.Date(2026, time.September, 7, 10, 0, 0, 0, time.UTC)
	if !IsAssigneeWorking(monWork, group, assignee) {
		t.Errorf("expected assignee to be working on Monday at 10:00 UTC")
	}

	// Monday at 20:00 UTC -> Not working (after 18:00)
	monAfterHours := time.Date(2026, time.September, 7, 20, 0, 0, 0, time.UTC)
	if IsAssigneeWorking(monAfterHours, group, assignee) {
		t.Errorf("expected assignee not to be working on Monday at 20:00 UTC")
	}

	// Sunday at 12:00 UTC -> Not working (weekend)
	sunWork := time.Date(2026, time.September, 13, 12, 0, 0, 0, time.UTC)
	if IsAssigneeWorking(sunWork, group, assignee) {
		t.Errorf("expected assignee not to be working on Sunday")
	}

	// Assignee with individual schedule (overnight shift)
	assigneeNight := AssigneeConfig{
		Login: "night-owl",
		Schedule: &ScheduleConfig{
			Timezone: "UTC",
			WorkDays: []int{1, 2, 3, 4, 5, 6, 7},
			WorkHours: WorkHours{
				Start: "22:00",
				End:   "06:00",
			},
		},
	}
	nightTime := time.Date(2026, time.September, 7, 23, 30, 0, 0, time.UTC)
	if !IsAssigneeWorking(nightTime, group, assigneeNight) {
		t.Errorf("expected night-owl to be working at 23:30")
	}
	morningTime := time.Date(2026, time.September, 7, 5, 15, 0, 0, time.UTC)
	if !IsAssigneeWorking(morningTime, group, assigneeNight) {
		t.Errorf("expected night-owl to be working at 05:15")
	}
	dayTime := time.Date(2026, time.September, 7, 12, 0, 0, 0, time.UTC)
	if IsAssigneeWorking(dayTime, group, assigneeNight) {
		t.Errorf("expected night-owl NOT to be working at 12:00")
	}
}

func TestAssigneeConfigMatches(t *testing.T) {
	assignee := AssigneeConfig{
		Login:    "tut0roff",
		UID:      "8000000000000002",
		CloudUID: "ajefr4bv6e90bheosaqr",
	}

	if !assignee.Matches("tut0roff") {
		t.Errorf("expected match by login")
	}
	if !assignee.Matches("TUT0ROFF") {
		t.Errorf("expected case-insensitive match by login")
	}
	if !assignee.Matches("8000000000000002") {
		t.Errorf("expected match by UID")
	}
	if !assignee.Matches("ajefr4bv6e90bheosaqr") {
		t.Errorf("expected match by CloudUID")
	}
	if assignee.Matches("unknown") {
		t.Errorf("unexpected match for unknown id")
	}
	if assignee.Matches("") {
		t.Errorf("unexpected match for empty id")
	}
}

func TestAssigneeAbsences(t *testing.T) {
	group := GroupConfig{
		Schedule: ScheduleConfig{
			Timezone: "UTC",
			WorkDays: []int{1, 2, 3, 4, 5},
			WorkHours: WorkHours{
				Start: "09:00",
				End:   "18:00",
			},
		},
	}
	assignee := AssigneeConfig{
		Login: "alice",
		Absences: []AbsencePeriod{
			{
				Start:  "2026-09-10",
				End:    "2026-09-15",
				Reason: "vacation",
			},
		},
	}

	// During absence: 2026-09-11 10:00 (Friday during work hours)
	vacationTime := time.Date(2026, time.September, 11, 10, 0, 0, 0, time.UTC)
	if !IsAssigneeAbsent(vacationTime, assignee) {
		t.Errorf("expected alice to be absent on 2026-09-11")
	}
	if IsAssigneeWorking(vacationTime, group, assignee) {
		t.Errorf("expected alice NOT to be working during vacation")
	}

	// After absence: 2026-09-16 10:00 (Wednesday during work hours)
	afterVacation := time.Date(2026, time.September, 16, 10, 0, 0, 0, time.UTC)
	if IsAssigneeAbsent(afterVacation, assignee) {
		t.Errorf("expected alice NOT to be absent on 2026-09-16")
	}
	if !IsAssigneeWorking(afterVacation, group, assignee) {
		t.Errorf("expected alice to be working after vacation")
	}
}
