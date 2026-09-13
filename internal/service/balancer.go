package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"tracker-assigner/internal/config"
	"tracker-assigner/internal/storage"
)

var (
	// ErrNoAvailableAssignee indicates that no candidate is on shift and under capacity.
	ErrNoAvailableAssignee = errors.New("no available assignee (all busy or off-shift)")
	// ErrNoAssigneesInGroup indicates that group has no members configured.
	ErrNoAssigneesInGroup = errors.New("group has no assignees configured")
)

// Candidate wraps assignee configuration with live repository stats.
type Candidate struct {
	Assignee       config.AssigneeConfig
	CurrentLoad    int
	MaxLoad        int
	LastAssignedAt *time.Time
}

// BalancerStrategy defines an interface for choosing an assignee for a ticket in a group.
type BalancerStrategy interface {
	PickAssignee(ctx context.Context, groupID string, group config.GroupConfig, now time.Time) (*config.AssigneeConfig, error)
}

// Balancer selects the best assignee for a ticket based on schedule, capacity, and strategy.
type Balancer struct {
	repo storage.Repository
}

// NewBalancer creates a new Balancer service.
func NewBalancer(repo storage.Repository) *Balancer {
	return &Balancer{repo: repo}
}

// PickAssignee chooses the next assignee for a group according to the group's strategy.
func (b *Balancer) PickAssignee(ctx context.Context, groupID string, group config.GroupConfig, now time.Time) (*config.AssigneeConfig, error) {
	if len(group.Assignees) == 0 {
		return nil, fmt.Errorf("%w for group %s", ErrNoAssigneesInGroup, groupID)
	}

	loads, err := b.repo.GetAssigneesLoad(ctx, groupID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch assignees load for group %s: %w", groupID, err)
	}

	var candidates []Candidate

	for _, assignee := range group.Assignees {
		// 1. Check schedule (working hours and days)
		if !config.IsAssigneeWorking(now, group, assignee) {
			continue
		}

		// 2. Check load limit
		maxLoad := group.MaxLoadPerUser
		if assignee.MaxLoad > 0 {
			maxLoad = assignee.MaxLoad
		}

		rec, exists := loads[assignee.Login]
		if !exists && assignee.UID != "" {
			rec, exists = loads[assignee.UID]
		}
		if !exists && assignee.CloudUID != "" {
			rec, exists = loads[assignee.CloudUID]
		}
		currentLoad := 0
		var lastAssigned *time.Time
		if exists {
			currentLoad = rec.CurrentLoad
			lastAssigned = rec.LastAssignedAt
		}

		if currentLoad >= maxLoad {
			// User reached max allowed active tickets
			continue
		}

		candidates = append(candidates, Candidate{
			Assignee:       assignee,
			CurrentLoad:    currentLoad,
			MaxLoad:        maxLoad,
			LastAssignedAt: lastAssigned,
		})
	}

	if len(candidates) == 0 {
		return nil, ErrNoAvailableAssignee
	}

	selected := b.selectCandidate(group.Strategy, candidates)
	return &selected.Assignee, nil
}

// selectCandidate applies the group's strategy on the filtered candidates.
func (b *Balancer) selectCandidate(strategy config.StrategyType, candidates []Candidate) Candidate {
	if len(candidates) == 1 {
		return candidates[0]
	}

	switch strategy {
	case config.StrategyLoadBased:
		// Sort by current load ASC, then oldest LastAssignedAt ASC
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].CurrentLoad != candidates[j].CurrentLoad {
				return candidates[i].CurrentLoad < candidates[j].CurrentLoad
			}
			return isOldestAssignment(candidates[i].LastAssignedAt, candidates[j].LastAssignedAt)
		})
		return candidates[0]

	case config.StrategyRoundRobin:
		fallthrough
	default:
		// Sort by oldest LastAssignedAt ASC (nil first: never assigned), then lower load
		sort.Slice(candidates, func(i, j int) bool {
			if isTimeDifferent(candidates[i].LastAssignedAt, candidates[j].LastAssignedAt) {
				return isOldestAssignment(candidates[i].LastAssignedAt, candidates[j].LastAssignedAt)
			}
			if candidates[i].CurrentLoad != candidates[j].CurrentLoad {
				return candidates[i].CurrentLoad < candidates[j].CurrentLoad
			}
			return candidates[i].Assignee.Login < candidates[j].Assignee.Login
		})
		return candidates[0]
	}
}

// isTimeDifferent returns true if the two timestamps are not equal.
func isTimeDifferent(t1, t2 *time.Time) bool {
	if t1 == nil && t2 == nil {
		return false
	}
	if t1 == nil || t2 == nil {
		return true
	}
	return !t1.Equal(*t2)
}

// isOldestAssignment returns true if t1 is considered older (or never assigned) than t2.
func isOldestAssignment(t1, t2 *time.Time) bool {
	if t1 == nil && t2 == nil {
		return false
	}
	if t1 == nil {
		// t1 was never assigned -> priority over t2
		return true
	}
	if t2 == nil {
		return false
	}
	return t1.Before(*t2)
}
