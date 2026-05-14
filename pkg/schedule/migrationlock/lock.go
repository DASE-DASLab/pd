// Copyright 2026 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Package migrationlock provides a process-wide registry of regions that are
// currently undergoing TiLiM DTCM live migration. Checkers (rule, replica,
// learner) consult this registry to skip migrating regions so PD's normal
// reconciliation logic does not race with the migration scheduler.
//
// Concretely, DTCM may transiently leave a region in a 3-voter + 1-learner
// configuration between PhaseAddLearner and PhaseTransferLeader's atomic swap.
// PD's rule_checker would otherwise schedule a "remove-orphan-peer" operator
// against the freshly added learner and destroy it before the migration
// completes.
package migrationlock

import "sync"

var (
	mu     sync.RWMutex
	locked = map[uint64]struct{}{}
)

// Lock marks a region as actively migrating. Safe to call multiple times.
func Lock(regionID uint64) {
	mu.Lock()
	locked[regionID] = struct{}{}
	mu.Unlock()
}

// Unlock clears the active-migration marker. Idempotent.
func Unlock(regionID uint64) {
	mu.Lock()
	delete(locked, regionID)
	mu.Unlock()
}

// IsLocked reports whether the region is currently in DTCM migration.
func IsLocked(regionID uint64) bool {
	mu.RLock()
	_, ok := locked[regionID]
	mu.RUnlock()
	return ok
}
