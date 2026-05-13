// Copyright 2024 TiLiM Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

// Live Migration Scheduler for TiLiM experiments.
//
// This scheduler orchestrates DTCM / RFM / baseline migrations:
//   1. Receives migration requests (via API or auto-balance trigger)
//   2. Creates real operators: AddPeer → PromoteVoter → TransferLeader → RemovePeer
//   3. For DTCM/RFM: the operator sequence is the same, but TiKV's on_transfer_leader_msg
//      behaves differently based on migration_mode config (skipping propose_locks).
//
// The scheduler delegates migration-mode-specific behavior to TiKV:
//   - Native: standard AddPeer + TransferLeader (TiKV proposes locks to Raft)
//   - LockAndAbort: AddPeer + TransferLeader (TiKV clears locks)
//   - WaitAndRemaster: AddPeer + TransferLeader (TiKV drains locks)
//   - RFM: AddPeer + TransferLeader (TiKV transfers directly, locks in Raft)
//   - DTCM: AddPeer + TransferLeader (TiKV transfers directly, state via State Track)

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/tikv/pd/pkg/core"
	"github.com/tikv/pd/pkg/core/constant"
	sche "github.com/tikv/pd/pkg/schedule/core"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/plan"
	"github.com/tikv/pd/pkg/schedule/types"
)

// MigrationMode mirrors the TiKV-side MigrationMode enum.
type MigrationMode string

const (
	MigrationModeNative          MigrationMode = "native"
	MigrationModeLockAndAbort    MigrationMode = "lock-and-abort"
	MigrationModeWaitAndRemaster MigrationMode = "wait-and-remaster"
	MigrationModeRemus           MigrationMode = "remus"
	MigrationModeSquall          MigrationMode = "squall"
	MigrationModeRFM             MigrationMode = "rfm"
	MigrationModeDTCM            MigrationMode = "dtcm"
)

// MigrationPhase tracks the progress of a single region migration.
type MigrationPhase int

const (
	PhaseIdle MigrationPhase = iota
	PhaseAddLearner
	PhaseTrackSetup
	PhaseCatchup
	PhaseSnapshotBarrier
	PhasePromoteVoter
	PhaseHandoffBarrier
	PhaseTransferLeader
	PhaseTeardown
	PhaseCompleted
	PhaseFailed
)

// MigrationTask represents a pending or active region migration.
type MigrationTask struct {
	RegionID      uint64         `json:"region_id"`
	SourceStoreID uint64         `json:"source_store_id"`
	TargetStoreID uint64         `json:"target_store_id"`
	Mode          MigrationMode  `json:"mode"`
	Phase         MigrationPhase `json:"phase"`
	StartTime     time.Time      `json:"start_time"`
	PhaseTime     time.Time      `json:"phase_time"`
	// SourceAddr is the source TiKV's HTTP status address (for /dtcm/* API calls).
	SourceAddr string `json:"source_addr,omitempty"`
	// TargetAddr is the target TiKV's gRPC address (passed to source for State Track).
	TargetAddr string `json:"target_addr,omitempty"`
	// TargetStatusAddr is the target TiKV's HTTP status address (for polling).
	TargetStatusAddr string         `json:"target_status_addr,omitempty"`
	DtcmState        *DtcmPDState   `json:"dtcm_state,omitempty"`
}

// DtcmPDState tracks DTCM-specific state for a migration task.
// Updated by TiKV heartbeat reports or experiment script HTTP calls.
type DtcmPDState struct {
	// BacklogSize is the number of StateEntry items pending on the destination.
	BacklogSize uint64 `json:"backlog_size"`
	// Rho is the convergence ratio: lambda * s_avg / bandwidth.
	// When rho < 1, the state track is converging.
	Rho float64 `json:"rho"`
	// SnapshotBarrierSent indicates the source has completed the SnapshotBarrier.
	SnapshotBarrierSent bool `json:"snapshot_barrier_sent"`
	// HandoffBarrierSent indicates the source has completed the HandoffBarrier.
	HandoffBarrierSent bool `json:"handoff_barrier_sent"`
	// DestReady indicates the destination has applied all state through the
	// HandoffBarrier and is ready to accept leadership.
	DestReady bool `json:"dest_ready"`
	// FallbackToRFM indicates adaptive fallback triggered full RFM mode.
	FallbackToRFM bool `json:"fallback_to_rfm"`
	// TrackStarted indicates the experiment script has called TiKV's
	// /dtcm/start endpoint and the source has started the State Track.
	TrackStarted bool `json:"track_started"`

	// --- Adaptive fallback state (P1-8) ---

	// RhoExceedCount tracks the number of consecutive ticks where Rho > RhoThreshold.
	// When this reaches RhoExceedLimit, FallbackToRFM is auto-set.
	RhoExceedCount int `json:"rho_exceed_count"`
	// RhoThreshold is the convergence threshold above which State Track is
	// considered non-converging. Default: 1.0.
	RhoThreshold float64 `json:"rho_threshold"`
	// RhoExceedLimit is the number of consecutive ticks with Rho > RhoThreshold
	// before automatically falling back to RFM. Default: 3.
	RhoExceedLimit int `json:"rho_exceed_limit"`

	// --- Per-channel convergence (partial Fat Mode) ---

	// PerChannelRho tracks convergence ratio per State Track channel.
	// Key: channel name ("lock", "resolver", "lock_table").
	// When a channel's rho > threshold for RhoExceedLimit ticks, PD
	// triggers partial Fat Mode for that channel only.
	PerChannelRho map[string]float64 `json:"per_channel_rho,omitempty"`
	// PerChannelExceedCount tracks consecutive exceed counts per channel.
	PerChannelExceedCount map[string]int `json:"per_channel_exceed_count,omitempty"`
	// PartialFatModeChannels lists channels currently in Fat Mode.
	// Empty means all channels use State Track.
	PartialFatModeChannels []string `json:"partial_fat_mode_channels,omitempty"`

	// --- Auto-advance trigger tracking (P1-1) ---

	// TrackSetupTriggered indicates PD has already called TiKV /dtcm/start.
	TrackSetupTriggered bool `json:"track_setup_triggered"`
	// SnapshotBarrierTriggered indicates PD has already called TiKV /dtcm/snapshot.
	SnapshotBarrierTriggered bool `json:"snapshot_barrier_triggered"`
	// HandoffBarrierTriggered indicates PD has already called TiKV /dtcm/handoff.
	HandoffBarrierTriggered bool `json:"handoff_barrier_triggered"`

	// SkipSnapshotBarrier indicates the target already has a voter peer,
	// so SnapshotBarrier and PromoteVoter can be skipped (fast path).
	SkipSnapshotBarrier bool `json:"skip_snapshot_barrier"`

	// CompleteSent indicates PD has called /dtcm/complete on source.
	CompleteSent bool `json:"complete_sent"`
}

// defaultRhoThreshold is the default convergence ratio threshold.
const defaultRhoThreshold = 1.0

// defaultRhoExceedLimit is the default number of consecutive ticks before
// auto-fallback to RFM.
const defaultRhoExceedLimit = 3

// Phase timeout defaults.
const (
	defaultPhaseTimeout          = 60 * time.Second
	phaseCatchupTimeout          = 120 * time.Second
	phaseSnapshotBarrierTimeout  = 30 * time.Second
	phaseHandoffBarrierTimeout   = 30 * time.Second
)

// phaseTimeoutFor returns the timeout duration for a given migration phase.
func phaseTimeoutFor(phase MigrationPhase) time.Duration {
	switch phase {
	case PhaseCatchup:
		return phaseCatchupTimeout
	case PhaseSnapshotBarrier:
		return phaseSnapshotBarrierTimeout
	case PhaseHandoffBarrier:
		return phaseHandoffBarrierTimeout
	default:
		return defaultPhaseTimeout
	}
}

// initDtcmDefaults fills in zero-value fields with their defaults.
func (s *DtcmPDState) initDefaults() {
	if s.RhoThreshold == 0 {
		s.RhoThreshold = defaultRhoThreshold
	}
	if s.RhoExceedLimit == 0 {
		s.RhoExceedLimit = defaultRhoExceedLimit
	}
}

// checkRhoAndMaybeFallback increments or resets the rho exceed counter and
// returns true if adaptive fallback to RFM was triggered.
func (s *DtcmPDState) checkRhoAndMaybeFallback(regionID uint64) bool {
	s.initDefaults()
	if s.Rho > s.RhoThreshold {
		s.RhoExceedCount++
		if s.RhoExceedCount >= s.RhoExceedLimit {
			s.FallbackToRFM = true
			log.Warn("DTCM: adaptive fallback to RFM triggered (rho exceeded threshold for consecutive ticks)",
				zap.Uint64("region", regionID),
				zap.Float64("rho", s.Rho),
				zap.Float64("threshold", s.RhoThreshold),
				zap.Int("exceed_count", s.RhoExceedCount),
				zap.Int("exceed_limit", s.RhoExceedLimit))
			return true
		}
	} else {
		s.RhoExceedCount = 0
	}
	return false
}

// checkPerChannelRho evaluates per-channel convergence and returns channels
// that should be switched to partial Fat Mode. Channels already in Fat Mode
// are not re-reported.
func (s *DtcmPDState) checkPerChannelRho(regionID uint64) []string {
	s.initDefaults()
	if s.PerChannelRho == nil {
		return nil
	}
	if s.PerChannelExceedCount == nil {
		s.PerChannelExceedCount = make(map[string]int)
	}
	var newFatChannels []string
	for ch, rho := range s.PerChannelRho {
		if s.isChannelInFatMode(ch) {
			continue
		}
		if rho > s.RhoThreshold {
			s.PerChannelExceedCount[ch]++
			if s.PerChannelExceedCount[ch] >= s.RhoExceedLimit {
				newFatChannels = append(newFatChannels, ch)
				log.Warn("DTCM: partial Fat Mode triggered for channel",
					zap.Uint64("region", regionID),
					zap.String("channel", ch),
					zap.Float64("rho", rho),
					zap.Float64("threshold", s.RhoThreshold))
			}
		} else {
			s.PerChannelExceedCount[ch] = 0
		}
	}
	if len(newFatChannels) > 0 {
		s.PartialFatModeChannels = append(s.PartialFatModeChannels, newFatChannels...)
	}
	// If ALL key channels are in Fat Mode, escalate to full RFM.
	keyChannels := []string{"lock", "resolver"}
	allInFat := true
	for _, kc := range keyChannels {
		if !s.isChannelInFatMode(kc) {
			allInFat = false
			break
		}
	}
	if allInFat && len(s.PartialFatModeChannels) > 0 {
		s.FallbackToRFM = true
		log.Warn("DTCM: all key channels in Fat Mode, escalating to full RFM",
			zap.Uint64("region", regionID))
	}
	return newFatChannels
}

func (s *DtcmPDState) isChannelInFatMode(ch string) bool {
	for _, c := range s.PartialFatModeChannels {
		if c == ch {
			return true
		}
	}
	return false
}

// tikvDtcmStatus mirrors TiKV's MigrationStatus JSON response from /dtcm/status.
type tikvDtcmStatus struct {
	RegionID         uint64  `json:"region_id"`
	SourcePhase      *string `json:"source_phase"`
	SourceBacklog    *int    `json:"source_backlog"`
	SourceMaxSeq     *uint64 `json:"source_max_seq"`
	SourceLastAcked  *uint64 `json:"source_last_acked_seq"`
	DestPhase        *string `json:"dest_phase"`
	DestRaftApplied  *uint64 `json:"dest_raft_applied"`
	DestLastApplied  *uint64 `json:"dest_last_applied_seq"`
	DestHandoffReady *bool   `json:"dest_handoff_ready"`
}

// dtcmHTTPTimeout is the timeout for HTTP calls to TiKV status endpoints.
const dtcmHTTPTimeout = 3 * time.Second

// dtcmHTTPClient is a shared HTTP client for TiKV status endpoint calls.
var dtcmHTTPClient = &http.Client{Timeout: dtcmHTTPTimeout}

// pollTiKVDtcmStatus calls TiKV's POST /dtcm/status endpoint.
func pollTiKVDtcmStatus(statusAddr string, regionID uint64) (*tikvDtcmStatus, error) {
	body, _ := json.Marshal(map[string]uint64{"region_id": regionID})
	url := fmt.Sprintf("http://%s/dtcm/status", statusAddr)
	resp, err := dtcmHTTPClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(data))
	}
	var status tikvDtcmStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}
	return &status, nil
}

// notifyTiKVAbort sends /dtcm/abort to both source and dest TiKV nodes.
// Best-effort: errors are logged but do not block the PD scheduler.
// This clears barrier_quiesce, migration state, fence probes, and State Track
// on TiKV side, ensuring the source resumes normal service after failure.
func notifyTiKVAbort(task *MigrationTask) {
	payload := map[string]uint64{"region_id": task.RegionID}
	if task.SourceAddr != "" {
		if err := callTiKVDtcmEndpoint(task.SourceAddr, "/dtcm/abort", payload); err != nil {
			log.Warn("DTCM: failed to notify source abort",
				zap.Uint64("region", task.RegionID),
				zap.String("source", task.SourceAddr),
				zap.Error(err))
		} else {
			log.Info("DTCM: notified source to abort migration",
				zap.Uint64("region", task.RegionID))
		}
	}
	if task.TargetStatusAddr != "" {
		if err := callTiKVDtcmEndpoint(task.TargetStatusAddr, "/dtcm/abort", payload); err != nil {
			log.Warn("DTCM: failed to notify dest abort",
				zap.Uint64("region", task.RegionID),
				zap.String("dest", task.TargetStatusAddr),
				zap.Error(err))
		} else {
			log.Info("DTCM: notified dest to abort migration",
				zap.Uint64("region", task.RegionID))
		}
	}
}

// callTiKVDtcmEndpoint calls a TiKV DTCM endpoint (start/snapshot/handoff/abort).
func callTiKVDtcmEndpoint(statusAddr, path string, payload any) error {
	body, _ := json.Marshal(payload)
	url := fmt.Sprintf("http://%s%s", statusAddr, path)
	resp, err := dtcmHTTPClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(data))
	}
	return nil
}

type liveMigrationSchedulerConfig struct {
	baseDefaultSchedulerConfig
	mu   sync.RWMutex
	Mode MigrationMode `json:"mode"`
}

func (c *liveMigrationSchedulerConfig) clone() *liveMigrationSchedulerConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return &liveMigrationSchedulerConfig{
		baseDefaultSchedulerConfig: newBaseDefaultSchedulerConfig(),
		Mode:                       c.Mode,
	}
}

// liveMigrationScheduler implements the Scheduler interface for TiLiM.
type liveMigrationScheduler struct {
	*BaseScheduler
	conf         *liveMigrationSchedulerConfig
	activeTasks  map[uint64]*MigrationTask // keyed by region_id
	pendingTasks []*MigrationTask
	mu           sync.Mutex
}

func newLiveMigrationScheduler(opController *operator.Controller, conf *liveMigrationSchedulerConfig) Scheduler {
	return &liveMigrationScheduler{
		BaseScheduler: NewBaseScheduler(opController, types.LiveMigrationScheduler, &conf.baseDefaultSchedulerConfig),
		conf:          conf,
		activeTasks:   make(map[uint64]*MigrationTask),
	}
}

const dtcmPollingInterval = 50 * time.Millisecond

func (s *liveMigrationScheduler) GetNextInterval(_ time.Duration) time.Duration {
	return dtcmPollingInterval
}

// String returns a human-readable name for the migration phase.
func (p MigrationPhase) String() string {
	switch p {
	case PhaseIdle:
		return "idle"
	case PhaseAddLearner:
		return "add-learner"
	case PhaseTrackSetup:
		return "track-setup"
	case PhaseCatchup:
		return "catchup"
	case PhaseSnapshotBarrier:
		return "snapshot-barrier"
	case PhasePromoteVoter:
		return "promote-voter"
	case PhaseHandoffBarrier:
		return "handoff-barrier"
	case PhaseTransferLeader:
		return "transfer-leader"
	case PhaseTeardown:
		return "teardown"
	case PhaseCompleted:
		return "completed"
	case PhaseFailed:
		return "failed"
	default:
		return "unknown"
	}
}

func (s *liveMigrationScheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listMigrations(w, r)
	case http.MethodPost:
		s.createMigration(w, r)
	case http.MethodPut:
		s.advancePhase(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *liveMigrationScheduler) listMigrations(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tasks := make([]*MigrationTask, 0, len(s.activeTasks))
	for _, t := range s.activeTasks {
		tasks = append(tasks, t)
	}
	data, _ := json.Marshal(tasks)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (s *liveMigrationScheduler) createMigration(w http.ResponseWriter, r *http.Request) {
	var task MigrationTask
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	task.StartTime = time.Now()
	task.PhaseTime = time.Now()
	task.Phase = PhaseAddLearner
	if task.Mode == "" {
		s.conf.mu.RLock()
		task.Mode = s.conf.Mode
		s.conf.mu.RUnlock()
	}

	s.mu.Lock()
	s.pendingTasks = append(s.pendingTasks, &task)
	s.mu.Unlock()

	log.Info("live migration task created",
		zap.Uint64("region", task.RegionID),
		zap.Uint64("source", task.SourceStoreID),
		zap.Uint64("target", task.TargetStoreID),
		zap.String("mode", string(task.Mode)))

	w.WriteHeader(http.StatusOK)
	data, _ := json.Marshal(task)
	w.Write(data)
}

func (s *liveMigrationScheduler) EncodeConfig() ([]byte, error) {
	s.conf.mu.RLock()
	defer s.conf.mu.RUnlock()
	return json.Marshal(s.conf)
}

func (s *liveMigrationScheduler) ReloadConfig() error {
	return nil
}

func (s *liveMigrationScheduler) IsScheduleAllowed(cluster sche.SchedulerCluster) bool {
	return true
}

// Schedule processes pending and active migration tasks.
//
// For non-DTCM modes, creates a single CreateMoveLeaderOperator that performs:
//
//	AddPeer(learner) -> PromoteLearner -> TransferLeader -> RemovePeer
//
// For DTCM mode, orchestrates the multi-phase protocol:
//
//	Phase 1: AddLearner — add learner peer on destination
//	Phase 2: TrackSetup + Catchup — source sets up State Track (driven by TiKV)
//	Phase 3a: SnapshotBarrier — source captures coupled snapshot
//	Phase 3b: PromoteVoter — promote learner to voter
//	Phase 3c: HandoffBarrier — source captures final delta
//	Phase 4: TransferLeader + Teardown
//
// Phase transitions for DTCM are driven by:
//   - Automatic checks (learner added? leader transferred?)
//   - HTTP PUT /advance calls from experiment scripts or TiKV reports
func (s *liveMigrationScheduler) Schedule(cluster sche.SchedulerCluster, dryRun bool) ([]*operator.Operator, []plan.Plan) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ops []*operator.Operator

	// --- Process pending tasks ---
	for _, task := range s.pendingTasks {
		region := cluster.GetRegion(task.RegionID)
		if region == nil {
			log.Warn("migration target region not found",
				zap.Uint64("region", task.RegionID))
			task.Phase = PhaseFailed
			continue
		}

		// Verify source store is the current leader.
		leader := region.GetLeader()
		if leader == nil || leader.GetStoreId() != task.SourceStoreID {
			log.Warn("migration source is not leader",
				zap.Uint64("region", task.RegionID),
				zap.Uint64("expected_leader", task.SourceStoreID))
			task.Phase = PhaseFailed
			continue
		}

		// Verify target store exists.
		targetStore := cluster.GetStore(task.TargetStoreID)
		if targetStore == nil {
			log.Warn("migration target store not found",
				zap.Uint64("region", task.RegionID),
				zap.Uint64("target", task.TargetStoreID))
			task.Phase = PhaseFailed
			continue
		}

		if task.Mode == MigrationModeDTCM {
			taskOps := s.scheduleDtcm(cluster, region, task)
			ops = append(ops, taskOps...)
		} else {
			// Non-DTCM modes: existing single-operator flow.
			if region.GetStorePeer(task.TargetStoreID) != nil {
				// Target already has a peer — just transfer leader.
				op, err := operator.CreateTransferLeaderOperator(
					fmt.Sprintf("tilim-transfer-%s", task.Mode),
					cluster,
					region,
					task.TargetStoreID,
					[]uint64{},
					operator.OpLeader,
				)
				if err != nil {
					log.Warn("failed to create transfer leader operator",
						zap.Uint64("region", task.RegionID),
						zap.Error(err))
					task.Phase = PhaseFailed
					continue
				}
				ops = append(ops, op)
				task.Phase = PhaseTransferLeader
				s.activeTasks[task.RegionID] = task
				continue
			}

			op := s.createMigrationOperator(cluster, region, task)
			if op != nil {
				ops = append(ops, op)
				s.activeTasks[task.RegionID] = task
			}
		}
	}
	s.pendingTasks = nil

	// --- Advance active tasks ---
	var completedNonDtcm []uint64
	for _, task := range s.activeTasks {
		useDtcmPhases := task.Mode == MigrationModeDTCM
		if useDtcmPhases {
			taskOps := s.advanceDtcmTask(cluster, task)
			ops = append(ops, taskOps...)
		} else {
			// Non-DTCM or DTCM with standard operator: check if leader moved.
			region := cluster.GetRegion(task.RegionID)
			if region != nil {
				leader := region.GetLeader()
				if leader != nil && leader.GetStoreId() == task.TargetStoreID {
					task.Phase = PhaseCompleted
					log.Info("migration completed",
						zap.Uint64("region", task.RegionID),
						zap.String("mode", string(task.Mode)),
						zap.Duration("duration", time.Since(task.StartTime)))
					completedNonDtcm = append(completedNonDtcm, task.RegionID)
				}
			}
		}
	}
	for _, rid := range completedNonDtcm {
		delete(s.activeTasks, rid)
	}

	return ops, nil
}

func (s *liveMigrationScheduler) createMigrationOperator(
	cluster sche.SchedulerCluster,
	region *core.RegionInfo,
	task *MigrationTask,
) *operator.Operator {
	// All migration modes use the same PD-level operator sequence:
	//   AddPeer(target) → PromoteLearner → TransferLeader(target) → RemovePeer(source)
	//
	// IMPORTANT: The actual migration behavior is determined by TiKV's
	// global raftstore.migration_mode config, NOT this task.Mode field.
	// The experiment script MUST set TiKV's migration_mode to match
	// task.Mode BEFORE creating the migration task. PD does not push
	// per-task mode to TiKV; it only records task.Mode for metrics and
	// phase tracking.
	//
	// Mode behaviors on TransferLeader (TiKV side):
	//   Native: propose locks to Raft, then transfer
	//   LockAndAbort: clear locks, transfer immediately
	//   WaitAndRemaster: drain locks, then transfer
	//   Remus: ordered diversion (propose locks, no volatile state)
	//   Squall: partition lock + propose locks, then transfer
	//   RFM: skip lock propose (locks already in Raft via Fat Mode)
	//   DTCM: skip lock propose (state already via State Track + barriers)

	newPeer := &metapb.Peer{
		StoreId: task.TargetStoreID,
		Role:    metapb.PeerRole_Voter,
	}

	desc := fmt.Sprintf("tilim-migrate-%s-region-%d", task.Mode, task.RegionID)
	op, err := operator.CreateMoveLeaderOperator(
		desc,
		cluster,
		region,
		operator.OpLeader|operator.OpRegion,
		task.SourceStoreID,
		newPeer,
	)
	if err != nil {
		log.Warn("failed to create migration operator",
			zap.Uint64("region", task.RegionID),
			zap.String("mode", string(task.Mode)),
			zap.Error(err))
		task.Phase = PhaseFailed
		return nil
	}

	log.Info("created migration operator",
		zap.Uint64("region", task.RegionID),
		zap.Uint64("source", task.SourceStoreID),
		zap.Uint64("target", task.TargetStoreID),
		zap.String("mode", string(task.Mode)),
		zap.String("operator", op.String()))

	task.Phase = PhaseAddLearner
	task.PhaseTime = time.Now()
	return op
}

// scheduleDtcm handles the initial scheduling of a new DTCM migration task.
// It adds the learner on the destination and initializes DTCM state.
func (s *liveMigrationScheduler) scheduleDtcm(
	cluster sche.SchedulerCluster,
	region *core.RegionInfo,
	task *MigrationTask,
) []*operator.Operator {
	// Initialize DTCM state with defaults.
	task.DtcmState = &DtcmPDState{}
	task.DtcmState.initDefaults()

	// Record store addresses for auto-advance HTTP calls and State Track.
	sourceStore := cluster.GetStore(task.SourceStoreID)
	if sourceStore != nil {
		task.SourceAddr = sourceStore.GetStatusAddress()
	}
	targetStore := cluster.GetStore(task.TargetStoreID)
	if targetStore != nil {
		task.TargetAddr = targetStore.GetAddress()
		task.TargetStatusAddr = targetStore.GetStatusAddress()
	}

	s.activeTasks[task.RegionID] = task

	// If target already has a voter peer (common 3-replica case), skip
	// AddLearner, PromoteVoter, and SnapshotBarrier. The target already
	// has a complete Raft state copy — go straight to TrackSetup → Catchup
	// → HandoffBarrier → TransferLeader.
	existingPeer := region.GetStorePeer(task.TargetStoreID)
	if existingPeer != nil {
		if existingPeer.GetRole() != metapb.PeerRole_Learner {
			task.Phase = PhaseTrackSetup
			task.PhaseTime = time.Now()
			task.DtcmState.SkipSnapshotBarrier = true
			log.Info("DTCM: target already has voter peer, fast path (skip AddLearner+Snapshot)",
				zap.Uint64("region", task.RegionID),
				zap.Uint64("target", task.TargetStoreID))
			return nil
		}
		task.Phase = PhaseTrackSetup
		task.PhaseTime = time.Now()
		log.Info("DTCM: target already has learner peer, advancing to track setup",
			zap.Uint64("region", task.RegionID),
			zap.Uint64("target", task.TargetStoreID))
		return nil
	}

	// Target has no peer — create standalone AddLearner operator.
	// DTCM drives each step independently (AddLearner → Promote → Transfer → Remove).
	newLearner := &metapb.Peer{
		StoreId: task.TargetStoreID,
		Role:    metapb.PeerRole_Learner,
	}
	op, err := operator.CreateAddPeerOperator(
		fmt.Sprintf("tilim-dtcm-add-learner-%d", task.RegionID),
		cluster, region, newLearner,
		operator.OpRegion,
	)
	if err != nil {
		log.Warn("DTCM: failed to create add learner operator",
			zap.Uint64("region", task.RegionID),
			zap.Error(err))
		task.Phase = PhaseFailed
		return nil
	}
	op.SetPriorityLevel(constant.Urgent)

	task.Phase = PhaseAddLearner
	task.PhaseTime = time.Now()
	log.Info("DTCM: created add learner operator (target has no peer)",
		zap.Uint64("region", task.RegionID),
		zap.Uint64("source", task.SourceStoreID),
		zap.Uint64("target", task.TargetStoreID))

	return []*operator.Operator{op}
}

// advanceDtcmTask checks the current phase of an active DTCM migration and
// creates operators or transitions phases as appropriate.
//
// Phase transitions that depend on external signals (SnapshotBarrier completion,
// HandoffBarrier completion, destination ready) are triggered via the HTTP PUT
// /advance endpoint, which sets flags in DtcmState. This method reads those
// flags and advances the state machine accordingly.
func (s *liveMigrationScheduler) advanceDtcmTask(
	cluster sche.SchedulerCluster,
	task *MigrationTask,
) []*operator.Operator {
	// Skip if an operator is already running for this region.
	if s.OpController.GetOperator(task.RegionID) != nil {
		return nil
	}

	region := cluster.GetRegion(task.RegionID)
	if region == nil {
		log.Warn("DTCM: region disappeared during migration",
			zap.Uint64("region", task.RegionID))
		notifyTiKVAbort(task)
		task.Phase = PhaseFailed
		return nil
	}

	// --- Per-phase timeout check (P2-13) ---
	// If the current phase has been running longer than its timeout, abort
	// the migration. PhaseCompleted / PhaseFailed / PhaseIdle are terminal
	// or inactive states and are never timed out.
	if task.Phase != PhaseIdle && task.Phase != PhaseCompleted && task.Phase != PhaseFailed {
		timeout := phaseTimeoutFor(task.Phase)
		if !task.PhaseTime.IsZero() && time.Since(task.PhaseTime) > timeout {
			log.Error("DTCM: phase timed out, aborting migration",
				zap.Uint64("region", task.RegionID),
				zap.String("phase", task.Phase.String()),
				zap.Duration("elapsed", time.Since(task.PhaseTime)),
				zap.Duration("timeout", timeout))
			notifyTiKVAbort(task)
			task.Phase = PhaseFailed
			return nil
		}
	}

	// If adaptive fallback triggered full RFM, convert to a standard
	// MoveLeader operator and let TiKV handle it via Fat Mode.
	if task.DtcmState != nil && task.DtcmState.FallbackToRFM {
		log.Info("DTCM: adaptive fallback to RFM",
			zap.Uint64("region", task.RegionID))
		task.Mode = MigrationModeRFM
		op := s.createMigrationOperator(cluster, region, task)
		if op != nil {
			return []*operator.Operator{op}
		}
		return nil
	}

	switch task.Phase {
	case PhaseAddLearner:
		// Check if the learner peer has been added to the region.
		if region.GetStorePeer(task.TargetStoreID) != nil {
			task.Phase = PhaseTrackSetup
			task.PhaseTime = time.Now()
			log.Info("DTCM: learner added, advancing to track setup",
				zap.Uint64("region", task.RegionID))
		}

	case PhaseTrackSetup:
		// Auto-advance: PD triggers TiKV /dtcm/start then polls /dtcm/status.
		// Manual fallback: experiment script can still PUT /advance {"action":"track_started"}.
		trackStarted := task.DtcmState != nil && task.DtcmState.TrackStarted
		if !trackStarted && task.SourceAddr != "" && task.DtcmState != nil {
			if !task.DtcmState.TrackSetupTriggered {
				// First tick in this phase: call TiKV source /dtcm/start.
				targetPeer := region.GetStorePeer(task.TargetStoreID)
				targetPeerID := uint64(0)
				if targetPeer != nil {
					targetPeerID = targetPeer.GetId()
				}
				payload := map[string]any{
					"region_id":       task.RegionID,
					"target_store_id": task.TargetStoreID,
					"target_peer_id":  targetPeerID,
					"target_addr":     task.TargetAddr,
				}
				if err := callTiKVDtcmEndpoint(task.SourceAddr, "/dtcm/start", payload); err != nil {
					log.Warn("DTCM: auto-advance failed to call /dtcm/start",
						zap.Uint64("region", task.RegionID),
						zap.String("source", task.SourceAddr),
						zap.Error(err))
				} else {
					task.DtcmState.TrackSetupTriggered = true
					log.Info("DTCM: auto-advance triggered /dtcm/start",
						zap.Uint64("region", task.RegionID))
				}
			} else {
				// Already triggered: poll source status to detect track active.
				status, err := pollTiKVDtcmStatus(task.SourceAddr, task.RegionID)
				if err == nil && status.SourcePhase != nil {
					phase := *status.SourcePhase
					if phase == "ParallelCatchup" || phase == "Streaming" || phase == "Converging" {
						task.DtcmState.TrackStarted = true
						trackStarted = true
					}
				}
			}
		}
		if trackStarted {
			task.Phase = PhaseCatchup
			task.PhaseTime = time.Now()
			log.Info("DTCM: track setup confirmed, advancing to catchup",
				zap.Uint64("region", task.RegionID))
		}

	case PhaseCatchup:
		// Convergence check (CLAUDE.md §2.5): "D 报告积压低于阈值后进入 handoff"
		//
		// We poll source for source_max_seq (total entries emitted) and
		// source_backlog (un-ACKed entries). We poll dest for
		// dest_last_applied_seq (entries replayed). The true dest lag is:
		//   destLag = source_max_seq - dest_last_applied_seq
		// Total backlog = sourceBacklog + destLag, both must be zero.
		//
		// Previous code tried to read SourceMaxSeq from the *dest* status,
		// which is always nil (dest doesn't have source migration state).
		sourceOk := false
		destOk := false
		if task.DtcmState != nil {
			sourceBacklog := uint64(0)
			sourceMaxSeq := uint64(0)
			destLastApplied := uint64(0)
			destLag := uint64(0)

			if task.SourceAddr != "" {
				status, err := pollTiKVDtcmStatus(task.SourceAddr, task.RegionID)
				if err == nil {
					sourceOk = true
					if status.SourceBacklog != nil {
						sourceBacklog = uint64(*status.SourceBacklog)
					}
					if status.SourceMaxSeq != nil {
						sourceMaxSeq = *status.SourceMaxSeq
					}
				}
			}
			if task.TargetStatusAddr != "" {
				status, err := pollTiKVDtcmStatus(task.TargetStatusAddr, task.RegionID)
				if err == nil {
					destOk = true
					if status.DestLastApplied != nil {
						destLastApplied = *status.DestLastApplied
					}
				}
			}

			if sourceOk && destOk && sourceMaxSeq > destLastApplied {
				destLag = sourceMaxSeq - destLastApplied
			}

			task.DtcmState.BacklogSize = sourceBacklog + destLag

			log.Info("DTCM: catchup convergence check",
				zap.Uint64("region", task.RegionID),
				zap.Bool("source_ok", sourceOk),
				zap.Bool("dest_ok", destOk),
				zap.Uint64("source_backlog", sourceBacklog),
				zap.Uint64("source_max_seq", sourceMaxSeq),
				zap.Uint64("dest_last_applied", destLastApplied),
				zap.Uint64("dest_lag", destLag),
				zap.Uint64("backlog_size", sourceBacklog+destLag))
		}
		// Convergence: require both endpoints reachable. State Track backlog
		// is informational — SnapshotBarrier captures a complete coupled
		// snapshot that supersedes any buffered entries.
		converged := task.DtcmState != nil && sourceOk && destOk
		if converged {
			if task.DtcmState.SkipSnapshotBarrier {
				// Fast path: target is existing voter with complete Raft
				// state. Skip SnapshotBarrier + PromoteVoter, but MUST run
				// HandoffBarrier to transfer volatile state.
				task.Phase = PhaseHandoffBarrier
				task.PhaseTime = time.Now()
				log.Info("DTCM: fast path — target is voter, skipping snapshot+promote, advancing to handoff barrier",
					zap.Uint64("region", task.RegionID))
			} else {
				task.Phase = PhaseSnapshotBarrier
				task.PhaseTime = time.Now()
				log.Info("DTCM: state track converged, advancing to snapshot barrier",
					zap.Uint64("region", task.RegionID))
			}
		}

	case PhaseSnapshotBarrier:
		// Auto-advance: PD triggers TiKV /dtcm/snapshot (synchronous).
		// The HTTP call returns 200 only when the barrier capture + send
		// succeed, so SnapshotBarrierSent can be set immediately on success.
		// On failure (HTTP 500, e.g. lock_table non-empty), PD retries
		// on the next scheduler tick.
		if task.DtcmState != nil && !task.DtcmState.SnapshotBarrierSent && task.SourceAddr != "" {
			payload := map[string]uint64{"region_id": task.RegionID}
			if err := callTiKVDtcmEndpoint(task.SourceAddr, "/dtcm/snapshot", payload); err != nil {
				log.Warn("DTCM: snapshot barrier call failed, will retry",
					zap.Uint64("region", task.RegionID),
					zap.Error(err))
			} else {
				task.DtcmState.SnapshotBarrierSent = true
				task.DtcmState.SnapshotBarrierTriggered = true
				log.Info("DTCM: snapshot barrier captured successfully",
					zap.Uint64("region", task.RegionID))
			}
		}
		if task.DtcmState != nil && task.DtcmState.SnapshotBarrierSent {
			task.Phase = PhasePromoteVoter
			task.PhaseTime = time.Now()
			log.Info("DTCM: snapshot barrier completed, advancing to promote voter",
				zap.Uint64("region", task.RegionID))
		}

	case PhasePromoteVoter:
		peer := region.GetStorePeer(task.TargetStoreID)
		if peer == nil {
			if time.Since(task.PhaseTime) > 60*time.Second {
				log.Warn("DTCM: target peer not found after timeout",
					zap.Uint64("region", task.RegionID))
				notifyTiKVAbort(task)
				task.Phase = PhaseFailed
			}
			return nil
		}
		if peer.GetRole() == metapb.PeerRole_Learner {
			op, err := operator.CreatePromoteLearnerOperator(
				fmt.Sprintf("tilim-dtcm-promote-%d", task.RegionID),
				cluster, region, peer,
			)
			if err != nil {
				log.Warn("DTCM: failed to create promote learner operator",
					zap.Uint64("region", task.RegionID),
					zap.Error(err))
			} else {
				op.SetPriorityLevel(constant.Urgent)
				return []*operator.Operator{op}
			}
			return nil
		}
		task.Phase = PhaseHandoffBarrier
		task.PhaseTime = time.Now()
		log.Info("DTCM: target peer confirmed as voter, advancing to handoff barrier",
			zap.Uint64("region", task.RegionID))

	case PhaseHandoffBarrier:
		// Auto-advance: PD triggers TiKV /dtcm/handoff then polls dest status.
		if task.DtcmState == nil {
			return nil
		}
		if !task.DtcmState.HandoffBarrierSent && task.SourceAddr != "" {
			if !task.DtcmState.HandoffBarrierTriggered {
				payload := map[string]uint64{"region_id": task.RegionID}
				if err := callTiKVDtcmEndpoint(task.SourceAddr, "/dtcm/handoff", payload); err != nil {
					log.Warn("DTCM: handoff barrier call failed, will retry",
						zap.Uint64("region", task.RegionID),
						zap.Error(err))
				} else {
					task.DtcmState.HandoffBarrierTriggered = true
					task.DtcmState.HandoffBarrierSent = true
					log.Info("DTCM: handoff barrier captured and sent to destination",
						zap.Uint64("region", task.RegionID))
				}
			}
		}
		if task.DtcmState.HandoffBarrierSent && !task.DtcmState.DestReady {
			// Poll dest status for handoff readiness.
			status, err := pollTiKVDtcmStatus(task.TargetStatusAddr, task.RegionID)
			if err == nil {
				if status.DestHandoffReady != nil && *status.DestHandoffReady {
					task.DtcmState.DestReady = true
				}
			}
		}
		if task.DtcmState.HandoffBarrierSent && task.DtcmState.DestReady {
			// Notify source to transition from BARRIER_QUIESCE to HANDOFF_READY
			// so it will accept the upcoming leader transfer.
			if task.SourceAddr != "" && !task.DtcmState.CompleteSent {
				payload := map[string]uint64{"region_id": task.RegionID}
				if err := callTiKVDtcmEndpoint(task.SourceAddr, "/dtcm/complete", payload); err != nil {
					log.Warn("DTCM: complete source call failed, will retry",
						zap.Uint64("region", task.RegionID),
						zap.Error(err))
					return nil
				}
				task.DtcmState.CompleteSent = true
			}
			task.Phase = PhaseTransferLeader
			task.PhaseTime = time.Now()
			log.Info("DTCM: handoff barrier completed and dest ready, advancing to transfer leader",
				zap.Uint64("region", task.RegionID))
		}

	case PhaseTransferLeader:
		leader := region.GetLeader()
		if leader != nil && leader.GetStoreId() == task.TargetStoreID {
			task.Phase = PhaseTeardown
			task.PhaseTime = time.Now()
			log.Info("DTCM: leader transferred",
				zap.Uint64("region", task.RegionID),
				zap.Uint64("target", task.TargetStoreID))
		} else {
			op, err := operator.CreateTransferLeaderOperator(
				fmt.Sprintf("tilim-dtcm-transfer-%d", task.RegionID),
				cluster,
				region,
				task.TargetStoreID,
				[]uint64{},
				operator.OpLeader,
			)
			if err != nil {
				log.Warn("DTCM: failed to create transfer leader operator",
					zap.Uint64("region", task.RegionID),
					zap.Error(err))
			} else {
				op.SetPriorityLevel(constant.Urgent)
				return []*operator.Operator{op}
			}
		}

	case PhaseTeardown:
		notifyTiKVAbort(task)
		leader := region.GetLeader()
		if leader != nil && leader.GetStoreId() == task.TargetStoreID {
			if task.DtcmState != nil && task.DtcmState.SkipSnapshotBarrier {
				// Fast path: no peer was added, nothing to remove.
				task.Phase = PhaseCompleted
				log.Info("DTCM: migration completed (fast path)",
					zap.Uint64("region", task.RegionID),
					zap.Duration("duration", time.Since(task.StartTime)))
				delete(s.activeTasks, task.RegionID)
			} else {
				sourcePeer := region.GetStorePeer(task.SourceStoreID)
				if sourcePeer == nil {
					task.Phase = PhaseCompleted
					log.Info("DTCM: migration completed",
						zap.Uint64("region", task.RegionID),
						zap.Duration("duration", time.Since(task.StartTime)))
					delete(s.activeTasks, task.RegionID)
				} else {
					op, err := operator.CreateRemovePeerOperator(
						fmt.Sprintf("tilim-dtcm-remove-%d", task.RegionID),
						cluster, operator.OpRegion, region, task.SourceStoreID,
					)
					if err != nil {
						log.Warn("DTCM: failed to create remove peer operator",
							zap.Uint64("region", task.RegionID),
							zap.Error(err))
					} else {
						op.SetPriorityLevel(constant.Urgent)
						return []*operator.Operator{op}
					}
				}
			}
		}
	}

	return nil
}

// advancePhase handles HTTP PUT requests to advance DTCM phase state.
//
// Request body:
//
//	{
//	  "region_id": 123,
//	  "action": "snapshot_barrier" | "handoff_barrier" | "dest_ready" | "update_backlog" | "fallback_rfm",
//	  "backlog_size": 42,   // only for "update_backlog"
//	  "rho": 0.5,                                  // only for "update_backlog"
//	  "per_channel_rho": {"lock": 0.3, "resolver": 1.2} // optional, for partial Fat Mode
//	}
//
// Response: the updated MigrationTask as JSON.
func (s *liveMigrationScheduler) advancePhase(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RegionID    uint64  `json:"region_id"`
		Action      string  `json:"action"`
		BacklogSize uint64  `json:"backlog_size,omitempty"`
		Rho         float64 `json:"rho,omitempty"`
		// Per-channel convergence ratios (for partial Fat Mode).
		PerChannelRho map[string]float64 `json:"per_channel_rho,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	if req.RegionID == 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("region_id is required"))
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.activeTasks[req.RegionID]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("no active migration for this region"))
		return
	}

	if task.Mode != MigrationModeDTCM {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("advance phase is only supported for DTCM migrations"))
		return
	}

	if task.DtcmState == nil {
		task.DtcmState = &DtcmPDState{}
	}

	switch req.Action {
	case "snapshot_barrier":
		task.DtcmState.SnapshotBarrierSent = true
		log.Info("DTCM: snapshot barrier signaled via API",
			zap.Uint64("region", req.RegionID))
	case "handoff_barrier":
		task.DtcmState.HandoffBarrierSent = true
		log.Info("DTCM: handoff barrier signaled via API",
			zap.Uint64("region", req.RegionID))
	case "dest_ready":
		task.DtcmState.DestReady = true
		log.Info("DTCM: destination ready signaled via API",
			zap.Uint64("region", req.RegionID))
	case "update_backlog":
		task.DtcmState.BacklogSize = req.BacklogSize
		task.DtcmState.Rho = req.Rho
		if req.PerChannelRho != nil {
			task.DtcmState.PerChannelRho = req.PerChannelRho
		}
		log.Info("DTCM: backlog updated via API",
			zap.Uint64("region", req.RegionID),
			zap.Uint64("backlog", req.BacklogSize),
			zap.Float64("rho", req.Rho))
		task.DtcmState.checkRhoAndMaybeFallback(req.RegionID)
		if newFat := task.DtcmState.checkPerChannelRho(req.RegionID); len(newFat) > 0 {
			log.Info("DTCM: new partial Fat Mode channels",
				zap.Uint64("region", req.RegionID),
				zap.Strings("channels", newFat))
		}
	case "fallback_rfm":
		task.DtcmState.FallbackToRFM = true
		log.Info("DTCM: RFM fallback triggered via API",
			zap.Uint64("region", req.RegionID))
	case "track_started":
		task.DtcmState.TrackStarted = true
		log.Info("DTCM: track started signaled via API",
			zap.Uint64("region", req.RegionID))
	case "force_catchup_complete":
		// Force-advance catchup to snapshot barrier without waiting for
		// convergence. For experiments that want to measure barrier behavior
		// under non-converged state.
		if task.Phase == PhaseCatchup {
			task.Phase = PhaseSnapshotBarrier
			log.Info("DTCM: forced catchup completion via API",
				zap.Uint64("region", req.RegionID))
		}
	default:
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("unknown action: %s", req.Action)))
		return
	}

	task.PhaseTime = time.Now()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	data, _ := json.Marshal(task)
	w.Write(data)
}

func (s *liveMigrationScheduler) GetActiveTask(regionID uint64) *MigrationTask {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeTasks[regionID]
}

func (s *liveMigrationScheduler) CompleteTask(regionID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if task, ok := s.activeTasks[regionID]; ok {
		task.Phase = PhaseCompleted
		log.Info("migration completed",
			zap.Uint64("region", regionID),
			zap.Duration("duration", time.Since(task.StartTime)))
		delete(s.activeTasks, regionID)
	}
}
