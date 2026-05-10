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

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPollTiKVDtcmStatus(t *testing.T) {
	re := require.New(t)

	sourcePhase := "ParallelCatchup"
	backlog := 42
	destReady := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		re.Equal("/dtcm/status", r.URL.Path)
		re.Equal("POST", r.Method)

		var req map[string]uint64
		re.NoError(json.NewDecoder(r.Body).Decode(&req))
		re.Equal(uint64(100), req["region_id"])

		resp := tikvDtcmStatus{
			RegionID:         100,
			SourcePhase:      &sourcePhase,
			SourceBacklog:    &backlog,
			DestHandoffReady: &destReady,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	// Strip "http://" from test server URL to get host:port.
	addr := srv.Listener.Addr().String()

	status, err := pollTiKVDtcmStatus(addr, 100)
	re.NoError(err)
	re.NotNil(status)
	re.Equal(uint64(100), status.RegionID)
	re.NotNil(status.SourcePhase)
	re.Equal("ParallelCatchup", *status.SourcePhase)
	re.NotNil(status.SourceBacklog)
	re.Equal(42, *status.SourceBacklog)
	re.NotNil(status.DestHandoffReady)
	re.True(*status.DestHandoffReady)
}

func TestCallTiKVDtcmEndpoint(t *testing.T) {
	re := require.New(t)

	var receivedBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		re.Equal("/dtcm/start", r.URL.Path)
		json.NewDecoder(r.Body).Decode(&receivedBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	addr := srv.Listener.Addr().String()
	payload := map[string]any{
		"region_id":       uint64(100),
		"target_store_id": uint64(2),
	}
	err := callTiKVDtcmEndpoint(addr, "/dtcm/start", payload)
	re.NoError(err)
	re.NotNil(receivedBody)
	re.Equal(float64(100), receivedBody["region_id"])
}

func TestCallTiKVDtcmEndpointError(t *testing.T) {
	re := require.New(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	addr := srv.Listener.Addr().String()
	err := callTiKVDtcmEndpoint(addr, "/dtcm/snapshot", map[string]uint64{"region_id": 1})
	re.Error(err)
	re.Contains(err.Error(), "500")
}

func TestDtcmPDStateAutoAdvanceFields(t *testing.T) {
	re := require.New(t)

	state := &DtcmPDState{}
	state.initDefaults()

	re.False(state.TrackSetupTriggered)
	re.False(state.SnapshotBarrierTriggered)
	re.False(state.HandoffBarrierTriggered)

	state.TrackSetupTriggered = true
	re.True(state.TrackSetupTriggered)

	state.SnapshotBarrierTriggered = true
	state.HandoffBarrierTriggered = true
	re.True(state.SnapshotBarrierTriggered)
	re.True(state.HandoffBarrierTriggered)
}

func TestDtcmPDStateRhoFallback(t *testing.T) {
	re := require.New(t)

	state := &DtcmPDState{Rho: 1.5}
	state.initDefaults()
	re.Equal(defaultRhoThreshold, state.RhoThreshold)
	re.Equal(defaultRhoExceedLimit, state.RhoExceedLimit)

	// First two ticks: no fallback yet.
	re.False(state.checkRhoAndMaybeFallback(1))
	re.Equal(1, state.RhoExceedCount)
	re.False(state.checkRhoAndMaybeFallback(1))
	re.Equal(2, state.RhoExceedCount)

	// Third tick: fallback triggered.
	re.True(state.checkRhoAndMaybeFallback(1))
	re.True(state.FallbackToRFM)
}

func TestDtcmPDStateRhoReset(t *testing.T) {
	re := require.New(t)

	state := &DtcmPDState{Rho: 1.5}
	state.initDefaults()

	state.checkRhoAndMaybeFallback(1)
	state.checkRhoAndMaybeFallback(1)
	re.Equal(2, state.RhoExceedCount)

	// Rho drops below threshold → counter resets.
	state.Rho = 0.5
	state.checkRhoAndMaybeFallback(1)
	re.Equal(0, state.RhoExceedCount)
	re.False(state.FallbackToRFM)
}

func TestMigrationPhaseString(t *testing.T) {
	re := require.New(t)
	re.Equal("idle", PhaseIdle.String())
	re.Equal("add-learner", PhaseAddLearner.String())
	re.Equal("track-setup", PhaseTrackSetup.String())
	re.Equal("catchup", PhaseCatchup.String())
	re.Equal("snapshot-barrier", PhaseSnapshotBarrier.String())
	re.Equal("promote-voter", PhasePromoteVoter.String())
	re.Equal("handoff-barrier", PhaseHandoffBarrier.String())
	re.Equal("transfer-leader", PhaseTransferLeader.String())
	re.Equal("teardown", PhaseTeardown.String())
	re.Equal("completed", PhaseCompleted.String())
	re.Equal("failed", PhaseFailed.String())
}

func TestMigrationTaskJSON(t *testing.T) {
	re := require.New(t)

	task := &MigrationTask{
		RegionID:         100,
		SourceStoreID:    1,
		TargetStoreID:    2,
		Mode:             MigrationModeDTCM,
		Phase:            PhaseTrackSetup,
		SourceAddr:       "127.0.0.1:20180",
		TargetAddr:       "127.0.0.1:20161",
		TargetStatusAddr: "127.0.0.1:20181",
		DtcmState: &DtcmPDState{
			TrackSetupTriggered: true,
		},
	}

	data, err := json.Marshal(task)
	re.NoError(err)

	var decoded MigrationTask
	re.NoError(json.Unmarshal(data, &decoded))
	re.Equal(uint64(100), decoded.RegionID)
	re.Equal("127.0.0.1:20180", decoded.SourceAddr)
	re.Equal("127.0.0.1:20161", decoded.TargetAddr)
	re.Equal("127.0.0.1:20181", decoded.TargetStatusAddr)
	re.NotNil(decoded.DtcmState)
	re.True(decoded.DtcmState.TrackSetupTriggered)
}
