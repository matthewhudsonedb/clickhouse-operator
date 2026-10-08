// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
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

package chi

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// TestAbortRetryBackoff pins the re-enqueue schedule: doubling from 10s, capped at 5m, reset on success.
func TestAbortRetryBackoff(t *testing.T) {
	limiter := newAbortRetryLimiter()
	const key = "ns/chi"

	want := []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second, 160 * time.Second, 5 * time.Minute, 5 * time.Minute}
	for i, d := range want {
		require.Equal(t, d, limiter.When(key), "attempt %d", i+1)
	}

	limiter.Forget(key)
	require.Equal(t, 10*time.Second, limiter.When(key), "Forget must reset the back-off")
	require.Equal(t, 10*time.Second, limiter.When("ns/other"), "back-off is per CHI")
}

// TestScheduleAbortRetrySkips verifies no back-off is consumed (and nothing is scheduled) for a CHI
// that is not an auto-recovery target.
func TestScheduleAbortRetrySkips(t *testing.T) {
	limiter := newAbortRetryLimiter()
	w := &worker{c: &Controller{abortRetry: limiter}}
	cr := &api.ClickHouseInstallation{ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "chi"}}
	cr.EnsureStatus().ReconcileComplete()

	w.scheduleAbortRetry(cr)
	require.Equal(t, 0, limiter.NumRequeues("ns/chi"))

	// Controllers built without a limiter (tests) must not panic.
	(&worker{c: &Controller{}}).scheduleAbortRetry(cr)
}
