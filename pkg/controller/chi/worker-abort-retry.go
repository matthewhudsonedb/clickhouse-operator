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
	"time"

	"k8s.io/client-go/util/workqueue"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi/cmd_queue"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/util"
)

// Back-off for re-enqueuing a CHI whose reconcile ended Aborted: 10s, 20s, 40s ... capped at 5m.
// The CHK controller gets the same behaviour from controller-runtime by returning the error.
const (
	abortRetryBaseDelay = 10 * time.Second
	abortRetryMaxDelay  = 5 * time.Minute
)

func newAbortRetryLimiter() workqueue.RateLimiter {
	return workqueue.NewItemExponentialFailureRateLimiter(abortRetryBaseDelay, abortRetryMaxDelay)
}

// scheduleAbortRetry re-enqueues a CHI after its reconcile ended Aborted.
//
// Nothing else would: the informer's periodic resync carries no spec change, so prepareCHIUpdate
// drops it, and recoverAbortedReconcileOnPodReady needs a pod that may never have been created
// (e.g. the API server transiently rejected the host's ConfigMap - see
// https://github.com/Altinity/clickhouse-operator/issues/2094). Aborts that only a spec edit can
// fix are skipped via shouldTriggerAutoRecovery.
func (w *worker) scheduleAbortRetry(cr *api.ClickHouseInstallation) {
	if w.c.abortRetry == nil || !shouldTriggerAutoRecovery(cr) {
		return
	}
	delay := w.c.abortRetry.When(util.NamespaceNameString(cr))
	w.a.V(1).M(cr).F().
		WithEvent(cr, a.EventActionReconcile, a.EventReasonAutoRecoveryTriggered).
		Info("Reconcile aborted - re-enqueuing CHI %s in %s", util.NamespaceNameString(cr), delay)

	// ReconcileAdd, like the pod-Ready recovery: it needs no spec diff and updateCHI re-reads the CR.
	time.AfterFunc(delay, func() {
		w.c.enqueueObject(cmd_queue.NewReconcileCHI(cmd_queue.ReconcileAdd, nil, cr))
	})
}

// resetAbortRetry clears the back-off once a reconcile completes.
func (w *worker) resetAbortRetry(cr *api.ClickHouseInstallation) {
	if w.c.abortRetry != nil {
		w.c.abortRetry.Forget(util.NamespaceNameString(cr))
	}
}
