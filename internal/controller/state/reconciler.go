package state

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/finalizer"
	"github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/state"
	"github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/status"
	internalunstructured "github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/unstructured"
)

type Result struct {
	reconcile.Result
	NextState state.ResourceState
	StateMsg  string
}

type StateReconciler interface {
	HandleInitial(context.Context, *unstructured.Unstructured) (Result, error)
	HandleImportRequested(context.Context, *unstructured.Unstructured) (Result, error)
	HandleImported(context.Context, *unstructured.Unstructured) (Result, error)
	HandleCreating(context.Context, *unstructured.Unstructured) (Result, error)
	HandleCreated(context.Context, *unstructured.Unstructured) (Result, error)
	HandleUpdating(context.Context, *unstructured.Unstructured) (Result, error)
	HandleUpdated(context.Context, *unstructured.Unstructured) (Result, error)
	HandleDeletionRequested(context.Context, *unstructured.Unstructured) (Result, error)
	HandleDeleting(context.Context, *unstructured.Unstructured) (Result, error)
	// Deleted, not handled as it is a terminal state
}

const (
	ReadyReasonError   = "Error"
	ReadyReasonPending = "Pending"
	ReadyReasonSettled = "Settled"
)

type Reconciler struct {
	Client      client.Client
	Reconciler  StateReconciler
	RateLimiter workqueue.TypedRateLimiter[reconcile.Request]
}

func (r *Reconciler) ReconcileUnstructured(ctx context.Context, req ctrl.Request, u *unstructured.Unstructured) (reconcile.Result, error) {
	logger := log.FromContext(ctx).WithName("state-controller")
	prevStatus := status.GetStatus(u)
	prevState := state.GetState(prevStatus.Status.Conditions)

	logger.Info("reconcile started", "state", prevState)

	if err := finalizer.EnsureFinalizers(ctx, r.Client, u, "mongodb.com/finalizer"); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to manage finalizers: %w", err)
	}

	result, reconcileErr := r.ReconcileState(ctx, u)
	observedGeneration := getObservedGeneration(u, prevStatus, result.NextState)

	stateStatus := true
	if reconcileErr != nil {
		// error message will be displayed in Ready state.
		stateStatus = false
	}
	newStatus := status.GetStatus(u)
	state.EnsureState(&newStatus.Status.Conditions, observedGeneration, result.NextState, result.StateMsg, stateStatus)
	internalunstructured.SetNestedFieldSlice(u.Object, newStatus.Status.Conditions, "status", "conditions")

	logger.Info("reconcile finished", "nextState", result.NextState)

	if result.NextState == state.StateDeleted {
		if err := finalizer.UnsetFinalizers(ctx, r.Client, u, "mongodb.com/finalizer"); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to unset finalizer: %w", err)
		}

		return result.Result, reconcileErr
	}

	ready := r.NewReadyCondition(result, req, reconcileErr)
	ready.ObservedGeneration = observedGeneration

	meta.SetStatusCondition(&newStatus.Status.Conditions, ready)
	internalunstructured.SetNestedFieldSlice(u.Object, newStatus.Status.Conditions, "status", "conditions")

	if err := status.PatchStatus(ctx, r.Client, u, u); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to patch status: %w", err)
	}

	return result.Result, reconcileErr
}

func (r *Reconciler) NewReadyCondition(result Result, req ctrl.Request, reconcileErr error) metav1.Condition {
	var (
		readyReason, msg string
		cond             metav1.ConditionStatus
	)

	switch result.NextState {
	case state.StateInitial:
		cond = metav1.ConditionFalse
		readyReason = ReadyReasonPending
		msg = "Resource is in initial state."

	case state.StateImportRequested:
		cond = metav1.ConditionFalse
		readyReason = ReadyReasonPending
		msg = "Resource is being imported."

	case state.StateCreating:
		cond = metav1.ConditionFalse
		readyReason = ReadyReasonPending
		msg = "Resource is pending."

	case state.StateUpdating:
		cond = metav1.ConditionFalse
		readyReason = ReadyReasonPending
		msg = "Resource is pending."

	case state.StateDeleting:
		cond = metav1.ConditionFalse
		readyReason = ReadyReasonPending
		msg = "Resource is pending."

	case state.StateDeletionRequested:
		cond = metav1.ConditionFalse
		readyReason = ReadyReasonPending
		msg = "Resource is pending."

	case state.StateImported:
		cond = metav1.ConditionTrue
		readyReason = ReadyReasonSettled
		msg = "Resource is imported."

	case state.StateCreated:
		cond = metav1.ConditionTrue
		readyReason = ReadyReasonSettled
		msg = "Resource is settled."

	case state.StateUpdated:
		cond = metav1.ConditionTrue
		readyReason = ReadyReasonSettled
		msg = "Resource is settled."

	default:
		cond = metav1.ConditionFalse
		readyReason = ReadyReasonError
		msg = fmt.Sprintf("unknown state: %s", result.NextState)

	}

	if reconcileErr != nil {
		cond = metav1.ConditionFalse
		readyReason = ReadyReasonError
		msg = reconcileErr.Error()
	}

	switch {
	case reconcileErr != nil:
		cond = metav1.ConditionFalse
		readyReason = ReadyReasonError
		nextReconcile := r.RateLimiter.When(req)
		msg = fmt.Sprintf("%v. Next reconcile after %v.", reconcileErr.Error(), nextReconcile)

	case result.RequeueAfter > 0:
		msg = fmt.Sprintf("%v Next reconcile after %v.", msg, result.RequeueAfter)

	case result.Requeue:
		nextReconcile := r.RateLimiter.When(req)
		msg = fmt.Sprintf("%v Next reconcile after %v.", msg, nextReconcile)
	}

	return metav1.Condition{
		Type:               state.ReadyCondition,
		Status:             cond,
		LastTransitionTime: metav1.NewTime(time.Now()),
		Reason:             readyReason,
		Message:            msg,
	}
}

func (r *Reconciler) ReconcileState(ctx context.Context, u *unstructured.Unstructured) (Result, error) {
	var (
		prevState = state.GetState(status.GetStatus(u).Status.Conditions)

		result = Result{
			Result:    reconcile.Result{},
			NextState: state.StateInitial,
		}

		err error
	)

	if prevState == state.StateInitial {
		for key := range u.GetAnnotations() {
			if strings.HasPrefix(key, "mongodb.com/external-") {
				prevState = state.StateImportRequested
			}
		}
	}

	if !u.GetDeletionTimestamp().IsZero() && prevState != state.StateDeleting {
		prevState = state.StateDeletionRequested
	}

	switch prevState {
	case state.StateInitial:
		result, err = r.Reconciler.HandleInitial(ctx, u)
	case state.StateImportRequested:
		result, err = r.Reconciler.HandleImportRequested(ctx, u)
	case state.StateImported:
		result, err = r.Reconciler.HandleImported(ctx, u)
	case state.StateCreating:
		result, err = r.Reconciler.HandleCreating(ctx, u)
	case state.StateCreated:
		result, err = r.Reconciler.HandleCreated(ctx, u)
	case state.StateUpdating:
		result, err = r.Reconciler.HandleUpdating(ctx, u)
	case state.StateUpdated:
		result, err = r.Reconciler.HandleUpdated(ctx, u)
	case state.StateDeletionRequested:
		result, err = r.Reconciler.HandleDeletionRequested(ctx, u)
	case state.StateDeleting:
		result, err = r.Reconciler.HandleDeleting(ctx, u)
	}

	if result.NextState == "" {
		result.NextState = state.StateInitial
	}

	return result, err
}

func getObservedGeneration(u client.Object, prevStatus *status.Resource, nextState state.ResourceState) int64 {
	observedGeneration := u.GetGeneration()
	prevState := state.GetState(prevStatus.Status.Conditions)

	if prevCondition := meta.FindStatusCondition(prevStatus.Status.Conditions, state.StateCondition); prevCondition != nil {
		from := prevState
		to := nextState

		// don't change observed generation if we are:
		// - creating/updating/deleting
		// - just finished creating/updating/deleting
		observedGeneration = prevCondition.ObservedGeneration
		switch {
		case from == state.StateUpdating && to == state.StateUpdating: // polling update
		case from == state.StateUpdating && to == state.StateUpdated: // finished updating

		case from == state.StateCreating && to == state.StateCreating: // polling creation
		case from == state.StateCreating && to == state.StateCreated: // finished creating

		case from == state.StateDeletionRequested && to == state.StateDeleting: // started deletion
		case from == state.StateDeleting && to == state.StateDeleting: // polling deletion
		case from == state.StateDeleting && to == state.StateDeleted: // finshed deletion
		default:
			observedGeneration = u.GetGeneration()
		}
	}

	return observedGeneration
}
