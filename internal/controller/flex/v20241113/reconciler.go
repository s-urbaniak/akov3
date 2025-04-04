package v20241113

import (
	"context"
	"errors"
	"fmt"

	atlas20241113 "go.mongodb.org/atlas-sdk/v20241113001/admin"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/atlas"
	ctrlstate "github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/controller/state"
	"github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/json"
	"github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/result"
	"github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/state"
	"github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/status"
	internalunstructured "github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/unstructured"
)

type Reconciler struct {
	ctrlstate.StateReconciler
	Client client.Client
}

func (r *Reconciler) HandleImportRequested(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	atlasClients := atlas.FromContext(ctx)

	externalName, ok := u.GetAnnotations()["mongodb.com/external-name"]
	if !ok {
		return result.Error(state.StateImportRequested, errors.New("missing mongodb.com/external-name"))
	}

	externalGroupID, ok := u.GetAnnotations()["mongodb.com/external-group-id"]
	if !ok {
		return result.Error(state.StateImportRequested, errors.New("missing mongodb.com/external-group-id"))
	}

	params := &atlas20241113.GetFlexClusterApiParams{
		GroupId: externalGroupID,
		Name:    externalName,
	}
	response, _, err := atlasClients.SdkClient20241113001.FlexClustersApi.GetFlexClusterWithParams(ctx, params).Execute()
	if err != nil {
		return result.Error(state.StateImportRequested, fmt.Errorf("failed to get group: %w", err))
	}

	internalunstructured.SetNestedFieldObject(u.Object, response, "spec", "v20241113", "entry")

	err = r.Client.Patch(ctx, u, client.RawPatch(types.MergePatchType, json.MustMarshal(u.Object)))
	if err != nil {
		return result.Error(state.StateImportRequested, fmt.Errorf("failed to patch cluster: %w", err))
	}

	setStatus(u, response)

	return result.NextState(state.StateImported, "Cluster imported")
}

func (r *Reconciler) HandleImported(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	return r.HandleIdle(ctx, u, state.StateUpdated)
}

func (r *Reconciler) HandleInitial(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	atlasClients := atlas.FromContext(ctx)

	params := getParams[atlas20241113.CreateFlexClusterApiParams](u)
	params.FlexClusterDescriptionCreate20241113 = getEntry[atlas20241113.FlexClusterDescriptionCreate20241113](u)

	response, _, err := atlasClients.SdkClient20241113001.FlexClustersApi.CreateFlexClusterWithParams(ctx, params).Execute()
	if err != nil {
		return result.Error(state.StateInitial, fmt.Errorf("failed to create project: %w", err))
	}

	internalunstructured.SetNestedFieldObject(u.Object, response, "status", "v20241113")
	return result.NextState(state.StateCreating, "Creating flex cluster.")
}

func (r *Reconciler) HandleCreated(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	return r.HandleIdle(ctx, u, state.StateCreated)
}

func (r *Reconciler) HandleUpdated(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	return r.HandleIdle(ctx, u, state.StateUpdated)
}

func (r *Reconciler) HandleIdle(ctx context.Context, u *unstructured.Unstructured, finalState state.ResourceState) (ctrlstate.Result, error) {
	atlasClients := atlas.FromContext(ctx)

	response, err := r.updateStatus(ctx, u)
	if err != nil {
		return result.Error(finalState, fmt.Errorf("failed to update status: %w", err))
	}

	if response.GetStateName() != "IDLE" {
		return result.NextState(state.StateUpdating, "Updating flex cluster.")
	}

	st := status.GetStatus(u)
	currentState := meta.FindStatusCondition(st.Status.Conditions, state.StateCondition)
	currentReady := meta.FindStatusCondition(st.Status.Conditions, state.ReadyCondition)
	if currentState.ObservedGeneration == u.GetGeneration() && currentReady.Reason != ctrlstate.ReadyReasonError {
		return result.NextState(finalState, "Upserted cluster")
	}

	status := getStatus[atlas20241113.GetFlexClusterApiParams](u)
	entry := getEntry[atlas20241113.FlexClusterDescriptionUpdate20241113](u)
	response, _, err = atlasClients.SdkClient20241113001.FlexClustersApi.UpdateFlexCluster(ctx, status.GroupId, status.Name, entry).Execute()
	if err != nil {
		return result.Error(state.StateUpdating, fmt.Errorf("failed to update flex cluster: %w", err))
	}

	setStatus(u, response)

	return result.NextState(state.StateUpdating, "Updating flex cluster")
}

func (r *Reconciler) HandleUpserting(ctx context.Context, u *unstructured.Unstructured, currentState, finalState state.ResourceState) (ctrlstate.Result, error) {
	atlasClients := atlas.FromContext(ctx)

	params := getStatus[atlas20241113.GetFlexClusterApiParams](u)
	response, _, err := atlasClients.SdkClient20241113001.FlexClustersApi.GetFlexClusterWithParams(ctx, params).Execute()
	if err != nil {
		return result.Error(currentState, fmt.Errorf("failed to get cluster: %w", err))
	}
	setStatus(u, response)

	if response.GetStateName() == "CREATING" || response.GetStateName() == "UPDATING" {
		return result.NextState(currentState, "Upserting flex cluster")
	}

	return result.NextState(finalState, "Upserted flex cluster")
}

func (r *Reconciler) HandleCreating(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	return r.HandleUpserting(ctx, u, state.StateCreating, state.StateCreated)
}

func (r *Reconciler) HandleUpdating(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	return r.HandleUpserting(ctx, u, state.StateUpdating, state.StateUpdated)
}

func (r *Reconciler) HandleDeletionRequested(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	atlasClients := atlas.FromContext(ctx)

	params := getStatus[atlas20241113.DeleteFlexClusterApiParams](u)
	_, _, err := atlasClients.SdkClient20241113001.FlexClustersApi.DeleteFlexClusterWithParams(ctx, params).Execute()

	switch {
	case atlas20241113.IsErrorCode(err, "CLUSTER_NOT_FOUND"):
		return result.NextState(state.StateDeleted, "Flex cluster has been deleted in Atlas.")
	case err != nil:
		return result.Error(state.StateDeletionRequested, fmt.Errorf("failed to delete flex cluster: %w", err))
	}

	_, err = r.updateStatus(ctx, u)
	if err != nil {
		return result.Error(state.StateDeletionRequested, fmt.Errorf("failed to update status: %w", err))
	}

	return result.NextState(state.StateDeleting, "Deleting flex cluster")
}

func (r *Reconciler) HandleDeleting(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	_, err := r.updateStatus(ctx, u)

	switch {
	case atlas20241113.IsErrorCode(err, "CLUSTER_NOT_FOUND"):
		return result.NextState(state.StateDeleted, "Flex cluster has been deleted in Atlas.")
	case err != nil:
		return result.Error(state.StateDeleting, fmt.Errorf("failed to update status: %w", err))
	}

	return result.NextState(state.StateDeleting, "Deleting flex cluster")
}

func (r *Reconciler) updateStatus(ctx context.Context, u *unstructured.Unstructured) (*atlas20241113.FlexClusterDescription20241113, error) {
	atlasClients := atlas.FromContext(ctx)

	params := getStatus[atlas20241113.GetFlexClusterApiParams](u)
	response, _, err := atlasClients.SdkClient20241113001.FlexClustersApi.GetFlexClusterWithParams(ctx, params).Execute()
	if err == nil {
		setStatus(u, response)
	}

	return response, err
}

func getParams[T any](u *unstructured.Unstructured) *T {
	return json.ConvertNestedField[T](u.Object, "spec", "v20241113", "parameters")
}

func getEntry[T any](u *unstructured.Unstructured) *T {
	return json.ConvertNestedField[T](u.Object, "spec", "v20241113", "entry")
}

func getStatus[T any](u *unstructured.Unstructured) *T {
	return json.ConvertNestedField[T](u.Object, "status", "v20241113")
}

func setStatus(u *unstructured.Unstructured, response *atlas20241113.FlexClusterDescription20241113) {
	internalunstructured.SetNestedFieldObject(u.Object, response, "status", "v20241113")
}
