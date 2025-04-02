package v20231115

import (
	"context"
	"errors"
	"fmt"

	"github.com/wI2L/jsondiff"
	atlas20231115 "go.mongodb.org/atlas-sdk/v20231115008/admin"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

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
		return result.Error(state.StateImportRequested, errors.New("missing mongodb.com/external-id"))
	}

	externalGroupID, ok := u.GetAnnotations()["mongodb.com/external-group-id"]
	if !ok {
		return result.Error(state.StateImportRequested, errors.New("missing mongodb.com/external-id"))
	}

	response, _, err := atlasClients.SdkClient20231115008.ClustersApi.GetCluster(ctx, externalGroupID, externalName).Execute()
	if err != nil {
		return result.Error(state.StateImportRequested, fmt.Errorf("failed to get group: %w", err))
	}

	internalunstructured.SetNestedFieldObject(u.Object, response, "spec", "v20231115", "entry")

	err = r.Client.Patch(ctx, u, client.RawPatch(types.MergePatchType, json.MustMarshal(u.Object)))
	if err != nil {
		return result.Error(state.StateImportRequested, fmt.Errorf("failed to patch cluster: %w", err))
	}

	setStatus(u, response)

	return result.NextState(state.StateImported, "Cluster imported")
}

func (r *Reconciler) HandleInitial(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	atlasClients := atlas.FromContext(ctx)

	params := getParams(u)
	params.AdvancedClusterDescription = getEntry(u)

	response, _, err := atlasClients.SdkClient20231115008.ClustersApi.CreateClusterWithParams(ctx, params).Execute()
	if err != nil {
		return result.Error(state.StateInitial, fmt.Errorf("failed to create cluster: %w", err))
	}

	setStatus(u, response)

	return result.NextState(state.StateCreating, "Creating cluster")
}
func (r *Reconciler) HandleImported(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	return r.HandleIdle(ctx, u, state.StateUpdated)
}

func (r *Reconciler) HandleCreated(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	return r.HandleIdle(ctx, u, state.StateUpdated)
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
		return result.NextState(state.StateUpdating, "Updating cluster")
	}

	st := status.GetStatus(u)
	currentState := meta.FindStatusCondition(st.Status.Conditions, state.StateCondition)
	currentReady := meta.FindStatusCondition(st.Status.Conditions, state.ReadyCondition)
	if currentState.ObservedGeneration == u.GetGeneration() && currentReady.Reason != ctrlstate.ReadyReasonError {
		return result.NextState(finalState, "Upserted cluster")
	}

	entry := getEntry(u)
	params := &atlas20231115.UpdateClusterApiParams{
		GroupId:                    getStatus(u).GetGroupId(),
		ClusterName:                entry.GetName(),
		AdvancedClusterDescription: entry,
	}

	r.logChanges(ctx, entry, response)

	response, _, err = atlasClients.SdkClient20231115008.ClustersApi.UpdateClusterWithParams(ctx, params).Execute()
	if err != nil {
		return result.Error(state.StateUpdating, fmt.Errorf("failed to update cluster: %w", err))
	}
	setStatus(u, response)

	return result.NextState(state.StateUpdating, "Updating cluster")
}

func (r *Reconciler) logChanges(ctx context.Context, ako, atlas *atlas20231115.AdvancedClusterDescription) {
	p, err := jsondiff.CompareJSON(json.MustMarshal(ako), json.MustMarshal(atlas))
	if err != nil {
		return
	}
	logger := log.FromContext(ctx).WithName("cluster-controller")

	for _, op := range p {
		logger.Info("patch", "op", op.String())
	}
}

func (r *Reconciler) HandleUpserting(ctx context.Context, u *unstructured.Unstructured, currentState, finalState state.ResourceState) (ctrlstate.Result, error) {
	atlasClients := atlas.FromContext(ctx)

	params := &atlas20231115.GetClusterApiParams{
		GroupId:     getStatus(u).GetGroupId(),
		ClusterName: getEntry(u).GetName(),
	}

	response, _, err := atlasClients.SdkClient20231115008.ClustersApi.GetClusterWithParams(ctx, params).Execute()
	if err != nil {
		return result.Error(currentState, fmt.Errorf("failed to get cluster: %w", err))
	}

	setStatus(u, response)

	if response.GetStateName() == "CREATING" || response.GetStateName() == "UPDATING" {
		return result.NextState(currentState, "Upserting cluster")
	}

	return result.NextState(finalState, "Upserted cluster")
}

func (r *Reconciler) HandleCreating(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	return r.HandleUpserting(ctx, u, state.StateCreating, state.StateCreated)
}

func (r *Reconciler) HandleUpdating(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	return r.HandleUpserting(ctx, u, state.StateUpdating, state.StateUpdated)
}

func (r *Reconciler) HandleDeletionRequested(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	atlasClients := atlas.FromContext(ctx)

	params := &atlas20231115.DeleteClusterApiParams{
		GroupId:     getStatus(u).GetGroupId(),
		ClusterName: getEntry(u).GetName(),
	}

	_, err := atlasClients.SdkClient20231115008.ClustersApi.DeleteClusterWithParams(ctx, params).Execute()
	switch {
	case atlas20231115.IsErrorCode(err, "CLUSTER_NOT_FOUND"):
		return result.NextState(state.StateDeleted, "Cluster has been deleted in Atlas.")
	case err != nil:
		return result.Error(state.StateDeletionRequested, fmt.Errorf("failed to delete cluster: %w", err))
	}

	_, err = r.updateStatus(ctx, u)
	if err != nil {
		return result.Error(state.StateDeletionRequested, fmt.Errorf("failed to update status: %w", err))
	}

	return result.NextState(state.StateDeleting, "Deleting cluster")
}

func (r *Reconciler) HandleDeleting(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	_, err := r.updateStatus(ctx, u)

	switch {
	case atlas20231115.IsErrorCode(err, "CLUSTER_NOT_FOUND"):
		return result.NextState(state.StateDeleted, "Cluster has been deleted in Atlas.")
	case err != nil:
		return result.Error(state.StateDeleting, fmt.Errorf("failed to update status: %w", err))
	}

	return result.NextState(state.StateDeleting, "Deleting cluster")
}

func (r *Reconciler) updateStatus(ctx context.Context, u *unstructured.Unstructured) (*atlas20231115.AdvancedClusterDescription, error) {
	atlasClients := atlas.FromContext(ctx)

	params := &atlas20231115.GetClusterApiParams{
		GroupId:     getStatus(u).GetGroupId(),
		ClusterName: getEntry(u).GetName(),
	}

	response, _, err := atlasClients.SdkClient20231115008.ClustersApi.GetClusterWithParams(ctx, params).Execute()
	if err == nil {
		setStatus(u, response)
	}

	return response, err
}

func getParams(u *unstructured.Unstructured) *atlas20231115.CreateClusterApiParams {
	return json.ConvertNestedField[atlas20231115.CreateClusterApiParams](u.Object, "spec", "v20231115", "parameters")
}

func getEntry(u *unstructured.Unstructured) *atlas20231115.AdvancedClusterDescription {
	return json.ConvertNestedField[atlas20231115.AdvancedClusterDescription](u.Object, "spec", "v20231115", "entry")
}

func getStatus(u *unstructured.Unstructured) *atlas20231115.AdvancedClusterDescription {
	return json.ConvertNestedField[atlas20231115.AdvancedClusterDescription](u.Object, "status", "v20231115")
}

func setStatus(u *unstructured.Unstructured, s *atlas20231115.AdvancedClusterDescription) {
	internalunstructured.SetNestedFieldObject(u.Object, s, "status", "v20231115")
}
