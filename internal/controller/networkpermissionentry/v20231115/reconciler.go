package v20231115

import (
	"context"

	atlas20231115 "go.mongodb.org/atlas-sdk/v20231115008/admin"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

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
}

func (r *Reconciler) HandleInitial(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	return r.HandleIdle(ctx, u, state.StateInitial, state.StateCreated)
}

func (r *Reconciler) HandleCreated(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	return r.HandleIdle(ctx, u, state.StateCreated, state.StateUpdated)
}

func (r *Reconciler) HandleUpdated(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	return r.HandleIdle(ctx, u, state.StateUpdated, state.StateUpdated)
}

func (r *Reconciler) HandleIdle(ctx context.Context, u *unstructured.Unstructured, currentState, finalState state.ResourceState) (ctrlstate.Result, error) {
	s := status.GetStatus(u)
	stateCondition := meta.FindStatusCondition(s.Status.Conditions, state.StateCondition)

	if stateCondition.ObservedGeneration == u.GetGeneration() {
		return ctrlstate.Result{NextState: finalState}, nil
	}

	atlasClients := atlas.FromContext(ctx)

	params := getParams[atlas20231115.CreateProjectIpAccessListApiParams](u)
	params.NetworkPermissionEntry = getEntry(u)

	listParams := &atlas20231115.ListProjectIpAccessListsApiParams{
		GroupId: params.GroupId,
	}
	entries, _, err := atlasClients.SdkClient20231115008.ProjectIPAccessListApi.ListProjectIpAccessListsWithParams(ctx, listParams).Execute()
	if err != nil {
		return result.Error(currentState, err)
	}

	for _, entry := range entries.GetResults() {
		entryValue := ""
		switch {
		case entry.AwsSecurityGroup != nil:
			entryValue = *entry.AwsSecurityGroup
		case entry.IpAddress != nil:
			entryValue = *entry.IpAddress
		case entry.CidrBlock != nil:
			entryValue = *entry.CidrBlock
		}

		deleteParams := &atlas20231115.DeleteProjectIpAccessListApiParams{
			GroupId:    params.GroupId,
			EntryValue: entryValue,
		}

		_, _, err := atlasClients.SdkClient20231115008.ProjectIPAccessListApi.DeleteProjectIpAccessListWithParams(ctx, deleteParams).Execute()
		if err != nil {
			return result.Error(currentState, err)
		}
	}

	response, _, err := atlasClients.SdkClient20231115008.ProjectIPAccessListApi.CreateProjectIpAccessListWithParams(ctx, params).Execute()
	if err != nil {
		return result.Error(currentState, err)
	}

	setStatus(u, response)

	return result.NextState(finalState, "Upserted network permission entry.")
}

func (r *Reconciler) HandleDeletionRequested(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	//atlasClients := atlas.FromContext(ctx)
	//
	//params := &atlas20231115.DeleteClusterApiParams{
	//	GroupId:     getStatus(u).GetGroupId(),
	//	ClusterName: getEntry(u).GetName(),
	//}
	//
	//_, err := atlasClients.SdkClient20231115008.ClustersApi.DeleteClusterWithParams(ctx, params).Execute()
	//switch {
	//case atlas20231115.IsErrorCode(err, "CLUSTER_NOT_FOUND"):
	//	return result.Deleted()
	//case err != nil:
	//	return result.Error(state.StateDeletionRequested, fmt.Errorf("failed to delete cluster: %w", err))
	//}
	//
	//_, err = r.updateStatus(ctx, u)
	//if err != nil {
	//	return result.Error(state.StateDeletionRequested, fmt.Errorf("failed to update status: %w", err))
	//}
	//
	//return result.Deleting()
	return ctrlstate.Result{}, nil
}

func (r *Reconciler) HandleDeleting(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	//_, err := r.updateStatus(ctx, u)
	//
	//switch {
	//case atlas20231115.IsErrorCode(err, "CLUSTER_NOT_FOUND"):
	//	return result.Deleted()
	//case err != nil:
	//	return result.Error(state.StateDeleting, fmt.Errorf("failed to update status: %w", err))
	//}
	//
	//return result.Deleting()
	return ctrlstate.Result{}, nil
}

func (r *Reconciler) updateStatus(ctx context.Context, u *unstructured.Unstructured) (*atlas20231115.AdvancedClusterDescription, error) {
	//atlasClients := atlas.FromContext(ctx)
	//
	//params := &atlas20231115.GetProjectIpListApiParams{
	//	GroupId: getStatus(),
	//}
	//
	//response, _, err := atlasClients.SdkClient20231115008.ProjectIPAccessListApi.GetProjectIpListWithParams(ctx, params).Execute()
	//if err == nil {
	//	setStatus(u, response)
	//}
	//
	//return response, err
	return nil, nil
}

func getParams[T any](u *unstructured.Unstructured) *T {
	return json.ConvertNestedField[T](u.Object, "spec", "v20231115", "parameters")
}

func getEntry(u *unstructured.Unstructured) *[]atlas20231115.NetworkPermissionEntry {
	return json.ConvertNestedField[[]atlas20231115.NetworkPermissionEntry](u.Object, "spec", "v20231115", "entry")
}

func getStatus(u *unstructured.Unstructured) *[]atlas20231115.NetworkPermissionEntry {
	return json.ConvertNestedField[[]atlas20231115.NetworkPermissionEntry](u.Object, "status", "v20231115")
}

func setStatus(u *unstructured.Unstructured, s *atlas20231115.PaginatedNetworkAccess) {
	internalunstructured.SetNestedFieldObject(u.Object, s, "status", "v20231115")
}
