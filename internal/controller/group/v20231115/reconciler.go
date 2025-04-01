package v20231115

import (
	"context"
	"errors"
	"fmt"

	atlas20231115 "go.mongodb.org/atlas-sdk/v20231115008/admin"
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
	id, ok := u.GetAnnotations()["mongodb.com/external-id"]
	if !ok {
		return result.Error(state.StateImportRequested, errors.New("missing mongodb.com/external-id"))
	}

	response, _, err := atlasClients.SdkClient20231115008.ProjectsApi.GetProject(ctx, id).Execute()
	if err != nil {
		return result.Error(state.StateImportRequested, fmt.Errorf("failed to get group: %w", err))
	}

	internalunstructured.SetNestedFieldObject(u.Object, response, "spec", "v20231115", "entry")

	patchErr := r.Client.Patch(ctx, u, client.RawPatch(types.MergePatchType, json.MustMarshal(u.Object)))
	if patchErr != nil {
		return result.Error(state.StateImportRequested, fmt.Errorf("failed to patch group: %w", err))
	}

	setStatus(u, response)

	return result.NextState(state.StateImported, "Project imported")
}

func (r *Reconciler) HandleInitial(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	atlasClients := atlas.FromContext(ctx)
	params := getParams[atlas20231115.CreateProjectApiParams](u)
	params.Group = getEntry[atlas20231115.Group](u)

	response, _, err := atlasClients.SdkClient20231115008.ProjectsApi.CreateProjectWithParams(ctx, params).Execute()
	if err != nil {
		return result.Error(state.StateInitial, fmt.Errorf("failed to create project: %w", err))
	}
	internalunstructured.SetNestedFieldObject(u.Object, response, "status", "v20231115")

	return result.NextState(state.StateCreated, "Project created.")
}

func (r *Reconciler) HandleImported(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	return r.HandleIdle(ctx, u, state.StateImported, state.StateUpdating)
}

func (r *Reconciler) HandleCreated(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	return r.HandleIdle(ctx, u, state.StateCreated, state.StateUpdating)
}

func (r *Reconciler) HandleUpdated(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	return r.HandleIdle(ctx, u, state.StateUpdated, state.StateUpdating)
}

func (r *Reconciler) HandleIdle(ctx context.Context, u *unstructured.Unstructured, currentState, finalState state.ResourceState) (ctrlstate.Result, error) {
	atlasClients := atlas.FromContext(ctx)
	groupStatus := getStatus[atlas20231115.Group](u)
	response, _, err := atlasClients.SdkClient20231115008.ProjectsApi.GetProject(ctx, groupStatus.GetId()).Execute()
	if err != nil {
		return result.Error(currentState, fmt.Errorf("failed to get group: %w", err))
	}
	setStatus(u, response)

	s := status.GetStatus(u)
	stateCondition := meta.FindStatusCondition(s.Status.Conditions, state.StateCondition)
	if stateCondition.ObservedGeneration == u.GetGeneration() {
		return result.NextState(currentState, "Upserted group.")
	}

	entry := getEntry[atlas20231115.GroupUpdate](u)
	p := &atlas20231115.UpdateProjectApiParams{
		GroupId:     groupStatus.GetId(),
		GroupUpdate: entry,
	}

	response, _, err = atlasClients.SdkClient20231115008.ProjectsApi.UpdateProjectWithParams(ctx, p).Execute()
	if err != nil {
		return result.Error(currentState, fmt.Errorf("failed to update project: %w", err))
	}

	setStatus(u, response)
	return result.NextState(state.StateUpdated, "Project updated.")
}

func (r *Reconciler) HandleDeletionRequested(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	atlasClients := atlas.FromContext(ctx)
	groupStatus := getStatus[atlas20231115.Group](u)
	id := groupStatus.Id
	if id == nil {
		return result.NextState(state.StateDeleted, "Project deleted.")
	}
	_, _, err := atlasClients.SdkClient20231115008.ProjectsApi.DeleteProject(ctx, *id).Execute()
	if atlas20231115.IsErrorCode(err, "GROUP_NOT_FOUND") {
		return result.NextState(state.StateDeleted, "Project deleted.")
	}
	if err != nil {
		return result.Error(state.StateDeletionRequested, fmt.Errorf("failed to delete project: %w", err))
	}

	return result.NextState(state.StateDeleting, "Deleting project.")
}

func (r *Reconciler) HandleDeleting(ctx context.Context, u *unstructured.Unstructured) (ctrlstate.Result, error) {
	atlasClients := atlas.FromContext(ctx)
	groupStatus := getStatus[atlas20231115.Group](u)
	_, _, err := atlasClients.SdkClient20231115008.ProjectsApi.GetProject(ctx, groupStatus.GetId()).Execute()
	if !atlas20231115.IsErrorCode(err, "GROUP_NOT_FOUND") && err != nil {
		return result.Error(state.StateDeleting, fmt.Errorf("failed to get project: %w", err))
	}

	return result.NextState(state.StateDeleted, "Project deleted.")
}

func getParams[T any](u *unstructured.Unstructured) *T {
	return json.ConvertNestedField[T](u.Object, "spec", "v20231115", "parameters")
}

func getEntry[T any](u *unstructured.Unstructured) *T {
	return json.ConvertNestedField[T](u.Object, "spec", "v20231115", "entry")
}

func getStatus[T any](u *unstructured.Unstructured) *T {
	return json.ConvertNestedField[T](u.Object, "status", "v20231115")
}

func setStatus(u *unstructured.Unstructured, response *atlas20231115.Group) {
	internalunstructured.SetNestedFieldObject(u.Object, response, "status", "v20231115")
}
