package status

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/json"
)

type Resource struct {
	client.Object `json:"-"`
	Status        Status `json:"status,omitempty"`
}

type Status struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

func GetStatus(u *unstructured.Unstructured) *Resource {
	result := &Resource{
		Object: u,
	}
	json.MustUnmarshal(json.MustMarshal(u.Object), result)
	return result
}

func PatchStatus(ctx context.Context, c client.Client, o client.Object, status any) error {
	patchErr := c.Status().Patch(ctx, o, client.RawPatch(types.MergePatchType, json.MustMarshal(status)))
	if patchErr != nil {
		return fmt.Errorf("failed to patch status: %w", patchErr)
	}
	return nil
}
