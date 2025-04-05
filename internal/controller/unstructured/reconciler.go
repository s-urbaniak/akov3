package unstructured

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/atlas"
	internalpredicate "github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/predicate"
)

type UnstructuredReconciler interface {
	ReconcileUnstructured(context.Context, ctrl.Request, *unstructured.Unstructured) (reconcile.Result, error)
}

type Reconciler struct {
	Reconciler  UnstructuredReconciler
	GVK         schema.GroupVersionKind
	Client      client.Client
	RateLimiter workqueue.TypedRateLimiter[reconcile.Request]
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Client = mgr.GetClient()

	return ctrl.NewControllerManagedBy(mgr).
		For(
			ObjectForGVK(r.GVK),
			builder.WithPredicates(
				predicate.GenerationChangedPredicate{},
				internalpredicate.IgnoreDeletedPredicate[client.Object](),
			),
		).
		WithOptions(controller.Options{
			RateLimiter: r.RateLimiter,
		}).
		Complete(r)
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(r.GVK)

	err := r.Client.Get(ctx, req.NamespacedName, u)
	if errors.IsNotFound(err) {
		// object is already gone, nothing to do.
		return reconcile.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("unable to get object: %w", err)
	}

	cs, err := atlas.NewClientSet()
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to create client: %w", err)
	}
	ctx = atlas.NewContext(ctx, cs)
	return r.Reconciler.ReconcileUnstructured(ctx, req, u)
}

func ObjectForGVK(gvk schema.GroupVersionKind) client.Object {
	u := unstructured.Unstructured{}
	u.SetAPIVersion(gvk.GroupVersion().String())
	u.SetKind(gvk.Kind)
	return &u
}
