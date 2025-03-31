package unstructured

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/time/rate"
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

// rateLimiter rate limits enqueuing to every 15 seconds and a max delay of a minute.
func rateLimiter[T comparable]() workqueue.TypedRateLimiter[T] {
	return workqueue.NewTypedMaxOfRateLimiter[T](
		workqueue.NewTypedItemExponentialFailureRateLimiter[T](15*time.Second, time.Minute),
		// 10 qps, 100 bucket size.  This is only for retry speed and its only the overall factor (not per item)
		&workqueue.TypedBucketRateLimiter[T]{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
	)
}

type UnstructuredReconciler interface {
	ReconcileUnstructured(context.Context, *unstructured.Unstructured) (reconcile.Result, error)
}

type Reconciler struct {
	Reconciler UnstructuredReconciler
	GVK        schema.GroupVersionKind
	client     client.Client
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.client = mgr.GetClient()

	return ctrl.NewControllerManagedBy(mgr).
		For(
			ObjectForGVK(r.GVK),
			builder.WithPredicates(
				predicate.GenerationChangedPredicate{},
				internalpredicate.IgnoreDeletedPredicate[client.Object](),
			),
		).
		WithOptions(controller.Options{
			RateLimiter: rateLimiter[reconcile.Request](),
		}).
		Complete(r)
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(r.GVK)

	err := r.client.Get(ctx, req.NamespacedName, u)
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
	return r.Reconciler.ReconcileUnstructured(ctx, u)
}

func ObjectForGVK(gvk schema.GroupVersionKind) client.Object {
	u := unstructured.Unstructured{}
	u.SetAPIVersion(gvk.GroupVersion().String())
	u.SetKind(gvk.Kind)
	return &u
}
