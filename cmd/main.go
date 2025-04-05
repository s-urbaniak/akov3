/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	uberzap "go.uber.org/zap"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cluster20231115 "github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/controller/cluster/v20231115"
	flexv20241113 "github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/controller/flex/v20241113"
	group20231115 "github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/controller/group/v20231115"
	networkpermissionentry20231115 "github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/controller/networkpermissionentry/v20231115"
	"github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/controller/state"
	"github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/controller/unstructured"
	"github.com/mongodb/mongodb-atlas-kubernetes/v3/internal/ratelimiter"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

func main() {
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	klog.InitFlags(fs)
	fs.Parse([]string{"--v=9"})

	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	opts := zap.Options{
		Level:       uberzap.NewAtomicLevelAt(-9),
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	syncPeriod := 30 * time.Second
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "ecaf1259.my.domain",
		Cache: cache.Options{
			SyncPeriod: &syncPeriod,
		},
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	type managerInitializer interface {
		SetupWithManager(mgr ctrl.Manager) error
	}

	rl := ratelimiter.NewRateLimiter[reconcile.Request]()

	for _, reconciler := range []managerInitializer{
		&unstructured.Reconciler{
			RateLimiter: rl,
			GVK: schema.GroupVersionKind{
				Group:   "atlas.generated.mongodb.com",
				Version: "v1",
				Kind:    "Group",
			},
			Reconciler: &state.Reconciler{
				RateLimiter: rl,
				Client:      mgr.GetClient(),
				Reconciler: &group20231115.Reconciler{
					Client: mgr.GetClient(),
				},
			},
		},

		&unstructured.Reconciler{
			RateLimiter: rl,
			GVK: schema.GroupVersionKind{
				Group:   "atlas.generated.mongodb.com",
				Version: "v1",
				Kind:    "FlexCluster",
			},
			Reconciler: &state.Reconciler{
				RateLimiter: rl,
				Client:      mgr.GetClient(),
				Reconciler: &flexv20241113.Reconciler{
					Client: mgr.GetClient(),
				},
			},
		},

		&unstructured.Reconciler{
			RateLimiter: rl,
			GVK: schema.GroupVersionKind{
				Group:   "atlas.generated.mongodb.com",
				Version: "v1",
				Kind:    "Cluster",
			},
			Reconciler: &state.Reconciler{
				RateLimiter: rl,
				Client:      mgr.GetClient(),
				Reconciler: &cluster20231115.Reconciler{
					Client: mgr.GetClient(),
				},
			},
		},

		&unstructured.Reconciler{
			RateLimiter: rl,
			GVK: schema.GroupVersionKind{
				Group:   "atlas.generated.mongodb.com",
				Version: "v1",
				Kind:    "NetworkPermissionEntry",
			},
			Reconciler: &state.Reconciler{
				RateLimiter: rl,
				Client:      mgr.GetClient(),
				Reconciler:  &networkpermissionentry20231115.Reconciler{},
			},
		},
	} {
		if err := reconciler.SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", fmt.Sprintf("%T", reconciler))
			os.Exit(1)
		}
	}

	//+kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
