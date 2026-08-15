package benchmark

import (
	"fmt"
	"strings"
	"testing"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/ray-project/kuberay/historyserver/test/support"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// buildOwnedClusterSpec returns the manifest's cluster spec prepared for
// embedding in a RayJob (Spec.RayClusterSpec). The operator generates the
// cluster's name in that mode, so every place the manifest hardcodes
// "raycluster-historyserver" must become self-describing:
//
//   - RAY_CLUSTER_NAME / RAY_CLUSTER_NAMESPACE come from the downward API (the
//     ray.io/cluster pod label and metadata.namespace); the collector prefers
//     these env vars over its --ray-cluster-name flag (cmd/collector/main.go:67).
//   - FQ_RAY_IP uses Kubernetes $(VAR) expansion over those two, which requires
//     them to appear EARLIER in the env list, hence the prepend.
func buildOwnedClusterSpec(test Test, namespace *corev1.Namespace, cfg benchConfig) *rayv1.RayClusterSpec {
	rayClusterFromYaml := DeserializeRayClusterYAML(test, RayClusterManifestPath)
	spec := rayClusterFromYaml.Spec.DeepCopy()

	groups := [][]corev1.Container{spec.HeadGroupSpec.Template.Spec.Containers}
	for wg := range spec.WorkerGroupSpecs {
		groups = append(groups, spec.WorkerGroupSpecs[wg].Template.Spec.Containers)
	}
	for _, containers := range groups {
		for i := range containers {
			if containers[i].Name != "collector" {
				continue
			}
			downward := []corev1.EnvVar{
				{Name: "RAY_CLUSTER_NAME", ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.labels['ray.io/cluster']"},
				}},
				{Name: "RAY_CLUSTER_NAMESPACE", ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
				}},
			}
			containers[i].Env = append(downward, containers[i].Env...)
			upsertEnv(&containers[i], "FQ_RAY_IP",
				"$(RAY_CLUSTER_NAME)-head-svc.$(RAY_CLUSTER_NAMESPACE).svc.cluster.local", nil)
		}
	}
	// The shared knobs (compression, rotation, extra env, resources) apply the
	// same way as in the pre-created-cluster path; an empty cluster name tells
	// the injector to leave the downward-API FQ_RAY_IP above in place.
	injectBenchCollectorSettings(spec.HeadGroupSpec.Template.Spec.Containers, "", namespace.Name, cfg)
	for wg := range spec.WorkerGroupSpecs {
		injectBenchCollectorSettings(spec.WorkerGroupSpecs[wg].Template.Spec.Containers, "", namespace.Name, cfg)
	}
	applyBenchRaySettings(spec, cfg)
	return spec
}

// applyBenchRaySettings applies the Ray-container knobs that are independent of
// how the cluster is created: the head-only status-buffer override, arbitrary
// Ray-side env (RAY_task_events_send_batch_size and friends), and the worker
// memory limit low num_cpus runs need.
func applyBenchRaySettings(spec *rayv1.RayClusterSpec, cfg benchConfig) {
	// Ray version override. The collector numbers are version-sensitive: 2.56
	// lowers the aggregator's HTTP batch cap from 10,000 to 1,000 and its buffer
	// from 1,000,000 to 100,000, and the batch size is what sets the collector's
	// memory ceiling.
	if cfg.RayImage != "" {
		groups := [][]corev1.Container{spec.HeadGroupSpec.Template.Spec.Containers}
		for wg := range spec.WorkerGroupSpecs {
			groups = append(groups, spec.WorkerGroupSpecs[wg].Template.Spec.Containers)
		}
		for _, containers := range groups {
			for i := range containers {
				if containers[i].Name == "ray-head" || containers[i].Name == "ray-worker" {
					containers[i].Image = cfg.RayImage
				}
			}
		}
	}

	// Optionally enlarge the DRIVER-side task status event buffer, head only.
	// The definition-event dropper candidate is the head CoreWorker's
	// TaskEventBufferImpl::status_events_ (capacity 100k, drained <=10k/s);
	// RAY_ray_event_recorder_max_queued_events is a DIFFERENT (GCS-side) buffer
	// and does not affect this path. Worker containers stay at defaults as the
	// experiment control arm.
	if cfg.HeadStatusBuffer != "" {
		headContainers := spec.HeadGroupSpec.Template.Spec.Containers
		for i := range headContainers {
			if headContainers[i].Name == "ray-head" {
				upsertEnv(&headContainers[i], "RAY_task_events_max_num_status_events_buffer_on_worker", cfg.HeadStatusBuffer, nil)
			}
		}
	}

	// Ray-side event constants, applied to head and workers alike. The collector's
	// memory is one decoded batch, and Ray decides how big a batch gets, so
	// the aggregator batch knobs are the ones that test that model directly.
	if cfg.RayEnv != "" {
		groups := [][]corev1.Container{spec.HeadGroupSpec.Template.Spec.Containers}
		for wg := range spec.WorkerGroupSpecs {
			groups = append(groups, spec.WorkerGroupSpecs[wg].Template.Spec.Containers)
		}
		for _, containers := range groups {
			for i := range containers {
				if containers[i].Name != "ray-head" && containers[i].Name != "ray-worker" {
					continue
				}
				for _, kv := range strings.Split(cfg.RayEnv, ",") {
					if name, val, ok := strings.Cut(kv, "="); ok {
						upsertEnv(&containers[i], name, val, nil)
					}
				}
			}
		}
	}

	// Low num_cpus multiplies concurrent Ray worker processes (2 CPU / 0.05 =
	// 40 python workers), which outgrows the sample manifest's 2G limit.
	if cfg.WorkerMemLimit != "" {
		qty := resource.MustParse(cfg.WorkerMemLimit)
		for wg := range spec.WorkerGroupSpecs {
			containers := spec.WorkerGroupSpecs[wg].Template.Spec.Containers
			for i := range containers {
				if containers[i].Name == "ray-worker" {
					if containers[i].Resources.Limits == nil {
						containers[i].Resources.Limits = corev1.ResourceList{}
					}
					containers[i].Resources.Limits[corev1.ResourceMemory] = qty
				}
			}
		}
	}
}

// applyBenchRayCluster deploys config/raycluster.yaml into the benchmark
// namespace. It mirrors support.ApplyRayClusterWithCollectorWithEnvs but also
// injects env vars into the collector container itself (the support helper only
// touches the head Ray container), which is required for the compression knob.
func applyBenchRayCluster(test Test, g *WithT, namespace *corev1.Namespace, cfg benchConfig) *rayv1.RayCluster {
	rayClusterFromYaml := DeserializeRayClusterYAML(test, RayClusterManifestPath)
	rayClusterFromYaml.Namespace = namespace.Name

	injectBenchCollectorSettings(rayClusterFromYaml.Spec.HeadGroupSpec.Template.Spec.Containers,
		rayClusterFromYaml.Name, namespace.Name, cfg)
	for wg := range rayClusterFromYaml.Spec.WorkerGroupSpecs {
		injectBenchCollectorSettings(rayClusterFromYaml.Spec.WorkerGroupSpecs[wg].Template.Spec.Containers,
			rayClusterFromYaml.Name, namespace.Name, cfg)
	}

	applyBenchRaySettings(&rayClusterFromYaml.Spec, cfg)

	rayCluster, err := test.Client().Ray().RayV1().
		RayClusters(namespace.Name).
		Create(test.Ctx(), rayClusterFromYaml, metav1.CreateOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s", rayCluster.Namespace, rayCluster.Name)

	g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))
	g.Eventually(HeadPod(test, rayCluster), TestTimeoutMedium).
		Should(WithTransform(IsPodRunningAndReady, BeTrue()))

	headPod, err := GetHeadPod(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(headPod.Spec.Containers).To(ContainElement(
		WithTransform(func(c corev1.Container) string { return c.Name }, Equal("collector")),
	))

	return rayCluster
}

// injectBenchCollectorSettings mirrors the unexported
// support.injectCollectorRayClusterNamespaceAndEnvVar and adds benchmark knobs.
func injectBenchCollectorSettings(containers []corev1.Container, rayClusterName, rayClusterNamespace string, cfg benchConfig) {
	for i := range containers {
		if containers[i].Name != "collector" {
			continue
		}
		containers[i].Command = append(containers[i].Command,
			fmt.Sprintf("--ray-cluster-namespace=%s", rayClusterNamespace))
		upsertEnv(&containers[i], "POD_IP", "", &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
		})
		// An empty name means the caller (buildOwnedClusterSpec) already set a
		// downward-API FQ_RAY_IP for an operator-generated cluster name;
		// overwriting it here would break every worker's head lookup.
		if rayClusterName != "" {
			upsertEnv(&containers[i], "FQ_RAY_IP",
				fmt.Sprintf("%s-head-svc.%s.svc.cluster.local", rayClusterName, rayClusterNamespace), nil)
		}
		if cfg.Compression {
			upsertEnv(&containers[i], "RAY_COLLECTOR_EVENT_COMPRESSION_ENABLED", "true", nil)
		}
		if cfg.RotationIntvl != "" {
			upsertEnv(&containers[i], "RAY_COLLECTOR_EVENT_ROTATION_INTERVAL", cfg.RotationIntvl, nil)
		}
		// Receive-time event load must use the same wall-clock windows as the
		// cgroup series. The collector aggregates once per HTTP batch and emits
		// one summary per 10-second window, avoiding per-event log overhead.
		upsertEnv(&containers[i], "RAY_COLLECTOR_EVENT_INGRESS_METRICS_WINDOW", "10s", nil)
		for _, kv := range strings.Split(cfg.CollectorEnv, ",") {
			if name, val, ok := strings.Cut(strings.TrimSpace(kv), "="); ok {
				upsertEnv(&containers[i], name, val, nil)
			}
		}
		// Apply the fixed bucket after arbitrary Collector env so no benchmark
		// knob can redirect writes into the shared e2e bucket.
		upsertEnv(&containers[i], "S3_BUCKET", benchmarkS3BucketName, nil)
		// The sample manifest gives the collector no resources at all. Keep empty
		// values as no-ops so existing benchmark runs retain that behavior.
		if cfg.CollectorCPURequest != "" || cfg.CollectorMemoryRequest != "" {
			if containers[i].Resources.Requests == nil {
				containers[i].Resources.Requests = corev1.ResourceList{}
			}
			if cfg.CollectorCPURequest != "" {
				containers[i].Resources.Requests[corev1.ResourceCPU] = resource.MustParse(cfg.CollectorCPURequest)
			}
			if cfg.CollectorMemoryRequest != "" {
				containers[i].Resources.Requests[corev1.ResourceMemory] = resource.MustParse(cfg.CollectorMemoryRequest)
			}
		}
		if cfg.CollectorCPU != "" || cfg.CollectorMemoryLimit != "" {
			if containers[i].Resources.Limits == nil {
				containers[i].Resources.Limits = corev1.ResourceList{}
			}
			if cfg.CollectorCPU != "" {
				containers[i].Resources.Limits[corev1.ResourceCPU] = resource.MustParse(cfg.CollectorCPU)
			}
			if cfg.CollectorMemoryLimit != "" {
				containers[i].Resources.Limits[corev1.ResourceMemory] = resource.MustParse(cfg.CollectorMemoryLimit)
			}
		}
	}
}

func TestInjectBenchCollectorSettingsUsesDedicatedS3Bucket(t *testing.T) {
	containers := []corev1.Container{
		{
			Name: "collector",
			Env:  []corev1.EnvVar{{Name: "S3_BUCKET", Value: S3BucketName}},
		},
		{
			Name: "ray-worker",
			Env:  []corev1.EnvVar{{Name: "S3_BUCKET", Value: "ray-container-sentinel"}},
		},
	}

	injectBenchCollectorSettings(containers, "cluster", "namespace", benchConfig{
		CollectorEnv: "S3_BUCKET=ray-historyserver",
	})

	find := func(container corev1.Container, name string) (string, bool) {
		for _, env := range container.Env {
			if env.Name == name {
				return env.Value, true
			}
		}
		return "", false
	}
	if got, ok := find(containers[0], "S3_BUCKET"); !ok || got != benchmarkS3BucketName {
		t.Fatalf("collector S3_BUCKET=%q present=%v, want %q", got, ok, benchmarkS3BucketName)
	}
	if got, _ := find(containers[1], "S3_BUCKET"); got != "ray-container-sentinel" {
		t.Fatalf("non-collector S3_BUCKET changed to %q", got)
	}
}

func TestInjectBenchCollectorSettingsResources(t *testing.T) {
	cfg := benchConfig{
		CollectorCPURequest:    "250m",
		CollectorCPU:           "1250m",
		CollectorMemoryRequest: "384Mi",
		CollectorMemoryLimit:   "1536Mi",
	}

	for _, role := range []string{"head", "worker"} {
		t.Run(role, func(t *testing.T) {
			containers := []corev1.Container{
				{Name: "ray-" + role},
				{Name: "collector"},
			}
			injectBenchCollectorSettings(containers, "cluster", "namespace", cfg)

			collector := containers[1]
			if got := len(collector.Resources.Requests); got != 2 {
				t.Fatalf("collector requests has %d entries, want 2", got)
			}
			if got := len(collector.Resources.Limits); got != 2 {
				t.Fatalf("collector limits has %d entries, want 2", got)
			}
			assertResourceQuantity(t, collector.Resources.Requests, corev1.ResourceCPU, "250m")
			assertResourceQuantity(t, collector.Resources.Limits, corev1.ResourceCPU, "1250m")
			assertResourceQuantity(t, collector.Resources.Requests, corev1.ResourceMemory, "384Mi")
			assertResourceQuantity(t, collector.Resources.Limits, corev1.ResourceMemory, "1536Mi")
		})
	}
}

func TestInjectBenchCollectorSettingsResourceBackwardCompatibility(t *testing.T) {
	t.Run("all unset preserves manifest resources", func(t *testing.T) {
		for _, role := range []string{"head", "worker"} {
			t.Run(role, func(t *testing.T) {
				containers := []corev1.Container{{Name: "collector"}}
				injectBenchCollectorSettings(containers, "cluster", "namespace", benchConfig{})

				if containers[0].Resources.Requests != nil {
					t.Fatalf("collector requests=%v, want nil", containers[0].Resources.Requests)
				}
				if containers[0].Resources.Limits != nil {
					t.Fatalf("collector limits=%v, want nil", containers[0].Resources.Limits)
				}
			})
		}
	})

	t.Run("legacy CollectorCPU still sets only CPU limit", func(t *testing.T) {
		containers := []corev1.Container{{Name: "collector"}}
		injectBenchCollectorSettings(containers, "cluster", "namespace", benchConfig{CollectorCPU: "600m"})

		if containers[0].Resources.Requests != nil {
			t.Fatalf("collector requests=%v, want nil", containers[0].Resources.Requests)
		}
		if got := len(containers[0].Resources.Limits); got != 1 {
			t.Fatalf("collector limits has %d entries, want 1", got)
		}
		assertResourceQuantity(t, containers[0].Resources.Limits, corev1.ResourceCPU, "600m")
	})
}

func assertResourceQuantity(t *testing.T, resources corev1.ResourceList, name corev1.ResourceName, want string) {
	t.Helper()
	got, ok := resources[name]
	if !ok {
		t.Fatalf("resource %q is missing", name)
	}
	wantQuantity := resource.MustParse(want)
	if !got.Equal(wantQuantity) {
		t.Fatalf("resource %q=%s, want %s", name, got.String(), wantQuantity.String())
	}
}

// upsertEnv updates an env var in place or appends it, avoiding duplicates with
// entries already present in the static YAML manifest.
func upsertEnv(container *corev1.Container, name, val string, valFrom *corev1.EnvVarSource) {
	for i := range container.Env {
		if container.Env[i].Name == name {
			container.Env[i].Value = val
			container.Env[i].ValueFrom = valFrom
			return
		}
	}
	container.Env = append(container.Env, corev1.EnvVar{Name: name, Value: val, ValueFrom: valFrom})
}
