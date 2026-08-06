package benchmark

import (
	"fmt"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/ray-project/kuberay/historyserver/test/support"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

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

	// Optionally enlarge the DRIVER-side task status event buffer, head only.
	// The definition-event dropper candidate is the head CoreWorker's
	// TaskEventBufferImpl::status_events_ (capacity 100k, drained <=10k/s);
	// RAY_ray_event_recorder_max_queued_events is a DIFFERENT (GCS-side) buffer
	// and does not affect this path. Worker containers stay at defaults as the
	// experiment control arm.
	if cfg.HeadStatusBuffer != "" {
		headContainers := rayClusterFromYaml.Spec.HeadGroupSpec.Template.Spec.Containers
		for i := range headContainers {
			if headContainers[i].Name == "ray-head" {
				upsertEnv(&headContainers[i], "RAY_task_events_max_num_status_events_buffer_on_worker", cfg.HeadStatusBuffer, nil)
			}
		}
	}

	// Low num_cpus multiplies concurrent Ray worker processes (2 CPU / 0.05 =
	// 40 python workers), which outgrows the sample manifest's 2G limit.
	if cfg.WorkerMemLimit != "" {
		qty := resource.MustParse(cfg.WorkerMemLimit)
		for wg := range rayClusterFromYaml.Spec.WorkerGroupSpecs {
			containers := rayClusterFromYaml.Spec.WorkerGroupSpecs[wg].Template.Spec.Containers
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
	fqdnRayIP := fmt.Sprintf("%s-head-svc.%s.svc.cluster.local", rayClusterName, rayClusterNamespace)
	for i := range containers {
		if containers[i].Name != "collector" {
			continue
		}
		containers[i].Command = append(containers[i].Command,
			fmt.Sprintf("--ray-cluster-namespace=%s", rayClusterNamespace))
		upsertEnv(&containers[i], "POD_IP", "", &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
		})
		upsertEnv(&containers[i], "FQ_RAY_IP", fqdnRayIP, nil)
		if cfg.Compression {
			upsertEnv(&containers[i], "RAY_COLLECTOR_EVENT_COMPRESSION_ENABLED", "true", nil)
		}
		if cfg.RotationIntvl != "" {
			upsertEnv(&containers[i], "RAY_COLLECTOR_EVENT_ROTATION_INTERVAL", cfg.RotationIntvl, nil)
		}
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
