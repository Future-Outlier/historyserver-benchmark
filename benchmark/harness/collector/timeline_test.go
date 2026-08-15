package benchmark

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// waitForOwnedCluster resolves the cluster an owned-mode RayJob created. The
// operator generates the name (rayjob-bench-xxxxx), so nothing can be assumed
// before Status.RayClusterName is populated; after that the same readiness
// gates as the pre-created path apply.
func waitForOwnedCluster(test Test, g *WithT, namespace *corev1.Namespace, jobName string) *rayv1.RayCluster {
	var clusterName string
	g.Eventually(func() string {
		job, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).
			Get(test.Ctx(), jobName, metav1.GetOptions{})
		if err != nil {
			return ""
		}
		clusterName = job.Status.RayClusterName
		return clusterName
	}, TestTimeoutMedium).ShouldNot(BeEmpty(), "RayJob should report its generated cluster name")
	LogWithTimestamp(test.T(), "operator created RayCluster %s/%s", namespace.Name, clusterName)

	g.Eventually(RayCluster(test, namespace.Name, clusterName), TestTimeoutLong).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))
	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).
		Get(test.Ctx(), clusterName, metav1.GetOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	g.Eventually(HeadPod(test, rayCluster), TestTimeoutMedium).
		Should(WithTransform(IsPodRunningAndReady, BeTrue()))
	return rayCluster
}

// TimelineEvent is one observed state transition during the run. The sequence
// answers the question the aggregate numbers cannot: how much time actually
// separated "driver finished" from "collector got SIGTERM".
type TimelineEvent struct {
	At    time.Time `json:"at"`
	Kind  string    `json:"kind"` // rayjob | raycluster | pod
	Name  string    `json:"name"`
	Event string    `json:"event"`
}

// PodTermination is the last container exit this recorder OBSERVED. It is
// best-effort evidence, never a verdict:
//
//   - Source distinguishes State.Terminated (current in the latest sampled
//     ContainerStatus) from LastTerminationState.Terminated (a PREVIOUS run,
//     e.g. after a restart). A later sample may legitimately turn a previously
//     current termination into a previous-run one; the newest snapshot wins.
//   - Observed is false when the pod disappeared before any terminated state was
//     seen. A 1 s poll can miss a short termination entirely, so "no data" means
//     unknown, NOT graceful.
type PodTermination struct {
	Pod          string `json:"pod"`
	Container    string `json:"container"`
	ContainerID  string `json:"containerID"`
	RestartCount int32  `json:"restartCount"`
	Observed     bool   `json:"observed"`
	Source       string `json:"source"` // "current" | "previous-run" | "" when not observed
	ExitCode     int32  `json:"exitCode"`
	Reason       string `json:"reason"`
}

type deletionTimeline struct {
	test    Test
	ns      string
	jobName string

	mu       sync.Mutex
	events   []TimelineEvent
	seen     map[string]string         // dedup key -> last value
	terms    map[string]PodTermination // pod/container -> best known termination
	podsSeen map[string]bool           // pods observed alive, to detect disappearance
	stop     chan struct{}
	done     chan struct{}
}

// startDeletionTimeline polls the RayJob, its cluster, and the Ray pods once a
// second and records every transition. Polling is the only option: the pods and
// the cluster are deleted at the end, and their final states are unreadable the
// moment that happens, so the recorder keeps the last thing it saw.
func startDeletionTimeline(test Test, namespace, jobName string) *deletionTimeline {
	d := &deletionTimeline{
		test: test, ns: namespace, jobName: jobName,
		seen:     map[string]string{},
		terms:    map[string]PodTermination{},
		podsSeen: map[string]bool{},
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go d.loop()
	return d
}

func (d *deletionTimeline) record(kind, name, event string) {
	key := kind + "/" + name
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen[key] == event {
		return
	}
	d.seen[key] = event
	d.events = append(d.events, TimelineEvent{At: time.Now(), Kind: kind, Name: name, Event: event})
}

func (d *deletionTimeline) loop() {
	defer close(d.done)
	ctx := d.test.Ctx()
	var clusterName string
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-d.stop:
			return
		case <-tick.C:
		}

		if job, err := d.test.Client().Ray().RayV1().RayJobs(d.ns).Get(ctx, d.jobName, metav1.GetOptions{}); err == nil {
			if clusterName == "" {
				clusterName = job.Status.RayClusterName
			}
			d.record("rayjob", d.jobName, fmt.Sprintf("jobStatus=%s deploy=%s endTime=%s",
				job.Status.JobStatus, job.Status.JobDeploymentStatus, formatTimePtr(job.Status.EndTime)))
		} else if k8serrors.IsNotFound(err) {
			d.record("rayjob", d.jobName, "deleted")
		}

		if clusterName != "" {
			if rc, err := d.test.Client().Ray().RayV1().RayClusters(d.ns).Get(ctx, clusterName, metav1.GetOptions{}); err == nil {
				if rc.DeletionTimestamp != nil {
					d.record("raycluster", clusterName, "deletionTimestamp set")
				} else {
					d.record("raycluster", clusterName, fmt.Sprintf("state=%s", rc.Status.State))
				}
			} else if k8serrors.IsNotFound(err) {
				d.record("raycluster", clusterName, "deleted")
			}
		}

		pods, err := d.test.Client().Core().CoreV1().Pods(d.ns).List(ctx, metav1.ListOptions{
			LabelSelector: "test=raycluster-historyserver",
		})
		if err != nil {
			continue
		}
		// Pods that were present before and are absent now: record the
		// disappearance explicitly, so a container whose exit was never sampled
		// ends up as observed=false rather than as a silent gap.
		present := map[string]bool{}
		for _, pod := range pods.Items {
			present[pod.Name] = true
		}
		d.mu.Lock()
		for name := range d.podsSeen {
			if !present[name] {
				delete(d.podsSeen, name)
				d.mu.Unlock()
				d.record("pod", name, "gone")
				d.mu.Lock()
			}
		}
		d.mu.Unlock()

		for _, pod := range pods.Items {
			d.mu.Lock()
			d.podsSeen[pod.Name] = true
			for _, c := range pod.Spec.Containers {
				key := pod.Name + "/" + c.Name
				if _, ok := d.terms[key]; !ok {
					// Placeholder: if this container's exit is never sampled the
					// report still carries a row, marked unobserved.
					d.terms[key] = PodTermination{Pod: pod.Name, Container: c.Name}
				}
			}
			d.mu.Unlock()

			if pod.DeletionTimestamp != nil {
				d.record("pod", pod.Name, "deletionTimestamp set")
			}
			for _, cs := range pod.Status.ContainerStatuses {
				// Within one API snapshot, the current terminated state wins over
				// LastTerminationState. ACROSS snapshots the newest one simply
				// replaces the old: the manifest sets no restartPolicy so pods
				// default to Always, and freezing the first "current" would pin
				// an early crash's exit code and report it as the shutdown exit.
				// Demoting it to "previous-run" on restart is the honest record.
				key := pod.Name + "/" + cs.Name
				next := PodTermination{
					Pod: pod.Name, Container: cs.Name,
					ContainerID: bareContainerID(cs.ContainerID), RestartCount: cs.RestartCount,
				}
				var term *corev1.ContainerStateTerminated
				source := ""
				if cs.State.Terminated != nil {
					term, source = cs.State.Terminated, "current"
				} else if cs.LastTerminationState.Terminated != nil {
					term, source = cs.LastTerminationState.Terminated, "previous-run"
				}
				if term != nil {
					next.Observed, next.Source = true, source
					next.ExitCode, next.Reason = term.ExitCode, term.Reason
				}
				d.mu.Lock()
				d.terms[key] = next
				d.mu.Unlock()
				if term == nil {
					continue
				}
				d.record("pod", key,
					fmt.Sprintf("terminated(%s) exit=%d reason=%s", source, term.ExitCode, term.Reason))
			}
		}
	}
}

// Stop ends polling and returns everything observed, termination states sorted
// for stable reports.
func (d *deletionTimeline) Stop() ([]TimelineEvent, []PodTermination) {
	select {
	case <-d.stop:
	default:
		close(d.stop)
	}
	<-d.done
	d.mu.Lock()
	defer d.mu.Unlock()
	terms := make([]PodTermination, 0, len(d.terms))
	for _, t := range d.terms {
		terms = append(terms, t)
	}
	sort.Slice(terms, func(i, j int) bool {
		if terms[i].Pod != terms[j].Pod {
			return terms[i].Pod < terms[j].Pod
		}
		return terms[i].Container < terms[j].Container
	})
	return d.events, terms
}

// WriteCSV dumps the transitions for offline inspection; call after Stop.
func (d *deletionTimeline) WriteCSV(path string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := fmt.Fprintln(f, "time_nano,kind,name,event"); err != nil {
		return err
	}
	for _, e := range d.events {
		if _, err := fmt.Fprintf(f, "%d,%s,%s,%q\n", e.At.UnixNano(), e.Kind, e.Name, e.Event); err != nil {
			return err
		}
	}
	return nil
}

func formatTimePtr(t *metav1.Time) string {
	if t == nil {
		return "nil"
	}
	return t.UTC().Format(time.RFC3339)
}
