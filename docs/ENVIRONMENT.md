# Environment

Everything in [RESULTS.md](RESULTS.md) was produced on this setup, 2026-08-05.

## Host

| | |
|---|---|
| Machine | Apple M3 Max, 16 cores, 64 GiB RAM |
| OS | macOS 15.3.1 (24D70) |
| Docker | 28.3.3 (Docker Desktop, Linux VM) |
| kind | 0.29.0 |
| Go | 1.26.3 (darwin/arm64) |
| Python | 3 (stdlib only, for the aggregation scripts) |

## Cluster

A **dedicated** kind cluster named `bench` with its own kubeconfig, so that
other work on the machine cannot change context, bucket, or image tags mid-run.

| | |
|---|---|
| Kubernetes | v1.33.1 (kind node image, Debian 12 bookworm) |
| Container runtime | containerd 2.1.1 |
| Kernel | 6.10.14-linuxkit |
| Node allocatable | 14 CPU, 32.8 GiB (single control-plane node) |
| Object storage | MinIO, deployed in-cluster (`minio-dev` namespace) |
| KubeRay operator | Built from source, run as a local binary against the cluster |

Running the operator as a local binary (`--use-kubernetes-proxy`) keeps operator
CPU out of the node's cgroup accounting, so collector and History Server numbers
are not polluted by reconcile work.

## Software under test

| | |
|---|---|
| KubeRay | `be7fca77` (fork of `ray-project/kuberay` master, History Server work in progress) |
| Ray | `rayproject/ray:2.54.0` |
| collector image | `sha256:636c5493313c7519954065ece0bff092c78d7665b832bd3266f14d984740ea76` |
| historyserver image | `sha256:3aaa38f96a0fdec707195bb38ff9610d10c164d26efa56a1d394a240e58d7cd5` |

Digests are recorded per matrix run in `results/matrix-20260805/images.txt`.

## RayCluster shape

```
head group    1 pod   ray-head    2 CPU / 4G   num-cpus: "0"   + collector sidecar
worker group  1 pod   ray-worker  2 CPU / 2G                   + collector sidecar
```

The head is started with `num-cpus: "0"`, so **no task runs on it** — it hosts
the driver only. It still emits 54% of all events (every `TASK_DEFINITION`, plus
the owner half of every `TASK_LIFECYCLE`), which is why its collector is not
optional and should not be sized smaller than a worker's.

Ray container memory in these runs peaked at 2.1–3.5 GiB on the head (it grows
~7 KiB per task from Ray's own task metadata) and stayed under the worker's 2G
limit at `num_cpus=0.2`. Going below `num_cpus=0.1` multiplies Python worker
processes and needs a larger worker limit — `BENCH_WORKER_MEMORY_LIMIT` exists
for that.

## Caveats

1. **CPU numbers are relative, not absolute.** kind on macOS runs inside a
   Docker Desktop VM; scheduling and accounting differ from bare-metal Linux.
   Ratios, scaling curves, and per-event constants transfer as *shapes*; the
   millicore values should be re-measured on your own hardware.
2. **Byte and event counts are exact.** They come from decoding every object in
   the bucket, not from sampling, and do not depend on the host.
3. **One node pair.** Per-node results generalize; anything cluster-wide is a
   sum over nodes, not a measured scale-out.
4. **No-op tasks, one job, one worker.** Task logs are near-empty here
   (~45 B/task); events per task, bytes per event and the gzip ratio are all
   specific to silent no-op tasks. Everything was a single job on a single
   worker, so per-job and per-node cardinality effects are untested.
5. **The collector sidecars have no `resources` block.** Their pods are still
   Burstable (the Ray containers have requests), but the collector itself has no
   CPU request, so under node contention it would get minimum shares. The kind
   node here was mostly idle, so these numbers are a best case.
5. **Sampling resolution.** Memory and CPU are sampled at 1 s from cgroup v2 and
   from the kubelet summary API (whose effective resolution is ~10 s). Peak
   values additionally come from the kernel's own `memory.peak`, which polling
   cannot miss.
