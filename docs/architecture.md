# Architecture

Apple Pod Control (APC) has two intentionally separated generations. APC v1 is
the deliberately small Kubernetes-inspired control plane described below. APC
v2 runs upstream Kubernetes semantics through K3s and keeps custom code focused
on Apple host lifecycle, image transport, recovery and diagnostics. New cluster
work targets v2; v1 remains available through `apc --legacy`.

## High-level diagram

APC v2 follows the Kubernetes control-plane boundary rather than reimplementing
its APIs:

```text
 apc / kubectl / Helm ── kubeconfig ──> K3s API, controllers, scheduler, SQLite
                                                │
                         ┌──────────────────────┴──────────────────────┐
                         v                                             v
              Apple VM: K3s server                         Apple VM: K3s agent
              MacBook + named volume                       Mac mini + named volume
                         └──────── Flannel VXLAN / kubelet ────────────┘

 APC lifecycle: labelled volumes, backup/restore, digest upgrade, image sync,
                deep doctor and Background LaunchAgent supervision
```

The original v1 component model remains:

```text
                     HTTPS / Kubernetes-shaped JSON
  deployment.yaml ── apc CLI ───────────────────────────────┐
                                                            v
  ┌────────────────────────── CONTROL PLANE (Go) ──────────────────────────┐
  │ REST API Server ──> Validation/defaulting ──> SQLite (WAL) Desired State│
  │       │                                            │                   │
  │       └──────── Deployment Controller <────────────┘                   │
  │                           │                                            │
  │                     Scheduler                                          │
  │         (labels, requests, ARM64, host ports, least-loaded)            │
  │                           │                                            │
  │                  gRPC Session Registry                                 │
  └───────────────────────────┬────────────────────────────────────────────┘
                              │ long-lived bidirectional gRPC
                 ┌────────────┴────────────┐
                 v                         v
  ┌──────────── MacBook ───────────┐  ┌─────────── Mac mini ────────────┐
  │ apc-agent                      │  │ apc-agent                       │
  │ heartbeat / probes / reconcile │  │ heartbeat / probes / reconcile │
  │         │                      │  │         │                       │
  │ apple/container 1.0 CLI        │  │ apple/container 1.0 CLI         │
  │         │                      │  │         │                       │
  │ isolated ARM64 micro-VMs       │  │ isolated ARM64 micro-VMs        │
  └────────────────────────────────┘  └─────────────────────────────────┘
```

## Kubernetes concepts retained

| Kubernetes behavior | APC implementation |
|---|---|
| Declarative desired state | `apc apply -f`, persisted deployment objects |
| API objects | `apiVersion`, `kind`, `metadata`, `spec`, `status` |
| Namespaces and labels | Namespaced deployments/pods, labels, node selectors |
| Controllers | Level-triggered deployment reconciliation loop |
| Scheduler | Filters by readiness, ARM64, node labels, CPU/RAM and host ports; scores least-loaded nodes |
| Pods | One APC workload maps to one `apple/container` lightweight VM |
| Probes | HTTP, TCP and exec readiness/liveness probes |
| Self-healing | Agent restarts on liveness failure; controller replaces exited workloads |
| Rollouts | `RollingUpdate` (`maxSurge`, `maxUnavailable`) and `Recreate` |
| kubectl-style UX | `apply`, `get`, `describe`, `delete`, `scale`, `rollout status`, `-n`, `-o` |

APC intentionally omits admission webhooks, CRDs, multi-container Pods,
Services/Ingress, distributed consensus and cloud-provider integrations in the
MVP. SQLite provides excellent single-control-plane latency but not HA. The API
and controller boundaries allow SQLite to be replaced by an etcd-backed store
when multi-control-plane availability becomes necessary.

## Reconciliation and failure handling

1. The REST API validates/defaults an object and atomically increments its
   generation only when `spec` changes.
2. The deployment controller compares a stable hash of the Pod template and
   the replica count. Scaling increments the Deployment generation but does not
   replace Pods whose template is unchanged.
3. Rolling updates create up to `maxSurge` new workloads and retire old ones
   while respecting `maxUnavailable`.
4. The scheduler selects a connected, ready ARM64 node. Static host ports are
   treated as per-node exclusive resources.
5. Commands are deduplicated per workload and operation while in flight. The
   `container run` and stop/delete paths are idempotent across reconnects.
6. Agents heartbeat every five seconds. Nodes become `NotReady` after 15
   seconds without a heartbeat.
7. After an agent reconnect, its assigned workloads become `Unknown`; the
   controller re-drives their start commands so the agent adopts existing VMs
   and resumes probes without recreating them.
8. Agents observe `container inspect` output every two seconds. Readiness gates
   rollout availability; repeated liveness failures restart the VM locally.
9. Failed starts enter a bounded retry backoff. A `Stopping` workload retains
   its CPU, memory and host-port allocation until the agent acknowledges
   deletion, preventing a replacement from racing the still-running VM.
10. If a process exits, the controller terminates the stale workload record and
    creates a replacement. Stop commands are re-driven after control-plane
    restarts until acknowledged.
11. Control-plane shutdown gives HTTP and gRPC up to ten seconds to drain, then
    force-closes long-lived streams so agents reconnect to a replacement.

## Apple Silicon runtime choices

- Every run explicitly selects `--arch arm64`, avoiding Rosetta and x86 image
  translation.
- Default APC requests/limits are 1 CPU and 512 MB when omitted, reducing the
  per-VM footprint compared with the `container` defaults. Limits are passed to
  `container run --cpus/--memory`.
- The agent uses the `container` 1.0 machine-readable `inspect` JSON rather
  than scraping tables.
- Image pulls and VM boot stay node-local. The persistent gRPC channel and
  two-second observation loop keep control latency small.
- SQLite uses WAL mode and a five-second busy timeout; the control plane uses a
  single connection to retain deterministic write ordering.

## Security boundary

The default local-development mode is plaintext. A LAN deployment should use:

- TLS 1.3 on REST and gRPC (`--tls-cert`, `--tls-key`);
- a private CA plus `--client-ca` for agent mutual TLS;
- an `APC_TOKEN` bearer token for `apc` REST access;
- a dedicated macOS user and Background LaunchAgent for each APC v2 node;
- a LaunchDaemon and dedicated service account for unattended multi-user or
  production-style v1 agents.

Environment variables in this MVP are stored in plaintext. Do not put secrets
there; a Secret API with encrypted-at-rest values is a follow-up.
