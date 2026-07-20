# Architecture

Apple Pod Control (APC) has two intentionally separated generations. APC v1 is
the deliberately small Kubernetes-inspired control plane described below. APC
v2 runs upstream Kubernetes semantics through K3s and keeps custom code focused
on Apple host lifecycle, image transport, recovery and diagnostics. New cluster
work targets v2; v1 remains available through `apc --legacy`.

## High-level diagram

APC v2 follows the Kubernetes control-plane boundary rather than reimplementing
its APIs. The physical two-Mac worker topology is:

```text
 apc / kubectl / apc helm ── kubeconfig ──> K3s API, controllers, scheduler
                                                │
                         ┌──────────────────────┴──────────────────────┐
                         v                                             v
              Apple VM: K3s server                         Apple VM: K3s agent
              MacBook + named volume                       Mac mini + named volume
                         └──────── Flannel VXLAN / kubelet ────────────┘

 APC lifecycle: labelled volumes, backup/restore, digest upgrade, image sync,
                deep doctor and Background LaunchAgent supervision
```

The separate local HA topology keeps three K3s/embedded-etcd servers on one
Mac and exposes them through a stable loopback TLS-pass-through proxy:

```text
 apc / kubectl / apc helm
             │ protected kubeconfig
             v
 https://127.0.0.1:17442
 APC HA proxy (TLS remains end to end)
       │ health-aware new connections
       ├──────────────┬──────────────┐
       v              v              v
 server VM 1      server VM 2      server VM 3
 K3s + etcd       K3s + etcd       K3s + etcd
 volume 1         volume 2         volume 3

 One physical Mac and therefore one physical failure domain
```

The HA Background LaunchAgent runs member reconciliation and the proxy
concurrently. Kubeconfig preparation authenticates and prefers the stable proxy
and falls back to a reachable Ready member when it is not serving. The proxy
does not terminate or inspect Kubernetes TLS. Reconciliation reads a private,
durable desired-state record under the same per-cluster operation lock used by
HA lifecycle commands. Proxy retry remains independent, so a blocked member
reconcile does not terminate the proxy loop.

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

APC v1 intentionally omits admission webhooks, CRDs, multi-container Pods,
Services/Ingress, distributed consensus and cloud-provider integrations.
SQLite provides excellent single-control-plane latency but not v1 control-plane
HA. APC v2 delegates those Kubernetes APIs to K3s; its local three-server mode
uses K3s embedded etcd, without changing the physical one-Mac fault boundary.

## APC v2 lifecycle and HA invariants

| Concern | Behavior |
|---|---|
| Helm | `apc helm` invokes native Helm with the selected protected kubeconfig; direct Helm remains supported |
| Stable HA API | Default loopback TLS-pass-through endpoint `127.0.0.1:17442`, health-aware backends and authenticated direct-member fallback |
| Persistent HA intent | Private per-cluster desired state records whole-cluster `Running`/`Stopped` plus at most one intentionally stopped member; start clears the applicable stop intent before reconciliation |
| Member maintenance | Only one member is intentionally stopped; quorum-reducing stop/restart requires 3/3 node/API readiness and exact healthy three-member etcd topology, while start requires the other two node/API pairs Ready |
| Supervisor repair | Intentional stops are preserved across ticks and process restarts; without stop intent, exactly one running-but-unhealthy member may be replaced only after a two-voter etcd-majority proof, with exponential backoff after failure |
| Operation serialization | Snapshot, restore and member lifecycle share a private per-cluster cross-process lock |
| HA image transport | All three exact running member targets are resolved before one ARM64 archive is imported and verified everywhere |
| HA diagnostics | Runtime, API, Node and exact embedded-etcd health/topology checks precede per-node DNS, egress, Pod-network and Service probes |
| HA snapshot | Exact healthy three-member etcd topology gates a native K3s snapshot plus matching server token and immutable checksum/topology manifest outside member volumes |
| HA restore | Destructive in-place rollback requiring saved configuration, matching current token, exact network and all three original volumes |
| Restore interlock | Any nonterminal, unsuccessful or untrusted recovery journal blocks automatic VM mutation; only completed recovery or a failed restore whose automatic recovery succeeded is runtime-safe |
| Runtime IPv4 guard | Detect primary-address collisions caused by apple/container's MAC-independent allocation before K3s/etcd mutation; delete only exact owned envelopes and retry within fixed bounds |
| Legacy envelope migration | Recognize only the immediately preceding exact launch identity and replace one member at a time while preserving quorum |

The full etcd proof comes from every member's loopback health endpoint and
metrics, not from Kubernetes readiness. APC requires three unique server IDs,
current health, an elected leader, non-learner voters and exact peer sets that
name the other two members. Repairing an unavailable target uses a narrower
proof from the two non-target voters: both must be healthy voting members,
mutually identify one another and agree on the target's third server ID. This
allows the failed target itself to be unreachable without weakening the
two-voter majority requirement.

The restore path is not replacement-host disaster recovery. A snapshot package
cannot recreate a lost Mac, replace the saved topology or recover three missing
member volumes. The included K3s server token is a credential and cryptographic
recovery dependency; off-host packages require encryption and strict access
control. The current command also requires the present on-host token file to
match the packaged copy, so token-loss recovery remains open.

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
- the HA-role LaunchAgent for continuous local proxy and member reconciliation;
- a LaunchDaemon and dedicated service account for unattended multi-user or
  production-style v1 agents.

Environment variables in this MVP are stored in plaintext. Do not put secrets
there; a Secret API with encrypted-at-rest values is a follow-up.

## Validated boundaries as of 2026-07-20

- The two-Mac deep doctor with public egress skipped reports 16 passes, two
  intentional egress warnings and zero failures. Public HTTPS from the MacBook
  Apple VM remains broken while the Mac mini VM passes.
- The local HA Helm gate placed three Ready nginx replicas across three native
  ARM64 K3s/embedded-etcd server VMs. This demonstrates VM-level quorum and
  scheduling on one Mac, not host-level HA.
- One intentional member stop remained effective for more than two supervisor
  ticks. The remaining two voters accepted and returned a Kubernetes write;
  restarting the stopped member restored 3/3 Ready.
- A fresh live HA snapshot/restore rolled a changed ConfigMap back, removed a
  post-snapshot-only object, retained the Helm release, three distributed Pods
  and PodDisruptionBudget, and completed its recovery journal. The final HA
  doctor reported 34 passes, three expected egress-skip warnings and zero
  failures.
- The validated snapshot package was mode `0700`, with a `0400` manifest and
  `0600` snapshot/token files. Recovery helpers explicitly request and validate
  apple/container's exact default network with MTU 1280 instead of assuming an
  omitted network produces no attachment.
- Peer-restricted PF install and privileged status pass on both Macs, but the
  tested path is still trusted-LAN PF rather than an authenticated overlay.
  Reboot persistence, rejection of an unlisted peer and overlay repetition are
  not yet proven.
- Tailscale is absent and unauthenticated. Installation, macOS network approval
  and account authentication require explicit user action.
- The manual hardware workflow is default-branch-only and read-only by default,
  and does not check out repository code on self-hosted Macs. The dedicated
  runners are not registered, so the GitHub-hosted hardware gate remains open.
