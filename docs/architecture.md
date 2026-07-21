# Architecture

Apple Pod Control (APC) is an Apple-host and cluster-lifecycle layer for real
Kubernetes. It runs pinned K3s images as native ARM64 node VMs through
`apple/container` 1.0 and deliberately leaves Kubernetes workload semantics to
K3s, kubectl and Helm.

## Responsibility boundary

| Kubernetes / K3s owns | APC owns |
|---|---|
| Kubernetes API and authentication | Creation and validation of Apple node VMs |
| Declarative workload desired state | Persistent Apple volumes for K3s data |
| Controllers and reconciliation | Kubeconfig protection and cluster selection |
| Scheduling and Pod placement | Single-server and agent lifecycle |
| Pods, Deployments, Services and Jobs | Local three-server HA lifecycle |
| Secrets, ConfigMaps and admission | Image transport into nested containerd |
| CNI behavior inside each node | Host PF/overlay integration and deep diagnostics |
| Single-server datastore or embedded etcd | macOS launchd supervision and recovery tooling |

There is one user-facing executable, `apc`. Kubernetes-style top-level commands
are streamed to native kubectl, and `apc helm` streams to native Helm with the
selected APC-managed kubeconfig. APC does not parse or store workload objects.

A Kubernetes Pod is a normal K3s Pod inside a node VM. APC creates node VMs,
not one Apple VM per Pod. Each K3s node can host many Pods.

## Topologies

### Single server or two-Mac lab

```text
 apc / kubectl / Helm
          │ protected kubeconfig
          v
 K3s API + controllers + scheduler
          │
          ├─────────────────────────────┐
          v                             v
 Apple VM: K3s server          Apple VM: K3s agent
 named data volume             named data volume
          │                             │
          └──── Flannel VXLAN/kubelet ──┘
      physical Mac A                physical Mac B

 APC reconciles each Apple VM envelope and volume from saved host-side state.
```

The API defaults to loopback for a single host. A two-Mac cluster publishes the
API, kubelet and VXLAN ports on explicitly chosen host identities and restricts
them to exact peers through a managed PF anchor. VXLAN is unencrypted and is
therefore limited to a trusted LAN or an authenticated host overlay.

### Local three-server HA lab

```text
 apc / kubectl / Helm
          │ protected kubeconfig
          v
 loopback APC TLS-pass-through proxy
          │ health-aware new connections
          ├──────────────┬──────────────┐
          v              v              v
 server VM 1        server VM 2        server VM 3
 K3s + etcd         K3s + etcd         K3s + etcd
 volume 1           volume 2           volume 3
          └──── APC-owned Apple network ┘

 All three VMs share one physical Mac and one physical failure domain.
```

Three embedded-etcd voters tolerate one server-VM failure. They do not tolerate
loss of the physical Mac. The proxy passes Kubernetes TLS through unchanged;
it neither terminates TLS nor sees Kubernetes credentials. Prepared
kubeconfigs prefer the authenticated proxy and fall back to a reachable Ready
member when the proxy is unavailable.

## Host-side state

APC stores only Apple/K3s lifecycle material outside the node VMs:

- cluster topology, immutable K3s image identity and selected host addresses;
- exact Apple resource names and ownership labels;
- persistent volume and node identity information;
- protected kubeconfig and K3s join token files;
- HA desired state, recovery journal and operation lock;
- launchd and PF definitions derived from explicit operator input.

Kubernetes objects and workload desired state remain in K3s. Kubeconfig and
token files are credentials, use private file modes and are never accepted as
literal join-token command arguments.

## Node lifecycle and reconciliation

An Apple VM envelope is disposable; its APC-labelled data volume is durable.
For start or replacement APC:

1. loads and validates the saved cluster or agent configuration;
2. resolves every existing Apple resource and verifies exact ownership labels;
3. starts Apple's container service when required;
4. creates a native ARM64 K3s VM with bounded CPU and memory;
5. attaches the existing named volume at `/var/lib/rancher/k3s`;
6. publishes only the role-specific API, kubelet and VXLAN ports;
7. waits for the Apple runtime, Kubernetes API and expected Node readiness;
8. writes or refreshes a protected kubeconfig only after successful validation.

The manager adopts only the exact saved identity. A similarly named, differently
labelled or differently configured resource fails closed. Delete, restore and
upgrade require explicit consent and verify targets again immediately before
mutation.

Apple assigns a dynamic private primary address to a newly created VM. APC
records each node's Kubernetes `InternalIP` and binds it as a secondary address
in a replacement envelope, allowing K3s, Flannel and NetworkPolicy to retain
node identity while the Apple primary address remains available for NAT.

On a LAN, host advertise addresses and a worker's K3s server URL are external
configuration. They are not portable across arbitrary DHCP changes. The saved
two-Mac worker is currently stopped because those physical host addresses
drifted; stable reservations or an authenticated overlay are required before
that topology is rejoined.

## kubectl and Helm data path

Cluster selection precedence is:

1. APC's `--cluster NAME` global selector;
2. `APC_CLUSTER`;
3. the persisted current cluster.

APC validates the selected kubeconfig's ownership and private mode, prepares a
reachable endpoint, then replaces its process with native kubectl or Helm while
preserving standard input, output, error and exit behavior. Watches,
server-side apply, interactive exec, log streaming, port forwarding, plugins
and future upstream flags therefore behave like the native clients. No selected
cluster means a clear selection error, not an implicit network request.

## Image path and Apple Silicon

- Node and helper VMs explicitly select `--arch arm64`.
- K3s is pinned by OCI digest; upgrades also require immutable digests.
- ARM64 or multi-platform workload images avoid Rosetta and x86 translation.
- Host-side prefetch exports a private OCI archive and imports it into the
  selected nested K3s containerd store.
- Peer sync streams the archive over key-authenticated SSH without storing it
  as a durable remote-host file.
- HA prefetch resolves all three exact members before importing and verifies
  the requested reference in every store before reporting success.

The node VMs are native ARM64 Linux VMs provided by Apple's Virtualization
framework through `apple/container`; APC does not add nested virtualization.

## Deep diagnostics

Kubernetes Node `Ready` cannot prove the health of the outer Apple VM's NAT or
cross-host route. `apc cluster doctor` therefore checks:

- exact Apple VM runtime and published API state;
- Kubernetes API and Node conditions;
- K3s version and kubelet exec on every Ready node;
- CoreDNS and optional public HTTPS from node-pinned Pods;
- HTTP over every directed cross-node Pod path;
- Service DNS and deterministic ClusterIP routing;
- for HA, every local embedded-etcd health endpoint and exact peer topology.

Probe Pods and Service use unique exact names, are registered for cleanup
before creation and are force-deleted unless the operator explicitly keeps
them. Below HA quorum the doctor stops before creating workload probes.

## Local HA invariants

| Concern | Invariant |
|---|---|
| Stable API | Loopback TLS-pass-through proxy with health-aware backends and authenticated direct-member fallback |
| Durable intent | Whole cluster is `Running` or `Stopped`; a running cluster may have at most one intentionally stopped member |
| Member maintenance | Quorum-reducing stop/restart requires all three Node/API pairs and exact healthy three-voter etcd topology |
| Missing member repair | The other two Node/API pairs must be Ready before one absent member is recreated |
| Unhealthy member repair | The two non-target etcd voters must prove a healthy, mutually consistent majority |
| Repair rate | At most one member per pass, with bounded exponential backoff after failure |
| Serialization | Snapshot, restore, recover and member lifecycle share one private cross-process lock |
| Snapshot | Requires 3/3 Ready plus exact etcd topology and exports a native snapshot with matching server token |
| Restore | In-place rollback requiring saved configuration, matching current token, exact network and original volumes |
| Recover | Externally anchored reconstruction from a protected package into intentionally empty local state |
| Interlock | Nonterminal, unsuccessful or untrusted recovery journal blocks automatic VM mutation |

The full etcd proof comes from each member's local health endpoint and metrics,
not from Kubernetes readiness. APC requires unique voting IDs, leader
agreement, non-learner state and exact peer membership. Repair of an
unreachable target uses the narrower proof from the two surviving voters.

## Recovery trust model

An HA snapshot package contains a native K3s etcd snapshot, immutable manifest
and matching server token. The token is both a credential and part of the
recovery identity. The package must be encrypted when stored off-host. The
manifest SHA-256 printed during snapshot must be retained independently; a
manifest stored beside its own package is not an independent trust anchor.

`restore` and `recover` intentionally differ:

- restore rolls back the same intact cluster and requires its current matching
  token, saved configuration, exact network and all three original volumes;
- recover validates the independent manifest digest before publishing local
  state, then reconstructs the exact saved configuration, token, network and
  three volumes on an intentionally empty host.

Same-Mac empty-host recovery is live-proven. A wrong digest caused no
configuration, token, network, volume or VM mutation; the correct digest
returned all three members and preserved Kubernetes objects.

The replacement-Mac drill progressed through package validation, external
digest verification, topology reconstruction and seed-member etcd reset. Peers
then could not route to the stored stable address through the target host's
newer `apple/container` vmnet behavior. APC did not weaken ownership or quorum
checks: it failed closed, preserved the failed operation for diagnosis and the
disposable target cluster was explicitly cleaned afterward. Off-host recovery
is not supported until member addressing no longer depends on that
host-specific secondary-address route.

## Network and security boundaries

Single-server APIs bind to loopback by default. Cross-host operation requires
explicit publish and advertise identities. APC can render a PF ruleset without
mutation, then install a root-owned helper and LaunchDaemon only after explicit
administrator consent. The managed anchor permits only listed peers on K3s API
TCP 16443, kubelet TCP 10250 and Flannel VXLAN UDP 8472.

PF limits peers but does not encrypt VXLAN. For an untrusted network, establish
an authenticated host overlay independently and use its stable identities for
cluster create, node join and PF. APC's Tailscale preflight reads only
machine-readable identity/route state; it never accepts an authentication key.

The node image requires broad Linux capabilities inside its dedicated Apple
VM. Those capabilities do not grant macOS host privileges. APC mounts only the
managed K3s data volume and does not mount the user's home directory into a
node.

## macOS supervision boundary

A per-user Background LaunchAgent can supervise a server, agent or HA cluster
inside an active login domain. It is not a zero-login boot guarantee.

The unattended profile installs a root-owned system LaunchDaemon but never
runs APC cluster logic as root. Its exact argument chain enters the user's
bootstrap namespace with `launchctl asuser`, drops to the selected account with
non-interactive `sudo`, and starts under a clean bounded environment. Launchd
stdout/stderr go to `/dev/null`; APC opens its protected 1 MiB log only after
privilege drop.

Installation and status reject symlinks, writable managed paths, non-root
LaunchDaemon ancestors and extended allow ACL grants. Status requires the
system job to report `running`, a positive PID and the complete expected
program/argument vector. Deterministic coverage exists, but the repeated live
zero-login APC reboot gate remains open.

## Validated and open boundaries

Live evidence currently includes:

- a single native ARM64 K3s server with ordinary kubectl and Helm workloads;
- a historical two-Mac server/agent run with DNS, ClusterIP, directed
  cross-host Pod HTTP, NetworkPolicy and image sync;
- local three-server embedded-etcd quorum, intentional member loss, Helm
  topology spread, deep diagnostics, snapshot/restore and same-Mac empty-host
  recovery;
- peer-restricted PF on both Macs and headless PF reconstruction on the Mac
  mini;
- root-owned GitHub runner services on both Macs and a headless Mac mini runner
  reboot.

Open boundaries include:

- current two-Mac readiness after physical DHCP address drift;
- public HTTPS from Apple VMs on every tested host;
- replacement-Mac/off-host HA recovery;
- three independent physical etcd failure domains;
- authenticated-overlay validation;
- MacBook PF and runner reboot evidence, unlisted-peer rejection and PF
  uninstall rollback;
- dedicated runner/service accounts and APC's live zero-login reboot repeat.

See [validation results](validation-results.md) and
[release readiness](release-readiness.md) for the dated evidence and objective
completion gates.
