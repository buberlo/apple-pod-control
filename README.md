# Apple Pod Control

Apple Pod Control (APC) is a lightweight, Kubernetes-inspired orchestrator for
[`apple/container` 1.0](https://github.com/apple/container). A small Go control
plane schedules isolated ARM64 Linux micro-VM workloads across Apple Silicon
Macs; a local agent drives Apple's native `container` CLI.

APC v2 is now being developed on a separate path that runs a real K3s control
plane inside an Apple container VM. This makes native `kubectl` and Helm
available without emulating more Kubernetes APIs. The tested design decision is
recorded in [ADR 0001](docs/adr/0001-k3s-control-plane.md), and the isolated
[K3s spike quickstart](docs/k3s-spike.md) does not disturb an APC v1 cluster.
The [two-Mac validation report](docs/k3s-spike-results.md) records what passed
and the Apple runtime networking issue found during restart testing.

APC can also run a local three-server K3s/embedded-etcd lab in three native
ARM64 Apple micro-VMs. It tolerates one server-VM failure while remaining one
physical Mac failure domain. A supervised local TLS-pass-through proxy gives
clients a stable loopback API endpoint while retaining direct-member fallback.
APC persists HA operator intent separately from observed VM state: the whole
cluster is either desired `Running` or `Stopped`, and a running cluster may
have at most one intentionally stopped member. The supervisor honors that
intent across process restarts instead of undoing maintenance operations.
See the [local K3s HA guide](docs/k3s-ha.md).

This repository is an MVP: it is useful for development and trusted LAN labs,
but it is not a production replacement for Kubernetes. The architecture and
failure behavior are documented in [docs/architecture.md](docs/architecture.md).

## What is included

- Kubernetes-shaped Deployment YAML with namespaces, labels/selectors,
  single-container Pod templates, resource requests/limits and probes
- REST API plus SQLite/WAL desired-state store
- level-triggered Deployment controller, self-healing and rolling updates
- resource-aware scheduler for connected Apple Silicon nodes
- long-lived bidirectional gRPC agent channel with heartbeat/status reporting
- `apple/container run --detach --arch arm64` execution and JSON inspection
- `apc`, a kubectl-like CLI:

```text
apc apply -f examples/deployment.yaml
apc get deployments
apc get pods -o wide
apc get nodes
apc describe deployment web
apc scale deployment web --replicas=3
apc rollout status deployment/web --timeout=2m
apc delete deployment web
```

Those commands target the APC v1 API when no APC v2 context exists or when
`--legacy` is used. With an active K3s cluster, APC v2 forwards Kubernetes
workload commands to native `kubectl` with full streaming and flag support:

```text
apc config use-cluster lan-spike
apc get pods -A -o wide
apc apply -f examples/kubernetes/web.yaml
apc logs -f deployment/web
apc exec -it deployment/web -- /bin/sh
apc rollout status deployment/web
apc port-forward service/web 8081:80
apc cluster doctor
apc image sync docker.io/library/busybox:1.36.1 --peer user@mac-mini.local
apc cluster backup lan-spike --output "$HOME/Backups/lan-spike.apcbackup"
apc system install --role server --cluster lan-spike
apc cluster network-policy enable lan-spike --yes
apc cluster ha create ha-lab
apc cluster ha status ha-lab
apc system install --role ha --cluster ha-lab \
  --executable "$HOME/.local/bin/apc"
apc --cluster ha-lab get nodes -o wide
apc helm --cluster ha-lab upgrade --install web examples/helm/web --wait
apc --cluster ha-lab image prefetch docker.io/library/busybox:1.36.1
apc cluster doctor ha-lab --skip-egress
apc cluster ha member restart 2 ha-lab --yes
apc cluster ha snapshot ha-lab --output "$HOME/Backups/ha-lab-snapshot"
```

APC owns cluster lifecycle and context selection. Kubernetes continues to own
its API and command semantics. `apc helm` invokes native Helm with the selected
protected kubeconfig, while direct Helm remains supported. See the
[APC v2 CLI contract](docs/cli-v2.md).

`apc cluster doctor` goes beyond Kubernetes `Ready`: it creates an isolated,
short-lived probe Pod on every Ready node and checks host API reachability,
kubelet exec, DNS, HTTPS egress, ClusterIP and directed cross-node HTTP. The
uniquely named probe Pods and Service are deleted automatically.

`apc image prefetch` imports host-pulled ARM64 images into the local nested K3s
containerd. `apc image sync` additionally streams the private OCI archive over
key-only SSH into remote APC agents, allowing deterministic scheduling even
when an Apple VM cannot reach a public registry. For a protected HA cluster,
prefetch validates and imports the image into all three server stores before it
reports success.

Cluster lifecycle includes ownership-checked delete/remove, consistent offline
volume backups, checksum-validated restore, digest-only upgrades with automatic
rollback, and per-user launchd supervision. Destructive operations require an
explicit `--yes`; `--keep-data` removes only the disposable VM envelope.

HA uses separate native-etcd snapshot and restore commands. Snapshot packages
include the matching K3s server token and must be protected as credentials.
Restore is an in-place rollback for the same saved cluster: it requires the
current matching token, saved configuration, exact network and all three member
volumes. It is not replacement-host disaster recovery. Snapshot, restore and
single-member lifecycle operations are serialized across independent APC
processes by a private per-cluster lock. Quorum-reducing member mutations and
snapshots also validate the real local embedded-etcd health and peer topology;
Kubernetes API or Node readiness alone is not accepted as quorum proof.

Without intentional stop state, the HA supervisor repairs at most one unhealthy
member at a time. A running-but-unhealthy member is restarted only after the
other two embedded-etcd voters prove a healthy, mutually consistent majority;
failed repairs use bounded exponential backoff. A nonterminal, unsuccessful or
untrusted restore journal blocks automatic VM mutation until recovery is safely
completed or retried. The independently supervised API proxy continues running
while reconciliation is blocked.

The final live HA gate on 2026-07-20 ran three native ARM64 K3s/embedded-etcd
VMs on one Mac at 3/3 Ready. Its Helm nginx release had one Ready Pod per VM and
an intact PodDisruptionBudget. One intentionally stopped member remained down
for more than two supervisor ticks while the two-voter quorum accepted and read
back a write; starting that member again returned the cluster to 3/3. A fresh
snapshot and restore reverted a changed ConfigMap and removed an object created
only after the snapshot, while preserving the Helm release. The final deep
doctor reported 34 passes, three expected public-egress-skip warnings and zero
failures.

The validated snapshot package used a private `0700` directory, an immutable
`0400` manifest and `0600` snapshot/token artifacts. The live snapshot also
exposed that apple/container automatically attaches a default network to a
recovery helper; APC now requests `default,mtu=1280` explicitly and requires
that exact identity during ownership checks. Replacement-host and token-loss
recovery remain unsupported, as does physical-host HA without three Macs.

New v2 clusters enable K3s NetworkPolicy enforcement by default. APC preserves
each node's Kubernetes `InternalIP` across disposable Apple-VM replacement, and
the policy controller has been exercised across consecutive server and agent
restarts. The example Helm chart includes a same-namespace ingress policy,
rolling updates, topology spread and an optional PodDisruptionBudget.

For a two-Mac cluster, APC can validate, load and persist exact-peer macOS PF
rules for the Kubernetes API, kubelet and Flannel VXLAN ports. Installation
copies a root-owned helper and creates a root LaunchDaemon; it requires an
interactive administrator authorization on each Mac:

```bash
apc system firewall render --cluster lan-spike --role server \
  --interface en0 --local-ip SERVER_IP --peer AGENT_IP
sudo apc system firewall install --cluster lan-spike --role server \
  --interface en0 --local-ip SERVER_IP --peer AGENT_IP --yes
```

VXLAN is not encrypted. Use only a trusted LAN, or bind the cluster and PF rules
to an authenticated host-overlay interface. Apple container 1.0's guest kernel
does not support WireGuard-native Flannel.

`apc system overlay check --peer-ip PEER_TAILSCALE_IP` validates an independently
authenticated Tailscale host path without accepting an auth key. PF supports
`--interface auto`, so its reboot reconciler can resolve macOS's current `utun`
name from the stable local overlay IP.

As of 2026-07-20, PF install and privileged status checks pass on both test
Macs. Reboot persistence and rejection of an unlisted peer have not yet been
demonstrated, and the overlay gate remains open. Tailscale is absent on both
machines; installation, macOS network approval and interactive account
authentication require the user.

## Requirements

- Control plane/CLI: Go 1.25 or newer
- Runtime nodes: Apple Silicon, macOS 26, `apple/container` 1.0
- Start Apple's service once on every node: `container system start`

Apple's runtime is optimized for Apple Silicon and runs each OCI container in
its own lightweight VM. APC uses the CLI contract from the 1.0 release:
`container run`, `stop`, `delete`, `exec`, and machine-readable `inspect` JSON.

## Build and test

```bash
make test
make build
make install
```

Binaries are written to `bin/apc`, `bin/apc-server`, and `bin/apc-agent`.
`make install` copies them to `~/.local/bin` by default; override this with
`PREFIX=/usr/local make install` when a system-wide installation is desired.
Ensure `~/.local/bin` is present in your shell's `PATH`.

## APC v2 K3s spike

Check the Mac and create a local Kubernetes node on the isolated development
port `16443`:

```bash
bin/apc doctor
bin/apc cluster create spike
export KUBECONFIG="$(bin/apc kubeconfig path spike)"

bin/apc get nodes
bin/apc get pods -A
bin/apc helm upgrade --install web examples/helm/web --wait
bin/apc port-forward service/web-apc-web 18081:80
```

The K3s version and multi-platform OCI image digest are pinned in code. The
node runs ARM64 natively and uses VXLAN because Apple container 1.0's guest
kernel does not provide a WireGuard interface. Kubernetes state lives on an
APC-labelled Apple volume; `apc cluster start` recreates the lightweight VM
envelope, reattaches that volume and retains a stable Kubernetes `InternalIP`.

## Local development smoke test

Run the control plane:

```bash
bin/apc-server --database .apc/dev.db
```

Run one or more fake agents (no `apple/container` installation required):

```bash
bin/apc-agent --fake-runtime --node-id macbook --label apc.dev/architecture=arm64
bin/apc-agent --fake-runtime --node-id mac-mini --label apc.dev/architecture=arm64
```

Apply and inspect:

```bash
bin/apc apply -f examples/deployment.yaml
bin/apc get deployments
bin/apc get pods -o wide
bin/apc rollout status deployment/web --timeout=30s
```

The example requests two replicas with static host port 18080. Since host ports
are exclusive per node, the scheduler intentionally places one replica on each
of two Macs. With one node, the second Pod remains `Pending` and explains why
in `apc describe deployment web`.

## Hardware validation

The end-to-end path has been exercised with `apple/container` 1.0.0 on an
Apple M3 Pro MacBook and an Apple M2 Mac mini. The test covered native ARM64 VM
startup, one replica per node, HTTP readiness/liveness, a rolling update,
scale-up/down without replacing existing Pods, control-plane restart and agent
re-adoption, and deletion of both VMs. No host-specific addresses or
credentials are required by the repository.

The two-Mac K3s cluster also passed the deep doctor with public egress skipped:
16 passes, two intentional egress warnings and zero failures. Public HTTPS from
the MacBook Apple VM remains a known failure; the Mac mini VM passes that gate.
The local HA lab has live-proven a Helm deployment with three Ready replicas,
one on each of its three VMs. That remains one physical fault domain.

The manual [two-Mac hardware workflow](.github/workflows/hardware-acceptance.yml)
runs only from the default branch and performs read-only/status checks unless
the operator separately enables mutation and the deep doctor. It never checks
out pull-request code on the self-hosted Macs and uploads redacted diagnostics.
The worker gate does not require the control-plane-only `kubectl` and Helm
clients.
The dedicated MacBook and Mac mini runner labels are defined, but the runners
have not been registered with GitHub, so this workflow has not yet run there.

Apple container 1.0 does not bind its dynamic primary IPv4 allocation to the
MAC requested by `container run`. APC therefore detects collisions with member
stable addresses before K3s or restore mutation, removes only the exact
APC-owned conflicting envelope and retries under fixed bounds. Exact envelopes
from the immediately preceding launch format are migrated one member at a time;
foreign or otherwise mismatched envelopes are refused.

## Two-Mac LAN setup

On the machine running the control plane, bind addresses already default to all
interfaces. For a first trusted-LAN test:

```bash
APC_TOKEN='replace-me' bin/apc-server \
  --http-address 0.0.0.0:8080 \
  --grpc-address 0.0.0.0:9090
```

On each Apple Silicon node:

```bash
container system start
bin/apc-agent \
  --server CONTROL_PLANE_LAN_IP:9090 \
  --node-id UNIQUE_NODE_NAME \
  --advertise-address NODE_LAN_IP \
  --label apc.dev/architecture=arm64
```

Point the CLI at the control plane:

```bash
APC_SERVER=http://CONTROL_PLANE_LAN_IP:8080 \
APC_TOKEN=replace-me \
bin/apc get nodes
```

Plaintext gRPC is only for the first trusted-LAN test. Use the TLS/mTLS flags
described in the architecture document for an ongoing deployment.

## Deployment data model

See [examples/deployment.yaml](examples/deployment.yaml). Its structure follows
the Kubernetes Deployment model while APC validates exactly one container per
Pod because `apple/container` maps each container to one micro-VM.

## License

MIT. Apple Pod Control is an independent project and is not affiliated with or
endorsed by Apple Inc. or the Kubernetes project.
