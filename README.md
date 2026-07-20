# Apple Pod Control

Apple Pod Control (APC) runs **real Kubernetes with K3s** in lightweight,
native ARM64 virtual machines created by
[`apple/container` 1.0](https://github.com/apple/container). APC is the
Apple-host and cluster-lifecycle layer: it creates node VMs, preserves their
data, manages kubeconfigs, moves images, diagnoses the outer VM network and
provides safe backup, recovery and macOS supervision.

K3s—not APC—owns the Kubernetes API, desired state, controllers, scheduler,
Pods, Deployments, Services, Secrets, Jobs, storage and embedded etcd. Workload
commands are passed to native `kubectl`; `apc helm` passes through to native
Helm. This keeps the Kubernetes behavior and ecosystem people already know.

> Project status: development and trusted-LAN lab. The implementation has
> substantial live Apple-Silicon validation, but it is not production-ready.
> Read [current limitations](#current-limitations) before relying on it.

## Architecture

```text
 apc apply/get/logs/exec ─┐
 native kubectl ──────────┼── protected kubeconfig ──> K3s API
 apc helm / native Helm ──┘                              │
                                                        │ Kubernetes owns
                         ┌──────────────────────────────┤ workloads and state
                         v                              v
                Apple VM: K3s server           Apple VM: K3s agent
                persistent Apple volume        persistent Apple volume
                         └──── Flannel VXLAN / kubelet ──┘
                   Mac A                              Mac B

 APC owns: apple/container VM and volume lifecycle, cluster contexts,
 image transport, diagnostics, PF/overlay integration, supervision and recovery.
```

A Kubernetes Pod runs inside K3s on a node VM. APC does **not** create one
Apple VM per Pod. K3s may run many Pods in each native ARM64 node VM.

Supported development topologies:

- one K3s server VM on one Apple Silicon Mac;
- one server Mac plus one K3s agent Mac on a trusted LAN;
- three K3s/embedded-etcd server VMs on one Mac for quorum and recovery labs.

The three-VM topology tolerates one **VM** failure, but all members still share
one physical Mac and therefore one physical failure domain.

## Kubernetes-native CLI

There is one binary: `apc`. Cluster lifecycle commands are APC-specific;
workload commands retain native kubectl semantics and streaming behavior.

```bash
apc cluster create dev
apc config use-cluster dev

apc get nodes -o wide
apc get pods -A
apc apply --server-side -f examples/kubernetes/web.yaml
apc rollout status deployment/web
apc logs -f deployment/web
apc exec -it deployment/web -- /bin/sh
apc port-forward service/web 18080:80

apc helm upgrade --install web examples/helm/web --wait
```

`--cluster NAME`, then `APC_CLUSTER`, then the persisted current cluster decide
which protected kubeconfig is used. If no cluster is selected, workload
commands fail with guidance instead of contacting an implicit API endpoint.

See the complete [CLI contract](docs/cli.md).

## Prerequisites

- Apple Silicon Mac;
- macOS 26;
- [`apple/container` 1.0](https://github.com/apple/container/releases/tag/1.0.0);
- Go 1.25 or newer to build APC;
- native `kubectl` and Helm for the full CLI experience.

Start Apple's service and install the Kubernetes clients:

```bash
container system start
brew install kubernetes-cli helm
```

## Build and install

```bash
make test
make build
make install
```

The build writes `bin/apc`. By default, installation copies it to
`~/.local/bin/apc`; use `PREFIX=/usr/local make install` for a system-wide
installation. Ensure the chosen directory is on `PATH`, then verify:

```bash
command -v apc
apc version
apc doctor
```

## Quickstart: nginx from a Kubernetes manifest

Create a local, single-server cluster. APC pins the K3s image by OCI digest,
runs it as Linux/ARM64 and enables K3s NetworkPolicy enforcement by default.

```bash
apc cluster create dev
apc cluster status dev
apc apply -f examples/kubernetes/web.yaml
apc rollout status deployment/web --timeout=2m
apc get deployment,pod,service -o wide
```

The example is ordinary Kubernetes YAML: a Deployment, ClusterIP Service,
NetworkPolicy and PodDisruptionBudget using the multi-architecture nginx image.
Reach it without permanently publishing a workload port:

```bash
apc port-forward service/web 18080:80
curl http://127.0.0.1:18080/
```

Delete only the workload when finished:

```bash
apc delete -f examples/kubernetes/web.yaml
```

## Quickstart: nginx with Helm

The included chart is a standard Helm chart. APC only supplies and validates
the selected kubeconfig before invoking Helm.

```bash
helm lint examples/helm/web
apc helm upgrade --install web examples/helm/web \
  --namespace web --create-namespace --wait --timeout 2m
apc get deployment,pod,service -n web -o wide
apc helm uninstall web -n web
```

Direct Helm is equally supported:

```bash
export KUBECONFIG="$(apc kubeconfig path dev)"
helm list --all-namespaces
```

## Cluster and node operations

```bash
apc cluster status dev
apc cluster doctor dev --skip-egress
apc cluster stop dev
apc cluster start dev

apc cluster backup dev --output "$HOME/Backups/dev.apcbackup"
apc cluster restore dev --from "$HOME/Backups/dev.apcbackup" --yes
apc cluster upgrade dev --image docker.io/rancher/k3s@sha256:DIGEST --yes

apc cluster write-join-token dev --output PRIVATE_TOKEN_FILE
apc node join dev --server-url https://SERVER_ADDRESS:16443 \
  --token-file PRIVATE_TOKEN_FILE --advertise-address AGENT_ADDRESS
```

APC checks exact ownership labels before destructive operations. Named Apple
volumes retain `/var/lib/rancher/k3s` while disposable VM envelopes are
replaced. Backups are checksum-validated, upgrades require immutable image
digests, and destructive operations require `--yes`.

`apc cluster doctor` goes beyond Kubernetes Node `Ready`: it creates
short-lived, node-pinned probe Pods and verifies runtime/API state, kubelet
exec, DNS, optional public HTTPS, ClusterIP and every directed cross-node HTTP
path. Its exact probe resources are cleaned automatically.

## Images on Apple Silicon

APC selects native `linux/arm64` images and can import them from the macOS host
store into nested K3s containerd:

```bash
apc image prefetch docker.io/library/busybox:1.36.1
apc image sync docker.io/library/busybox:1.36.1 --peer ACCOUNT@PEER_HOST
```

For a protected three-member HA cluster, prefetch validates and imports the
same exact image into all three member stores. This avoids x86 translation and
can make workloads independent of guest-VM registry egress.

## Local three-VM HA lab

```bash
apc cluster ha create ha-lab
apc cluster ha status ha-lab
apc --cluster ha-lab get nodes -o wide
apc helm --cluster ha-lab upgrade --install web examples/helm/web \
  --set replicaCount=3 --set podDisruptionBudget.enabled=true --wait
```

The HA mode uses three K3s servers with embedded etcd, distinct persistent
volumes, an APC-owned Apple network and a supervised loopback TLS-pass-through
API endpoint. Member operations, snapshots, restore and recovery are serialized
and quorum-gated. See the [HA operations guide](docs/k3s-ha.md).

Same-Mac empty-host recovery has passed, including a negative digest test that
caused no mutation. A replacement-Mac drill validated the package, independent
manifest digest and seed etcd reset, but the other members could not reach the
stored stable address through the target host's newer `apple/container` vmnet.
APC failed closed and the disposable drill resources were cleaned. Off-host
recovery is therefore **not supported yet**.

## macOS networking and supervision

For cross-host labs APC can render and install peer-restricted PF rules for the
K3s API, kubelet and Flannel VXLAN ports. VXLAN is not encrypted; use only a
trusted LAN or bind the cluster consistently to an authenticated host overlay.
APC can validate an existing Tailscale identity and route, but it never accepts
an overlay authentication key.

APC supports a per-user LaunchAgent and a hardened root-owned LaunchDaemon that
drops to a selected non-root account. The unattended service uses an exact
`launchctl asuser`/`sudo -n`/`env -i` chain, protected bounded logs and strict
status/ownership validation.

## Current limitations

- The project remains a development/trusted-LAN lab, not a production
  Kubernetes distribution.
- Three local etcd VMs are one physical failure domain; physical HA needs three
  independent Macs.
- Replacement-Mac/off-host HA recovery is blocked by non-portable stable-IP
  routing across tested `apple/container` vmnet versions.
- The saved two-Mac worker is currently stopped because DHCP changed the host
  addresses stored as the K3s server URL and advertise addresses. Reserve
  stable LAN addresses or use a stable authenticated overlay before rejoining.
- Public HTTPS from Apple VMs is not reliable on every tested host. Host-side
  image prefetch is a mitigation, not a general NAT fix.
- PF persistence passed a headless reboot on the Mac mini; the corresponding
  MacBook reboot, an unlisted-peer rejection and uninstall rollback remain
  open.
- Hardware runners currently use administrator accounts as a lab deviation;
  dedicated unprivileged accounts are still required.
- APC's unattended service has deterministic security tests, but its repeated
  zero-login live reboot gate remains open.

The authoritative gate list is [release readiness](docs/release-readiness.md),
and dated evidence is in [validation results](docs/validation-results.md).

## Documentation

- [Architecture and trust boundaries](docs/architecture.md)
- [Quickstart and two-Mac setup](docs/quickstart.md)
- [CLI contract](docs/cli.md)
- [Local K3s HA operations](docs/k3s-ha.md)
- [Live validation results](docs/validation-results.md)
- [Hardware runner trust model](docs/hardware-runners.md)
- [Release-readiness gates](docs/release-readiness.md)
- [ADR 0001: K3s is APC's control plane](docs/adr/0001-k3s-control-plane.md)

## License

MIT. Apple Pod Control is an independent project and is not affiliated with or
endorsed by Apple Inc. or the Kubernetes project.
