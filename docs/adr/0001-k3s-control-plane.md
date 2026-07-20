# ADR 0001: Use K3s as APC's Kubernetes control plane

- Status: Accepted and implemented
- Decision date: 2026-07-17
- Amended: 2026-07-20

## Context

The first APC prototype implemented a Kubernetes-shaped API, its own workload
store, scheduler, controller and host agent. Extending that approach far enough
to support Helm, Services, Secrets, Jobs, admission, operators and the wider
Kubernetes ecosystem would require maintaining a second, incomplete
Kubernetes-compatible control plane.

The valuable project-specific work is instead at the Apple host boundary:
creating native ARM64 node VMs with `apple/container`, retaining their data,
moving images into nested containerd, diagnosing VM/LAN paths and safely
supervising, backing up and recovering clusters.

## Decision

K3s is APC's only Kubernetes control plane. K3s owns:

- the Kubernetes API and authentication;
- desired state, controllers and reconciliation;
- scheduling, Pods, Deployments, Services, Secrets and the Kubernetes object
  model;
- the single-server datastore or embedded-etcd quorum;
- kubelet, containerd and in-node CNI behavior.

APC owns:

- Apple node-VM and persistent-volume lifecycle;
- protected kubeconfig generation and cluster selection;
- single-server, agent and local three-server HA operations;
- host-mediated ARM64 image transport;
- deep outer-VM and cross-node diagnostics;
- macOS PF/overlay integration and launchd supervision;
- K3s backup, snapshot, restore and recovery orchestration.

The former custom control-plane and agent implementation is retired, including
its API, database, scheduler, controller, protocol definitions, binaries and
CLI compatibility mode. It remains visible only in Git history; it is not a
supported product path. The repository builds and installs one binary, `apc`.

## Command and API boundary

Cluster/node/host commands are implemented by APC. Kubernetes workload commands
such as `apc get`, `apc apply`, `apc logs` and `apc exec` invoke native kubectl
with an APC-managed kubeconfig and inherited standard streams. `apc helm`
similarly invokes native Helm. Direct kubectl and Helm remain first-class.

This preserves upstream watches, server-side apply, interactive terminals,
plugins, exit behavior, object schemas and future flags. APC does not build a
second Kubernetes command parser and does not persist workload objects.

`apc kubectl CLUSTER -- ...` uses K3s's bundled client as a bootstrap
convenience when native kubectl is not installed; it is not the normal workload
interface.

## Runtime choices

The implementation pins:

- K3s `v1.36.2+k3s1`;
- the official K3s multi-platform OCI image by immutable digest;
- native Linux/ARM64 without Rosetta;
- four virtual CPUs and 4 GiB RAM for a default server node;
- Flannel VXLAN because the Apple container 1.0 guest kernel does not support
  WireGuard-native Flannel;
- API port `16443` both inside and outside a server VM;
- APC-labelled Apple volumes for `/var/lib/rancher/k3s`.

The API defaults to loopback. Cross-host use requires explicit host identities,
peer-restricted PF rules and either a trusted LAN or authenticated host overlay.
VXLAN is not encrypted by PF.

## Pod and VM model

One APC node maps to one Apple ARM64 VM containing K3s, containerd and kubelet.
Kubernetes schedules multiple ordinary Pods inside that node VM. A Kubernetes
Pod does not map to an individual Apple VM. Micro-VM-per-Pod would be a separate
Virtual Kubelet/runtime-provider design and is outside this decision.

## Health and image consequences

Kubernetes Node `Ready` does not cover the outer Apple VM's NAT or routed
cross-host path. APC therefore retains an end-to-end doctor that creates
short-lived node-pinned Pods and verifies DNS, optional egress, ClusterIP,
kubelet exec and every directed cross-node HTTP path. HA diagnostics also
validate local embedded-etcd health and exact peer topology.

Apple VM registry egress can fail independently of the macOS host. APC can
pull ARM64 images through the host, export a private OCI archive and import the
same reference into nested K3s containerd locally or over key-authenticated
SSH. This is an image-availability path, not a general NAT repair.

## Persistence and HA consequences

Node VM envelopes are disposable while named Apple volumes retain K3s state.
The local HA mode runs three K3s/embedded-etcd server VMs, but all three share
one Mac and therefore one physical failure domain.

Same-Mac empty-host recovery is live-proven. Replacement-Mac recovery is not:
the package, independent manifest digest and seed etcd reset passed, but peers
could not route to the stored stable secondary address through the target
host's newer `apple/container` vmnet. APC failed closed. Portable member
addressing is required before off-host recovery can become supported.

## Security consequences

The official K3s image needs broad Linux capabilities inside its dedicated
Apple VM; those do not grant macOS host privileges. APC does not mount the
user's home directory into nodes.

Kubeconfigs, join tokens and HA snapshot tokens are credentials and remain
private files. Snapshot trust additionally requires the manifest digest to be
retained separately from the package. Production use still requires an
authenticated encrypted host network, Kubernetes secret encryption and tested
off-host escrow/rotation.

## Consequences

- Users get native Kubernetes and Helm semantics rather than an emulation.
- Kubernetes ecosystem compatibility advances with the pinned K3s version.
- APC code stays focused on Apple-specific lifecycle, safety and diagnostics.
- There is intentionally no migration promise for objects from the retired
  prototype; workloads are expressed as standard Kubernetes manifests or Helm
  charts.
- Two physical Macs provide one server plus one worker, not control-plane HA.
- Host-level control-plane HA requires three independent physical failure
  domains and portable, authenticated peer networking.
