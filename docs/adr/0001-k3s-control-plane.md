# ADR 0001: Use K3s as APC v2's Kubernetes control plane

- Status: Accepted for the v2 spike
- Date: 2026-07-17

## Context

APC v1 implements a small Kubernetes-shaped REST API, SQLite desired-state
storage, a scheduler and a reconciler. Recreating more Kubernetes APIs would
make Helm, operators, Services, Secrets, Jobs and the wider ecosystem depend on
an increasingly complex compatibility layer.

APC v2 instead needs to provide real Kubernetes semantics while retaining the
Apple-specific value: lifecycle and networking for lightweight ARM64 Linux VMs
created by `apple/container`.

## Decision

APC v2 uses K3s as the Kubernetes distribution. APC owns the Apple host and
node-VM lifecycle; Kubernetes owns workload desired state, scheduling and
reconciliation.

The first node envelope is an `apple/container run` VM containing the official
K3s image. `container machine` remains a future option, but Apple container 1.0
does not expose host-port publishing on `container machine create`. The
Kubernetes API and cross-host CNI therefore cannot currently be made reachable
with that interface alone.

The spike pins:

- K3s `v1.36.2+k3s1`
- OCI index digest
  `sha256:6a47cea22c4b834d4ba72c89d291696b79ebe406251f90b446e4dff03513dd87`
- Linux/ARM64, without Rosetta
- four virtual CPUs and 4 GiB RAM by default
- Flannel VXLAN
- API port `16443` inside and outside the server VM while APC v1 remains live
- labelled Apple volumes for persistent K3s state

The `apc` CLI becomes the cluster-lifecycle interface. Native `kubectl` and
Helm operate on the generated kubeconfig. The temporary `apc kubectl` command
uses K3s's bundled client as a bootstrap convenience, not as a replacement for
native Kubernetes tooling.

## Verified on Apple container 1.0

The following was exercised on an Apple Silicon MacBook with
`apple/container` 1.0.0:

1. The K3s API server, SQLite datastore, controller manager, scheduler,
   containerd and kubelet start in the Apple VM.
2. The node becomes Kubernetes `Ready` on the native ARM64 kernel.
3. Flannel VXLAN creates Pod networking successfully.
4. CoreDNS resolves a ClusterIP Service from another Pod.
5. A nested nginx Pod serves HTTP.
6. Host `kubectl` 1.36.2 reaches the published API.
7. Helm installs and waits for the example `apc-web` release.
8. `kubectl port-forward` exposes that Service to the Mac host.
9. Re-running `apc cluster create` adopts the existing APC-labelled node
   instead of recreating it.
10. The generated kubeconfig is stored with mode `0600`.
11. A second physical Mac joins as an agent and reaches `Ready`.
12. Helm places one nginx replica on each Mac, and Pod-to-Pod HTTP, DNS,
    ClusterIP, logs and exec work across the two physical hosts.

## Rejected networking option

`wireguard-native` was tested first because it is attractive for cross-host
encryption. K3s reached control-plane startup but Flannel terminated with:

```text
could not create wireguard interface: operation not supported
```

The Apple container 1.0 guest kernel therefore cannot be treated as having a
WireGuard interface. APC's preflight and bootstrap must not select this backend
for this runtime version.

## Two-host networking result

The MacBook plus Mac mini test uses the following envelope:

- publish TCP 16443 on the server, matching K3s's internal listen port;
- publish UDP 8472 on each physical Mac;
- publish TCP 10250 on each physical Mac;
- use the physical Mac's LAN address as K3s `--node-external-ip`;
- enable K3s `--flannel-external-ip`.

The first full run passed bidirectional Pod-to-Pod HTTP, DNS, ClusterIP, logs,
exec and Helm scheduling across both hosts. Restart testing then exposed two
Apple container 1.0 behaviors that APC has to absorb:

1. the private VM address can change even when a deterministic MAC is used;
2. a port-published VM can lose outbound routing after restart.

APC therefore resolves the private address inside every boot and passes it to
K3s as `--node-ip`. K3s data is stored on an Apple named volume, while `start`
recreates the disposable VM envelope instead of invoking `container start`.
During the final test run, outbound routing from every Apple VM on the MacBook
remained unavailable even after restarting the Apple container service, while
the macOS host and the Mac mini VMs remained healthy. That host-runtime state
currently prevents a repeatable post-restart cross-host pass and is not caused
by K3s or Flannel. The detailed evidence is in
[the spike report](../k3s-spike-results.md).

K3s's built-in network-policy controller also failed to reconcile a changed
private VM address. It is disabled for the spike; production NetworkPolicy
support requires a CNI/controller that passes the same restart test.

## Security consequences

The official K3s container requires broad Linux capabilities inside its own
dedicated Apple VM. This is a larger guest privilege set than APC v1 workload
VMs, but it does not grant macOS host privileges. APC still limits mounts and
does not expose the macOS home directory to the node.

The API defaults to loopback. LAN mode requires an explicit advertise address.
Kubeconfig and join tokens are credentials and must remain mode `0600`, must
never enter Git, and must not be printed in logs. UDP 8472 is unencrypted and
must never be exposed to an untrusted network. Host firewall automation is a
required follow-up before treating LAN mode as more than a lab feature.

## Consequences

- Helm and standard Kubernetes APIs work without translation.
- APC's custom scheduler, REST workload API and desired-state store become
  legacy v1 components rather than foundations for v2.
- A K3s node is initially one Apple VM containing several Kubernetes Pods;
  micro-VM-per-Pod is a separate, later Virtual Kubelet provider.
- Two physical Macs do not provide control-plane HA. Embedded-etcd HA requires
  at least three server nodes.
