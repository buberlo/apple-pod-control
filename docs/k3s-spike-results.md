# K3s on Apple container 1.0: two-Mac spike results

Initial run: 2026-07-17. CLI/context follow-up and network recheck: 2026-07-18.

The spike ran a pinned, native ARM64 K3s server in an Apple container VM on a
MacBook and a K3s agent in a second Apple container VM on a Mac mini. APC v1
continued running in parallel. Host addresses, credentials and join tokens are
intentionally omitted.

## Acceptance matrix

| Gate | Result | Evidence |
|---|---|---|
| Required preflight on both Apple Silicon Macs | Pass | Darwin/arm64, Apple container 1.0, service, capabilities and required ports |
| K3s server, SQLite, scheduler, containerd and kubelet | Pass | Server node reached Kubernetes `Ready` |
| Native `kubectl` and Helm | Pass | Host clients reached the generated kubeconfig; chart install completed |
| kubectl-compatible `apc` frontend | Pass | `get`, server-side dry-run `apply`, `logs`, `exec`, `auth can-i` and `cluster-info` ran against K3s |
| Deep `apc cluster doctor` | Pass as a diagnostic | Created one Pod per node, found the known network failures, and verified exact-resource cleanup |
| Second physical node | Pass | Mac mini agent joined and reached `Ready` |
| Scheduler placement across hosts | Pass | Two nginx replicas, one on each physical Mac |
| Bidirectional cross-host Pod HTTP | Pass before restart test | Pods in different Flannel subnets reached each other in both directions |
| CoreDNS and ClusterIP | Pass | Pod DNS lookup and Service request succeeded |
| `kubectl logs` and `kubectl exec` across hosts | Pass | API-to-kubelet path worked for both nodes |
| Server state persistence | Pass | Kubernetes and Helm state survived VM-envelope replacement on a named volume |
| Dynamic private VM address | Pass | Boot wrapper set K3s `--node-ip`; Kubernetes updated `InternalIP` |
| Full simultaneous restart/reconnect | Blocked by host runtime | MacBook Apple VMs lost all LAN/Internet routing while the host and Mac mini remained healthy |
| WireGuard-native Flannel | Unsupported | Apple container 1.0 guest kernel returned `operation not supported` |

## Runtime findings reflected in the implementation

- The K3s API uses port `16443` both inside the VM and on the Mac. Publishing
  host `16443` to guest `6443` broke K3s's remotedialer because it advertised
  the internal port to agents.
- Apple VM private addresses are treated as ephemeral. The K3s entrypoint
  discovers the current address on every boot instead of trusting a previous
  DHCP lease or deterministic MAC.
- `/var/lib/rancher/k3s` is stored on an APC-labelled 8 GiB Apple volume. Start
  recreates the lightweight VM envelope and reattaches the volume.
- K3s's built-in network-policy controller is disabled because it crashed after
  an `InternalIP` change. NetworkPolicy enforcement is not available yet.
- Flannel VXLAN works across the two Macs while VM-to-LAN routing is healthy.
  Its traffic is unencrypted and suitable only for a trusted lab network.
- The bidirectional cross-host Pod probe was repeated on 2026-07-18 and still
  timed out in both directions. Both Kubernetes nodes and local Pods remained
  `Ready`, confirming that readiness alone cannot detect this host-runtime
  routing failure; a deep cluster doctor remains necessary.

## Remaining gates for a usable alpha

1. Add `apc image prefetch/sync` for deterministic or air-gapped recovery when
   a node cannot pull OCI layers.
2. Repeat automated cold-start and simultaneous-restart tests after restoring
   the affected Mac's Apple-VM networking; run them in CI on two real Macs.
3. Add host firewall management and a secure host overlay before leaving a
   trusted LAN. Do not expose VXLAN directly to an untrusted network.
4. Select a restart-safe CNI/network-policy implementation and re-enable
   Kubernetes NetworkPolicy semantics.
5. Add cluster deletion, upgrades, backup/restore and three-server embedded-etcd
   HA. A two-node server/agent cluster is not control-plane HA.

The kubectl-compatible APC frontend is now implemented. Additional Kubernetes
verbs inherit their behavior from the installed native `kubectl`; APC does not
translate or reimplement those APIs.

## Deep-doctor result on 2026-07-18

The first live `apc cluster doctor lan-spike` run produced 12 passes and six
failures. Host-specific addresses are omitted:

- passed: control plane, published API, both Node conditions, one Ready probe
  Pod per Mac, kubelet exec on both nodes, and Mac mini DNS/HTTPS egress;
- failed: MacBook Pod DNS and HTTPS egress;
- failed: direct Pod HTTP in both MacBook-to-mini and mini-to-MacBook
  directions, which is the end-to-end VXLAN gate;
- failed: ClusterIP access from both nodes because the selected endpoint was
  across the broken route, with DNS also unavailable on the MacBook Pod.

This is the desired diagnostic behavior: the cluster remains superficially
`Ready`, but APC now returns a nonzero exit code and precise per-path
remediation instead of treating readiness as proof of network health.

The initial implementation requested asynchronous namespace deletion. That
namespace remained `Terminating` because Kubernetes discovery of the remote
Metrics API was stale across the broken route. The doctor now uses uniquely
named Pods and a Service in `default`, records their exact names, and deletes
those resources directly with zero grace instead of depending on namespace
finalization.

A final run with deterministic Service endpoint selection, exact-resource
cleanup and public egress intentionally skipped reported 11 passes, two warnings
and five network failures. No `apc-doctor-*` Pod or Service remained afterward.
