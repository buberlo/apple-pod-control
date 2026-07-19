# K3s on Apple container 1.0: two-Mac spike results

Initial run: 2026-07-17. Lifecycle and recovery follow-up: 2026-07-19.

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
| Host-mediated image sync | Pass | A new ARM64 image was streamed into both K3s stores and ran with `imagePullPolicy: Never` on both Macs |
| Second physical node | Pass | Mac mini agent joined and reached `Ready` |
| Scheduler placement across hosts | Pass | Two nginx replicas, one on each physical Mac |
| Bidirectional cross-host Pod HTTP | Pass before restart test | Pods in different Flannel subnets reached each other in both directions |
| CoreDNS and ClusterIP | Pass | Pod DNS lookup and Service request succeeded |
| `kubectl logs` and `kubectl exec` across hosts | Pass | API-to-kubelet path worked for both nodes |
| Server state persistence | Pass | Kubernetes and Helm state survived VM-envelope replacement on a named volume |
| Delete/keep-data recovery | Pass | Label-checked deletion retained the selected volume and `start` recreated the missing envelope |
| Offline backup and restore | Pass | A ConfigMap changed after backup returned to its original value after checksum-validated restore |
| Digest-only upgrade and rollback base | Pass | Server recreated from an alternate ARM64 manifest digest, retained data and reached Ready; pre-upgrade backup recorded |
| launchd supervision | Pass | Background LaunchAgents ran on both Macs and reconciled server/agent without failures |
| Stable node identity across private VM address changes | Pass | Replacement VMs bound the saved Kubernetes `InternalIP`; both Nodes remained Ready without an address collision |
| Kubernetes NetworkPolicy | Pass | Controller survived consecutive server/agent envelope replacements; cross-host default-deny and label-selected TCP allow both passed |
| Peer-restricted macOS PF rules | Pass in implementation/validation | `pfctl` accepted server and agent rules; root-owned reboot persistence and reversible PF references are implemented |
| Full simultaneous restart/reconnect | Pass | Both envelopes were stopped together; LaunchAgents recreated them, retained stable InternalIPs and returned both Nodes to Ready in about 15 seconds |
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
- APC retains each node's Kubernetes `InternalIP` as a secondary address when
  Apple's replacement VM receives a different primary address. K3s's built-in
  network-policy controller is enabled and restart-safe with that invariant.
- Flannel VXLAN works across the two Macs while VM-to-LAN routing is healthy.
  Its traffic is unencrypted and suitable only for a trusted lab network.
- The bidirectional cross-host Pod probe was repeated on 2026-07-18 and still
  timed out in both directions. Both Kubernetes nodes and local Pods remained
  `Ready`, confirming that readiness alone cannot detect this host-runtime
  routing failure; a deep cluster doctor remains necessary.

## Remaining gates for a usable alpha

1. Add the simultaneous supervisor recovery gate to dedicated two-Mac hardware
   CI; the manual real-hardware repetition now passes.
2. Perform the explicit administrator-authorized PF LaunchDaemon installation
   on both Macs. Rule rendering and parser validation are already complete.
3. Install and authenticate the selected host overlay on both Macs, then run
   APC's implemented identity/peer/route preflight and repeat the network gates;
   do not expose unencrypted VXLAN directly to an untrusted network.
4. Add three-server embedded-etcd HA. A two-node server/agent cluster is not
   control-plane HA and cannot prove a three-fault-domain design.

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

## Cold-start and image-sync result on 2026-07-19

After the MacBook's Apple container service stopped, APC recreated the server
from its persistent volume. DHCP had assigned the MacBook a new LAN address;
the hardened start path detected the change, updated K3s external IP and TLS
SAN, rewrote kubeconfig and preserved Kubernetes/Helm state.

The agent VM was also replaced. APC now persists its local K3s node password
inside the named data volume and saves non-secret join configuration, preventing
future VM replacement from losing node identity. The one stale pre-fix Node was
deleted and rejoined cleanly.

The next full doctor run passed both directed cross-node HTTP paths, DNS from
both nodes, ClusterIP from both nodes, API and kubelet checks. Public HTTPS
egress from the MacBook K3s VM remains the only failure; the Mac mini egress
passes.

`apc image sync` then pulled `busybox:1.36.1` through the healthy macOS host,
streamed a private ARM64 OCI archive into both nested K3s containerd stores and
verified the exact references. Node-pinned test Pods using `imagePullPolicy:
Never` reached `Ready` and executed as `aarch64` on both Macs. Test Pods and the
temporary host archive were removed.

Finally, `apc node stop/start` replaced the Mini agent from its saved
configuration and persistent identity. A post-restart doctor with public egress
intentionally skipped reported 16 passes, two warnings and zero failures,
including both directed VXLAN paths and deterministic ClusterIP routing.

The lifecycle follow-up then exercised an isolated cluster through full
create/delete-with-keep-data/start/delete. A second run wrote a Kubernetes
ConfigMap, made an offline volume backup, changed the ConfigMap and restored the
backup; the original value and Ready state returned. A third run changed the
server from the pinned multi-platform digest to its pinned ARM64 manifest
digest through `cluster upgrade`; Kubernetes data remained present and the
pre-upgrade rollback backup was valid. All temporary clusters, volumes and
backups were removed afterward.

Finally, stable `~/.local/bin/apc` builds were installed on both Macs. The
server and agent Background LaunchAgents are active in each user's headless-safe
launchd domain, their supervisor logs are empty, and a follow-up deep doctor
reported 16 passes, two intentionally skipped egress warnings and zero failures.

## Stable-IP, NetworkPolicy and simultaneous recovery follow-up

APC now records the first Kubernetes `InternalIP` for each server and agent.
When Apple assigns a replacement VM a different primary private address, APC
binds the recorded address as a secondary guest address and continues using it
for K3s `--node-ip`. Server and agent replacements retained distinct stable
addresses while their Apple primary addresses changed repeatedly.

With that invariant, K3s's built-in NetworkPolicy controller started and
completed full sync after every replacement. An isolated two-host test first
denied both MacBook clients from an nginx Pod on the Mac mini, then allowed only
the label-selected client on TCP 80 while the other remained denied.

For the final crash gate, both Apple VM envelopes were stopped in the same test
window. The two Background LaunchAgents created new server and agent envelopes,
both Nodes returned to `Ready` in about 15 seconds, and their Kubernetes
InternalIPs stayed unchanged. The post-recovery doctor reported 16 passes, two
intentionally skipped public-egress warnings and zero failures. It covered
CoreDNS, kubelet exec, both directed cross-host HTTP paths, ClusterIP routing
and exact cleanup. The cross-host NetworkPolicy deny/allow test then passed
again. Supervisor logs remained empty.

The example Helm release was upgraded in place to include NetworkPolicy,
rolling-update strategy, seccomp defaults and a PodDisruptionBudget. A live
same-namespace BusyBox client retrieved the nginx page through the ClusterIP.
