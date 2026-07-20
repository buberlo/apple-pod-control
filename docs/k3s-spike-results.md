# K3s on Apple container 1.0: two-Mac spike results

Initial run: 2026-07-17. Lifecycle and recovery follow-ups: 2026-07-19 and
2026-07-20.

The spike ran a pinned, native ARM64 K3s server in an Apple container VM on a
MacBook and a K3s agent in a second Apple container VM on a Mac mini. APC v1
continued running in parallel. Host addresses, credentials and join tokens are
intentionally omitted.

## Acceptance matrix

| Gate | Result | Evidence |
|---|---|---|
| Required preflight on both Apple Silicon Macs | Pass | Darwin/arm64, Apple container 1.0, service, capabilities and required ports |
| K3s server, SQLite, scheduler, containerd and kubelet | Pass | Server node reached Kubernetes `Ready` |
| Native `kubectl`, native Helm and `apc helm` | Pass | Host clients reached the protected generated kubeconfig; chart installs completed |
| kubectl-compatible `apc` frontend | Pass | `get`, server-side dry-run `apply`, `logs`, `exec`, `auth can-i` and `cluster-info` ran against K3s |
| Deep `apc cluster doctor` | Pass as a diagnostic | The final two-Mac skip-egress gate reported 16 pass, two intentional warnings and zero failures; exact-resource cleanup passed |
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
| Peer-restricted macOS PF rules | Partial live pass | Install and privileged status passed on both Macs for the root-owned helpers, LaunchDaemons, references and exact-peer anchors; reboot, unlisted-peer rejection and overlay repetition remain open |
| Full simultaneous restart/reconnect | Pass | Both envelopes were stopped together; LaunchAgents recreated them, retained stable InternalIPs and returned both Nodes to Ready in about 15 seconds |
| WireGuard-native Flannel | Unsupported | Apple container 1.0 guest kernel returned `operation not supported` |
| Local three-server embedded-etcd HA | Pass at VM level | Three Ready K3s/etcd server VMs formed quorum on one Mac and tolerated one member being offline; this is one physical fault domain |
| Stable HA client API | Pass | The HA LaunchAgent serves a loopback TLS-pass-through endpoint on the default port 17442 with authenticated direct-member fallback |
| HA Helm topology spread | Pass | A live Helm release placed three Ready replicas, one on each of the three HA VMs |
| HA image prefetch | Pass in implementation/tests | Exact topology is resolved before mutation and one ARM64 archive is imported and verified on all three running members |
| HA deep doctor | Live pass | Final gate: 34 passes, three expected public-egress-skip warnings and zero failures |
| HA in-place rollback | Live pass | Fresh restore returned 3/3 Ready, reverted changed data, removed a post-snapshot-only object and preserved the Helm release/three Pods/PDB |
| HA member lifecycle and serialization | Live pass | One intentional stop survived more than two supervisor ticks; the two-voter quorum accepted a write and member start restored 3/3 |
| Manual two-Mac hardware workflow | Pending runners | Default-branch-only, read-only-by-default workflow exists; dedicated self-hosted runners are not registered |
| Authenticated host overlay | Pending user action | Tailscale is absent on both Macs and requires installation, macOS approval and interactive user authentication |

## Runtime findings reflected in the implementation

- The K3s API uses port `16443` both inside the VM and on the Mac. Publishing
  host `16443` to guest `6443` broke K3s's remotedialer because it advertised
  the internal port to agents.
- Apple VM private addresses are treated as ephemeral. The K3s entrypoint
  discovers the current address on every boot instead of trusting a previous
  DHCP lease or deterministic MAC.
- Apple container 1.0 assigns the dynamic primary IPv4 independently of a MAC
  requested on `container run`; it cannot reserve a fixed primary address. HA
  startup and restore therefore guard against allocation of another member's
  stable address before K3s or etcd mutation.
- A colliding exact APC-owned envelope is stopped, deleted and retried only
  under fixed attempt and time bounds. The preceding known envelope format is
  migrated one Ready member at a time. Foreign or identity-mismatched
  containers are never adopted or removed by this path.
- apple/container automatically attaches its default network when a generic
  recovery helper omits `--network`. APC therefore requests
  `default,mtu=1280` explicitly and accepts such a helper only when it has that
  exact single network identity; the corrected live snapshot/restore passed.
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

1. Register dedicated `apc-macbook` and `apc-macmini` self-hosted runners and
   dispatch the manual hardware workflow from the default branch. It must never
   run untrusted pull-request code on those hosts.
2. Reboot both Macs and verify that the root LaunchDaemons reconstruct the exact
   PF anchors and references. Then prove that a listed peer succeeds and an
   unlisted peer is rejected.
3. Have the user install and authenticate Tailscale on both Macs, approve the
   macOS network configuration, run APC's identity/peer/route preflight and
   repeat the network gates over the authenticated overlay.
4. Move control-plane members to three independent physical failure domains
   before making host-level HA claims. Three etcd VMs on one Mac prove quorum
   behavior, not physical-host availability.
5. Resolve or explicitly accept the MacBook Apple VM's public HTTPS egress
   failure. Host-mediated image distribution is a mitigation, not a NAT fix.
6. Design and prove replacement-host and token-loss recovery. The current HA
   restore requires the original saved configuration, exact network, all three
   member volumes and the current token file matching the packaged token.

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

## HA, PF and hardware-workflow follow-up on 2026-07-20

The local `ha-lab` consists of three native ARM64 K3s server VMs with embedded
etcd, three named member volumes and one APC-owned private network. All three
Node/API pairs reached Ready. A Helm deployment with three replicas and strict
topology spread placed one Ready nginx Pod on each VM; its PodDisruptionBudget
reported one allowed disruption. This is live evidence for Kubernetes
scheduling across three Apple VMs, but all three still share one Mac.

The HA supervisor now reconciles the member set and serves a deterministic
loopback TLS-pass-through endpoint. With the default member API ports its
address is `https://127.0.0.1:17442`. APC authenticates the proxy with the
existing Kubernetes client credentials before writing it into kubeconfig. If
the proxy is unavailable, kubeconfig preparation selects a reachable Ready
member directly. Native kubectl, `apc helm` and direct Helm therefore share the
same protected endpoint-selection path.

Dedicated HA operations now include:

- preflighted image import into all three running member containerd stores;
- per-member runtime, API, Node and quorum checks in the deep doctor;
- quorum-safe member stop, start and restart with best-effort recovery;
- native K3s etcd snapshot export with the matching server token;
- checksum-, identity- and topology-validated three-member restore;
- a private cross-process lock around snapshot, restore and member lifecycle.

The final live sequence first stopped one member intentionally. It remained
stopped for more than two supervisor ticks, while the other two embedded-etcd
voters accepted a ConfigMap write and returned it through the Kubernetes API.
Starting the member again restored 3/3 Ready.

A fresh snapshot was then taken, after which one ConfigMap was changed and a
second object was created. Restore returned the first object to its
before-snapshot value and removed the post-snapshot-only object. All three
member runtime/API/Node pairs returned Ready; the existing Helm nginx release
still had three topology-spread Ready Pods and its PodDisruptionBudget. The
protected recovery journal recorded `completed` and recovery success. The HA
LaunchAgent's TLS-pass-through proxy remained available at
`https://127.0.0.1:17442` after recovery. The published package was mode `0700`,
with a `0400` manifest and `0600` snapshot/token artifacts.

The final HA deep doctor reported 34 passes, three expected warnings for
intentionally skipped public HTTPS egress and zero failures.

The HA restore is intentionally limited to rollback on the same saved cluster.
It requires the protected saved configuration, current matching token, exact
network and all three original member volumes. It is not a package that can
bootstrap a replacement Mac, and it is not recovery from loss of all volumes.
The packaged token must be encrypted and protected as a credential; without
the matching current on-host token file, this restore refuses to proceed.
Replacement-host and token-loss DR therefore remain open.

Persistent PF install and privileged status now pass on both physical Macs over
the trusted LAN. The running-state verification covered the root-owned helper,
LaunchDaemon, PF reference and complete anchor. Neither a host reboot, a
connection from an unlisted peer nor the authenticated-overlay repetition has
been tested, so those remain open gates.

The manual two-Mac workflow now contains default-branch and no-checkout guards,
read-only/status checks by default, explicit opt-in for the mutating deep
doctor, and redacted artifacts. The matching local read-only harness passed;
the GitHub workflow has not run because neither dedicated self-hosted runner is
registered. Tailscale is also absent on both hosts and cannot be completed
without user installation, macOS approval and interactive authentication.

The current two-Mac network baseline remains 16 passes, two intentionally
skipped public-egress warnings and zero failures with `--skip-egress`. Without
that flag, public HTTPS from the MacBook Apple VM fails while the Mac mini VM
passes; cross-host VXLAN, DNS and ClusterIP routing remain healthy.
