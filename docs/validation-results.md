# K3s on Apple container 1.0: validation results

Initial run: 2026-07-17. Lifecycle and recovery follow-ups: 2026-07-19 and
2026-07-20.

The initial two-host run used a pinned, native ARM64 K3s server in an Apple
container VM on a MacBook and a K3s agent in a second Apple container VM on a
Mac mini. Later runs added a local three-server embedded-etcd lab and same-host
and replacement-host recovery drills. Host addresses, account names,
credentials and join tokens are intentionally omitted.

## Acceptance matrix

| Gate | Result | Evidence |
|---|---|---|
| Required preflight on both Apple Silicon Macs | Pass | Darwin/arm64, Apple container 1.0, service, capabilities and required ports |
| K3s API, controllers, scheduler, datastore, containerd and kubelet | Pass | Server node reached Kubernetes `Ready` |
| Native `kubectl`, native Helm and `apc helm` | Pass | Host clients reached the protected generated kubeconfig; chart installs completed |
| kubectl-compatible `apc` frontend | Pass | `get`, server-side dry-run `apply`, `logs`, `exec`, `auth can-i` and `cluster-info` ran against K3s |
| Deep `apc cluster doctor` | Pass as a diagnostic | The final two-Mac skip-egress gate reported 16 pass, two intentional warnings and zero failures; exact-resource cleanup passed |
| Host-mediated image sync | Pass | A new ARM64 image was streamed into both K3s stores and ran with `imagePullPolicy: Never` on both Macs |
| Second physical node | Pass | Mac mini agent joined and reached `Ready` |
| Current saved two-Mac topology | Stopped; rejoin required | DHCP changed the stored server URL and host advertise addresses after the successful run; the worker is stopped pending stable LAN reservations or an authenticated overlay |
| Scheduler placement across hosts | Pass | Two nginx replicas, one on each physical Mac |
| Bidirectional cross-host Pod HTTP | Pass before restart test | Pods in different Flannel subnets reached each other in both directions |
| CoreDNS and ClusterIP | Pass | Pod DNS lookup and Service request succeeded |
| `kubectl logs` and `kubectl exec` across hosts | Pass | API-to-kubelet path worked for both nodes |
| Server state persistence | Pass | Kubernetes and Helm state survived VM-envelope replacement on a named volume |
| Delete/keep-data recovery | Pass | Label-checked deletion retained the selected volume and `start` recreated the missing envelope |
| Offline backup and restore | Pass | A ConfigMap changed after backup returned to its original value after checksum-validated restore |
| Digest-only upgrade and rollback base | Pass | Server recreated from an alternate ARM64 manifest digest, retained data and reached Ready; pre-upgrade backup recorded |
| launchd supervision | Partial live pass | Background LaunchAgents reconcile correctly in an active user launchd domain; a headless Mac mini reboot proved they are not auto-loaded before login |
| APC unattended system supervision | Implementation/test pass; live reboot pending | Root LaunchDaemon definition has exact non-root privilege drop, `/dev/null` launchd streams, bounded protected log, running/PID/exact-argv status and root-ancestor/allow-ACL refusal; no accepted two-Mac APC zero-login reboot repeat exists |
| Stable node identity across private VM address changes | Pass | Replacement VMs bound the saved Kubernetes `InternalIP`; both Nodes remained Ready without an address collision |
| Kubernetes NetworkPolicy | Pass | Controller survived consecutive server/agent envelope replacements; cross-host default-deny and label-selected TCP allow both passed |
| Peer-restricted macOS PF rules | Partial live pass | Install and privileged status passed on both Macs; a headless Mac mini reboot reconstructed its complete anchor and PF reference, while MacBook reboot, unlisted-peer rejection and overlay repetition remain open |
| Full simultaneous restart/reconnect | Pass | Both envelopes were stopped together; LaunchAgents recreated them, retained stable InternalIPs and returned both Nodes to Ready in about 15 seconds |
| WireGuard-native Flannel | Unsupported | Apple container 1.0 guest kernel returned `operation not supported` |
| Local three-server embedded-etcd HA | Pass at VM level | Three Ready K3s/etcd server VMs formed quorum on one Mac and tolerated one member being offline; this is one physical fault domain |
| Stable HA client API | Pass | The HA LaunchAgent serves a loopback TLS-pass-through endpoint on the default port 17442 with authenticated direct-member fallback |
| HA Helm topology spread | Pass | A live Helm release placed three Ready replicas, one on each of the three HA VMs |
| HA image prefetch | Live pass | One host-pulled ARM64 archive was imported and verified on all three exact running members |
| HA deep doctor | Pass as a diagnostic | The skip-egress gate produced 34 passes and three expected warnings; the full gate produced 34 passes, three public-HTTPS failures and exact-resource cleanup |
| HA in-place rollback | Live pass | Fresh restore returned 3/3 Ready, reverted changed data, removed a post-snapshot-only object and preserved the Helm release/three Pods/PDB |
| HA empty-host recovery | Live pass on the same physical Mac | A wrong external manifest digest caused zero mutations; the correct digest reconstructed the exact configuration/token/network/three-volume topology at 3/3 and preserved the Helm release, ConfigMap, three Pods and PDB |
| HA replacement-Mac recovery | Failed safely; unsupported | Package and independent digest validation plus seed-member etcd reset passed, but peers could not route to the stored stable secondary address through the target host's newer apple/container vmnet; APC failed closed and the disposable target resources were cleaned after diagnosis |
| HA member lifecycle and serialization | Live pass | One intentional stop survived more than two supervisor ticks; the two-voter quorum accepted a write and member start restored 3/3 |
| Manual two-Mac hardware workflow | Partial live pass | Root-owned runner system LaunchDaemons are installed on both Macs and GitHub reported both online and idle; the Mac mini passed a headless reboot, while the MacBook reboot, dedicated-account migration and guarded workflow run remain open |
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
  startup, restore and recovery therefore guard against allocation of another
  member's stable address before K3s or etcd mutation.
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

1. Move both runner services from administrator accounts to dedicated
   unprivileged accounts, prove the MacBook runner returns online after a
   headless reboot, then dispatch the manual hardware workflow from reviewed
   default-branch code. It must never run untrusted pull-request code.
2. Reboot the MacBook and verify that its root LaunchDaemon reconstructs the
   exact PF anchor and reference; the equivalent Mac mini reboot now passes.
   Then prove that a listed peer succeeds and an unlisted peer is rejected.
3. Install APC's unattended system service for dedicated non-root accounts and
   repeat the two-Mac zero-login reboot, exact-status, readiness, bounded-log
   and clean-uninstall gate. The deterministic design tests are not live proof.
4. Have the user install and authenticate Tailscale on both Macs, approve the
   macOS network configuration, run APC's identity/peer/route preflight and
   repeat the network gates over the authenticated overlay.
5. Move control-plane members to three independent physical failure domains
   before making host-level HA claims. Three etcd VMs on one Mac prove quorum
   behavior, not physical-host availability.
6. Resolve or explicitly accept the MacBook Apple VM's public HTTPS egress
   failure. Host-mediated image distribution is a mitigation, not a NAT fix.
7. Redesign or validate portable member addressing, then prove replacement-Mac
   recovery, protected off-host escrow and measured RPO/RTO in
   [issue #9](https://github.com/buberlo/apple-pod-control/issues/9). Same-Mac
   recovery after loss of the current token and all three member volumes passes;
   the replacement-Mac drill failed closed after seed reset because peer
   routing to the stored stable address was not portable. Ordinary restore
   retains its stricter in-place preconditions.

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
server and agent Background LaunchAgents were active in each user's Background
launchd domain, their supervisor logs were empty, and a follow-up deep doctor
reported 16 passes, two intentionally skipped egress warnings and zero failures.
This established headless SSH bootstrap and runtime reconciliation, not
automatic loading after a host reboot.

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
- externally anchored reconstruction of the exact saved cluster from empty
  local host state with `apc cluster ha recover`;
- a private cross-process lock around snapshot, restore, recover and member
  lifecycle.

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

The final skip-egress HA deep doctor reported 34 passes, three expected
warnings and zero failures. A subsequent full run without that exception again
passed all 34 runtime, quorum, API, DNS, kubelet, pod-network, ClusterIP and
cleanup checks, while public HTTPS failed independently on all three VMs. A
live HA image prefetch also imported and verified the same ARM64 archive in all
three member stores.

A separate destructive empty-host gate retained the printed manifest SHA-256
outside the package, then removed the local configuration, current token,
network, all three volumes and VMs on the same physical Mac. Running
`apc cluster ha recover --from ... --expected-manifest-sha256 ... --yes` first
with a wrong independently retained digest caused exactly zero configuration,
token, network, volume or VM mutation. Running it with the correct digest
reconstructed the exact saved topology and returned all three embedded-etcd
members to Ready.

The recovered cluster retained its Helm release, ConfigMap,
PodDisruptionBudget and three Pods placed on three virtual nodes. Its
skip-egress doctor reported 34 passes, three expected warnings and zero
failures. The snapshot directory was mode `0700`, manifest `0400`, snapshot and
token `0600`, and the recovered token and kubeconfig `0600`. Temporary staging
and the recovery helper were absent after completion.

Air-gapped recovery required the host image store to hold the pinned pause 3.6,
CoreDNS 1.14.4, local-path-provisioner 0.0.36 and metrics-server 0.8.1 images,
plus the workload images. Once fresh member volumes existed,
`apc image prefetch --pull=false ...` populated their K3s stores without a
registry pull.

The HA restore command remains intentionally limited to in-place rollback of
the same intact saved cluster: it requires the saved configuration, current
matching token, exact network and all three original member volumes. Recover is
the separate exact empty-host path, including on-host token loss and loss of
all three volumes. The live gate proves that path on the same physical Mac; it
does not by itself prove replacement-Mac recovery. A subsequent replacement-Mac
drill validated the package, independent manifest digest and reconstructed
topology. Seed-member etcd reset completed, but the remaining members could not
route to the stored stable secondary address through the target host's newer
`apple/container` vmnet behavior, so they could not join. APC recorded failure,
retained the volumes for diagnosis and blocked automatic VM mutation. The
disposable drill cluster and temporary target resources were explicitly removed
after evidence collection. This is a fail-safe result, not replacement-host
recovery; protected escrow, RPO/RTO and three-host HA remain open in
[issue #9](https://github.com/buberlo/apple-pod-control/issues/9).

Persistent PF install and privileged status now pass on both physical Macs over
the trusted LAN. The running-state verification covered the root-owned helper,
LaunchDaemon, PF reference and complete anchor. At that point no host reboot,
connection from an unlisted peer or authenticated-overlay repetition had been
tested.

A later headless reboot of the Mac mini proved that its root PF LaunchDaemon
reconstructed the complete six-rule anchor and a live PF reference. That same
reboot exposed a separate service gap: per-user Background LaunchAgents were
not automatically loaded without a login session. Manually bootstrapping the
saved K3s agent supervisor plist and the GitHub runner plist, which was still user-scoped,
returned the Kubernetes Node to Ready and the runner to online at that point.

The GitHub runners were subsequently migrated to exact root-owned system
LaunchDaemons on both Macs through the hardened root-owned installer. GitHub
reported both online and idle, and a headless Mac mini reboot returned its
runner online without an interactive login. The MacBook runner reboot remains
open. Both live services currently select administrator accounts instead of
the recommended dedicated unprivileged runner accounts.

APC's separate unattended service implementation now renders a root
LaunchDaemon with `/dev/null` stdout/stderr, an exact `launchctl asuser` +
`sudo -n` + `env -i` privilege drop, a protected 1-MiB non-root log and
running/PID/exact-argv status validation. Root-owned ancestor and extended
allow-ACL checks fail closed. These paths have deterministic test coverage, but
the live two-Mac APC zero-login reboot repeat is still pending and is not
established by the runner or PF result.

The manual two-Mac workflow now contains default-branch and no-checkout guards,
read-only/status checks by default, explicit opt-in for the mutating deep
doctor, and redacted artifacts. Dedicated isolated self-hosted runners are
registered, online and idle on both Macs, and the matching local read-only
harness passed. The GitHub workflow has not run because it intentionally
becomes dispatchable only after reviewed code reaches the default branch.
Tailscale is also absent on both hosts and cannot be completed without user
installation, macOS approval and interactive authentication.

The last successful two-Mac network baseline was 16 passes, two intentionally
skipped public-egress warnings and zero failures with `--skip-egress`. That is
historical evidence, not current readiness. After later DHCP changes, the saved
K3s server URL and host advertise addresses no longer matched the physical LAN
identities. The worker is intentionally stopped rather than left in a restart
loop. Stable address reservations or a stable authenticated overlay are
required before rejoining and rerunning the matrix.

During the last successful run, public HTTPS from the MacBook Apple VM failed
while the Mac mini VM passed; cross-host VXLAN, DNS and ClusterIP routing were
healthy.

The same asymmetry is visible at the host boundary: the MacBook host can reach
public HTTPS while its default route uses a tunnel interface, but every local
Apple VM times out; the second Mac uses a non-tunnel default route and its VM
passes. This strongly implicates tunnel handling of Apple vmnet/NAT forwarding,
rather than DNS or K3s. Changing or disconnecting the user's host tunnel remains
an explicit operator decision.
