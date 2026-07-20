# APC CLI contract

APC separates Apple host lifecycle from Kubernetes workload semantics.
Cluster and node commands are implemented by APC; workload commands are
streamed through the installed native `kubectl` using an APC-managed
kubeconfig.

The project installs one executable, `apc`. K3s remains the API server,
controller manager, scheduler and desired-state store; APC does not translate
Kubernetes objects or maintain a parallel workload database.

```text
apc cluster/node/doctor/config  -> APC -> apple/container
apc get/apply/logs/exec/...     -> native kubectl -> K3s API
apc helm ...                    -> native Helm + selected kubeconfig -> K3s API
helm                            -> native Helm + exported kubeconfig -> K3s API
```

## Select a cluster

`apc cluster create` and `apc cluster start` select the successful cluster
automatically. Context can also be managed explicitly:

```bash
apc config get-clusters
apc config use-cluster lan-spike
apc config current-cluster
apc kubeconfig path
```

Selection precedence is:

1. `--cluster NAME` before the Kubernetes command;
2. `APC_CLUSTER`;
3. the persisted current cluster.

The current-cluster file contains only a DNS-label name. Kubeconfigs remain
mode `0600` and are rejected by the passthrough when group- or world-readable.
Without an explicit or current cluster, a Kubernetes command fails with context
selection guidance; APC does not guess an API endpoint.

## Kubernetes commands

Common kubectl top-level commands are accepted directly, including `get`,
`apply`, `create`, `delete`, `describe`, `logs`, `exec`, `port-forward`, `cp`,
`run`, `expose`, `rollout`, `scale`, `set`, `wait`, `patch`, `edit`, `label`,
`annotate`, `top`, `auth`, `debug`, `drain` and `cluster-info`.

APC reserves only its lifecycle commands (`cluster`, `node`, `image`, `system`,
`doctor`, `config`, `kubeconfig`, `kubectl`, `helm` and `version`). Other
top-level names are forwarded as well, allowing future kubectl commands and
kubectl plugins without an APC release.

Arguments are not interpreted by APC after cluster selection. Standard input,
output and error are connected directly to kubectl, which keeps `-f -`,
`--watch`, `logs -f`, interactive `exec -it`, port forwarding and normal
kubectl exit behavior working with minimal added latency.

```bash
apc get pods -A -o wide
apc apply --server-side -f examples/kubernetes/web.yaml
apc logs -n web -f deployment/web
apc exec -n web -it deployment/web -- /bin/sh
apc rollout status -n web deployment/web
```

`apc helm` selects and validates the APC kubeconfig, removes only APC's own
`--cluster` selector and forwards all remaining arguments and streams to the
installed native Helm binary:

```bash
apc helm --cluster ha-lab upgrade --install web examples/helm/web \
  --namespace web --create-namespace --wait

# Direct native Helm remains first-class:
export KUBECONFIG="$(apc kubeconfig path ha-lab)"
helm list --all-namespaces
```

`apc kubectl CLUSTER -- ...` remains available as a non-interactive bootstrap
fallback when native kubectl has not yet been installed. Direct workload
commands intentionally require native kubectl.

## Deep cluster diagnostics

Kubernetes Node readiness does not prove that Apple VM NAT or cross-host VXLAN
still works. APC therefore provides an end-to-end diagnostic gate:

```bash
apc cluster doctor
apc cluster doctor lan-spike --output json
apc cluster doctor --skip-egress
apc cluster doctor ha-lab --skip-egress
```

The doctor creates uniquely named `apc-doctor-*` resources in the `default`
namespace and pins one `nginx:alpine` probe Pod to every Ready node. It verifies:

- the Apple server VM, published API port and Kubernetes Node conditions;
- Pod creation and kubelet exec on every node;
- CoreDNS and public HTTPS egress from every node;
- HTTP between every directed pair of nodes, exercising the real VXLAN path;
- Service DNS and ClusterIP routing from every node to a deterministic probe
  endpoint.

The exact probe Pods and Service are force-deleted even when checks fail or the
main diagnostic context times out. APC deliberately does not create a temporary
namespace: namespace finalization can itself block when an aggregated API such
as Metrics Server is unreachable. `--keep` retains the listed resources for
manual inspection; the operator must then delete them. The doctor exits nonzero
when a required check fails, making it suitable as an alpha acceptance gate.

For an HA configuration, the report begins with every server VM, published
member API, Kubernetes Node and the embedded-etcd quorum. At two Ready members
it retains the failed-member evidence and can still run workload probes. Below
quorum it stops before creating probe resources.

## Image prefetch and sync

Workload images can be distributed without registry access from the K3s VMs:

```bash
apc image prefetch docker.io/library/busybox:1.36.1
apc --cluster ha-lab image prefetch \
  docker.io/library/busybox:1.36.1 --pull=false
apc image sync docker.io/library/busybox:1.36.1 \
  --peer user@mac-mini.local
```

APC pulls the requested `linux/arm64` image into Apple's host image store,
exports one private OCI archive, imports and verifies exact references in the
local nested K3s containerd, then streams the same archive over key-only SSH to
each peer's K3s agent VM. The archive is staged in a private temporary
directory and removed on both success and failure; it is never written to the
remote Mac. `--pull=false` reuses the host cache.

Image references, cluster names, platforms and SSH peers are validated before
constructing commands. Sync targets the persistent K3s data volumes, so images
remain available when APC replaces the surrounding Apple VM.

When the selected cluster has a protected HA configuration, prefetch resolves
all targets before pulling or exporting. It requires the exact APC-owned
network, three volumes and three running member envelopes, then imports and
verifies the image in all three K3s containerd stores from one host archive.

Empty-host recovery creates fresh member stores, so the usual volume
persistence cannot supply an air-gapped bootstrap. The host image store must
already contain the exact references used by the pinned K3s build for pause
3.6, CoreDNS 1.14.4, local-path-provisioner 0.0.36 and metrics-server 0.8.1,
plus every workload image. After recovery has created the fresh volumes and
member envelopes, import the complete set without a registry pull before
expecting bootstrap and workloads to become Ready:

```bash
apc --cluster ha-lab image prefetch --pull=false \
  EXACT_PAUSE_IMAGE EXACT_COREDNS_IMAGE EXACT_LOCAL_PATH_IMAGE \
  EXACT_METRICS_SERVER_IMAGE EXACT_WORKLOAD_IMAGE
```

## Lifecycle, backup and upgrades

```bash
apc cluster stop lan-spike
apc cluster start lan-spike
apc cluster delete lan-spike --keep-data --yes
apc cluster start lan-spike

apc cluster backup lan-spike --output "$HOME/Backups/lan-spike.apcbackup"
apc cluster restore lan-spike --from "$HOME/Backups/lan-spike.apcbackup" --yes
apc cluster upgrade lan-spike \
  --image docker.io/rancher/k3s@sha256:DIGEST \
  --yes
```

APC checks exact `managed`, `cluster` and `role` labels before stopping or
deleting any Apple container or volume. Full deletion and restore require
`--yes`; `--keep-data` retains both the data volume and enough configuration
for `start` to recover a missing VM envelope.

Backups are private `0700` directories containing `volume.tar` and a manifest.
The server is stopped during the stream for a consistent K3s data-volume
snapshot, then returned to its previous running state. Restore verifies the
manifest, SHA-256 checksum, cluster identity and tar paths before stopping the
server. Upgrades require immutable OCI digests, always make a backup and use it
for automatic rollback when the replacement node fails readiness.

On each Mac, install the reboot/crash supervisor from a stable binary path:

```bash
apc system install --role server --cluster lan-spike \
  --executable "$HOME/.local/bin/apc"
apc system status --role server --cluster lan-spike

# On an agent Mac:
apc system install --role agent --cluster lan-spike \
  --executable "$HOME/.local/bin/apc"
```

The generated user LaunchAgent is restricted to launchd's Background session
and invokes a long-running APC reconcile loop. It starts Apple's container
service if needed and recreates a missing/stopped APC envelope from saved state.
It can be managed over headless SSH once the user's launchd domain exists, but
it is not an unattended boot service: a live Mac mini reboot showed that macOS
does not automatically load these per-user plists before login. Use a
root-managed LaunchDaemon/service account for that production requirement.

For that unattended profile, first remove the matching per-user supervisor and
then install the system service for an explicit non-root account. Use the same
stable executable, role, cluster, interval and account for every lifecycle
command because status verifies the complete service definition:

```bash
apc_binary=/Users/APC_ACCOUNT/.local/bin/apc

sudo "$apc_binary" system install --role agent --cluster lan-spike \
  --executable "$apc_binary" --unattended --user APC_ACCOUNT --yes

"$apc_binary" system status --role agent --cluster lan-spike \
  --executable "$apc_binary" --unattended --user APC_ACCOUNT

sudo "$apc_binary" system uninstall --role agent --cluster lan-spike \
  --executable "$apc_binary" --unattended --user APC_ACCOUNT --yes
```

The exact root LaunchDaemon sends its own stdout/stderr to `/dev/null` and
enters the target bootstrap namespace with `launchctl asuser`. It then drops to
the selected account through non-interactive `sudo -n -H -u` and starts APC
under `env -i` with only the exact `HOME` and bounded `PATH`. APC opens its
1-MiB bounded log only after that privilege drop, beneath the target account's
protected `~/Library/Logs/APC` directory. Status fails unless the system job is
`running` with a positive PID and the exact wrapper program and argument vector.
The root-owned plist and its real root-owned ancestor chain reject writable
paths and extended allow ACLs.

This unattended design and its deterministic tests are complete, but the live
APC zero-login reboot repeat is still pending. The Mac mini's runner and PF
LaunchDaemon reboot proofs are separate and do not establish this APC service
gate.

## Local three-server HA operations

The local embedded-etcd lab is three Apple VMs on one physical Mac:

```bash
apc cluster ha create ha-lab
apc cluster ha status ha-lab

apc system install --role ha --cluster ha-lab \
  --executable "$HOME/.local/bin/apc"
apc system status --role ha --cluster ha-lab
```

The HA LaunchAgent reconciles all three members and serves a stable local
TLS-pass-through API. For the default member ports, clients use
`https://127.0.0.1:17442`. Kubernetes TLS remains end to end. Kubeconfig
preparation authenticates this endpoint and prefers it; if the proxy is absent
or unhealthy, APC falls back to a reachable Ready member endpoint.

HA lifecycle commands persist operator intent before changing VM state. The
private desired-state record contains whole-cluster `Running` or `Stopped` and,
while running, at most one intentionally stopped member. Whole-cluster `stop`
and `delete --keep-data` remain stopped across supervisor restarts; whole-cluster
`start` clears that state and any stale member stop. Member `start` clears only
its target's stop intent, while `restart` is transient and leaves no suppression.

The supervisor reconciles toward that record rather than blindly starting all
three envelopes. With no stop intent, it repairs at most one unhealthy member.
A running-but-unhealthy target is restarted only if the other two Node/API
pairs are Ready and their local embedded-etcd state proves a healthy,
mutually-consistent voting majority. Failed repair attempts use bounded
exponential backoff. Intentional member and whole-cluster stops are preserved.

Operate one member at a time while retaining quorum:

```bash
apc cluster ha member stop 2 ha-lab --yes --wait 3m
apc cluster ha member start 2 ha-lab --wait 3m
apc cluster ha member restart 3 ha-lab --yes --wait 3m
```

APC does not treat three Ready Kubernetes APIs as an etcd proof. Before a
quorum-reducing member stop/restart it probes every member's loopback etcd
health and metrics and verifies unique voting IDs, leader presence, non-learner
state and exact two-peer membership. A divergent, unhealthy or incomplete
topology fails before the VM mutation.

Create, restore or recover from the dedicated HA package rather than using the
single-server backup format:

```bash
umask 077
snapshot="$HOME/Backups/ha-lab-2026-07-20"
apc cluster ha snapshot ha-lab --output "$snapshot"
# Copy the printed manifest-sha256 to separately protected storage.
manifest_sha256=MANIFEST_SHA256

# In-place rollback while the saved local cluster still exists:
apc cluster ha restore ha-lab --from "$snapshot" --yes --wait 5m

# Alternatively, exact reconstruction after deliberately clearing local state:
apc cluster ha recover ha-lab --from "$snapshot" \
  --expected-manifest-sha256 "$manifest_sha256" --yes --wait 5m
```

The package contains a native K3s etcd snapshot, immutable manifest and the
matching server token. The token is a credential and part of the recovery
identity: encrypt off-host copies and never print, email or commit it. Snapshot
requires 3/3 Ready members plus the same exact embedded-etcd topology proof used
for quorum-sensitive maintenance. The printed manifest SHA-256 must be retained
separately: the manifest inside the same package is not an independent trust
anchor.

Restore validates checksums, topology, image, cluster identity and the current
matching token before mutation, then resets member 1 and rejoins members 2 and
3. It is only an in-place rollback of the same saved cluster and requires the
saved configuration, exact current network, all three exact member volumes and
the current token file matching the packaged token.

Recover has the complementary empty-host contract. It requires the package and
the independently retained digest, then reconstructs the exact saved
configuration, token, network and three-volume topology before running the same
embedded-etcd recovery sequence. A digest mismatch is checked before any local
state is published: the live negative gate caused exactly zero configuration,
token, network, volume or VM mutation.

The private recovery journal is a supervisor interlock: nonterminal,
unsuccessful or untrusted recovery state blocks automatic VM changes until the
operation is safely completed or retried. A terminal failed restore is
considered runtime-safe only when its recorded automatic recovery succeeded. A
failed empty-host recovery that created new volumes must be retried with
`recover` and the independent manifest digest; ordinary `restore` fails closed
before touching that lineage.

Snapshot, restore, recover and member lifecycle commands share a private
per-cluster cross-process lock so separate APC processes cannot overlap
quorum-sensitive operations.

The live 2026-07-20 restore returned 3/3 Ready and changed a ConfigMap from its
after-snapshot value back to the before-snapshot value. The existing Helm
release, three topology-spread Ready Pods and PodDisruptionBudget remained
present, the recovery journal reached `completed`, and the launchd proxy
continued serving `https://127.0.0.1:17442`.

A separate destructive gate removed the local configuration, current token,
network, all three volumes and VMs, then recovered the exact topology on the
same physical Mac. It returned 3/3 embedded-etcd Ready and retained the Helm
release, ConfigMap, PodDisruptionBudget and three Pods on three virtual nodes.
The skip-egress doctor reported 34 passes, three expected warnings and zero
failures. The package directory was `0700`, manifest `0400`, snapshot and token
`0600`, and the recovered token and kubeconfig `0600`; temporary staging and
the helper were absent after completion.

This proves same-Mac empty-host reconstruction, including on-host token loss
and loss of all three volumes. A later replacement-Mac drill validated the
package, independent manifest digest and seed-member etcd reset, but peers could
not route to the stored stable address through the target host's newer
`apple/container` vmnet. APC failed closed, retained diagnostic state until it
was explicitly inspected, and the disposable drill cluster was then removed.
Replacement-Mac and off-host recovery remain unsupported and are tracked in
[issue #9](https://github.com/buberlo/apple-pod-control/issues/9).

Apple container 1.0 assigns a dynamic primary IPv4 independently of the MAC
requested on `container run` and offers no fixed-IPv4 reservation there. APC
checks peer reservations and the new primary address before K3s starts or a
restore/recover reset mutates etcd. On collision it removes only the exact
APC-owned envelope and retries under fixed attempt, command and probe bounds.
The immediately preceding exact envelope identity is migrated one member at a
time through the quorum-safe path; foreign or mismatched envelopes fail closed.

## Network policy and host firewall

New clusters enable K3s's NetworkPolicy controller by default. Existing
clusters can change it through a volume-preserving envelope replacement:

```bash
apc cluster network-policy enable lan-spike --yes
```

APC also renders peer-restricted PF rules before any privileged action. For a
server Mac and one agent Mac:

```bash
apc system firewall render --cluster lan-spike --role server \
  --interface en0 --local-ip SERVER_IP --peer AGENT_IP

sudo apc system firewall install --cluster lan-spike --role server \
  --interface en0 --local-ip SERVER_IP --peer AGENT_IP --yes

# Reverse role and addresses on the agent Mac.
sudo apc system firewall install --cluster lan-spike --role agent \
  --interface en0 --local-ip AGENT_IP --peer SERVER_IP --yes

sudo apc system firewall status --cluster lan-spike
```

`render` runs `pfctl`'s parser but does not mutate the host. `install` copies a
root-owned APC helper to `/Library/PrivilegedHelperTools`, writes a root
LaunchDaemon and loads the `com.apple/apc/CLUSTER` anchor. Only the exact peers
may reach server TCP 16443, node TCP 10250 and VXLAN UDP 8472. `firewall
uninstall --yes` unloads the daemon, flushes the anchor and releases APC's PF
reference. Use an encrypted overlay interface and its addresses instead of
`en0` before operating across an untrusted network.

`firewall status` verifies the root:wheel ownership and exact modes of the
privileged helper, LaunchDaemon and PF reference token, validates the plist,
checks the loaded system launchd service and requires a complete live PF anchor.
`install` runs the same verification before reporting success and restores the
previous daemon configuration if any step fails.

On 2026-07-20 both the persistent PF install and privileged `firewall status`
check passed on both test Macs. A headless Mac mini reboot subsequently
reconstructed its complete six-rule anchor and live PF reference. The MacBook
reboot, a connection attempt from an unlisted peer, uninstall rollback and the
authenticated-overlay repetition remain unproven.

### Authenticated host overlay

APC does not accept or persist Tailscale authentication keys. Install and log in
to Tailscale independently on each Mac, then validate the two identities and
the actual macOS route before creating a cluster. APC consumes only the
[official CLI's machine-readable status](https://tailscale.com/docs/reference/tailscale-cli?tab=macos):

```bash
apc system overlay check --provider tailscale --peer-ip PEER_TAILSCALE_IP
```

The check uses Tailscale's machine-readable status, requires both devices to be
online, restricts addresses to `100.64.0.0/10`, discovers the local tunnel
interface and confirms that the peer route uses that same interface. It never
prints user identity, auth keys or the complete Tailscale status document.

For a new secure cluster, use each Mac's Tailscale address consistently as its
listen and advertise address and use the server's Tailscale address in the
agent `--server-url`. Install PF with `--interface auto --local-ip
LOCAL_TAILSCALE_IP`; the root LaunchDaemon resolves a potentially changing
macOS `utun` interface from the stable local overlay address every time it
reconciles:

```bash
apc cluster create secure \
  --listen-address SERVER_TAILSCALE_IP \
  --advertise-address SERVER_TAILSCALE_IP

apc node join secure \
  --server-url https://SERVER_TAILSCALE_IP:16443 \
  --token-file PRIVATE_TOKEN_FILE \
  --listen-address AGENT_TAILSCALE_IP \
  --advertise-address AGENT_TAILSCALE_IP

sudo apc system firewall install --cluster secure --role server \
  --interface auto --local-ip SERVER_TAILSCALE_IP \
  --peer AGENT_TAILSCALE_IP --yes
```

This profile transports APC's published K3s/VXLAN ports over the authenticated
host overlay. It is distinct from K3s's [experimental in-guest `--vpn-auth`
integration](https://docs.k3s.io/networking/distributed-multicloud) and avoids
copying a provider join key into the Apple VM.

Tailscale is not currently installed or authenticated on either test Mac. The
user must install it, approve the macOS network extension/configuration and
complete interactive account authentication before this overlay gate can run.

## Manual two-Mac hardware workflow

`.github/workflows/hardware-acceptance.yml` is `workflow_dispatch` only and
accepts dispatches only from the default branch. Its default mode checks the
installed APC/container/K3s state without live-cluster mutation. `kubectl` and
Helm are required and checked only on the control-plane Mac; the worker keeps
its agent footprint to APC, `apple/container` and K3s.
The deep doctor runs only when both the mutation authorization and doctor input
are explicitly enabled. It does not check out repository code on either
self-hosted Mac and uploads redacted diagnostics.

The two hardware runners are registered, installed through
the hardened root-owned installer as root-owned system LaunchDaemons, and were
reported by GitHub as online and idle. The Mac mini returned online after a
headless reboot; the MacBook runner reboot remains open. The lab currently uses
administrator accounts instead of the recommended dedicated unprivileged
runner accounts. The workflow itself remains pending until
reviewed code reaches the default branch, even though the equivalent local
read-only harness passed. See the
[hardware-runner guide](hardware-runners.md) for the trust and account model.

The saved two-Mac worker is currently stopped because DHCP changes made its
stored K3s server URL and host advertise addresses stale. It should be rejoined
only after reserving stable LAN addresses or selecting a stable authenticated
overlay; this document does not claim that topology is currently Ready.
