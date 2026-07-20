# Local K3s HA lab on Apple container

Status: 2026-07-20.

Apple Pod Control can run a three-server K3s cluster on one Apple Silicon Mac.
Every server is an OCI container executed by Apple container as its own native
ARM64 Linux micro-VM; APC does not use nested virtualization. K3s supplies the
Kubernetes API, controllers, scheduler and one embedded-etcd member per VM.

```text
 apc / kubectl / apc helm / native Helm
                 |
                 | kubeconfig: https://127.0.0.1:17442
                 v
       APC TLS-pass-through HA proxy
          /             |             \
         v              v              v
 API 17443          API 17444          API 17445
 server-1 VM        server-2 VM        server-3 VM
 K3s + etcd  <----> K3s + etcd  <----> K3s + etcd
 named volume       named volume       named volume
         \____________ Flannel ____________/

 All components above are on one physical Apple Silicon Mac.
```

Three members form an odd-sized etcd quorum and tolerate one server-VM
failure. They remain **one physical failure domain**: loss or reboot of the Mac
makes the entire lab unavailable. This is useful for development, quorum
testing and in-place rollback, but it is not three-host production HA.

The quorum rules follow the upstream
[K3s embedded-etcd HA documentation](https://docs.k3s.io/datastore/ha-embedded).
The VM lifecycle uses the interfaces documented by
[Apple container 1.0](https://github.com/apple/container/blob/1.0.0/docs/how-to.md).

## Create and supervise the lab

```bash
container system start
apc cluster ha create ha-lab
apc cluster ha status ha-lab
```

Creation is idempotent for exact APC-owned resources. APC creates a private
Apple network, three persistent member volumes and three ARM64 server VM
envelopes. The protected cluster configuration, join token and kubeconfig are
stored under the current user's APC configuration directory. Tokens are passed
through protected files, never command-line arguments.

Install the user LaunchAgent from a stable binary path:

```bash
apc system install --role ha --cluster ha-lab \
  --executable "$HOME/.local/bin/apc"
apc system status --role ha --cluster ha-lab
```

The HA supervisor reconciles all three VM envelopes and runs the local API
proxy. It retries proxy startup with bounded backoff while continuing member
reconciliation. For foreground troubleshooting only, the proxy can also be run
as:

```bash
apc cluster ha proxy ha-lab
```

### Durable intent and bounded self-healing

APC persists HA operator intent in a private per-cluster desired-state file
before the corresponding runtime mutation. It records whole-cluster `Running`
or `Stopped` and permits at most one intentionally stopped member while the
cluster is running. This separates desired state from observations such as a
VM being missing, stopped or temporarily unhealthy.

The supervisor takes the same per-cluster operation lock before reading intent
and reconciling:

- `Stopped` drives every remaining member envelope to stopped and never
  reconstructs an envelope removed by `delete --keep-data`;
- one intentionally stopped member stays stopped across ticks, supervisor
  restarts and host reboots;
- member `start` clears the target's stop intent before repair, and
  whole-cluster `start` clears both cluster and member stop intent;
- with no stop intent, one missing or stopped member can rejoin when the other
  two Node/API pairs are Ready;
- exactly one running-but-unhealthy member can be replaced only after the two
  non-target embedded-etcd voters prove a healthy, consistent majority.

That two-voter proof is taken from each voter's loopback health endpoint and
metrics. The voters must be healthy non-learners with unique server IDs,
mutually list one another as peers, agree on the target's third server ID and
not report competing leaders. The target itself may be unreachable. Failed
unhealthy-member repairs enter exponential backoff from 30 seconds up to five
minutes, preventing a tight destructive loop.

## Stable client endpoint and fallback

With the default member API base port, the proxy listens only on
`https://127.0.0.1:17442`, one port below the first member. It passes Kubernetes
TLS through unchanged; APC neither terminates TLS nor gains access to client
credentials. The proxy validates exact APC ownership, health-checks all three
backends and routes new TCP connections only to healthy members.

Before returning a kubeconfig, APC performs an authenticated Kubernetes
readiness probe. It prefers the stable proxy when the HA supervisor is serving
it. If the proxy is absent or unhealthy, APC safely rewrites the protected
kubeconfig to a reachable Ready member API. This direct-member fallback keeps
one-shot `apc`, kubectl and Helm invocations usable, while the supervised proxy
gives long-running clients a stable address. A connection already in flight
may still need to reconnect if its selected backend fails.

```bash
apc kubeconfig path ha-lab
apc --cluster ha-lab get nodes -o wide
apc --cluster ha-lab get pods -A -o wide
```

## Helm and workload placement

`apc helm` runs the installed native Helm binary with APC's selected,
mode-protected kubeconfig. Helm flags and streaming input/output are forwarded
unchanged:

```bash
apc helm --cluster ha-lab upgrade --install web examples/helm/web \
  --namespace web --create-namespace \
  --set replicaCount=3 \
  --set topologySpread.whenUnsatisfiable=DoNotSchedule \
  --set podDisruptionBudget.enabled=true \
  --wait --timeout 3m

apc --cluster ha-lab get deployment,pod,service -n web -o wide
```

Native Helm remains available as well:

```bash
export KUBECONFIG="$(apc kubeconfig path ha-lab)"
helm list --all-namespaces
```

The live lab deployed three nginx replicas with strict topology spread: one
Ready Pod on each of the three Apple VMs. This proves VM-level scheduling and
Helm compatibility, not three physical fault domains.

## Import an image into all three members

For an HA configuration, `image prefetch` validates the exact network, all
three member volumes and all three running APC-owned VM envelopes before it
pulls or exports anything. It exports one ARM64 archive and imports and verifies
the exact image reference in every member's K3s containerd:

```bash
apc --cluster ha-lab image prefetch \
  docker.io/library/busybox:1.36.1

# Reuse an image already present in Apple's host store:
apc --cluster ha-lab image prefetch \
  docker.io/library/busybox:1.36.1 --pull=false
```

An image on only one member is insufficient because Kubernetes may reschedule
a Pod to either of the other members. Prefer ARM64 or multi-platform images so
the Apple Silicon hosts do not need x86 translation.

## Deep diagnostics

The same end-to-end doctor understands protected HA configurations:

```bash
apc cluster doctor ha-lab --skip-egress
apc cluster doctor ha-lab --output json
```

It reports every VM runtime, published member API, Kubernetes Node and the real
embedded-etcd topology before creating probe workloads. The etcd gate probes
each member locally and requires three unique healthy voting IDs, an elected
leader and exact peer sets. Kubernetes API/Node readiness alone is not treated
as proof of quorum. A divergent or unhealthy topology stops diagnostics before
probe resources are created. Probe Pods and the Service are registered for
exact-name cleanup before creation and deleted unless `--keep` is requested.

## Quorum-safe member lifecycle

Use member IDs 1, 2 or 3; never edit etcd data or stop a second member by hand:

```bash
apc cluster ha member stop 2 ha-lab --yes --wait 3m
apc cluster ha status ha-lab
apc cluster ha member start 2 ha-lab --wait 3m

apc cluster ha member restart 3 ha-lab --yes --wait 3m
```

`stop` and `restart` require all three node/API pairs to be Ready before the
intentional availability change. `start` requires the other two members to be
Ready and returns only after all three have rejoined. Restart performs a
best-effort recovery start if an error occurs after the target may have stopped.
Member stop intent is durable before runtime mutation, so an interrupted CLI
does not cause the supervisor to start the member again. At most one member may
carry intentional stop state; starting or restarting it clears that suppression.

Before the quorum-reducing part of `stop` or `restart`, APC validates actual
embedded-etcd state on all three members. Every loopback health check must pass;
server IDs must be unique; every member must have a leader, be a voting
non-learner and report exactly the other two server IDs as peers. Exactly one
member must report the leader role. Any mismatch fails before the stop call,
even when all Kubernetes Nodes and APIs appear Ready.

Snapshot, restore and member stop/start/restart acquire one private per-cluster
cross-process file lock. Independent `apc` processes therefore cannot overlap
these quorum-sensitive operations. Lock waiting respects the command context
and timeout.

## HA snapshot and in-place restore

The single-server `apc cluster backup` format is not valid for embedded-etcd
HA. Use the dedicated commands and a destination directory that does not yet
exist:

```bash
umask 077
snapshot="$HOME/Backups/ha-lab-2026-07-20"
apc cluster ha snapshot ha-lab --output "$snapshot"

# Destructive rollback of this same saved cluster:
apc cluster ha restore ha-lab --from "$snapshot" --yes --wait 5m
```

Snapshot requires all three members to be Ready and the same exact three-member
embedded-etcd topology proof used before member maintenance. APC then asks K3s
for a native etcd snapshot, briefly stops one member while the other two retain
quorum, copies the snapshot and matching server token outside all member
volumes, checks sizes and SHA-256 digests, restarts the member and publishes a
private package only after the cluster is 3/3 Ready again. The package contains
exactly the etcd snapshot, the server token and a read-only manifest.

apple/container attaches a VM to its default network when no network is
specified. The first live snapshot exposed that a recovery-helper validator
must not assume an omitted network means no attachment. Generic snapshot/clear
helpers now request `default,mtu=1280` explicitly and are accepted only with
exactly that one network, no requested MAC and MTU 1280. The reset helper still
requires the exact APC-owned cluster network, member MAC and MTU. The corrected
snapshot and restore were rerun successfully.

The server token is part of the cryptographic recovery identity and is a
credential. Keep the complete package mode `0700` or stricter, encrypt any
off-host copy, and never print, email or commit the token. Losing the token can
make K3s bootstrap data in the snapshot unusable; disclosing it compromises the
cluster.

Restore validates the package, checksums, saved topology, image, cluster
identity and token **before** stopping members. It then resets member 1 from the
snapshot, clears stale etcd databases on members 2 and 3, rejoins them and
requires all three node/API pairs to become Ready. A private recovery journal
records progress and safe next action if the sequence fails. The supervisor
loads this journal under the HA operation lock before any VM reconciliation. A
nonterminal, unsuccessful, malformed, insecure or otherwise untrusted journal
fails closed and blocks automatic VM mutations; the independently managed API
proxy loop continues. Automatic reconciliation resumes only after a completed
successful restore or a failed restore whose recorded automatic recovery
successfully returned the original cluster to a runtime-safe state. Otherwise,
rerun the explicit restore command with the validated snapshot after resolving
the journal's reported failure.

The final live HA sequence on 2026-07-20 completed successfully:

- three native ARM64 K3s/embedded-etcd VMs on one Mac reached 3/3 Ready;
- the Helm nginx release placed one Ready Pod on each VM and retained its
  PodDisruptionBudget;
- one intentional member stop remained effective for more than two supervisor
  ticks; the two-voter quorum accepted and returned a ConfigMap write;
- starting the stopped member again returned the cluster to 3/3 Ready;
- a fresh snapshot/restore returned a changed ConfigMap to its before-snapshot
  value and removed a second object created only after the snapshot;
- the Helm release, three topology-spread Ready Pods and PodDisruptionBudget
  remained intact, and the recovery journal reached `completed` with recovery
  success;
- the final deep doctor reported 34 passes, three expected warnings for
  intentionally skipped public egress and zero failures;
- the published package was mode `0700`, with a read-only `0400` manifest and
  `0600` snapshot and token artifacts;
- the HA LaunchAgent continued serving the stable proxy on
  `https://127.0.0.1:17442`.

This restore is deliberately an **in-place rollback**, not replacement-host
disaster recovery. It requires all of the following current state:

- the original saved HA cluster configuration and exact APC-owned network;
- the current protected token file, matching the token inside the package;
- all three original, exact APC-owned member volumes;
- the saved topology and image identity recorded in the manifest.

It cannot recreate a lost Mac, bootstrap a new host from the package alone,
recover after all three volumes disappear, or substitute a different current
token. The current implementation also does not recover from loss of the
on-host token file even though a protected copy is present in the snapshot
package: validation requires both to exist and match. Preserve independent
encrypted copies, but do not describe this command as replacement-host or
token-loss DR.

## apple/container 1.0 runtime-address limitation

Apple container 1.0 accepts a requested MAC address on `container run`, but it
does not let APC reserve a fixed primary IPv4 address. The runtime's dynamic
IPv4 allocation is independent of that requested MAC. A newly created envelope
can therefore receive an address that APC has reserved as another etcd member's
stable secondary address.

APC guards this before K3s starts or a restore reset mutates etcd. It validates
the current peer reservations, inspects the new envelope's runtime address and
rejects a collision. Only the exact APC-owned conflicting envelope may be
stopped and deleted. Creation is retried a small fixed number of times under
bounded command and address-probe contexts; exhaustion fails closed with no
conflicting envelope left behind. APC never deletes a foreign or merely
similarly named container to obtain another address.

Envelopes created before this guard used a known legacy init identity. APC
accepts only that exact preceding identity and migrates it one member at a time
through the same quorum-safe stop/start path. Each migrated member must return
Ready before reconciliation proceeds. Unknown launch arguments, labels,
network settings, mounts or resource identity are refused rather than adopted.

## Whole-cluster lifecycle

```bash
apc cluster ha stop ha-lab
apc cluster ha start ha-lab
apc cluster ha status ha-lab

# Remove VM envelopes but retain all three member volumes:
apc cluster ha delete ha-lab --keep-data --yes

# Permanently remove APC-owned envelopes and member data:
apc cluster ha delete ha-lab --yes
```

Whole-cluster `stop` persists `Stopped` before the first VM mutation and
intentionally removes API availability. If the command or host is interrupted,
the supervisor finishes stopping any remaining running member instead of
restarting stopped members. `delete --keep-data` records the same stopped intent
while retaining all three volumes. `start` persists `Running`, clears any member
stop intent, reuses the member volumes and rejoins the same etcd members. A full
data deletion removes the desired-state record with the cluster configuration.
Wait for 3/3 Ready before a member operation or snapshot.

## Boundaries still open

- The three-VM lab survives one VM failure, not loss or reboot of its one Mac.
  Host-level HA still needs three independent physical failure domains and
  mutually routed or overlaid peer traffic.
- Replacement-host and token-loss disaster recovery remain open even after the
  successful in-place restore.
- The two-Mac server/agent cluster is separate; it is not converted into this
  etcd cluster.
- The current cross-host path is peer-restricted PF on the trusted LAN.
  Persistence across host reboot and rejection of a non-peer are not yet live
  proven.
- Tailscale is not installed or authenticated on the Macs. A user must install
  it, approve the macOS network configuration and complete interactive account
  authentication before overlay gates can run. APC never accepts an auth key.
- The dedicated hardware CI runners are not registered, so the default-branch
  hardware workflow has not yet run on the two Macs.
- The MacBook's Apple VM still lacks public HTTPS egress in the tested setup.
  Host-mediated image prefetch avoids making registry reachability a scheduling
  prerequisite, but it does not repair VM NAT.
