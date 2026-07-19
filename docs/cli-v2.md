# APC v2 CLI contract

APC v2 separates Apple host lifecycle from Kubernetes workload semantics.
Cluster and node commands are implemented by APC; workload commands are
streamed through the installed native `kubectl` using an APC-managed
kubeconfig.

```text
apc cluster/node/doctor/config  -> APC -> apple/container
apc get/apply/logs/exec/...     -> native kubectl -> K3s API
helm                            -> same kubeconfig -> K3s API
apc --legacy get/apply/...      -> APC v1 REST API
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

## Kubernetes commands

Common kubectl top-level commands are accepted directly, including `get`,
`apply`, `create`, `delete`, `describe`, `logs`, `exec`, `port-forward`, `cp`,
`run`, `expose`, `rollout`, `scale`, `set`, `wait`, `patch`, `edit`, `label`,
`annotate`, `top`, `auth`, `debug`, `drain` and `cluster-info`.

APC reserves only its lifecycle commands (`cluster`, `node`, `image`, `system`,
`doctor`, `config`, `kubeconfig`, `kubectl` and `version`). Other top-level names
are forwarded as well, allowing future kubectl commands and kubectl plugins
without an APC release.

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

Native Helm remains first-class:

```bash
export KUBECONFIG="$(apc kubeconfig path)"
helm upgrade --install web examples/helm/web --wait
```

`apc kubectl CLUSTER -- ...` remains available as a non-interactive bootstrap
fallback when native kubectl has not yet been installed. Direct v2 workload
commands intentionally require native kubectl.

## Deep cluster diagnostics

Kubernetes Node readiness does not prove that Apple VM NAT or cross-host VXLAN
still works. APC therefore provides an end-to-end diagnostic gate:

```bash
apc cluster doctor
apc cluster doctor lan-spike --output json
apc cluster doctor --skip-egress
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

## Image prefetch and sync

Workload images can be distributed without registry access from the K3s VMs:

```bash
apc image prefetch docker.io/library/busybox:1.36.1
apc image sync docker.io/library/busybox:1.36.1 \
  --peer user@mac-mini.local
```

APC pulls the requested `linux/arm64` image into Apple's host image store,
exports one private OCI archive, imports and verifies exact references in the
local nested K3s containerd, then streams the same archive over key-only SSH to
each peer's APC agent container. The archive is staged in a private temporary
directory and removed on both success and failure; it is never written to the
remote Mac. `--pull=false` reuses the host cache.

Image references, cluster names, platforms and SSH peers are validated before
constructing commands. Sync targets the persistent K3s data volumes, so images
remain available when APC replaces the surrounding Apple VM.

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
The server is stopped during the stream for a consistent SQLite and filesystem
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

## APC v1 compatibility

If no current v2 cluster exists, overlapping commands retain their APC v1
behavior. Once a v2 context is selected, use `--legacy` explicitly:

```bash
apc --legacy get pods -o wide
apc --legacy apply -f examples/deployment.yaml
```
