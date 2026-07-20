# APC quickstart

This guide creates a real K3s cluster in an `apple/container` ARM64 node VM,
deploys nginx with Kubernetes YAML or Helm, and shows the path to a second Mac.
APC manages the Apple-side lifecycle; normal Kubernetes tools manage workloads.

## Prerequisites

```bash
container system start
brew install kubernetes-cli helm
make build
bin/apc doctor
```

Fix every required doctor failure before creating a node. Warnings about
unsupported optional Apple runtime features do not block creation.

## Create a single-node cluster

```bash
bin/apc cluster create dev
bin/apc cluster status dev
bin/apc get nodes -o wide
bin/apc get pods -A
```

Successful `create` selects `dev` as the current APC context. The K3s image is
pinned by OCI digest and runs as native Linux/ARM64. The API listens on port
`16443` inside the node VM and defaults to the host loopback address. K3s state
lives on an APC-labelled Apple volume; the surrounding VM is replaceable.

Creation is idempotent for resources with the exact expected ownership labels.
New clusters enable K3s NetworkPolicy enforcement by default.

## Deploy nginx from Kubernetes YAML

```bash
bin/apc apply -f examples/kubernetes/web.yaml
bin/apc rollout status deployment/web --timeout=2m
bin/apc get deployment,pod,service -o wide
```

The example contains an ordinary `apps/v1` Deployment, ClusterIP Service,
NetworkPolicy and PodDisruptionBudget. K3s owns their desired state,
reconciliation and scheduling. A Pod is scheduled inside the K3s node VM; APC
does not create one Apple VM per Pod.

Reach the Service temporarily from macOS:

```bash
bin/apc port-forward service/web 18081:80
curl http://127.0.0.1:18081/
```

## Deploy nginx with Helm

```bash
helm lint examples/helm/web
bin/apc helm upgrade --install web examples/helm/web \
  --namespace web --create-namespace --wait --timeout 2m
bin/apc get deployment,pod,service -n web -o wide
```

`apc helm` invokes native Helm with the selected, protected kubeconfig. Direct
Helm is supported too:

```bash
export KUBECONFIG="$(bin/apc kubeconfig path dev)"
helm list --all-namespaces
```

The chart demonstrates rolling updates, topology spread, same-namespace
NetworkPolicy and an optional PodDisruptionBudget. Any ARM64 or
multi-architecture OCI image supported by Kubernetes can be configured in its
values file.

## Contexts and kubectl behavior

```bash
bin/apc config get-clusters
bin/apc config current-cluster
bin/apc config use-cluster dev
bin/apc kubeconfig path dev
```

Kubernetes commands after APC's own global cluster selector are forwarded to
native kubectl with standard input, output and error attached directly:

```bash
bin/apc --cluster dev get pods -A --watch
bin/apc --cluster dev logs -f deployment/web
bin/apc --cluster dev exec -it deployment/web -- /bin/sh
```

`apc kubectl dev -- get pods -A` uses K3s's bundled client only as a bootstrap
convenience. Install native kubectl for normal use.

## Lifecycle, images and diagnostics

```bash
bin/apc cluster doctor dev --skip-egress
bin/apc image prefetch docker.io/library/busybox:1.36.1
bin/apc cluster stop dev
bin/apc cluster start dev

bin/apc cluster backup dev --output "$HOME/Backups/dev.apcbackup"
bin/apc cluster restore dev --from "$HOME/Backups/dev.apcbackup" --yes
```

`cluster stop` stops the disposable VM envelope. `cluster start` replaces that
envelope and reattaches the named volume containing `/var/lib/rancher/k3s`.
This preserves Kubernetes and Helm state and avoids relying on direct restart
behavior for port-published Apple VMs.

`cluster delete --keep-data --yes` removes the VM while retaining the volume
and saved configuration. A later `cluster start` recreates it. Full deletion,
restore and upgrades verify exact APC ownership and require explicit consent.

The deep doctor verifies more than Kubernetes Node `Ready`: it checks Apple VM
runtime, published API, kubelet exec, DNS, optional public HTTPS, ClusterIP and
directed cross-node HTTP. Probe objects have unique names and are deleted even
when a check fails.

## Add a worker on a second Mac

Cross-host VXLAN is unencrypted. Use this only on a trusted LAN with stable
host addresses, or bind all cluster and firewall identities to an authenticated
host overlay. Do not expose K3s ports to the public internet.

Run server-side preflight and create the server with explicit host addresses:

```bash
bin/apc doctor --role server --listen-address 0.0.0.0 --peer AGENT_ADDRESS

bin/apc cluster create lan-lab \
  --listen-address 0.0.0.0 \
  --advertise-address SERVER_ADDRESS
```

Write the join token to a protected file. It is deliberately not accepted as a
command-line value:

```bash
token_file="$HOME/Library/Application Support/apc/clusters/lan-lab/agent-token"
bin/apc cluster write-join-token lan-lab --output "$token_file"
```

Copy `bin/apc` and the protected token file to the other Mac using your normal
key-authenticated administration channel. On that Mac:

```bash
container system start
apc node join lan-lab \
  --server-url https://SERVER_ADDRESS:16443 \
  --token-file PRIVATE_TOKEN_FILE \
  --node-name worker-1 \
  --advertise-address AGENT_ADDRESS
```

Back on the server Mac:

```bash
apc config use-cluster lan-lab
apc get nodes -o wide
apc helm upgrade --install web examples/helm/web \
  --set replicaCount=2 \
  --set topologySpread.whenUnsatisfiable=DoNotSchedule \
  --wait --timeout 2m
apc get pods -o wide
apc cluster doctor lan-lab --skip-egress
```

### Restrict cluster ports with PF

Render and inspect the exact rules before a privileged installation on each
Mac:

```bash
apc system firewall render --cluster lan-lab --role server \
  --interface en0 --local-ip SERVER_ADDRESS --peer AGENT_ADDRESS

sudo apc system firewall install --cluster lan-lab --role server \
  --interface en0 --local-ip SERVER_ADDRESS --peer AGENT_ADDRESS --yes
```

Reverse role and addresses on the agent. The managed anchor permits only
listed peers to reach TCP 16443 (K3s API), TCP 10250 (kubelet) and UDP 8472
(Flannel VXLAN). `firewall status` validates both the root-owned installation
and the complete live anchor.

### Supervise a node

A per-user Background LaunchAgent is convenient for an active login session:

```bash
apc system install --role server --cluster lan-lab \
  --executable "$HOME/.local/bin/apc"
apc system status --role server --cluster lan-lab
```

For zero-login boot, use the explicit unattended profile with a dedicated
non-root account. Its root LaunchDaemon drops privileges before APC opens its
bounded log. See [the CLI guide](cli.md#lifecycle-backup-and-upgrades).

## Current two-Mac lab state

The two-Mac topology previously passed Kubernetes readiness, Helm placement,
DNS, ClusterIP, directed cross-host Pod HTTP, NetworkPolicy, image sync and
simultaneous VM-envelope replacement. It is **not currently Ready**: DHCP
changed the physical host addresses stored as the K3s server URL and advertise
addresses, so the saved worker is stopped instead of being left in a restart
loop. Reserve stable LAN addresses or use a stable authenticated overlay before
recreating/rejoining this topology.

The complete evidence and open gates are in the
[validation report](validation-results.md) and
[release-readiness checklist](release-readiness.md).
