# K3s spike quickstart

This is the isolated APC v2 development path. It uses different container
names, state and ports from the running APC v1 installation.

## Prerequisites

```bash
container system start
make build
bin/apc doctor
```

Warnings about `container machine` are expected with Apple container 1.0. A
failed required check must be fixed before creating a node.

Install native Kubernetes clients on macOS:

```bash
brew install kubernetes-cli helm
```

## Create the single-node cluster

```bash
bin/apc cluster create spike
export KUBECONFIG="$(bin/apc kubeconfig path spike)"

bin/apc get nodes -o wide
bin/apc get pods -A
```

The default image is pinned by OCI digest. The API listens on port `16443`
inside the node VM and is published only at `https://127.0.0.1:16443`. Keeping
the internal and published ports equal is required because K3s advertises that
port to its remotedialer clients.

The create operation is idempotent. If the labelled node already exists, APC
adopts or starts it and refreshes the kubeconfig. Successful create/start also
selects the cluster as the active APC context, so subsequent Kubernetes
workload commands use the short `apc get/apply/logs/exec/...` form.

## Install the example web server with Helm

```bash
helm lint examples/helm/web
helm upgrade --install web examples/helm/web --wait --timeout 2m
helm list
bin/apc get deployment,pod,service -o wide
```

Expose it temporarily to the Mac:

```bash
bin/apc port-forward service/web-apc-web 18081:80
curl http://127.0.0.1:18081/
```

This flow pulls `docker.io/library/nginx:alpine` through K3s's containerd. Any
ARM64 or multi-architecture OCI image supported by Kubernetes can be configured
in `examples/helm/web/values.yaml`.

## Lifecycle and diagnostics

```bash
bin/apc cluster status spike
bin/apc cluster doctor spike
bin/apc cluster stop spike
bin/apc cluster start spike

# Bootstrap convenience when host kubectl is not installed:
bin/apc kubectl spike -- get pods -A

# Context management:
bin/apc config get-clusters
bin/apc config use-cluster spike
bin/apc config current-cluster
```

`cluster stop` stops the disposable VM envelope. `cluster start` deletes the
stopped envelope, creates a fresh one and reattaches the labelled Apple volume
that contains `/var/lib/rancher/k3s`. This preserves Kubernetes and Helm state
while avoiding an Apple container 1.0 issue where a directly restarted,
port-published VM can lose outbound connectivity. Destructive cluster deletion
is deliberately not part of this first spike command set.

## LAN preparation for the Mac mini gate

Run the preflight with the peer hostname or address before creating the LAN
server:

```bash
bin/apc doctor \
  --role server \
  --listen-address 0.0.0.0 \
  --peer MAC_MINI_LAN_ADDRESS
```

The LAN server will be created explicitly with both listen and advertise
addresses. Do not expose these ports outside the trusted network:

```bash
bin/apc cluster create lan-spike \
  --listen-address 0.0.0.0 \
  --advertise-address MACBOOK_LAN_ADDRESS
```

Write the join token to a protected file and copy the binary and token to the
second Mac. The token is never accepted as a command-line value:

```bash
JOIN_DIR="$HOME/Library/Application Support/apc/clusters/lan-spike"
bin/apc cluster write-join-token lan-spike \
  --output "$JOIN_DIR/agent-token"

ssh MAC_MINI 'mkdir -p "$HOME/.local/bin" && chmod 700 "$HOME/.local/bin"'
scp bin/apc MAC_MINI:.local/bin/apc
ssh MAC_MINI 'mkdir -p "$HOME/Library/Application Support/apc/clusters/lan-spike" && chmod 700 "$HOME/Library/Application Support/apc/clusters/lan-spike"'
scp "$JOIN_DIR/agent-token" \
  'MAC_MINI:Library/Application Support/apc/clusters/lan-spike/agent-token'
ssh MAC_MINI 'chmod 600 "$HOME/Library/Application Support/apc/clusters/lan-spike/agent-token"'
```

On the second Mac:

```bash
container system start
"$HOME/.local/bin/apc" node join lan-spike \
  --server-url https://MACBOOK_LAN_ADDRESS:16443 \
  --token-file "$HOME/Library/Application Support/apc/clusters/lan-spike/agent-token" \
  --node-name apc-macmini \
  --advertise-address MAC_MINI_LAN_ADDRESS
```

Then validate from the server Mac:

```bash
export KUBECONFIG="$(bin/apc kubeconfig path lan-spike)"
bin/apc config use-cluster lan-spike
bin/apc get nodes -o wide
helm upgrade --install web examples/helm/web \
  --set replicaCount=2 \
  --set topologySpread.whenUnsatisfiable=DoNotSchedule \
  --wait --timeout 2m
bin/apc get pods -o wide
```

The current spike uses unencrypted Flannel VXLAN and must remain on a trusted
LAN. K3s's built-in network-policy controller is disabled because it did not
recover correctly when Apple container assigned a new private VM address;
Kubernetes `NetworkPolicy` is therefore not enforced in this phase. See the
[validation report](k3s-spike-results.md) for the complete acceptance matrix
and remaining work.

When an APC v2 context is active, overlapping commands target Kubernetes. Use
`bin/apc --legacy get pods` to explicitly address the APC v1 REST control plane.
The complete routing and compatibility contract is documented in
[cli-v2.md](cli-v2.md).
