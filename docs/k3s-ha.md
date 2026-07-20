# Local K3s HA lab on Apple container

Apple Pod Control can run a three-server K3s cluster on one Apple Silicon Mac.
Every server is an OCI container executed by Apple container as its own native
ARM64 Linux micro-VM; APC does not use nested virtualization. K3s supplies the
Kubernetes API, controllers, scheduler and an embedded etcd member in every
server VM.

```text
 apc / kubectl / Helm
          |
          | kubeconfig (localhost API endpoint)
          v
 +---------------------- Apple Silicon Mac -----------------------+
 |                                                                |
 |  Apple network: private, routed VM subnet                       |
 |                                                                |
 |  server-1 VM          server-2 VM          server-3 VM          |
 |  K3s + etcd  <------> K3s + etcd  <------> K3s + etcd          |
 |  stable guest IP      stable guest IP      stable guest IP      |
 |  named data volume    named data volume    named data volume    |
 |       |                    |                    |                |
 |       +----------- Flannel VXLAN / Pod network ----------------+
 +-----------------------------------------------------------------+
```

Three members form a functional odd-sized etcd quorum and tolerate the loss of
one server VM. This is useful for development, upgrades and quorum-failure
testing. It is **one physical failure domain**, however: losing or rebooting the
Mac still makes the whole cluster unavailable. A future multi-Mac control plane
must give the guest VMs stable, mutually routed addresses, normally through an
authenticated host overlay. Three VMs on one Mac must not be described as
three-host production HA.

The quorum rules and server requirements follow the upstream
[K3s embedded-etcd HA documentation](https://docs.k3s.io/datastore/ha-embedded).
The VM and networking lifecycle uses the interfaces documented by
[Apple container 1.0](https://github.com/apple/container/blob/1.0.0/docs/how-to.md).

## Create and inspect the lab

Start Apple's runtime and create the three-member cluster:

```bash
container system start
apc cluster ha create ha-lab
```

When adopting the manually bootstrapped validation lab that predates this
command, declare its existing address allocation exactly. APC then verifies the
ownership labels and runtime identity before saving the configuration:

```bash
apc cluster ha create ha-lab \
  --network apc-ha-lab \
  --subnet 192.168.96.0/24 \
  --stable-ip-start 192.168.96.11 \
  --api-port-base 17443
```

Creation is idempotent for APC-owned resources. APC creates a private Apple
network, one persistent data volume per member and three server VM envelopes.
The K3s image is selected for ARM64, so the servers run without x86 emulation.
The join token and generated kubeconfig are stored in the cluster state
directory with mode `0600`; token values are passed through protected files,
never through command-line arguments.

Inspect both the Apple runtime and Kubernetes views:

```bash
apc cluster ha status ha-lab
apc --cluster ha-lab get nodes -o wide
apc --cluster ha-lab get pods -A -o wide
```

All three Kubernetes Nodes should report `Ready` and the roles
`control-plane,etcd`. Status checks each published API endpoint; Kubernetes
workload commands use the selected cluster's kubeconfig.

Each member binds a stable secondary guest address. Its route for the private
member subnet must explicitly select that stable address as the source. Without
that source route, etcd may originate a peer connection from Apple's transient
DHCP address, which does not match the peer certificate and prevents a member
from joining or recovering.

## Deploy images and Helm charts

Helm works unchanged because APC exposes a normal Kubernetes kubeconfig:

```bash
export KUBECONFIG="$(apc kubeconfig path ha-lab)"
helm upgrade --install web examples/helm/web \
  --set replicaCount=3 \
  --wait --timeout 3m

apc --cluster ha-lab get deployment,pod,service -o wide
```

Helm installs Kubernetes resources; it does not install container images into
Pods. A chart's `image` fields can reference Docker Hub or any OCI registry.
Prefer ARM64 or multi-platform images, for example
`docker.io/library/nginx:alpine`, so workloads run natively on Apple Silicon.
K3s's internal containerd pulls the selected platform.

The private Apple network may not have working registry egress on every macOS
and Apple container combination. In an air-gapped lab, pull or export the ARM64
OCI images on the Mac and import the archive into K3s containerd on **every**
server before scheduling Pods with `imagePullPolicy: Never`. An image present
on only one member is not sufficient when the Kubernetes scheduler moves a Pod.

## Stop, start and delete

```bash
apc cluster ha stop ha-lab
apc cluster ha start ha-lab
apc cluster ha status ha-lab

# Remove VM envelopes but retain the member data volumes:
apc cluster ha delete ha-lab --keep-data --yes

# Permanently remove APC-owned envelopes and member data:
apc cluster ha delete ha-lab --yes
```

`stop` intentionally stops all three servers and therefore takes the API
offline. `start` reuses the named member volumes and rejoins the same etcd
members; wait for all three Nodes before testing another failure. Deletion
checks APC ownership labels and requires `--yes`. Use `--keep-data` when the VM
envelopes are disposable but the cluster state must remain recoverable.

Do not delete or clone individual etcd volume contents by hand. Quorum changes
must be performed while enough healthy members remain, one member at a time.

## Current limitations

- A kubeconfig contains one selected localhost endpoint rather than a load
  balancer. Before every kubectl-compatible `apc` command, APC probes the HA
  members and rewrites that protected kubeconfig to the first reachable Ready
  API, so one server-VM outage is handled automatically. An already running
  external Helm or kubectl process cannot switch endpoints mid-request; a local
  VIP or proxy remains a future improvement for fully transparent clients.
- The legacy single-server `volume.tar` backup path is not an HA backup and must
  not be used for this cluster. Embedded-etcd recovery needs a K3s
  `etcd-snapshot`, a copy stored outside all member volumes, the matching server
  token, and a tested restore procedure. APC's HA snapshot/restore workflow is
  not yet exposed through the legacy backup commands.
- The lab survives one server-VM failure, not loss of the Mac. Cross-Mac HA
  requires routed or overlaid guest-to-guest peer traffic and three independent
  physical failure domains before it can claim host-level fault tolerance.
- The existing two-Mac APC worker cluster is separate. Creating `ha-lab` does
  not convert, join or replace that cluster.
