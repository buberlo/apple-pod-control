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

APC reserves only its lifecycle commands (`cluster`, `node`, `doctor`,
`config`, `kubeconfig`, `kubectl` and `version`). Other top-level names are
forwarded as well, allowing future kubectl commands and kubectl plugins without
an APC release.

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

## APC v1 compatibility

If no current v2 cluster exists, overlapping commands retain their APC v1
behavior. Once a v2 context is selected, use `--legacy` explicitly:

```bash
apc --legacy get pods -o wide
apc --legacy apply -f examples/deployment.yaml
```
