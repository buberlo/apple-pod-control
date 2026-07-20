#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
harness="$script_dir/hardware-acceptance.sh"
test_root=$(mktemp -d "${TMPDIR:-/tmp}/apc-hardware-test.XXXXXX")
trap 'rm -rf "$test_root"' EXIT

fake_bin="$test_root/bin"
mkdir -p "$fake_bin"
calls="$test_root/calls.log"
kubeconfig="$test_root/kubeconfig.yaml"
: >"$calls"
printf '%s\n' 'apiVersion: v1' >"$kubeconfig"
chmod 600 "$kubeconfig"

cat >"$fake_bin/apc" <<'FAKE_APC'
#!/usr/bin/env bash
set -eu
printf 'apc %s\n' "$*" >>"$APC_FAKE_CALLS"
case "$*" in
  "--help")
    printf '%s\n' 'Usage: apc' '--token string (default "fake-default-token")'
    ;;
  "kubeconfig path "*)
    printf '%s\n' "$APC_FAKE_KUBECONFIG"
    ;;
  *"kubectl "*" get nodes -o wide")
    printf '%s\n' \
      'NAME               STATUS   ROLES                  AGE   VERSION          INTERNAL-IP' \
      'private-macbook    Ready    control-plane,master   1d    v1.36.2+k3s1   192.168.50.10' \
      'private-macmini    Ready    <none>                 1d    v1.36.2+k3s1   fd00:abcd::20'
    ;;
  *"kubectl "*" get pods,services -A -o name")
    printf '%s\n' 'pod/web-123' 'service/kubernetes'
    ;;
  *"cluster doctor "*)
    printf '%s\n' '{"failures":0}'
    ;;
  *"cluster status "*)
    printf '%s\n' \
      '{"runtimeState":"running","nodeName":"private-macbook","api":"https://192.168.50.10:16443"}' \
      'authorization: Bearer should-not-survive'
    ;;
  *"node status "*)
    printf '%s\n' 'lan-spike apc-k3s-lan-spike-agent running'
    ;;
  *"system firewall status"*)
    printf '%s\n' 'helper=/Library/PrivilegedHelperTools/apc-firewall peer=192.168.50.20'
    ;;
  *"version")
    printf '%s\n' 'APC Version: test'
    ;;
  *)
    printf '%s\n' 'ok peer=192.168.50.20 mac=aa:bb:cc:dd:ee:ff'
    ;;
esac
FAKE_APC

cat >"$fake_bin/container" <<'FAKE_CONTAINER'
#!/usr/bin/env bash
set -eu
printf 'container %s\n' "$*" >>"$APC_FAKE_CALLS"
case "$*" in
  "--version") printf '%s\n' 'container CLI version 1.0.0' ;;
  "system status") printf '%s\n' 'status: running' ;;
  *"/bin/k3s --version") printf '%s\n' 'k3s version v1.36.2+k3s1' ;;
  *) printf '%s\n' 'ok' ;;
esac
FAKE_CONTAINER

cat >"$fake_bin/kubectl" <<'FAKE_KUBECTL'
#!/usr/bin/env bash
set -eu
printf 'kubectl %s\n' "$*" >>"$APC_FAKE_CALLS"
printf '%s\n' 'clientVersion: test' 'serverVersion: v1.36.2+k3s1'
FAKE_KUBECTL

cat >"$fake_bin/helm" <<'FAKE_HELM'
#!/usr/bin/env bash
set -eu
printf 'helm %s\n' "$*" >>"$APC_FAKE_CALLS"
printf '%s\n' 'v4.0.0'
FAKE_HELM

cat >"$fake_bin/uname" <<'FAKE_UNAME'
#!/usr/bin/env bash
case "${1:-}" in
  -s) printf '%s\n' Darwin ;;
  -m) printf '%s\n' arm64 ;;
  *) printf '%s\n' 'Darwin 25.0.0 arm64' ;;
esac
FAKE_UNAME

cat >"$fake_bin/sw_vers" <<'FAKE_SW_VERS'
#!/usr/bin/env bash
if [[ "${APC_FAKE_HANG_SW_VERS:-}" == "1" ]]; then
  if [[ -n "${APC_FAKE_CHILD_MARKER:-}" ]]; then
    (
      trap '' TERM INT
      sleep 5
      printf '%s\n' survived >"$APC_FAKE_CHILD_MARKER"
      while :; do sleep 1; done
    ) &
  fi
  trap 'exit 143' TERM INT
  while :; do sleep 1; done
fi
printf '%s\n' 'ProductName: macOS' 'ProductVersion: 26.0'
FAKE_SW_VERS

cat >"$fake_bin/sudo" <<'FAKE_SUDO'
#!/usr/bin/env bash
set -eu
printf 'sudo %s\n' "$*" >>"$APC_FAKE_CALLS"
[[ "${1:-}" == "-n" ]] || {
  printf '%s\n' 'interactive sudo invocation refused by test fake' >&2
  exit 90
}
if [[ "${APC_FAKE_SUDO_FAIL:-}" == "1" ]]; then
  printf '%s\n' 'sudo: a password is required' >&2
  exit 1
fi
shift
exec "$@"
FAKE_SUDO

chmod 755 "$fake_bin"/*

run_harness() {
  env \
    PATH="$fake_bin:/usr/bin:/bin" \
    HOME="$test_root/home" \
    RUNNER_TEMP="$test_root" \
    APC_FAKE_CALLS="$calls" \
    APC_FAKE_KUBECONFIG="$kubeconfig" \
    APC_FAKE_HANG_SW_VERS="${APC_FAKE_HANG_SW_VERS:-}" \
    APC_FAKE_CHILD_MARKER="${APC_FAKE_CHILD_MARKER:-}" \
    "$harness" "$@"
}

mkdir -p "$test_root/home"

observe_output="$test_root/server-observe"
run_harness --role server --cluster lan-spike --output "$observe_output" --expected-nodes 2
grep -q $'^PASS\tready-node-count\t' "$observe_output/results.tsv"
grep -q '^mutation_executed=false$' "$observe_output/metadata.txt"
grep -q '^status=complete$' "$observe_output/sanitization.txt"
if grep -R -E 'private-macbook|private-macmini|192\.168\.50\.|fd00:abcd|aa:bb:cc:dd:ee:ff|should-not-survive|fake-default-token|/Library/PrivilegedHelperTools' "$observe_output" >/dev/null; then
  printf '%s\n' 'artifact sanitizer left host-specific or credential data behind' >&2
  exit 1
fi
if grep -R -F "$test_root" "$observe_output" >/dev/null; then
  printf '%s\n' 'artifact sanitizer left a runner/home path behind' >&2
  exit 1
fi
grep -q '\[NODE\]' "$observe_output/kubernetes-nodes.log"
grep -q '\[IPv4\]' "$observe_output/kubernetes-nodes.log"
grep -q '\[IPv6\]' "$observe_output/kubernetes-nodes.log"
grep -q '\[REDACTED-SENSITIVE-LINE\]' "$observe_output/cluster-status.log"
grep -q 'sudo -n .*system firewall status' "$calls"
if grep -qE '^sudo ([^-]|$)' "$calls"; then
  printf '%s\n' 'firewall check attempted interactive sudo' >&2
  exit 1
fi
if grep -q 'cluster doctor lan-spike' "$calls"; then
  printf '%s\n' 'observe mode unexpectedly ran the mutating doctor' >&2
  exit 1
fi

: >"$calls"
if run_harness --role server --cluster lan-spike --output "$test_root/refused" --deep-doctor >/dev/null 2>&1; then
  printf '%s\n' 'deep doctor ran without explicit mutation authorization' >&2
  exit 1
fi
if grep -q 'cluster doctor' "$calls"; then
  printf '%s\n' 'refused deep doctor still invoked APC' >&2
  exit 1
fi

: >"$calls"
mutation_output="$test_root/server-mutation"
run_harness --role server --cluster lan-spike --output "$mutation_output" \
  --expected-nodes 2 --allow-mutation --deep-doctor
grep -q 'cluster doctor lan-spike.*--skip-egress' "$calls"
grep -q '^mutation_executed=true$' "$mutation_output/metadata.txt"
grep -q $'^PASS\tdoctor-cleanup\t' "$mutation_output/results.tsv"
grep -q '^status=complete$' "$mutation_output/sanitization.txt"

: >"$calls"
agent_output="$test_root/agent-observe"
mv "$fake_bin/kubectl" "$fake_bin/kubectl.not-required-on-agent"
mv "$fake_bin/helm" "$fake_bin/helm.not-required-on-agent"
agent_started=$SECONDS
APC_FAKE_SUDO_FAIL=1 run_harness --role agent --cluster lan-spike --output "$agent_output"
if ((SECONDS - agent_started > 10)); then
  printf '%s\n' 'completed agent checks waited for a stale watchdog sleeper' >&2
  exit 1
fi
grep -q 'apc node status lan-spike' "$calls"
grep -q 'apc version$' "$calls"
if grep -q 'apc version --client' "$calls"; then
  printf '%s\n' 'agent mode used the retired client/server version split' >&2
  exit 1
fi
grep -q 'container exec apc-k3s-lan-spike-agent /bin/k3s --version' "$calls"
grep -q '^status=complete$' "$agent_output/sanitization.txt"
grep -q $'^WARN\tfirewall-status\t' "$agent_output/results.tsv"
grep -q $'^PASS\tapc-version\t' "$agent_output/results.tsv"
if grep -qE $'^(PASS|WARN|FAIL)\t(tool-kubectl|tool-helm|kubectl-client-version|helm-client-version)\t' "$agent_output/results.tsv"; then
  printf '%s\n' 'agent mode unexpectedly required control-plane administration tools' >&2
  exit 1
fi
if grep -q 'apc cluster status' "$calls"; then
  printf '%s\n' 'agent mode unexpectedly ran the server status path' >&2
  exit 1
fi

if run_harness --role server --cluster '../unsafe' --output "$test_root/invalid" >/dev/null 2>&1; then
  printf '%s\n' 'unsafe cluster name was accepted' >&2
  exit 1
fi

timeout_output="$test_root/server-timeout"
child_marker="$test_root/watchdog-child-survived"
timeout_started=$SECONDS
if APC_FAKE_HANG_SW_VERS=1 APC_FAKE_CHILD_MARKER="$child_marker" run_harness --role server --cluster lan-spike --output "$timeout_output" --check-timeout 1s >/dev/null 2>&1; then
  printf '%s\n' 'watchdog accepted a hanging required check' >&2
  exit 1
fi
if ((SECONDS - timeout_started > 10)); then
  printf '%s\n' 'portable per-check watchdog did not terminate promptly' >&2
  exit 1
fi
grep -q $'^FAIL\tos-version\ttimed out after 1s;' "$timeout_output/results.tsv"
grep -q $'^PASS\tplatform\t' "$timeout_output/results.tsv"
grep -q '^status=complete$' "$timeout_output/sanitization.txt"
sleep 3
if [[ -e "$child_marker" ]]; then
  printf '%s\n' 'watchdog left a descendant process alive after timeout' >&2
  exit 1
fi

printf '%s\n' 'hardware acceptance harness tests passed'
