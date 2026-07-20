#!/usr/bin/env bash

# Read-only by default acceptance checks for the two physical APC Macs.
# The only live-cluster mutation implemented here is `apc cluster doctor`, and
# it requires both --deep-doctor and --allow-mutation.

set -uo pipefail

umask 077

role=""
cluster=""
output_dir=""
expected_nodes=2
doctor_timeout="5m"
check_timeout="2m"
allow_mutation=false
deep_doctor=false
include_egress=false

usage() {
  printf '%s\n' \
    "Usage: hardware-acceptance.sh --role server|agent --cluster NAME --output DIR [options]" \
    "" \
    "Options:" \
    "  --expected-nodes N    Minimum Ready nodes checked by the server (default: 2)" \
    "  --doctor-timeout D    Deep-doctor timeout, such as 5m (default: 5m)" \
    "  --check-timeout D     Portable watchdog for each check (default: 2m)" \
    "  --allow-mutation      Explicitly permit requested live-cluster mutations" \
    "  --deep-doctor         Run apc cluster doctor (requires --allow-mutation)" \
    "  --include-egress      Include public HTTPS probes in the deep doctor" \
    "  --help                Show this help"
}

die_usage() {
  printf 'error: %s\n' "$1" >&2
  usage >&2
  exit 64
}

while (($# > 0)); do
  case "$1" in
    --role)
      (($# >= 2)) || die_usage "--role requires a value"
      role=$2
      shift 2
      ;;
    --cluster)
      (($# >= 2)) || die_usage "--cluster requires a value"
      cluster=$2
      shift 2
      ;;
    --output)
      (($# >= 2)) || die_usage "--output requires a value"
      output_dir=$2
      shift 2
      ;;
    --expected-nodes)
      (($# >= 2)) || die_usage "--expected-nodes requires a value"
      expected_nodes=$2
      shift 2
      ;;
    --doctor-timeout)
      (($# >= 2)) || die_usage "--doctor-timeout requires a value"
      doctor_timeout=$2
      shift 2
      ;;
    --check-timeout)
      (($# >= 2)) || die_usage "--check-timeout requires a value"
      check_timeout=$2
      shift 2
      ;;
    --allow-mutation)
      allow_mutation=true
      shift
      ;;
    --deep-doctor)
      deep_doctor=true
      shift
      ;;
    --include-egress)
      include_egress=true
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      die_usage "unknown argument: $1"
      ;;
  esac
done

[[ "$role" == "server" || "$role" == "agent" ]] || die_usage "--role must be server or agent"
[[ "$cluster" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ && ${#cluster} -le 63 ]] || die_usage "--cluster must be a lowercase DNS label"
[[ -n "$output_dir" ]] || die_usage "--output is required"
[[ "$expected_nodes" =~ ^[1-9][0-9]*$ ]] || die_usage "--expected-nodes must be a positive integer"
[[ "$doctor_timeout" =~ ^[1-9][0-9]*[smh]$ ]] || die_usage "--doctor-timeout must be a positive duration ending in s, m, or h"
[[ "$check_timeout" =~ ^[1-9][0-9]*[smh]$ ]] || die_usage "--check-timeout must be a positive duration ending in s, m, or h"

if [[ "$deep_doctor" == true && "$allow_mutation" != true ]]; then
  die_usage "--deep-doctor is mutating and requires --allow-mutation"
fi
if [[ "$include_egress" == true && "$deep_doctor" != true ]]; then
  die_usage "--include-egress requires --deep-doctor"
fi
if [[ "$role" != "server" && "$deep_doctor" == true ]]; then
  die_usage "--deep-doctor must run on the server runner"
fi
if [[ -L "$output_dir" ]]; then
  die_usage "--output must not be a symbolic link"
fi
if [[ -d "$output_dir" && -n "$(ls -A "$output_dir" 2>/dev/null)" ]]; then
  die_usage "--output must be a new or empty directory"
fi

mkdir -p "$output_dir" || die_usage "could not create output directory"
chmod 700 "$output_dir" || die_usage "could not protect output directory"
output_dir=$(cd "$output_dir" && pwd -P) || die_usage "could not resolve output directory"

PATH="$HOME/.local/bin:${PATH:-}:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export PATH

# Artifacts intentionally contain operational evidence, not host identity or
# credentials. This EXIT trap runs on both passing and failing checks. If the
# platform redactor is unavailable or fails for a file, that raw file is
# discarded instead of being left uploadable.
sanitize_artifacts() {
  local artifact node_names redactor_failed=false
  local nodes_log="$output_dir/kubernetes-nodes.log"
  node_names=""
  if [[ -f "$nodes_log" && ! -L "$nodes_log" ]]; then
    node_names=$(awk 'NR > 1 && $1 ~ /^[A-Za-z0-9]([-A-Za-z0-9.]*[A-Za-z0-9])?$/ {print $1}' "$nodes_log" 2>/dev/null || true)
  fi

  for artifact in "$output_dir"/*; do
    [[ -f "$artifact" && ! -L "$artifact" ]] || continue
    if command -v perl >/dev/null 2>&1; then
      APC_REDACT_HOME=${HOME:-} \
      APC_REDACT_RUNNER_TEMP=${RUNNER_TEMP:-} \
      APC_REDACT_OUTPUT="$output_dir" \
      APC_REDACT_NODES="$node_names" \
      perl -pi -e '
        BEGIN {
          @paths = grep { length($_) } (
            $ENV{"APC_REDACT_OUTPUT"},
            $ENV{"APC_REDACT_RUNNER_TEMP"},
            $ENV{"APC_REDACT_HOME"},
          );
          @paths = sort { length($b) <=> length($a) } @paths;
          @nodes = grep { length($_) } split(/\n/, $ENV{"APC_REDACT_NODES"} || "");
        }
        for $path (@paths) { s/\Q$path\E/[PRIVATE-PATH]/g; }
        s{(?<![A-Za-z0-9_])/(?:Users|home)/[^/\s]+}{[PRIVATE-HOME]}g;
        s{(?<![:/A-Za-z0-9._-])/(?:Applications|Library|opt|private|tmp|usr|var)/[^\s"'"'"',;)\]]+}{[SYSTEM-PATH]}g;
        for $node (@nodes) { s/\Q$node\E/[NODE]/g; }
        if (
          /(?:authorization|password|secret|token|client-key-data|client-certificate-data|certificate-authority-data)["'"'"']?\s*[:=]/i ||
          /(?:--token|APC_TOKEN).*\bdefault\b/i ||
          /\bBearer\s+\S+/i
        ) {
          $_ = "[REDACTED-SENSITIVE-LINE]\n";
          next;
        }
        s{(?<![0-9A-Fa-f])(?:[0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}(?![0-9A-Fa-f])}{[MAC]}g;
        s{(?<![0-9])(?:(?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1?[0-9]?[0-9])(?![0-9])}{[IPv4]}g;
        s{(?<![0-9A-Fa-f:])([0-9A-Fa-f:]*:[0-9A-Fa-f:]+)(?![0-9A-Fa-f:])}{
          $candidate = $1;
          $colon_count = ($candidate =~ tr/:/:/);
          ($candidate =~ /::/ || $colon_count >= 3) ? "[IPv6]" : $candidate;
        }gex;
      ' "$artifact" >/dev/null 2>&1 || redactor_failed=true
    else
      redactor_failed=true
    fi
    if [[ "$redactor_failed" == true ]]; then
      printf '%s\n' 'Raw diagnostic discarded because artifact redaction was unavailable.' >"$artifact"
      redactor_failed=false
    fi
    chmod 600 "$artifact" 2>/dev/null || true
  done

  printf '%s\n' \
    'status=complete' \
    'redacted=private-paths,home-paths,system-helper-paths,node-identifiers,ipv4,ipv6,mac-addresses,credential-lines' \
    >"$output_dir/sanitization.txt"
  chmod 600 "$output_dir/sanitization.txt" 2>/dev/null || true
}

trap sanitize_artifacts EXIT

results_file="$output_dir/results.tsv"
summary_file="$output_dir/summary.md"
metadata_file="$output_dir/metadata.txt"
: >"$results_file"
: >"$metadata_file"
printf 'status\tcheck\tdetail\n' >>"$results_file"

checks=0
failures=0
warnings=0
mutation_executed=false

record_result() {
  local status=$1
  local name=$2
  local detail=$3
  checks=$((checks + 1))
  case "$status" in
    PASS) ;;
    WARN) warnings=$((warnings + 1)) ;;
    FAIL) failures=$((failures + 1)) ;;
    *) printf 'internal error: invalid result status %s\n' "$status" >&2; exit 70 ;;
  esac
  printf '%s\t%s\t%s\n' "$status" "$name" "$detail" >>"$results_file"
  printf '%-4s %s (%s)\n' "$status" "$name" "$detail"
}

duration_seconds() {
  local value=$1
  local number=${value%?}
  local multiplier seconds
  [[ "$number" =~ ^[1-9][0-9]*$ && ${#number} -le 5 ]] || return 1
  case "${value: -1}" in
    s) multiplier=1 ;;
    m) multiplier=60 ;;
    h) multiplier=3600 ;;
    *) return 1 ;;
  esac
  seconds=$((10#$number * multiplier))
  ((seconds <= 86400)) || return 1
  printf '%s\n' "$seconds"
}

run_with_watchdog() {
  local timeout=$1
  local log_file=$2
  shift 2
  local seconds command_pid command_pgid watchdog_pid status=0 attempt
  local timeout_marker="$log_file.timeout"
  seconds=$(duration_seconds "$timeout") || return 70
  rm -f -- "$timeout_marker"

  # A timed-out kubectl/apc process can have container/runtime children. Run
  # every check as its own job-control process group so TERM/KILL reaches the
  # complete tree before artifacts are sanitized and uploaded.
  set -m
  "$@" >"$log_file" 2>&1 &
  command_pid=$!
  command_pgid=$command_pid
  set +m
  (
    local watchdog_sleep_pid=""
    trap '
      if [[ -n "$watchdog_sleep_pid" ]]; then
        kill "$watchdog_sleep_pid" 2>/dev/null || true
        wait "$watchdog_sleep_pid" 2>/dev/null || true
      fi
      exit 0
    ' TERM INT
    sleep "$seconds" &
    watchdog_sleep_pid=$!
    wait "$watchdog_sleep_pid" || exit 0
    watchdog_sleep_pid=""
    if kill -0 -- "-$command_pgid" 2>/dev/null; then
      : >"$timeout_marker"
      kill -TERM -- "-$command_pgid" 2>/dev/null || true
      for attempt in {1..20}; do
        kill -0 -- "-$command_pgid" 2>/dev/null || exit 0
        sleep 0.1
      done
      kill -KILL -- "-$command_pgid" 2>/dev/null || true
      for attempt in {1..20}; do
        kill -0 -- "-$command_pgid" 2>/dev/null || exit 0
        sleep 0.1
      done
    fi
  ) &
  watchdog_pid=$!

  wait "$command_pid" || status=$?
  if [[ -f "$timeout_marker" ]]; then
    # The leader can exit on TERM before a child does. Let the watchdog finish
    # its group-wide escalation before returning to the sanitizer.
    wait "$watchdog_pid" 2>/dev/null || true
    printf 'check exceeded portable watchdog timeout %s\n' "$timeout" >>"$log_file"
    return 124
  fi
  kill "$watchdog_pid" 2>/dev/null || true
  wait "$watchdog_pid" 2>/dev/null || true
  # A command that exits after daemonizing a child is also fully reaped. Checks
  # are not allowed to leave background work mutating the host or artifacts.
  if kill -0 -- "-$command_pgid" 2>/dev/null; then
    kill -TERM -- "-$command_pgid" 2>/dev/null || true
    for attempt in {1..20}; do
      kill -0 -- "-$command_pgid" 2>/dev/null || break
      sleep 0.1
    done
    if kill -0 -- "-$command_pgid" 2>/dev/null; then
      kill -KILL -- "-$command_pgid" 2>/dev/null || true
    fi
  fi
  return "$status"
}

run_check_with_timeout() {
  local requirement=$1
  local name=$2
  local timeout=$3
  shift 3
  local log_file="$output_dir/$name.log"
  local status=0

  run_with_watchdog "$timeout" "$log_file" "$@" || status=$?
  if ((status == 0)); then
    record_result PASS "$name" "ok"
    return 0
  fi
  local detail="exit $status; see $name.log"
  if [[ -f "$log_file.timeout" ]]; then
    rm -f -- "$log_file.timeout"
    detail="timed out after $timeout; see $name.log"
  fi
  if [[ "$requirement" == "required" ]]; then
    record_result FAIL "$name" "$detail"
  else
    record_result WARN "$name" "$detail"
  fi
  return "$status"
}

run_check() {
  local requirement=$1
  local name=$2
  shift 2
  run_check_with_timeout "$requirement" "$name" "$check_timeout" "$@"
}

check_tool() {
  local name=$1
  local path=$2
  local log_file="$output_dir/tool-$name.log"
  if [[ -n "$path" && -x "$path" ]]; then
    printf '%s\n' "$path" >"$log_file"
    record_result PASS "tool-$name" "available"
    return 0
  fi
  printf '%s\n' "$name not found in the hardware runner PATH" >"$log_file"
  record_result FAIL "tool-$name" "not found"
  return 1
}

platform_check() {
  local system machine
  system=$(uname -s) || return
  machine=$(uname -m) || return
  printf 'system=%s\narchitecture=%s\n' "$system" "$machine"
  [[ "$system" == "Darwin" && "$machine" == "arm64" ]]
}

kubeconfig_permissions_check() {
  local path=$1
  local mode=""
  [[ -f "$path" && ! -L "$path" ]] || return 1
  mode=$(stat -f '%Lp' "$path" 2>/dev/null) || mode=$(stat -c '%a' "$path" 2>/dev/null) || return 1
  printf 'regular-file=true\nsymlink=false\nmode=%s\n' "$mode"
  [[ "$mode" == "600" ]]
}

minimum_ready_nodes_check() {
  local nodes_log=$1
  local minimum=$2
  local ready
  ready=$(awk 'NR > 1 && $2 ~ /^Ready/ {count++} END {print count + 0}' "$nodes_log") || return
  printf 'ready=%s\nminimum=%s\n' "$ready" "$minimum"
  ((ready >= minimum))
}

no_doctor_resources_check() {
  local listing
  listing=$("$apc_bin" kubectl "$cluster" -- get pods,services -A -o name) || return
  printf '%s\n' "$listing"
  if printf '%s\n' "$listing" | grep -E '(^|/)apc-doctor-' >/dev/null; then
    printf 'temporary apc-doctor resources remain after the diagnostic\n' >&2
    return 1
  fi
}

firewall_status_check() {
  local arguments=(--cluster "$cluster" system firewall status -o json)
  if ((EUID == 0)); then
    "$apc_bin" "${arguments[@]}"
    return
  fi
  if command -v sudo >/dev/null 2>&1; then
    # Never prompt or consume a credential in CI. A pre-existing, narrowly
    # scoped sudo policy may allow this read-only verification; APC does not
    # install or recommend a broad passwordless sudo rule.
    if sudo -n "$apc_bin" "${arguments[@]}"; then
      return 0
    fi
  fi
  printf '%s\n' \
    'Privileged PF status is unavailable to this unprivileged runner.' \
    'Verify PF state and reboot persistence separately with the exact manual root gate.' >&2
  return 77
}

{
  printf 'role=%s\n' "$role"
  printf 'cluster=%s\n' "$cluster"
  printf 'expected_ready_nodes=%s\n' "$expected_nodes"
  printf 'mutation_authorized=%s\n' "$allow_mutation"
  printf 'deep_doctor_requested=%s\n' "$deep_doctor"
  printf 'public_egress_requested=%s\n' "$include_egress"
  printf 'per_check_watchdog=%s\n' "$check_timeout"
  printf 'privileged_pf_policy=best-effort-sudo-n; separate-manual-root-and-reboot-gate\n'
  printf 'started_utc=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  if [[ -n "${GITHUB_SHA:-}" ]]; then
    printf 'repository_commit=%s\n' "$GITHUB_SHA"
  fi
} >>"$metadata_file"

run_check required platform platform_check || true
run_check required os-version sw_vers || true
run_check required kernel uname -mrs || true

apc_bin=$(command -v apc 2>/dev/null || true)
container_bin=$(command -v container 2>/dev/null || true)
kubectl_bin=$(command -v kubectl 2>/dev/null || true)
helm_bin=$(command -v helm 2>/dev/null || true)

check_tool apc "$apc_bin" || true
check_tool container "$container_bin" || true

# kubectl and Helm are control-plane administration tools. Requiring them on
# an agent-only Mac would add packages to the worker without improving its
# ability to run K3s or apple/container workloads.
if [[ "$role" == "server" ]]; then
  check_tool kubectl "$kubectl_bin" || true
  check_tool helm "$helm_bin" || true
fi

if [[ -n "$container_bin" ]]; then
  run_check required apple-container-version "$container_bin" --version || true
  run_check required apple-container-system "$container_bin" system status || true
fi
if [[ "$role" == "server" && -n "$kubectl_bin" ]]; then
  run_check required kubectl-client-version "$kubectl_bin" version --client --output=yaml || true
fi
if [[ "$role" == "server" && -n "$helm_bin" ]]; then
  run_check required helm-client-version "$helm_bin" version --short || true
fi

if [[ -n "$apc_bin" ]]; then
  run_check required apc-help "$apc_bin" --help || true
  run_check required apc-version "$apc_bin" version || true
  run_check required host-doctor "$apc_bin" doctor --role "$role" || true
  run_check required supervisor-status "$apc_bin" --cluster "$cluster" system status --role "$role" || true
  run_check optional firewall-status firewall_status_check || true

  if [[ "$role" == "agent" ]]; then
    run_check required agent-status "$apc_bin" node status "$cluster" || true
    if [[ -n "$container_bin" ]]; then
      run_check required k3s-agent-version "$container_bin" exec "apc-k3s-$cluster-agent" /bin/k3s --version || true
    fi
  else
    run_check required cluster-status "$apc_bin" cluster status "$cluster" -o json || true
    run_check required k3s-api-ready "$apc_bin" kubectl "$cluster" -- get --raw=/readyz || true
    run_check required k3s-version "$apc_bin" kubectl "$cluster" -- version -o yaml || true
    if run_check required kubernetes-nodes "$apc_bin" kubectl "$cluster" -- get nodes -o wide; then
      run_check required ready-node-count minimum_ready_nodes_check "$output_dir/kubernetes-nodes.log" "$expected_nodes" || true
    fi
    run_check required kubernetes-pods "$apc_bin" kubectl "$cluster" -- get pods -A -o wide || true
    run_check optional kubernetes-events "$apc_bin" kubectl "$cluster" -- get events -A --sort-by=.metadata.creationTimestamp || true

    kubeconfig_path=""
    if run_check required kubeconfig-path "$apc_bin" kubeconfig path "$cluster"; then
      kubeconfig_path=$(tail -n 1 "$output_dir/kubeconfig-path.log")
      if run_check required kubeconfig-permissions kubeconfig_permissions_check "$kubeconfig_path"; then
        if [[ -n "$kubectl_bin" ]]; then
          run_check required native-kubectl-server env "KUBECONFIG=$kubeconfig_path" "$kubectl_bin" version --output=yaml || true
        fi
        if [[ -n "$helm_bin" ]]; then
          run_check required helm-releases env "KUBECONFIG=$kubeconfig_path" "$helm_bin" list --all-namespaces || true
        fi
      fi
    fi

    if [[ "$deep_doctor" == true ]]; then
      mutation_executed=true
      doctor_arguments=(cluster doctor "$cluster" --output json --timeout "$doctor_timeout")
      if [[ "$include_egress" != true ]]; then
        doctor_arguments+=(--skip-egress)
      fi
      run_check_with_timeout required deep-cluster-doctor "$doctor_timeout" "$apc_bin" "${doctor_arguments[@]}" || true
      run_check required doctor-cleanup no_doctor_resources_check || true
    fi
  fi
fi

{
  printf 'finished_utc=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  printf 'mutation_executed=%s\n' "$mutation_executed"
  printf 'checks=%s\nfailures=%s\nwarnings=%s\n' "$checks" "$failures" "$warnings"
} >>"$metadata_file"

{
  printf '# APC two-Mac hardware acceptance\n\n'
  printf -- '- Role: `%s`\n' "$role"
  printf -- '- Cluster: `%s`\n' "$cluster"
  printf -- '- Live mutation authorized: `%s`\n' "$allow_mutation"
  printf -- '- Live mutation executed: `%s`\n' "$mutation_executed"
  printf -- '- Privileged PF and reboot-persistence proof: separate manual gate (`sudo -n` is only tried when already permitted)\n'
  printf -- '- Checks: `%s`; failures: `%s`; warnings: `%s`\n\n' "$checks" "$failures" "$warnings"
  printf '| Status | Check | Detail |\n'
  printf '| --- | --- | --- |\n'
  tail -n +2 "$results_file" | while IFS=$'\t' read -r status name detail; do
    printf '| %s | `%s` | %s |\n' "$status" "$name" "$detail"
  done
} >"$summary_file"

printf 'Artifacts: %s\n' "$output_dir"
if ((failures > 0)); then
  printf 'Hardware acceptance failed: %d required check(s) failed.\n' "$failures" >&2
  exit 1
fi
printf 'Hardware acceptance passed with %d warning(s).\n' "$warnings"
