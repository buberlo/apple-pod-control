#!/usr/bin/env bash

set -euo pipefail
umask 077

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
installer="$script_dir/install-github-runner.sh"
temporary_base=${TMPDIR:-/tmp}
temporary_base=${temporary_base%/}
test_root=$(/usr/bin/mktemp -d "$temporary_base/apc-runner-installer-test.XXXXXX")
test_root=$(cd "$test_root" && pwd -P)
trap '/bin/rm -rf "$test_root"' EXIT

test_home="$test_root/home"
fixture_root="$test_root/fixture"
fake_commands="$test_root/commands"
archive="$test_root/actions-runner-fixture.tar.gz"
launch_state="$test_root/launch-state"
launch_calls="$test_root/launch-calls"
system_bootout_delay="$test_root/system-bootout-delay"
config_calls="$test_root/config-calls"
install_calls="$test_root/install-calls"
system_plist_dir="$test_root/LaunchDaemons"
service_stage_root="$test_root/service-stage"

/bin/mkdir "$test_home" "$fixture_root" "$fixture_root/bin" "$fake_commands" "$system_plist_dir" "$service_stage_root"
/bin/chmod 700 "$test_home" "$fixture_root" "$fake_commands" "$system_plist_dir" "$service_stage_root"
: >"$launch_state"
: >"$launch_calls"
: >"$system_bootout_delay"
: >"$config_calls"
: >"$install_calls"
/bin/chmod 600 "$launch_state" "$launch_calls" "$system_bootout_delay" "$config_calls" "$install_calls"

cat >"$fixture_root/config.sh" <<'FAKE_CONFIG'
#!/usr/bin/env bash
set -euo pipefail

mode=configure
if [[ "${1:-}" == "remove" ]]; then
  mode=remove
  shift
fi
token=""
repository_url=""
runner_name=""
runner_label=""
replace_requested=false
while (($# > 0)); do
  case "$1" in
    --token) token=$2; shift 2 ;;
    --url) repository_url=$2; shift 2 ;;
    --name) runner_name=$2; shift 2 ;;
    --labels) runner_label=$2; shift 2 ;;
    --work) shift 2 ;;
    --replace) replace_requested=true; shift ;;
    --unattended) shift ;;
    *) shift ;;
  esac
done
[[ ${#token} -ge 20 ]] || exit 71
printf 'official config diagnostic containing %s must be redacted\n' "$token"
printf '%s:replace=%s\n' "$mode" "$replace_requested" >>"$APC_RUNNER_FAKE_CONFIG_CALLS"
if [[ "$mode" == "remove" ]]; then
  /bin/rm -f .runner .credentials .credentials_rsaparams
  exit 0
fi
printf '{"agentName":"%s","gitHubUrl":"%s"}\n' "$runner_name" "$repository_url" >.runner
printf '%s\n' 'opaque-runner-credential' >.credentials
printf '%s\n' 'opaque-rsa-parameters' >.credentials_rsaparams
printf '%s\n' "$runner_label" >.env
/bin/chmod 644 .runner .credentials .credentials_rsaparams .env
FAKE_CONFIG

cat >"$fixture_root/bin/runsvc.sh" <<'FAKE_SERVICE'
#!/usr/bin/env bash
exit 0
FAKE_SERVICE

/bin/chmod 755 "$fixture_root/config.sh" "$fixture_root/bin/runsvc.sh"
(
  cd "$fixture_root"
  /usr/bin/tar -czf "$archive" .
)
fixture_sha=$(/usr/bin/shasum -a 256 "$archive" | /usr/bin/awk '{print $1}')

cat >"$fake_commands/uname" <<'FAKE_UNAME'
#!/usr/bin/env bash
case "${1:-}" in
  -s) printf '%s\n' Darwin ;;
  -m) printf '%s\n' arm64 ;;
  *) printf '%s\n' 'Darwin fixture arm64' ;;
esac
FAKE_UNAME

cat >"$fake_commands/curl" <<'FAKE_CURL'
#!/usr/bin/env bash
set -euo pipefail
output=""
while (($# > 0)); do
  case "$1" in
    --output) output=$2; shift 2 ;;
    *) shift ;;
  esac
done
[[ -n "$output" ]]
/bin/cp "$APC_RUNNER_FAKE_ARCHIVE" "$output"
FAKE_CURL

cat >"$fake_commands/launchctl" <<'FAKE_LAUNCHCTL'
#!/usr/bin/env bash
set -euo pipefail
command=${1:-}
shift || true
state=$APC_RUNNER_FAKE_LAUNCH_STATE
calls=$APC_RUNNER_FAKE_LAUNCH_CALLS
system_delay_state=$APC_RUNNER_FAKE_SYSTEM_BOOTOUT_DELAY_STATE
system_delay_prints=${APC_RUNNER_FAKE_SYSTEM_BOOTOUT_DELAY_PRINTS:-0}

remove_target() {
  local target=$1 temporary="$state.tmp"
  /usr/bin/grep -Fxv "$target" "$state" >"$temporary" || true
  /bin/mv "$temporary" "$state"
}

case "$command" in
  print)
    target=${1:-}
    if [[ "$target" == system/* && -s "$system_delay_state" ]]; then
      remaining=$(<"$system_delay_state")
      [[ "$remaining" =~ ^[1-9][0-9]*$ ]]
      remaining=$((remaining - 1))
      if ((remaining == 0)); then
        : >"$system_delay_state"
        remove_target "$target"
      else
        printf '%s\n' "$remaining" >"$system_delay_state"
      fi
      exit 0
    fi
    /usr/bin/grep -Fxq "$target" "$state"
    ;;
  enable)
    target=${1:-}
    printf 'enable:%s\n' "$target" >>"$calls"
    ;;
  bootstrap)
    domain=${1:-}
    plist=${2:-}
    label=$(/usr/bin/awk '/<key>Label<\/key>/ { getline; gsub(/^.*<string>|<\/string>.*$/, ""); print; exit }' "$plist")
    [[ -n "$domain" && -n "$label" ]]
    target="$domain/$label"
    if ! /usr/bin/grep -Fxq "$target" "$state"; then
      printf '%s\n' "$target" >>"$state"
    fi
    printf 'bootstrap:%s:%s\n' "$domain" "$plist" >>"$calls"
    ;;
  bootout)
    target=${1:-}
    if [[ "$target" == system/* && "$system_delay_prints" =~ ^[1-9][0-9]*$ ]]; then
      printf '%s\n' "$system_delay_prints" >"$system_delay_state"
    else
      remove_target "$target"
    fi
    printf 'bootout:%s\n' "$target" >>"$calls"
    ;;
  *)
    printf 'unexpected launchctl command: %s\n' "$command" >&2
    exit 72
    ;;
esac
FAKE_LAUNCHCTL

cat >"$fake_commands/id" <<'FAKE_ID'
#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
  -u)
    if (($# == 1)); then
      printf '%s\n' "$APC_RUNNER_FAKE_EFFECTIVE_UID"
    else
      printf '%s\n' "$APC_RUNNER_FAKE_SERVICE_UID"
    fi
    ;;
  -g)
    printf '%s\n' "$APC_RUNNER_FAKE_SERVICE_GID"
    ;;
  *) exit 73 ;;
esac
FAKE_ID

cat >"$fake_commands/dscl" <<'FAKE_DSCL'
#!/usr/bin/env bash
set -euo pipefail
[[ "${1:-}" == "." && "${2:-}" == "-read" && "${4:-}" == "NFSHomeDirectory" ]]
printf 'NFSHomeDirectory: %s\n' "$APC_RUNNER_FAKE_SERVICE_HOME"
FAKE_DSCL

cat >"$fake_commands/install" <<'FAKE_INSTALL'
#!/usr/bin/env bash
set -euo pipefail
owner=""
group=""
mode=""
while (($# > 2)); do
  case "$1" in
    -o) owner=$2; shift 2 ;;
    -g) group=$2; shift 2 ;;
    -m) mode=$2; shift 2 ;;
    *) exit 74 ;;
  esac
done
source=$1
destination=$2
[[ "$owner" == "root" && "$group" == "wheel" && "$mode" == "0644" ]]
/bin/cp "$source" "$destination"
/bin/chmod 644 "$destination"
printf 'install:owner=%s:group=%s:mode=%s\n' "$owner" "$group" "$mode" >>"$APC_RUNNER_FAKE_INSTALL_CALLS"
FAKE_INSTALL

/bin/chmod 700 "$fake_commands"/*

run_installer() {
  local actual_uid actual_gid
  actual_uid=$(/usr/bin/id -u)
  actual_gid=$(/usr/bin/id -g)
  env \
    HOME="$test_home" \
    APC_RUNNER_TESTING=1 \
    APC_RUNNER_TEST_COMMAND_DIR="$fake_commands" \
    APC_RUNNER_FAKE_ARCHIVE="$archive" \
    APC_RUNNER_FAKE_LAUNCH_STATE="$launch_state" \
    APC_RUNNER_FAKE_LAUNCH_CALLS="$launch_calls" \
    APC_RUNNER_FAKE_SYSTEM_BOOTOUT_DELAY_STATE="$system_bootout_delay" \
    APC_RUNNER_FAKE_SYSTEM_BOOTOUT_DELAY_PRINTS="${APC_RUNNER_FAKE_SYSTEM_BOOTOUT_DELAY_PRINTS:-0}" \
    APC_RUNNER_FAKE_CONFIG_CALLS="$config_calls" \
    APC_RUNNER_FAKE_INSTALL_CALLS="$install_calls" \
    APC_RUNNER_FAKE_EFFECTIVE_UID="${APC_RUNNER_FAKE_EFFECTIVE_UID:-$actual_uid}" \
    APC_RUNNER_FAKE_SERVICE_UID="$actual_uid" \
    APC_RUNNER_FAKE_SERVICE_GID="${APC_RUNNER_FAKE_SERVICE_GID_OVERRIDE:-$actual_gid}" \
    APC_RUNNER_FAKE_SERVICE_HOME="$test_home" \
    APC_RUNNER_TEST_SYSTEM_PLIST_DIR="$system_plist_dir" \
    APC_RUNNER_TEST_SERVICE_STAGE_ROOT="$service_stage_root" \
    "$installer" "$@"
}

run_service_installer() {
  APC_RUNNER_FAKE_EFFECTIVE_UID=0 run_installer "$@"
}

fixture_token="fixture-registration-value-${RANDOM}-$$"
install_output="$test_root/install-output"
if ! printf '%s\n' "$fixture_token" | run_installer install \
    --repository example/apple-pod-control \
    --name apc-test \
    --label apc-test \
    --version 9.9.9 \
    --sha256 "$fixture_sha" >"$install_output" 2>&1; then
  /usr/bin/perl -pe 's/fixture-registration-value-[A-Za-z0-9_-]+/[REDACTED-FIXTURE]/g' "$install_output" >&2
  printf '%s\n' 'fixture installation failed' >&2
  exit 1
fi

managed_root="$test_home/.local/share/apc-github-runner"
managed_plist="$test_home/Library/LaunchAgents/dev.applepodcontrol.github-runner.apc-test.plist"
uid=$(/usr/bin/id -u)

if [[ ! -d "$managed_root" || ! -f "$managed_plist" ]]; then
  /usr/bin/perl -pe 's/fixture-registration-value-[A-Za-z0-9_-]+/[REDACTED-FIXTURE]/g' "$install_output" >&2
  printf '%s\n' 'fixture installation returned success without its managed root and plist' >&2
  exit 1
fi

[[ "$(/usr/bin/stat -f '%Lp' "$managed_root")" == "700" ]]
for credential in .runner .credentials .credentials_rsaparams; do
  [[ "$(/usr/bin/stat -f '%Lp' "$managed_root/$credential")" == "600" ]]
done
[[ "$(/usr/bin/stat -f '%Lp' "$managed_root/.apc-runner-install")" == "600" ]]
[[ "$(/usr/bin/stat -f '%Lp' "$managed_plist")" == "600" ]]
/usr/bin/grep -q '<key>LimitLoadToSessionType</key>' "$managed_plist"
/usr/bin/grep -A1 '<key>LimitLoadToSessionType</key>' "$managed_plist" | /usr/bin/grep -q '<string>Background</string>'
/usr/bin/grep -A1 '<key>ProcessType</key>' "$managed_plist" | /usr/bin/grep -q '<string>Background</string>'
/usr/bin/grep -q '^bootstrap:user/'"$uid"':' "$launch_calls"
if /usr/bin/grep -q 'bootstrap:gui/' "$launch_calls"; then
  printf '%s\n' 'installer used the GUI launchd domain instead of the user Background domain' >&2
  exit 1
fi
/usr/bin/grep -q '\[REDACTED\]' "$install_output"
if /usr/bin/grep -R -F "$fixture_token" "$managed_root" "$managed_plist" "$install_output" "$launch_calls" "$config_calls" >/dev/null; then
  printf '%s\n' 'registration token survived in output, metadata, credentials, or local logs' >&2
  exit 1
fi
/usr/bin/grep -q '^configure:replace=false$' "$config_calls"

run_installer status --name apc-test >"$test_root/status-output"
/usr/bin/grep -q '^runner apc-test: loaded ' "$test_root/status-output"

/bin/chmod 770 "$test_home"
if run_installer status --name apc-test >/dev/null 2>&1; then
  printf '%s\n' 'runner status accepted a group-writable HOME directory' >&2
  exit 1
fi
/bin/chmod 700 "$test_home"
/bin/chmod +a "everyone allow read" "$test_home"
if run_installer status --name apc-test >/dev/null 2>&1; then
  printf '%s\n' 'runner status accepted an ACL grant on HOME' >&2
  exit 1
fi
/bin/chmod -N "$test_home"
run_installer status --name apc-test >"$test_root/status-after-home-hardening"

/bin/chmod +a "everyone allow read" "$managed_root/.credentials"
if run_installer status --name apc-test >/dev/null 2>&1; then
  printf '%s\n' 'runner status accepted an ACL grant on protected credentials' >&2
  exit 1
fi
/bin/chmod -N "$managed_root/.credentials"
run_installer status --name apc-test >"$test_root/status-after-acl-removal"

configure_count_before=$(/usr/bin/wc -l <"$config_calls" | /usr/bin/tr -d ' ')
bootstrap_count_before=$(/usr/bin/grep -c '^bootstrap:' "$launch_calls")
run_installer install \
  --repository example/apple-pod-control \
  --name apc-test \
  --label apc-test \
  --version 9.9.9 \
  --sha256 "$fixture_sha" </dev/null >"$test_root/idempotent-output"
configure_count_after=$(/usr/bin/wc -l <"$config_calls" | /usr/bin/tr -d ' ')
bootstrap_count_after=$(/usr/bin/grep -c '^bootstrap:' "$launch_calls")
[[ "$configure_count_before" == "$configure_count_after" ]]
[[ "$bootstrap_count_before" == "$bootstrap_count_after" ]]

service_user=runnerfixture
system_plist="$system_plist_dir/dev.applepodcontrol.github-runner.apc-test.plist"
if APC_RUNNER_FAKE_SERVICE_GID_OVERRIDE=0 run_service_installer status-service \
    --name apc-test \
    --user "$service_user" \
    --root "$managed_root" >/dev/null 2>&1; then
  printf '%s\n' 'system service accepted a non-root account with primary gid 0' >&2
  exit 1
fi
if run_installer install-service --name apc-test --user "$service_user" --root "$managed_root" --confirm >/dev/null 2>&1; then
  printf '%s\n' 'system service installation ran without root authority' >&2
  exit 1
fi
if run_service_installer install-service --name apc-test --user "$service_user" --root "$managed_root" >/dev/null 2>&1; then
  printf '%s\n' 'system service installation ran without explicit --confirm' >&2
  exit 1
fi
service_config_count_before=$(/usr/bin/wc -l <"$config_calls" | /usr/bin/tr -d ' ')
run_service_installer install-service \
  --name apc-test \
  --user "$service_user" \
  --root "$managed_root" \
  --confirm >"$test_root/service-install-output"
service_config_count_after=$(/usr/bin/wc -l <"$config_calls" | /usr/bin/tr -d ' ')
[[ "$service_config_count_before" == "$service_config_count_after" ]]
[[ -f "$system_plist" && ! -e "$managed_plist" ]]
[[ "$(/usr/bin/stat -f '%Lp' "$system_plist")" == "644" ]]
/usr/bin/grep -A1 '<key>UserName</key>' "$system_plist" | /usr/bin/grep -q "<string>$service_user</string>"
/usr/bin/grep -A1 '<key>ProcessType</key>' "$system_plist" | /usr/bin/grep -q '<string>Background</string>'
/usr/bin/grep -A1 '<key>SessionCreate</key>' "$system_plist" | /usr/bin/grep -q '<true/>'
if /usr/bin/grep -q '<key>LimitLoadToSessionType</key>' "$system_plist"; then
  printf '%s\n' 'system LaunchDaemon was incorrectly scoped to a login session' >&2
  exit 1
fi
/usr/bin/grep -q '^install:owner=root:group=wheel:mode=0644$' "$install_calls"
/usr/bin/grep -q '^bootstrap:system:' "$launch_calls"
/usr/bin/grep -Fxq "system/dev.applepodcontrol.github-runner.apc-test" "$launch_state"
if /usr/bin/grep -Fxq "user/$uid/dev.applepodcontrol.github-runner.apc-test" "$launch_state"; then
  printf '%s\n' 'system service installation left a duplicate user-domain job loaded' >&2
  exit 1
fi
run_service_installer status-service \
  --name apc-test \
  --user "$service_user" \
  --root "$managed_root" >"$test_root/service-status-output"
/usr/bin/grep -q '^runner apc-test system service: loaded ' "$test_root/service-status-output"
if run_installer status --name apc-test >/dev/null 2>&1; then
  printf '%s\n' 'user-mode status accepted a system-managed runner' >&2
  exit 1
fi

# A changed system plist must wait until an asynchronously booting-out old
# job is truly absent before bootstrap. Otherwise the stale `print` result can
# make the installer skip the replacement job and report a false success.
/usr/bin/plutil -replace ThrottleInterval -integer 11 "$system_plist"
delayed_bootstrap_before=$(/usr/bin/grep -c '^bootstrap:system:' "$launch_calls")
APC_RUNNER_FAKE_SYSTEM_BOOTOUT_DELAY_PRINTS=3 run_service_installer install-service \
  --name apc-test \
  --user "$service_user" \
  --root "$managed_root" \
  --confirm >"$test_root/service-delayed-replace-output"
delayed_bootstrap_after=$(/usr/bin/grep -c '^bootstrap:system:' "$launch_calls")
[[ "$delayed_bootstrap_after" -eq $((delayed_bootstrap_before + 1)) ]]
[[ ! -s "$system_bootout_delay" ]]
/usr/bin/grep -Fxq "system/dev.applepodcontrol.github-runner.apc-test" "$launch_state"
run_service_installer status-service \
  --name apc-test \
  --user "$service_user" \
  --root "$managed_root" >"$test_root/service-delayed-replace-status"

service_install_count_before=$(/usr/bin/wc -l <"$install_calls" | /usr/bin/tr -d ' ')
service_bootstrap_count_before=$(/usr/bin/grep -c '^bootstrap:system:' "$launch_calls")
run_service_installer install-service \
  --name apc-test \
  --user "$service_user" \
  --root "$managed_root" \
  --confirm >"$test_root/service-idempotent-output"
service_install_count_after=$(/usr/bin/wc -l <"$install_calls" | /usr/bin/tr -d ' ')
service_bootstrap_count_after=$(/usr/bin/grep -c '^bootstrap:system:' "$launch_calls")
[[ "$service_install_count_before" == "$service_install_count_after" ]]
[[ "$service_bootstrap_count_before" == "$service_bootstrap_count_after" ]]

run_service_installer uninstall-service \
  --name apc-test \
  --user "$service_user" \
  --root "$managed_root" \
  --confirm >"$test_root/service-uninstall-output"
[[ ! -e "$system_plist" && -d "$managed_root" ]]
if /usr/bin/grep -Fxq "system/dev.applepodcontrol.github-runner.apc-test" "$launch_state"; then
  printf '%s\n' 'system service uninstall left the LaunchDaemon loaded' >&2
  exit 1
fi

run_installer install \
  --repository example/apple-pod-control \
  --name apc-test \
  --label apc-test \
  --version 9.9.9 \
  --sha256 "$fixture_sha" </dev/null >"$test_root/user-service-restore-output"
[[ -f "$managed_plist" ]]
/usr/bin/grep -Fxq "user/$uid/dev.applepodcontrol.github-runner.apc-test" "$launch_state"

if run_installer install \
  --repository example/apple-pod-control \
  --name apc-test \
  --label changed-label \
  --version 9.9.9 \
  --sha256 "$fixture_sha" </dev/null >/dev/null 2>&1; then
  printf '%s\n' 'installer replaced a different managed configuration without --replace' >&2
  exit 1
fi

replacement_token="fixture-replacement-value-${RANDOM}-$$"
replacement_output="$test_root/replacement-output"
printf '%s\n' "$replacement_token" | run_installer install \
  --repository example/apple-pod-control \
  --name apc-test \
  --label changed-label \
  --version 9.9.9 \
  --sha256 "$fixture_sha" \
  --replace >"$replacement_output" 2>&1
/usr/bin/grep -q '\[REDACTED\]' "$replacement_output"
/usr/bin/grep -q '^configure:replace=true$' "$config_calls"
run_installer status --name apc-test >"$test_root/replacement-status-output"
/usr/bin/grep -q 'label=changed-label' "$test_root/replacement-status-output"
if /usr/bin/grep -R -F "$replacement_token" "$managed_root" "$managed_plist" "$replacement_output" "$launch_calls" "$config_calls" >/dev/null; then
  printf '%s\n' 'replacement token survived in output, metadata, credentials, or local logs' >&2
  exit 1
fi

wrong_root="$test_home/.local/share/wrong-digest-runner"
wrong_sha=$(printf '%064d' 0)
if printf '%s\n' "$fixture_token" | run_installer install \
  --repository example/apple-pod-control \
  --name wrong-digest \
  --label wrong-digest \
  --root "$wrong_root" \
  --version 9.9.8 \
  --sha256 "$wrong_sha" >/dev/null 2>&1; then
  printf '%s\n' 'installer accepted an archive with the wrong digest' >&2
  exit 1
fi
[[ ! -e "$wrong_root" ]]

if run_installer install --repository '../unsafe' --name apc-test --label apc-test >/dev/null 2>&1; then
  printf '%s\n' 'installer accepted an unsafe repository value' >&2
  exit 1
fi
if run_installer install --repository example/repository --name 'Bad Name' --label apc-test >/dev/null 2>&1; then
  printf '%s\n' 'installer accepted an unsafe runner name' >&2
  exit 1
fi
if run_installer install --repository example/repository --name apc-test --label 'UPPER' >/dev/null 2>&1; then
  printf '%s\n' 'installer accepted an unsafe runner label' >&2
  exit 1
fi
if run_installer install --repository example/repository --name apc-test --label apc-test --version 9.9.9 >/dev/null 2>&1; then
  printf '%s\n' 'installer accepted a version override without a digest override' >&2
  exit 1
fi
if run_installer install --repository example/repository --name apc-test --label apc-test --token value >/dev/null 2>&1; then
  printf '%s\n' 'installer accepted a token command-line option' >&2
  exit 1
fi
token_argument_output="$test_root/token-argument-output"
if run_installer install --repository example/repository --name apc-test --label apc-test \
  "--token=$fixture_token" >"$token_argument_output" 2>&1; then
  printf '%s\n' 'installer accepted an inline token command-line option' >&2
  exit 1
fi
if /usr/bin/grep -Fq "$fixture_token" "$token_argument_output"; then
  printf '%s\n' 'installer echoed a rejected token-like command-line value' >&2
  exit 1
fi
if run_installer uninstall --name apc-test </dev/null >/dev/null 2>&1; then
  printf '%s\n' 'uninstall proceeded without explicit --confirm' >&2
  exit 1
fi

removal_token="fixture-removal-value-${RANDOM}-$$"
uninstall_output="$test_root/uninstall-output"
printf '%s\n' "$removal_token" | run_installer uninstall --name apc-test --confirm >"$uninstall_output" 2>&1
/usr/bin/grep -q '\[REDACTED\]' "$uninstall_output"
if /usr/bin/grep -R -F "$removal_token" "$uninstall_output" "$launch_calls" "$config_calls" >/dev/null; then
  printf '%s\n' 'removal token survived in output or local logs' >&2
  exit 1
fi
[[ ! -e "$managed_root" && ! -e "$managed_plist" ]]
if /usr/bin/grep -Fxq "user/$uid/dev.applepodcontrol.github-runner.apc-test" "$launch_state"; then
  printf '%s\n' 'uninstall left the Background LaunchAgent loaded' >&2
  exit 1
fi

printf '%s\n' 'GitHub runner installer fixture tests passed'
