#!/usr/bin/env bash

# Install a pinned GitHub Actions repository runner for an ARM64 macOS host.
# Registration and removal tokens are accepted only on stdin. The official
# GitHub config program necessarily receives the token in its transient argv;
# this wrapper disables xtrace, redacts its output, and never persists it.

set +x
set -euo pipefail
umask 077
PATH=/usr/bin:/bin:/usr/sbin:/sbin
export PATH
unset BASH_ENV ENV CDPATH GLOBIGNORE

readonly default_version="2.335.1"
readonly default_sha256="e1a9bc7a3661e06fa0b129d15c2064fe65dc81a431001d8958a9db1409b73769"
readonly metadata_name=".apc-runner-install"

uid=""
process_uid=""
effective_uid=""
share_root=""
install_root=""
runner_name=""
service_user=""
service_home=""
service_gid=""
repository=""
runner_label=""
runner_version="$default_version"
archive_sha256="$default_sha256"
version_overridden=false
sha_overridden=false
replace_existing=false
confirm_uninstall=false
registration_token=""
stage_dir=""
lock_dir=""
previous_dir=""
restore_previous=false
previous_agent_loaded=false
job_label=""
plist_path=""
status_temp=""
system_plist_dir="/Library/LaunchDaemons"
system_plist_path=""
service_stage_root="/var/tmp"
service_stage_dir=""
service_lock_dir=""
system_temp=""
system_owner_uid="0"
system_owner_gid="0"

UNAME=/usr/bin/uname
CURL=/usr/bin/curl
TAR=/usr/bin/tar
SHASUM=/usr/bin/shasum
LAUNCHCTL=/bin/launchctl
ID=/usr/bin/id
DSCL=/usr/bin/dscl
INSTALL=/usr/bin/install

usage() {
  cat <<'EOF'
Usage:
  install-github-runner.sh install --repository OWNER/REPO --name NAME --label LABEL [options]
  install-github-runner.sh status --name NAME [--root ABSOLUTE_PATH]
  install-github-runner.sh uninstall --name NAME [--root ABSOLUTE_PATH] --confirm
  sudo install-github-runner.sh install-service --name NAME --user USER --root PATH --confirm
  sudo install-github-runner.sh status-service --name NAME --user USER --root PATH
  sudo install-github-runner.sh uninstall-service --name NAME --user USER --root PATH --confirm

Install options:
  --root PATH       Install below $HOME/.local/share (default: apc-github-runner)
  --version V       Runner version override; requires --sha256
  --sha256 HEX      Archive digest override; requires --version
  --replace         Explicitly replace a different existing local installation

Token input:
  install reads one GitHub repository registration token from stdin.
  uninstall reads one GitHub repository removal token from stdin.
  Service commands are tokenless and never execute GitHub's config program.
  Tokens are deliberately not accepted as command-line options.
EOF
}

die() {
  printf 'error: %s\n' "$1" >&2
  exit "${2:-1}"
}

die_usage() {
  printf 'error: %s\n' "$1" >&2
  usage >&2
  exit 64
}

stat_uid() {
  /usr/bin/stat -f '%u' "$1"
}

stat_mode() {
  /usr/bin/stat -f '%Lp' "$1"
}

stat_gid() {
  /usr/bin/stat -f '%g' "$1"
}

require_no_acl_grants() {
  local path=$1 output line
  output=$(/bin/ls -lde "$path") || die "could not inspect ACLs on $path"
  while IFS= read -r line; do
    if [[ "$line" =~ ^[[:space:]]*[0-9]+: && "$line" == *" allow "* ]]; then
      die "$path has an extended ACL grant"
    fi
  done <<<"$output"
}

strip_tree_acls() {
  local root=$1 entry
  while IFS= read -r -d '' entry; do
    [[ -L "$entry" ]] && continue
    /bin/chmod -N "$entry" || die "could not remove inherited ACLs from the runner tree"
  done < <(/usr/bin/find "$root" -xdev -print0)
}

validate_tree_acl_grants() {
  local root=$1 entry
  while IFS= read -r -d '' entry; do
    require_no_acl_grants "$entry"
  done < <(/usr/bin/find "$root" -xdev -print0)
}

require_owned_not_writable_directory() {
  local path=$1 mode permissions
  [[ -d "$path" && ! -L "$path" ]] || die "$path must be a real directory, not a symbolic link"
  [[ "$(stat_uid "$path")" == "$uid" ]] || die "$path is not owned by the current user"
  mode=$(stat_mode "$path") || die "could not inspect $path"
  [[ "$mode" =~ ^[0-7]{3,4}$ ]] || die "could not validate permissions on $path"
  permissions=$((8#$mode))
  (( (permissions & 0022) == 0 )) || die "$path must not be group- or world-writable"
  require_no_acl_grants "$path"
}

require_root_owned_not_writable_directory() {
  local path=$1 mode permissions
  [[ -d "$path" && ! -L "$path" ]] || die "$path must be a real root-owned directory, not a symbolic link"
  [[ "$(stat_uid "$path")" == "0" ]] || die "$path must be owned by root"
  mode=$(stat_mode "$path") || die "could not inspect $path"
  [[ "$mode" =~ ^[0-7]{3,4}$ ]] || die "could not validate permissions on $path"
  permissions=$((8#$mode))
  (( (permissions & 0022) == 0 )) || die "$path must not be group- or world-writable"
  require_no_acl_grants "$path"
}

require_root_owned_ancestor_chain() {
  local path=$1 ancestor
  ancestor=${path%/*}
  [[ -n "$ancestor" ]] || ancestor=/
  while :; do
    require_root_owned_not_writable_directory "$ancestor"
    [[ "$ancestor" == "/" ]] && break
    ancestor=${ancestor%/*}
    [[ -n "$ancestor" ]] || ancestor=/
  done
}

require_system_plist_directory_chain() {
  local path
  [[ "$system_plist_dir" == "/Library/LaunchDaemons" ]] || \
    die "system LaunchDaemons directory has an unexpected path"
  for path in / /Library /Library/LaunchDaemons; do
    require_root_owned_not_writable_directory "$path"
  done
  [[ "$(stat_gid "$system_plist_dir")" == "0" ]] || \
    die "system LaunchDaemons directory has unexpected group ownership"
  require_mode "$system_plist_dir" 755
}

require_owned_regular_file() {
  local path=$1
  [[ -f "$path" && ! -L "$path" ]] || die "$path must be a regular file, not a symbolic link"
  [[ "$(stat_uid "$path")" == "$uid" ]] || die "$path is not owned by the current user"
  require_no_acl_grants "$path"
}

require_mode() {
  local path=$1 expected=$2 actual
  actual=$(stat_mode "$path") || die "could not inspect permissions on $path"
  [[ "$actual" == "$expected" ]] || die "$path must have mode $expected (found $actual)"
}

create_or_validate_directory() {
  local path=$1
  if [[ ! -e "$path" ]]; then
    /bin/mkdir "$path" || die "could not create $path"
    /bin/chmod -N "$path" || die "could not remove inherited ACLs from $path"
    /bin/chmod 700 "$path" || die "could not protect $path"
  fi
  require_owned_not_writable_directory "$path"
}

configure_test_commands() {
  local test_dir=${APC_RUNNER_TEST_COMMAND_DIR:-} tool
  [[ -z "$test_dir" ]] && return 0
  [[ "${APC_RUNNER_TESTING:-}" == "1" ]] || die "test command overrides require APC_RUNNER_TESTING=1"
  [[ "$test_dir" == /* && -d "$test_dir" && ! -L "$test_dir" ]] || die "invalid test command directory"
  [[ "$(stat_uid "$test_dir")" == "$uid" ]] || die "test command directory has the wrong owner"
  require_mode "$test_dir" 700
  for tool in uname curl launchctl id dscl install; do
    require_owned_regular_file "$test_dir/$tool"
    [[ -x "$test_dir/$tool" ]] || die "test command $tool is not executable"
  done
  UNAME="$test_dir/uname"
  CURL="$test_dir/curl"
  LAUNCHCTL="$test_dir/launchctl"
  ID="$test_dir/id"
  DSCL="$test_dir/dscl"
  INSTALL="$test_dir/install"
  system_owner_uid=$process_uid
  system_owner_gid=$(/usr/bin/id -g)
  system_plist_dir=${APC_RUNNER_TEST_SYSTEM_PLIST_DIR:-}
  service_stage_root=${APC_RUNNER_TEST_SERVICE_STAGE_ROOT:-}
  [[ "$system_plist_dir" == /* && "$service_stage_root" == /* ]] || die "test service paths must be absolute"
  require_owned_not_writable_directory "$system_plist_dir"
  require_mode "$system_plist_dir" 700
  require_owned_not_writable_directory "$service_stage_root"
  require_mode "$service_stage_root" 700
}

platform_preflight() {
  local mode=$1
  [[ "$($UNAME -s)" == "Darwin" ]] || die "the runner installer requires macOS"
  [[ "$($UNAME -m)" == "arm64" ]] || die "the runner installer requires native Apple Silicon (arm64)"
  case "$mode" in
    user) [[ "$effective_uid" != "0" ]] || die "registration and user-service commands must not run as root" ;;
    system) [[ "$effective_uid" == "0" ]] || die "system-service commands must run as root" ;;
    *) die "internal platform mode is invalid" ;;
  esac
}

validate_service_executable() {
  local source_path source_directory source_name mode permissions
  source_path=${BASH_SOURCE[0]}
  source_directory=$(cd "$(dirname "$source_path")" && pwd -P) || die "could not resolve the service installer path"
  source_name=${source_path##*/}
  source_path="$source_directory/$source_name"
  [[ -f "$source_path" && ! -L "$source_path" ]] || die "system-service commands require a regular installer file"
  [[ "$(stat_uid "$source_path")" == "$system_owner_uid" ]] || \
    die "system-service commands require a root-owned installer copy"
  mode=$(stat_mode "$source_path") || die "could not inspect the service installer"
  [[ "$mode" =~ ^[0-7]{3,4}$ ]] || die "could not validate service installer permissions"
  permissions=$((8#$mode))
  (( (permissions & 0022) == 0 )) || die "system-service installer must not be group- or world-writable"
  [[ -x "$source_path" ]] || die "system-service installer is not executable"
  require_no_acl_grants "$source_path"
}

validate_dns_label() {
  local value=$1 description=$2
  [[ ${#value} -le 63 && "$value" =~ ^[a-z0-9]([a-z0-9-]*[a-z0-9])?$ ]] || \
    die_usage "$description must be a lowercase DNS label of at most 63 characters"
}

validate_user_name() {
  local value=$1
  [[ ${#value} -le 32 && "$value" =~ ^[a-z_][a-z0-9_-]*$ ]] || \
    die_usage "--user must be a local macOS account name"
}

validate_repository() {
  local value=$1 owner repo
  [[ "$value" == */* && "$value" != */*/* ]] || die_usage "--repository must be OWNER/REPO"
  owner=${value%%/*}
  repo=${value#*/}
  [[ ${#owner} -le 39 && "$owner" =~ ^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$ ]] || \
    die_usage "invalid repository owner"
  [[ ${#repo} -le 100 && "$repo" =~ ^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$ ]] || \
    die_usage "invalid repository name"
}

validate_version_and_digest() {
  if [[ "$version_overridden" != "$sha_overridden" ]]; then
    die_usage "--version and --sha256 must be overridden together"
  fi
  [[ "$runner_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die_usage "invalid runner version"
  [[ "$archive_sha256" =~ ^[0-9a-f]{64}$ ]] || die_usage "--sha256 must be 64 lowercase hexadecimal characters"
}

prepare_paths() {
  local home_path relative root_leaf
  home_path=${HOME:-}
  [[ "$home_path" == /* && -d "$home_path" && ! -L "$home_path" ]] || die "HOME must identify a real absolute directory"
  [[ "$(stat_uid "$home_path")" == "$uid" ]] || die "HOME is not owned by the current user"
  [[ "$(cd "$home_path" && pwd -P)" == "$home_path" ]] || die "HOME must not resolve through symbolic links"
  require_owned_not_writable_directory "$home_path"
  if [[ -z "${APC_RUNNER_TEST_COMMAND_DIR:-}" ]]; then
    require_root_owned_ancestor_chain "$home_path"
  fi

  create_or_validate_directory "$home_path/.local"
  create_or_validate_directory "$home_path/.local/share"
  share_root="$home_path/.local/share"

  if [[ -z "$install_root" ]]; then
    install_root="$share_root/apc-github-runner"
  fi
  [[ "$install_root" == "$share_root"/* ]] || die_usage "--root must be directly below $share_root"
  relative=${install_root#"$share_root"/}
  [[ "$relative" != */* ]] || die_usage "--root must be directly below $share_root"
  root_leaf=$relative
  [[ "$root_leaf" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$ ]] || die_usage "invalid --root leaf name"

  create_or_validate_directory "$home_path/Library"
  create_or_validate_directory "$home_path/Library/LaunchAgents"
  job_label="dev.applepodcontrol.github-runner.$runner_name"
  plist_path="$home_path/Library/LaunchAgents/$job_label.plist"
  system_plist_path="$system_plist_dir/$job_label.plist"
}

prepare_service_paths() {
  local dscl_output relative root_leaf
  validate_user_name "$service_user"
  uid=$("$ID" -u "$service_user" 2>/dev/null) || die "the requested runner service user does not exist"
  service_gid=$("$ID" -g "$service_user" 2>/dev/null) || die "could not resolve the runner service user's primary group"
  [[ "$uid" =~ ^[1-9][0-9]*$ ]] || die "the runner service user must be non-root"
  [[ "$service_gid" =~ ^[1-9][0-9]*$ ]] || die "the runner service group must be non-root"
  dscl_output=$("$DSCL" . -read "/Users/$service_user" NFSHomeDirectory 2>/dev/null) || \
    die "could not resolve the runner service user's home directory"
  service_home=$(printf '%s\n' "$dscl_output" | /usr/bin/awk '$1 == "NFSHomeDirectory:" {$1=""; sub(/^ /, ""); print; exit}')
  [[ "$service_home" == /* && -d "$service_home" && ! -L "$service_home" ]] || die "runner service user has an invalid home directory"
  [[ "$(cd "$service_home" && pwd -P)" == "$service_home" ]] || die "runner service home must not resolve through symbolic links"
  require_owned_not_writable_directory "$service_home"
  if [[ -z "${APC_RUNNER_TEST_COMMAND_DIR:-}" ]]; then
    require_root_owned_ancestor_chain "$service_home"
  fi
  require_owned_not_writable_directory "$service_home/.local"
  require_owned_not_writable_directory "$service_home/.local/share"

  [[ -n "$install_root" ]] || die_usage "system-service commands require an explicit --root"
  [[ "$install_root" == "$service_home/.local/share"/* ]] || \
    die_usage "--root must be directly below the service user's .local/share directory"
  relative=${install_root#"$service_home/.local/share"/}
  [[ "$relative" != */* ]] || die_usage "--root must be directly below the service user's .local/share directory"
  root_leaf=$relative
  [[ "$root_leaf" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$ ]] || die_usage "invalid --root leaf name"
  [[ -d "$install_root" && ! -L "$install_root" ]] || die "managed runner installation does not exist"
  [[ "$(cd "$install_root" && pwd -P)" == "$install_root" ]] || die "runner install root must not resolve through symbolic links"

  job_label="dev.applepodcontrol.github-runner.$runner_name"
  plist_path="$service_home/Library/LaunchAgents/$job_label.plist"
  system_plist_path="$system_plist_dir/$job_label.plist"

  if [[ -z "${APC_RUNNER_TEST_COMMAND_DIR:-}" ]]; then
    require_system_plist_directory_chain
    [[ -d "$service_stage_root" && ! -L "$service_stage_root" ]] || die "service staging root is unavailable"
    [[ "$(stat_uid "$service_stage_root")" == "0" && "$(stat_gid "$service_stage_root")" == "0" ]] || \
      die "service staging root has unexpected ownership"
    [[ "$(stat_mode "$service_stage_root")" == "777" && -k "$service_stage_root" ]] || \
      die "service staging root must be a sticky mode-1777 directory"
    require_no_acl_grants "$service_stage_root"
  fi
}

acquire_lock() {
  lock_dir="$install_root.lock"
  if ! /bin/mkdir "$lock_dir" 2>/dev/null; then
    die "another runner install or uninstall operation is active for this root"
  fi
  /bin/chmod -N "$lock_dir" || die "could not remove inherited ACLs from runner operation lock"
  /bin/chmod 700 "$lock_dir" || die "could not protect runner operation lock"
}

agent_target() {
  printf 'user/%s/%s\n' "$uid" "$job_label"
}

agent_loaded() {
  "$LAUNCHCTL" print "$(agent_target)" >/dev/null 2>&1
}

bootout_agent() {
  if agent_loaded; then
    "$LAUNCHCTL" bootout "$(agent_target)" >/dev/null || die "could not unload the runner Background LaunchAgent"
  fi
  # launchctl may acknowledge bootout before `print` stops exposing the old
  # job. Do not publish or validate a system service while that duplicate can
  # still race it for the same durable runner identity.
  local attempt
  for attempt in {1..50}; do
    if ! agent_loaded; then
      return 0
    fi
    /bin/sleep 0.1
  done
  die "runner Background LaunchAgent remained visible after bootout"
}

bootstrap_agent() {
  "$LAUNCHCTL" enable "$(agent_target)" >/dev/null || die "could not enable the runner Background LaunchAgent"
  "$LAUNCHCTL" bootstrap "user/$uid" "$plist_path" >/dev/null || die "could not load the runner in the user launchd domain"
  agent_loaded || die "runner Background LaunchAgent did not appear in the user launchd domain"
}

system_target() {
  printf 'system/%s\n' "$job_label"
}

system_service_loaded() {
  "$LAUNCHCTL" print "$(system_target)" >/dev/null 2>&1
}

bootout_system_service() {
  if system_service_loaded; then
    "$LAUNCHCTL" bootout "$(system_target)" >/dev/null || die "could not unload the runner system LaunchDaemon"
  fi
  # launchctl can acknowledge a system-domain bootout before the old job
  # disappears from `print`. Do not let a stale positive result suppress the
  # bootstrap of the replacement plist or make uninstall report a false
  # success.
  local attempt
  for attempt in {1..50}; do
    if ! system_service_loaded; then
      return 0
    fi
    /bin/sleep 0.1
  done
  die "runner system LaunchDaemon remained visible after bootout"
}

bootstrap_system_service() {
  "$LAUNCHCTL" enable "$(system_target)" >/dev/null || die "could not enable the runner system LaunchDaemon"
  "$LAUNCHCTL" bootstrap system "$system_plist_path" >/dev/null || die "could not load the runner in the system launchd domain"
  system_service_loaded || die "runner LaunchDaemon did not appear in the system launchd domain"
}

xml_escape() {
  local value=$1
  value=${value//&/\&amp;}
  value=${value//</\&lt;}
  value=${value//>/\&gt;}
  value=${value//\"/\&quot;}
  value=${value//\'/\&apos;}
  printf '%s' "$value"
}

render_plist() {
  local destination=$1 home_path=${2:-$HOME} escaped_root escaped_home
  escaped_root=$(xml_escape "$install_root")
  escaped_home=$(xml_escape "$home_path")
  cat >"$destination" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$job_label</string>
  <key>ProgramArguments</key>
  <array>
    <string>$escaped_root/runsvc.sh</string>
  </array>
  <key>WorkingDirectory</key>
  <string>$escaped_root</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>ACTIONS_RUNNER_SVC</key>
    <string>1</string>
    <key>HOME</key>
    <string>$escaped_home</string>
  </dict>
  <key>LimitLoadToSessionType</key>
  <string>Background</string>
  <key>ProcessType</key>
  <string>Background</string>
  <key>SessionCreate</key>
  <true/>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>10</integer>
  <key>StandardOutPath</key>
  <string>/dev/null</string>
  <key>StandardErrorPath</key>
  <string>/dev/null</string>
</dict>
</plist>
EOF
  /bin/chmod -N "$destination" || die "could not remove inherited ACLs from the runner LaunchAgent plist"
  /bin/chmod 600 "$destination"
  /usr/bin/plutil -lint "$destination" >/dev/null || die "generated an invalid runner LaunchAgent plist"
}

render_system_plist() {
  local destination=$1 escaped_root escaped_home escaped_user
  escaped_root=$(xml_escape "$install_root")
  escaped_home=$(xml_escape "$service_home")
  escaped_user=$(xml_escape "$service_user")
  cat >"$destination" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$job_label</string>
  <key>ProgramArguments</key>
  <array>
    <string>$escaped_root/runsvc.sh</string>
  </array>
  <key>WorkingDirectory</key>
  <string>$escaped_root</string>
  <key>UserName</key>
  <string>$escaped_user</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>ACTIONS_RUNNER_SVC</key>
    <string>1</string>
    <key>HOME</key>
    <string>$escaped_home</string>
  </dict>
  <key>ProcessType</key>
  <string>Background</string>
  <key>SessionCreate</key>
  <true/>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>10</integer>
  <key>StandardOutPath</key>
  <string>/dev/null</string>
  <key>StandardErrorPath</key>
  <string>/dev/null</string>
</dict>
</plist>
EOF
  /bin/chmod -N "$destination" || die "could not remove inherited ACLs from the runner system LaunchDaemon plist"
  /bin/chmod 600 "$destination"
  /usr/bin/plutil -lint "$destination" >/dev/null || die "generated an invalid runner system LaunchDaemon plist"
}

install_or_validate_plist() {
  local expected=$1 changed=false loaded=false
  render_plist "$expected"
  if [[ -e "$plist_path" || -L "$plist_path" ]]; then
    require_owned_regular_file "$plist_path"
    require_mode "$plist_path" 600
    if ! /usr/bin/cmp -s "$expected" "$plist_path"; then
      changed=true
    fi
  else
    changed=true
  fi

  if agent_loaded; then
    loaded=true
  fi
  if [[ "$changed" == true ]]; then
    if [[ "$loaded" == true ]]; then
      bootout_agent
      loaded=false
    fi
    /bin/mv -f "$expected" "$plist_path"
    require_owned_regular_file "$plist_path"
    require_mode "$plist_path" 600
  fi
  if [[ "$loaded" != true ]]; then
    bootstrap_agent
  fi
}

require_system_plist() {
  local path=$1
  require_owned_regular_file_as "$path" "$system_owner_uid"
  [[ "$(stat_gid "$path")" == "$system_owner_gid" ]] || die "$path has the wrong group"
  require_mode "$path" 644
  require_no_acl_grants "$path"
}

require_owned_regular_file_as() {
  local path=$1 expected_uid=$2
  [[ -f "$path" && ! -L "$path" ]] || die "$path must be a regular file, not a symbolic link"
  [[ "$(stat_uid "$path")" == "$expected_uid" ]] || die "$path has unexpected ownership"
}

acquire_service_lock() {
  service_lock_dir="$service_stage_root/.apc-runner-service-$runner_name.lock"
  if ! /bin/mkdir "$service_lock_dir" 2>/dev/null; then
    die "another system-service operation is active for this runner"
  fi
  /bin/chmod -N "$service_lock_dir" || die "could not remove inherited ACLs from system-service operation lock"
  /bin/chmod 700 "$service_lock_dir" || die "could not protect system-service operation lock"
  [[ "$(stat_uid "$service_lock_dir")" == "$process_uid" ]] || die "system-service operation lock has the wrong owner"
}

install_system_plist_file() {
  local source=$1
  system_temp="$system_plist_dir/.$job_label.new"
  [[ ! -e "$system_temp" && ! -L "$system_temp" ]] || die "a stale system LaunchDaemon staging file exists"
  "$INSTALL" -o root -g wheel -m 0644 "$source" "$system_temp" || die "could not stage the system LaunchDaemon plist"
  /bin/chmod -N "$system_temp" || die "could not remove inherited ACLs from the staged system LaunchDaemon plist"
  require_system_plist "$system_temp"
  /bin/mv "$system_temp" "$system_plist_path" || die "could not atomically install the system LaunchDaemon plist"
  system_temp=""
  require_system_plist "$system_plist_path"
}

remove_exact_user_agent() {
  local expected=$1
  if [[ -e "$plist_path" || -L "$plist_path" ]]; then
    require_owned_regular_file "$plist_path"
    require_mode "$plist_path" 600
    /usr/bin/cmp -s "$expected" "$plist_path" || die "user LaunchAgent differs from the exact managed definition"
  fi
  if agent_loaded; then
    bootout_agent
  fi
  if [[ -e "$plist_path" || -L "$plist_path" ]]; then
    /bin/rm -f -- "$plist_path"
  fi
}

validate_system_service() {
  local expected=$1
  [[ -e "$system_plist_path" || -L "$system_plist_path" ]] || die "runner system LaunchDaemon plist is missing"
  require_system_plist "$system_plist_path"
  /usr/bin/cmp -s "$expected" "$system_plist_path" || die "runner system LaunchDaemon plist differs from the managed definition"
  system_service_loaded || die "runner system LaunchDaemon is not loaded in the system domain"
  if agent_loaded; then
    die "runner is also loaded in the user domain"
  fi
  [[ ! -e "$plist_path" && ! -L "$plist_path" ]] || die "user LaunchAgent plist remains and could start a duplicate runner after login"
}

validate_symlinks_within() {
  local root=$1 link resolved
  while IFS= read -r -d '' link; do
    resolved=$(/usr/bin/perl -MCwd=abs_path -e 'my $p = abs_path($ARGV[0]); exit 2 unless defined $p; print $p' "$link") || \
      die "runner tree contains an unresolved symbolic link"
    case "$resolved" in
      "$root"/*) ;;
      *) die "runner tree contains a symbolic link outside its install root" ;;
    esac
  done < <(/usr/bin/find "$root" -xdev -type l -print0)
}

validate_install_tree() {
  local foreign credential
  require_owned_not_writable_directory "$install_root"
  require_mode "$install_root" 700
  foreign=$(/usr/bin/find "$install_root" -xdev ! -user "$uid" -print -quit)
  [[ -z "$foreign" ]] || die "runner installation contains an entry owned by another user"
  validate_symlinks_within "$install_root"
  validate_tree_acl_grants "$install_root"
  for credential in .runner .credentials .credentials_rsaparams; do
    require_owned_regular_file "$install_root/$credential"
    require_mode "$install_root/$credential" 600
  done
  for credential in config.sh runsvc.sh; do
    require_owned_regular_file "$install_root/$credential"
    [[ -x "$install_root/$credential" ]] || die "$credential is not executable"
  done
  require_owned_regular_file "$install_root/$metadata_name"
  require_mode "$install_root/$metadata_name" 600
}

metadata_repository=""
metadata_runner_name=""
metadata_label=""
metadata_version=""
metadata_sha256=""

read_metadata() {
  local file="$install_root/$metadata_name" key value seen_format=0 seen_repo=0 seen_name=0 seen_label=0 seen_version=0 seen_sha=0
  metadata_repository=""
  metadata_runner_name=""
  metadata_label=""
  metadata_version=""
  metadata_sha256=""
  [[ -f "$file" && ! -L "$file" ]] || return 1
  while IFS='=' read -r key value; do
    case "$key" in
      format)
        ((seen_format == 0)) || return 1
        [[ "$value" == "1" ]] || return 1
        seen_format=1
        ;;
      repository)
        ((seen_repo == 0)) || return 1
        metadata_repository=$value
        seen_repo=1
        ;;
      name)
        ((seen_name == 0)) || return 1
        metadata_runner_name=$value
        seen_name=1
        ;;
      label)
        ((seen_label == 0)) || return 1
        metadata_label=$value
        seen_label=1
        ;;
      version)
        ((seen_version == 0)) || return 1
        metadata_version=$value
        seen_version=1
        ;;
      sha256)
        ((seen_sha == 0)) || return 1
        metadata_sha256=$value
        seen_sha=1
        ;;
      *) return 1 ;;
    esac
  done <"$file"
  ((seen_format && seen_repo && seen_name && seen_label && seen_version && seen_sha)) || return 1
  validate_repository "$metadata_repository"
  validate_dns_label "$metadata_runner_name" "stored runner name"
  validate_dns_label "$metadata_label" "stored runner label"
  [[ "$metadata_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || return 1
  [[ "$metadata_sha256" =~ ^[0-9a-f]{64}$ ]] || return 1
}

metadata_matches_request() {
  read_metadata || return 1
  [[ "$metadata_repository" == "$repository" &&
     "$metadata_runner_name" == "$runner_name" &&
     "$metadata_label" == "$runner_label" &&
     "$metadata_version" == "$runner_version" &&
     "$metadata_sha256" == "$archive_sha256" ]]
}

write_metadata() {
  local root=$1 temporary
  temporary="$root/$metadata_name.tmp"
  cat >"$temporary" <<EOF
format=1
repository=$repository
name=$runner_name
label=$runner_label
version=$runner_version
sha256=$archive_sha256
EOF
  /bin/chmod -N "$temporary" || die "could not remove inherited ACLs from runner metadata"
  /bin/chmod 600 "$temporary"
  /bin/mv -f "$temporary" "$root/$metadata_name"
}

read_stdin_token() {
  local purpose=$1 read_status=0
  registration_token=""
  if [[ -t 0 ]]; then
    printf 'GitHub %s token (input hidden): ' "$purpose" >&2
    IFS= read -r -s registration_token || read_status=$?
    printf '\n' >&2
  else
    IFS= read -r registration_token || read_status=$?
  fi
  if ((read_status != 0)) && [[ -z "$registration_token" ]]; then
    die "could not read the GitHub $purpose token from stdin"
  fi
  [[ ${#registration_token} -ge 20 && ${#registration_token} -le 4096 ]] || {
    unset registration_token
    die "invalid GitHub $purpose token input"
  }
  [[ "$registration_token" =~ ^[A-Za-z0-9._-]+$ ]] || {
    unset registration_token
    die "invalid GitHub $purpose token input"
  }
  [[ "$registration_token" != *[[:space:]]* && "$registration_token" != *$'\n'* && "$registration_token" != *$'\r'* ]] || {
    unset registration_token
    die "invalid GitHub $purpose token input"
  }
}

redact_config_output() {
  local line redacted
  while IFS= read -r line || [[ -n "$line" ]]; do
    redacted=${line//"$registration_token"/[REDACTED]}
    printf '%s\n' "$redacted"
  done
}

run_configure() {
  local root=$1 status
  local config_arguments=(
    --unattended
    --url "https://github.com/$repository"
    --name "$runner_name"
    --labels "$runner_label"
    --work _work
    --disableupdate
  )
  if [[ "$replace_existing" == true ]]; then
    config_arguments+=(--replace)
  fi
  set +e
  (
    cd "$root" || exit 1
    ./config.sh "${config_arguments[@]}" --token "$registration_token"
  ) 2>&1 | redact_config_output
  status=${PIPESTATUS[0]}
  set -e
  unset registration_token
  return "$status"
}

run_remove_config() {
  local status
  set +e
  (
    cd "$install_root" || exit 1
    ./config.sh remove --unattended --token "$registration_token"
  ) 2>&1 | redact_config_output
  status=${PIPESTATUS[0]}
  set -e
  unset registration_token
  return "$status"
}

protect_configured_tree() {
  local root=$1 file
  validate_symlinks_within "$root"
  strip_tree_acls "$root"
  /bin/chmod 700 "$root"
  for file in .runner .credentials .credentials_rsaparams .env .path .service; do
    if [[ -e "$root/$file" || -L "$root/$file" ]]; then
      [[ -f "$root/$file" && ! -L "$root/$file" ]] || die "configured runner created an unsafe $file"
      [[ "$(stat_uid "$root/$file")" == "$uid" ]] || die "configured runner created $file with the wrong owner"
      /bin/chmod 600 "$root/$file"
    fi
  done
  for file in .runner .credentials .credentials_rsaparams; do
    [[ -f "$root/$file" && ! -L "$root/$file" ]] || die "GitHub configuration did not create $file"
    /bin/chmod 600 "$root/$file"
  done
  if [[ -d "$root/_diag" && ! -L "$root/_diag" ]]; then
    /usr/bin/find "$root/_diag" -type f -delete
  fi
}

validate_archive_members() {
  local archive=$1 member
  while IFS= read -r member; do
    [[ -n "$member" && "$member" != /* ]] || die "runner archive contains an unsafe member path"
    case "/$member/" in
      */../*|*/./../*|*/.././*) die "runner archive contains parent traversal" ;;
    esac
  done < <("$TAR" -tzf "$archive")
}

prepare_candidate() {
  local archive="$stage_dir/runner.tar.gz" candidate="$stage_dir/candidate" actual_sha url
  url="https://github.com/actions/runner/releases/download/v$runner_version/actions-runner-osx-arm64-$runner_version.tar.gz"
  "$CURL" --disable --fail --location --proto '=https' --tlsv1.2 --output "$archive" "$url" || die "could not download the pinned GitHub runner archive"
  /bin/chmod -N "$archive" || die "could not remove inherited ACLs from runner archive"
  /bin/chmod 600 "$archive"
  actual_sha=$("$SHASUM" -a 256 "$archive" | /usr/bin/awk '{print $1}') || die "could not hash the runner archive"
  [[ "$actual_sha" == "$archive_sha256" ]] || die "runner archive SHA-256 mismatch"
  validate_archive_members "$archive"
  /bin/mkdir "$candidate"
  /bin/chmod -N "$candidate" || die "could not remove inherited ACLs from runner staging"
  /bin/chmod 700 "$candidate"
  "$TAR" -xzf "$archive" -C "$candidate" || die "could not extract the verified runner archive"
  [[ -f "$candidate/config.sh" && ! -L "$candidate/config.sh" && -x "$candidate/config.sh" ]] || die "verified runner archive is missing config.sh"
  [[ -f "$candidate/bin/runsvc.sh" && ! -L "$candidate/bin/runsvc.sh" && -x "$candidate/bin/runsvc.sh" ]] || \
    die "verified runner archive is missing bin/runsvc.sh"
  validate_symlinks_within "$candidate"
  strip_tree_acls "$candidate"
  validate_tree_acl_grants "$candidate"
  printf '%s\n' "$candidate"
}

# GitHub's macOS archive keeps the service entrypoint below bin/. The official
# svc.sh copies it to the configured runner root during service installation.
# APC does not call svc.sh because it renders its own fail-closed LaunchAgent or
# LaunchDaemon, so reproduce only that tokenless, local copy step here.
prepare_service_entrypoint() {
  local root=$1 source="$1/bin/runsvc.sh" destination="$1/runsvc.sh"
  require_owned_regular_file "$source"
  [[ -x "$source" ]] || die "verified runner service entrypoint is not executable"
  [[ ! -e "$destination" && ! -L "$destination" ]] || \
    die "runner configuration created an unexpected root service entrypoint"
  /bin/cp "$source" "$destination" || die "could not prepare runner service entrypoint"
  /bin/chmod -N "$destination" || die "could not remove inherited ACLs from runner service entrypoint"
  /bin/chmod 700 "$destination" || die "could not protect runner service entrypoint"
  require_owned_regular_file "$destination"
  require_mode "$destination" 700
}

validate_existing_for_replacement() {
  local foreign
  require_owned_not_writable_directory "$install_root"
  require_mode "$install_root" 700
  foreign=$(/usr/bin/find "$install_root" -xdev ! -user "$uid" -print -quit)
  [[ -z "$foreign" ]] || die "existing runner root contains an entry owned by another user"
  validate_symlinks_within "$install_root"
  validate_tree_acl_grants "$install_root"
}

cleanup() {
  local status=$?
  set +e
  unset registration_token
  if [[ "$restore_previous" == true && -n "$previous_dir" && -d "$previous_dir" && ! -e "$install_root" ]]; then
    /bin/mv "$previous_dir" "$install_root"
    if [[ "$previous_agent_loaded" == true && -f "$plist_path" ]]; then
      "$LAUNCHCTL" bootstrap "user/$uid" "$plist_path" >/dev/null 2>&1 || true
    fi
  fi
  if [[ -n "$stage_dir" ]]; then
    case "$stage_dir" in
      "$share_root"/.apc-runner-stage.*) /bin/rm -rf -- "$stage_dir" ;;
    esac
  fi
  if [[ -n "$status_temp" ]]; then
    case "$status_temp" in
      "$share_root"/.apc-runner-status.*) /bin/rm -f -- "$status_temp" ;;
    esac
  fi
  if [[ -n "$system_temp" ]]; then
    case "$system_temp" in
      "$system_plist_dir"/.dev.applepodcontrol.github-runner.*.new) /bin/rm -f -- "$system_temp" ;;
    esac
  fi
  if [[ -n "$service_stage_dir" ]]; then
    case "$service_stage_dir" in
      "$service_stage_root"/.apc-runner-service.*) /bin/rm -rf -- "$service_stage_dir" ;;
    esac
  fi
  if [[ -n "$service_lock_dir" ]]; then
    case "$service_lock_dir" in
      "$service_stage_root"/.apc-runner-service-*.lock) /bin/rmdir "$service_lock_dir" >/dev/null 2>&1 || true ;;
    esac
  fi
  if [[ -n "$lock_dir" ]]; then
    case "$lock_dir" in
      "$share_root"/*.lock) /bin/rmdir "$lock_dir" >/dev/null 2>&1 || true ;;
    esac
  fi
  return "$status"
}

status_command() {
  local expected
  [[ ! -e "$system_plist_path" && ! -L "$system_plist_path" ]] || \
    die "runner is managed by a system LaunchDaemon; use status-service as root"
  [[ -d "$install_root" ]] || die "managed runner installation does not exist"
  validate_install_tree
  read_metadata || die "runner metadata is invalid"
  [[ "$metadata_runner_name" == "$runner_name" ]] || die "runner name does not match the managed installation"
  expected=$(/usr/bin/mktemp "$share_root/.apc-runner-status.XXXXXX") || die "could not create protected status staging file"
  status_temp=$expected
  /bin/chmod 600 "$expected"
  render_plist "$expected"
  [[ -e "$plist_path" || -L "$plist_path" ]] || {
    /bin/rm -f "$expected"
    die "runner Background LaunchAgent plist is missing"
  }
  require_owned_regular_file "$plist_path"
  require_mode "$plist_path" 600
  if ! /usr/bin/cmp -s "$expected" "$plist_path"; then
    /bin/rm -f "$expected"
    die "runner Background LaunchAgent plist differs from the managed definition"
  fi
  /bin/rm -f "$expected"
  status_temp=""
  agent_loaded || die "runner Background LaunchAgent is not loaded in user/$uid"
  printf 'runner %s: loaded (repository=%s, label=%s, version=%s)\n' \
    "$runner_name" "$metadata_repository" "$metadata_label" "$metadata_version"
}

install_command() {
  local candidate expected existing=false
  [[ ! -e "$system_plist_path" && ! -L "$system_plist_path" ]] || \
    die "runner is managed by a system LaunchDaemon; remove that service before user-mode installation"
  acquire_lock
  if [[ -e "$install_root" || -L "$install_root" ]]; then
    [[ -d "$install_root" && ! -L "$install_root" ]] || die "runner install root is not a real directory"
    existing=true
    if metadata_matches_request; then
      validate_install_tree
      stage_dir=$(/usr/bin/mktemp -d "$share_root/.apc-runner-stage.XXXXXX") || die "could not create protected staging directory"
      /bin/chmod -N "$stage_dir" || die "could not remove inherited ACLs from runner staging directory"
      /bin/chmod 700 "$stage_dir"
      expected="$stage_dir/runner.plist"
      install_or_validate_plist "$expected"
      status_command
      return 0
    fi
    [[ "$replace_existing" == true ]] || die "an existing or differently configured runner was found; pass --replace to replace it explicitly"
    validate_existing_for_replacement
  fi

  stage_dir=$(/usr/bin/mktemp -d "$share_root/.apc-runner-stage.XXXXXX") || die "could not create protected staging directory"
  /bin/chmod -N "$stage_dir" || die "could not remove inherited ACLs from runner staging directory"
  /bin/chmod 700 "$stage_dir"
  require_owned_not_writable_directory "$stage_dir"
  require_mode "$stage_dir" 700
  candidate=$(prepare_candidate)

  read_stdin_token registration
  if [[ "$existing" == true ]]; then
    if agent_loaded; then
      previous_agent_loaded=true
      bootout_agent
    fi
    previous_dir="$stage_dir/previous"
    /bin/mv "$install_root" "$previous_dir"
    restore_previous=true
  fi

  if ! run_configure "$candidate"; then
    die "GitHub runner configuration failed"
  fi
  prepare_service_entrypoint "$candidate"
  protect_configured_tree "$candidate"
  write_metadata "$candidate"
  /bin/mv "$candidate" "$install_root"
  restore_previous=false
  validate_install_tree

  expected="$stage_dir/runner.plist"
  install_or_validate_plist "$expected"
  status_command
}

uninstall_command() {
  [[ "$confirm_uninstall" == true ]] || die_usage "uninstall requires --confirm"
  [[ ! -e "$system_plist_path" && ! -L "$system_plist_path" ]] || \
    die "runner system LaunchDaemon must be removed with uninstall-service before unregistering"
  acquire_lock
  validate_install_tree
  read_metadata || die "runner metadata is invalid"
  [[ "$metadata_runner_name" == "$runner_name" ]] || die "runner name does not match the managed installation"
  read_stdin_token removal
  previous_agent_loaded=false
  if agent_loaded; then
    previous_agent_loaded=true
    bootout_agent
  fi
  if ! run_remove_config; then
    if [[ "$previous_agent_loaded" == true ]]; then
      bootstrap_agent || true
    fi
    die "GitHub runner removal failed; the local installation was retained"
  fi
  if [[ -e "$plist_path" || -L "$plist_path" ]]; then
    require_owned_regular_file "$plist_path"
    /bin/rm -f -- "$plist_path"
  fi
  validate_existing_for_replacement
  /bin/rm -rf -- "$install_root"
  printf 'runner %s: unregistered and removed\n' "$runner_name"
}

prepare_service_stage() {
  service_stage_dir=$(/usr/bin/mktemp -d "$service_stage_root/.apc-runner-service.XXXXXX") || \
    die "could not create protected system-service staging directory"
  /bin/chmod -N "$service_stage_dir" || die "could not remove inherited ACLs from system-service staging"
  /bin/chmod 700 "$service_stage_dir"
  [[ "$(stat_uid "$service_stage_dir")" == "$process_uid" ]] || die "system-service staging directory has the wrong owner"
  require_mode "$service_stage_dir" 700
}

validate_service_registration() {
  validate_install_tree
  read_metadata || die "runner metadata is invalid"
  [[ "$metadata_runner_name" == "$runner_name" ]] || die "runner name does not match the managed installation"
}

install_service_command() {
  local expected_system expected_user system_changed=false system_exists=false
  [[ "$confirm_uninstall" == true ]] || die_usage "install-service requires --confirm"
  acquire_service_lock
  validate_service_registration
  prepare_service_stage
  expected_system="$service_stage_dir/system.plist"
  expected_user="$service_stage_dir/user.plist"
  render_system_plist "$expected_system"
  render_plist "$expected_user" "$service_home"

  if [[ -e "$plist_path" || -L "$plist_path" ]]; then
    require_owned_regular_file "$plist_path"
    require_mode "$plist_path" 600
    /usr/bin/cmp -s "$expected_user" "$plist_path" || \
      die "user LaunchAgent differs from the exact managed definition"
  fi

  if [[ -e "$system_plist_path" || -L "$system_plist_path" ]]; then
    system_exists=true
    require_system_plist "$system_plist_path"
    if ! /usr/bin/cmp -s "$expected_system" "$system_plist_path"; then
      system_changed=true
    fi
  else
    system_changed=true
    system_service_loaded && die "a runner system job is loaded without its exact plist"
    [[ -e "$plist_path" || -L "$plist_path" ]] || \
      die "initial service installation requires the exact managed user LaunchAgent"
  fi

  if [[ "$system_changed" == true ]]; then
    if system_service_loaded; then
      bootout_system_service
    fi
    install_system_plist_file "$expected_system"
  fi

  remove_exact_user_agent "$expected_user"
  if ! system_service_loaded; then
    bootstrap_system_service
  fi
  validate_system_service "$expected_system"
  if [[ "$system_exists" == true && "$system_changed" == false ]]; then
    printf 'runner %s system service: already installed and loaded\n' "$runner_name"
  else
    printf 'runner %s system service: installed and loaded\n' "$runner_name"
  fi
}

status_service_command() {
  local expected_system
  validate_service_registration
  prepare_service_stage
  expected_system="$service_stage_dir/system.plist"
  render_system_plist "$expected_system"
  validate_system_service "$expected_system"
  printf 'runner %s system service: loaded (repository=%s, label=%s, version=%s)\n' \
    "$runner_name" "$metadata_repository" "$metadata_label" "$metadata_version"
}

uninstall_service_command() {
  local expected_system
  [[ "$confirm_uninstall" == true ]] || die_usage "uninstall-service requires --confirm"
  acquire_service_lock
  validate_service_registration
  prepare_service_stage
  expected_system="$service_stage_dir/system.plist"
  render_system_plist "$expected_system"
  [[ -e "$system_plist_path" || -L "$system_plist_path" ]] || die "runner system LaunchDaemon plist is missing"
  require_system_plist "$system_plist_path"
  /usr/bin/cmp -s "$expected_system" "$system_plist_path" || \
    die "runner system LaunchDaemon plist differs from the managed definition"
  if system_service_loaded; then
    bootout_system_service
  fi
  /bin/rm -f -- "$system_plist_path"
  printf 'runner %s system service: removed; registration retained and stopped\n' "$runner_name"
}

parse_arguments() {
  local command=$1
  shift
  while (($# > 0)); do
    case "$1" in
      --repository)
        (($# >= 2)) || die_usage "--repository requires a value"
        repository=$2
        shift 2
        ;;
      --name)
        (($# >= 2)) || die_usage "--name requires a value"
        runner_name=$2
        shift 2
        ;;
      --user)
        (($# >= 2)) || die_usage "--user requires a value"
        service_user=$2
        shift 2
        ;;
      --label)
        (($# >= 2)) || die_usage "--label requires a value"
        runner_label=$2
        shift 2
        ;;
      --root)
        (($# >= 2)) || die_usage "--root requires a value"
        install_root=$2
        shift 2
        ;;
      --version)
        (($# >= 2)) || die_usage "--version requires a value"
        runner_version=$2
        version_overridden=true
        shift 2
        ;;
      --sha256)
        (($# >= 2)) || die_usage "--sha256 requires a value"
        archive_sha256=$2
        sha_overridden=true
        shift 2
        ;;
      --replace)
        replace_existing=true
        shift
        ;;
      --confirm)
        confirm_uninstall=true
        shift
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      --token|--registration-token|--removal-token)
        die_usage "tokens are accepted only on stdin, never as command-line options"
        ;;
      *) die_usage "unknown argument" ;;
    esac
  done

  [[ -n "$runner_name" ]] || die_usage "--name is required"
  validate_dns_label "$runner_name" "--name"
  case "$command" in
    install)
      [[ -n "$repository" ]] || die_usage "--repository is required for install"
      [[ -n "$runner_label" ]] || die_usage "--label is required for install"
      [[ -z "$service_user" ]] || die_usage "install does not accept --user"
      validate_repository "$repository"
      validate_dns_label "$runner_label" "--label"
      validate_version_and_digest
      [[ "$confirm_uninstall" == false ]] || die_usage "install does not accept --confirm"
      ;;
    status|uninstall)
      [[ -z "$repository" && -z "$runner_label" ]] || die_usage "$command does not accept --repository or --label"
      [[ -z "$service_user" ]] || die_usage "$command does not accept --user"
      [[ "$version_overridden" == false && "$sha_overridden" == false ]] || die_usage "$command does not accept version overrides"
      [[ "$replace_existing" == false ]] || die_usage "$command does not accept --replace"
      if [[ "$command" == "status" && "$confirm_uninstall" == true ]]; then
        die_usage "status does not accept --confirm"
      fi
      ;;
    install-service|status-service|uninstall-service)
      [[ -z "$repository" && -z "$runner_label" ]] || die_usage "$command does not accept --repository or --label"
      [[ -n "$service_user" ]] || die_usage "$command requires --user"
      [[ -n "$install_root" ]] || die_usage "$command requires --root"
      [[ "$version_overridden" == false && "$sha_overridden" == false ]] || die_usage "$command does not accept version overrides"
      [[ "$replace_existing" == false ]] || die_usage "$command does not accept --replace"
      if [[ "$command" == "status-service" && "$confirm_uninstall" == true ]]; then
        die_usage "status-service does not accept --confirm"
      fi
      ;;
  esac
}

main() {
  local command=${1:-}
  case "$command" in
    install|status|uninstall|install-service|status-service|uninstall-service) shift ;;
    --help|-h) usage; return 0 ;;
    "") usage >&2; return 64 ;;
    *) die_usage "unknown command" ;;
  esac

  process_uid=$(/usr/bin/id -u)
  uid=$process_uid
  configure_test_commands
  effective_uid=$("$ID" -u)
  parse_arguments "$command" "$@"
  case "$command" in
    install|status|uninstall)
      platform_preflight user
      uid=$effective_uid
      prepare_paths
      ;;
    install-service|status-service|uninstall-service)
      platform_preflight system
      validate_service_executable
      prepare_service_paths
      ;;
  esac
  trap cleanup EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  trap 'exit 129' HUP

  case "$command" in
    install) install_command ;;
    status) status_command ;;
    uninstall) uninstall_command ;;
    install-service) install_service_command ;;
    status-service) status_service_command ;;
    uninstall-service) uninstall_service_command ;;
  esac
}

main "$@"
