# Dedicated Apple Silicon hardware runners

APC's hardware workflow uses one repository-scoped GitHub Actions runner on
each physical Mac. The managed installer is
[`scripts/install-github-runner.sh`](../scripts/install-github-runner.sh). It
pins the official ARM64 macOS runner archive to version `2.335.1` and SHA-256
`e1a9bc7a3661e06fa0b129d15c2064fe65dc81a431001d8958a9db1409b73769`.

The runner must use a dedicated, unprivileged macOS account. Registration must
never run with `sudo`. The installation root is mode `0700`; GitHub credential
files and APC metadata are mode `0600`. Both managed plists route service stdout
and stderr to `/dev/null`; the runner's own `_diag` data remains inside the
protected installation tree. GitHub's generated `svc.sh load` path is
deliberately not used.

Registration initially loads a `Background` LaunchAgent in the current
`user/<uid>` launchd domain. Explicit bootstrap works from SSH during that
boot, but this is still session-scoped. A live reboot with zero logged-in users
proved that macOS did **not** automatically reload the user's LaunchAgents,
even though the Background job remained enabled. Do not claim unattended or
reboot-persistent operation from the user LaunchAgent.

For a login-independent service, complete the separate tokenless
`install-service` phase as root. It installs a root-owned system LaunchDaemon
but declares the runner account with `UserName`, so `runsvc.sh` and every
workflow continue to execute unprivileged. The root phase never runs
`config.sh` and never receives a GitHub token. Its behavior across a reboot is
not accepted as proven until the post-reboot check below passes on that host.

## Live lab state on 2026-07-20

Both physical Macs now use the system-service path. The reviewed installer was
copied to the root-owned, non-writable
`/usr/local/bin/apc-github-runner-installer`, and both exact root-owned
LaunchDaemons were installed in the `system` launchd domain. GitHub reported
`apc-macbook` and `apc-macmini` online with `busy: false`.

A headless Mac mini reboot then proved that its system LaunchDaemon loaded
without an interactive login and the runner returned online and idle. The
equivalent MacBook reboot remains open; the fact that its service is currently
loaded and online is not reboot evidence.

The live lab currently selects an administrator account as `UserName` on each
host. The runner processes still execute as non-root identities rather than as
root, but an administrator account has a broader local authority boundary than
the dedicated unprivileged account required by the target deployment policy
above. This is an explicit lab deviation to remove, not a relaxation of the
recommendation. The guarded GitHub hardware workflow has not yet run from
reviewed default-branch code.

## Trust policy

A self-hosted runner can execute arbitrary code with all permissions of its
macOS account. The runner itself cannot enforce a Git branch policy. APC
therefore treats all of the following as mandatory repository policy:

- only the manual hardware workflow may target the dedicated labels
  `apc-macbook` and `apc-macmini`;
- that workflow must reject every ref other than the repository's default
  branch before executing host commands;
- do not add `pull_request`, `pull_request_target`, fork, tag or arbitrary-ref
  triggers to a workflow using these labels;
- protect the default branch and require review for `.github/workflows/**`,
  the hardware harness and this installer;
- never check out or execute pull-request content on either hardware runner;
- keep the runner repository-scoped, use a dedicated macOS account, and grant
  that account no passwordless general-purpose `sudo` access;
- review GitHub's Actions and fork-approval settings after every policy change.

The current workflow's default-branch guard remains the enforcement point.
Installing a runner does not weaken or replace that policy.

## Register without privileges

The operator needs repository administration permission and an authenticated
GitHub CLI. Generate a short-lived registration token and pipe it directly to
the installer; do not put it in a shell argument, environment file or log:

```bash
gh api --method POST \
  repos/OWNER/REPOSITORY/actions/runners/registration-token \
  --jq .token |
  scripts/install-github-runner.sh install \
    --repository OWNER/REPOSITORY \
    --name apc-macbook \
    --label apc-macbook
```

Use `apc-macmini` for the second host. To execute the already-copied installer
over SSH, keep the token on the pipe:

```bash
gh api --method POST \
  repos/OWNER/REPOSITORY/actions/runners/registration-token \
  --jq .token |
  ssh RUNNER_HOST \
    '~/.local/bin/install-github-runner.sh install --repository OWNER/REPOSITORY --name apc-macmini --label apc-macmini'
```

The wrapper reads exactly one token from stdin, disables shell tracing and
redacts the official configuration program's output. GitHub's official
`config.sh` interface still transiently receives the token as its required
`--token` child-process argument. The wrapper never accepts a token option and
never writes the registration token to its own files or logs; it unsets the
value immediately when `config.sh` returns.

The download is staged in a private directory and is extracted only after its
digest matches. Automatic runner binary updates are disabled so that the
verified version remains the version being executed. GitHub imposes an update
window on runners with automatic updates disabled, so review upstream runner
releases regularly and update the pin before it expires. A future runner
update may override the pin only by supplying both values together:

```bash
scripts/install-github-runner.sh install \
  --repository OWNER/REPOSITORY \
  --name apc-macbook \
  --label apc-macbook \
  --version X.Y.Z \
  --sha256 64_LOWERCASE_HEXADECIMAL_CHARACTERS
```

Verify the new digest against GitHub's release assets before using this form.
An identical managed installation is idempotent and does not read stdin,
download the archive, reconfigure GitHub or restart the LaunchAgent.

An unknown, manually installed or differently configured tree is refused. The
`--replace` option is intentionally required before moving such a tree aside
and asking GitHub to replace the named registration. Prefer the clean removal
and reinstallation procedure below. Never use `--replace` merely to refresh
status or restart a runner.

## Install the system service

After registration succeeds, use the explicit root phase. Its `--root` must be
the already managed tree directly below the specified account's
`.local/share`; no default is accepted for a root invocation. Review the
installer first, then copy that exact revision to a root-owned, non-writable
path. System-service commands reject a user-owned installer:

```bash
sudo /usr/bin/install -o root -g wheel -m 0755 \
  scripts/install-github-runner.sh \
  /usr/local/bin/apc-github-runner-installer

sudo /usr/local/bin/apc-github-runner-installer install-service \
  --name apc-macbook \
  --user RUNNER_ACCOUNT \
  --root /Users/RUNNER_ACCOUNT/.local/share/apc-github-runner \
  --confirm
```

Repeat with `apc-macmini` on the second host. This command is intentionally
tokenless. It verifies the user, physical home path, exact managed metadata,
user ownership, mode `0700`, credential modes `0600`, executable entrypoint and
contained symlinks. It then installs an exact root-owned, wheel-group, mode
`0644` plist below `/Library/LaunchDaemons`, removes only the exact managed user
plist, unloads any duplicate user-domain job, and bootstraps the system job.
The LaunchDaemon has no login-session constraint and invokes `runsvc.sh` with
`UserName=RUNNER_ACCOUNT`; runner code never executes as root.

`install-service` is idempotent. It fails closed on foreign ownership, a
different plist, an escaping symlink, a mismatched runner name, an unknown
user, or an unconfirmed request. It does not adopt a manually registered tree.
The root phase does not make workflow code trusted; the repository policy
above remains mandatory.

## Status

User-mode status validates the install root, owner, modes, credential files,
metadata, exact plist content and the loaded user-domain job. It fails closed
on a symbolic link escaping the install root, foreign ownership, or a detected
system service:

```bash
scripts/install-github-runner.sh status --name apc-macbook
scripts/install-github-runner.sh status --name apc-macmini
```

The expected session-scoped target is
`user/<uid>/dev.applepodcontrol.github-runner.<name>`. A successful status does
not prove reboot persistence.

For the system mode, validate the root-owned plist, absence of a duplicate user
job and the loaded `system` job:

```bash
sudo /usr/local/bin/apc-github-runner-installer status-service \
  --name apc-macbook \
  --user RUNNER_ACCOUNT \
  --root /Users/RUNNER_ACCOUNT/.local/share/apc-github-runner
```

The expected target is
`system/dev.applepodcontrol.github-runner.<name>`. Run this check after a reboot
with no interactive login before treating the runner as persistent. Neither
status mode proves that the GitHub control plane currently reports the runner
online; verify that separately without exposing host details:

```bash
gh api repos/OWNER/REPOSITORY/actions/runners \
  --jq '.runners[] | {name, status, busy, labels: [.labels[].name]}'
```

## Credential rotation and uninstall

GitHub registration and removal tokens are short-lived bootstrap credentials;
the runner's durable credential is generated during registration. Rotate the
durable runner identity by unregistering and registering it again. First stop
dispatching hardware jobs and wait until GitHub reports `busy: false`.

Remove the system service before unregistering. This tokenless command unloads
and removes only the exact system plist; it retains the user-owned registration
tree in a stopped state:

```bash
sudo /usr/local/bin/apc-github-runner-installer uninstall-service \
  --name apc-macbook \
  --user RUNNER_ACCOUNT \
  --root /Users/RUNNER_ACCOUNT/.local/share/apc-github-runner \
  --confirm
```

Then pipe a removal token into the non-root confirmed uninstall:

```bash
gh api --method POST \
  repos/OWNER/REPOSITORY/actions/runners/remove-token \
  --jq .token |
  scripts/install-github-runner.sh uninstall \
    --name apc-macbook \
    --confirm
```

The non-root command removes the local tree and user plist only after GitHub's
official removal succeeds; otherwise it retains the registration. It refuses
to unregister while the system plist is present. The removal token has the same
stdin, redaction and transient-official-argv behavior as registration. After a
successful uninstall, repeat both registration and `install-service` with a
newly issued registration token. Perform the sequence independently on the
second Mac.

To leave GitHub registration intact and return only to session-scoped mode,
run `uninstall-service` and then rerun the identical non-root `install` command
without stdin. The managed, idempotent path recreates and bootstraps the user
LaunchAgent without reconfiguration or a new token. It still will not autoload
after a zero-login reboot.

If an operator deletes a runner in the GitHub UI before local uninstall, obtain
a removal token and try the confirmed uninstall normally. If GitHub no longer
accepts removal, preserve the tree for investigation rather than deleting its
credential files by hand; clean up only after confirming that the repository
has no matching live registration.

## Verification

The fixture suite substitutes the archive downloader, GitHub configuration
program and launchd. It neither contacts GitHub nor calls the host's real
`launchctl`:

```bash
bash -n scripts/install-github-runner.sh
bash -n scripts/test-install-github-runner.sh
scripts/test-install-github-runner.sh
```

It covers token redaction, wrong-digest refusal, input validation, refusal of
implicit replacement, credential permissions, idempotent registration and
status, the session-scoped Background plist, root-only/tokenless service
installation, exact `UserName`, root/wheel install intent, system-domain
bootstrap, duplicate user-job removal, both service status paths and confirmed
removal. All launchd, archive, identity, directory-service and root-install
operations are fakes; the suite never contacts GitHub or the host's real
`launchctl`.
