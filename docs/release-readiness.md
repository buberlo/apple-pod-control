# Release readiness

This is the authoritative readiness checklist for Apple Pod Control. It keeps
three decisions separate:

- whether the current pull request can be merged;
- whether the project is a usable alpha for its documented Apple-Silicon lab
  scope; and
- whether the project is ready for production use.

A lower tier does not imply a higher tier. In particular, merging a pull
request is not an alpha or production-readiness claim. Evidence must come from
the linked live result, automated check or GitHub state; implemented code by
itself is not proof of a live hardware or recovery gate.

## Status snapshot: 2026-07-20

- [PR #5](https://github.com/buberlo/apple-pod-control/pull/5) is open and
  draft. The last published head passed its `test` check, but the final
  K3s-only head must run the required checks again and still needs an
  independent review. It is therefore **not merge-ready** under the checklist
  below.
- [Issue #2](https://github.com/buberlo/apple-pod-control/issues/2),
  [issue #3](https://github.com/buberlo/apple-pod-control/issues/3) and
  [issue #4](https://github.com/buberlo/apple-pod-control/issues/4) are open.
  Public egress is tracked separately in
  [issue #6](https://github.com/buberlo/apple-pod-control/issues/6). Replacement
  host recovery and off-host escrow are tracked in
  [issue #9](https://github.com/buberlo/apple-pod-control/issues/9).
- The two-Mac cluster and local three-VM HA lab retain the successful results
  recorded in the [live validation report](validation-results.md). The Mac mini
  now has narrow headless-reboot proofs for its PF and GitHub runner system
  LaunchDaemons. Those results do not prove the MacBook equivalents, the APC
  unattended supervisor reboot gate, rejection of an unlisted host, encrypted
  overlay operation, public egress from every node, physical-host HA or
  successful replacement-host recovery. The saved two-Mac worker is currently
  stopped because DHCP changes made its stored server URL and advertise
  addresses stale.

Status values below mean:

- **Pass**: the stated completion condition has direct evidence;
- **Partial**: useful evidence exists, but it does not cover the completion
  condition; and
- **Open**: the required evidence is absent or the condition is known to be
  false.

## Tier 1: PR merge readiness

These gates apply only to PR #5. They deliberately do not require completing
the hardware- and account-dependent alpha gates.

| Gate | State on 2026-07-20 | Evidence | Owner / dependency category | Objective completion condition |
|---|---|---|---|---|
| M1. Required CI | **Open** | The previous published head passed; the final K3s-only head must be published and rerun | APC engineering / repository automation | Every required check for the final head commit completes successfully |
| M2. Merge conflict state | **Open** | The final head has not yet been evaluated against the then-current `main` | APC engineering / Git history | GitHub reports the final head mergeable into the current base branch |
| M3. Independent review | **Open** | GitHub reports no submitted review | Maintainer / human review | At least one authorized reviewer approves the final diff and no blocking review remains |
| M4. Publication state | **Open** | PR #5 is still a draft | Maintainer / repository action | The PR is converted from draft only after M1-M3 pass and its description states the alpha limitations below |
| M5. Claim accuracy | **Partial** | The validation report and operator docs now scope the Mac mini PF/runner reboot proofs separately from the unproved MacBook and APC-service gates | APC engineering / documentation | Diff review finds no generalized claim of reboot persistence, overlay encryption, public egress, physical HA or replacement-host recovery beyond its direct evidence |

PR #5 may be merged when M1-M5 all pass. Its merge message and release notes
must describe it as the secure networking and local HA foundation, not as a
production release.

## Tier 2: usable alpha

All seven gates are mandatory for the documented usable-alpha claim. The list is
canonical; other documents should link here instead of redefining it.

| Gate | State on 2026-07-20 | Current evidence | Owner / dependency category | Objective completion condition |
|---|---|---|---|---|
| A1. Real-hardware CI | **Partial** | Root-owned system LaunchDaemons are installed on both runners through the hardened root-owned installer, and GitHub reported both online and idle. The Mac mini returned online after a headless reboot; the MacBook reboot and any trusted default-branch workflow run remain open. The lab currently uses administrator accounts rather than dedicated unprivileged runner accounts. Tracked by [#4](https://github.com/buberlo/apple-pod-control/issues/4) | Release engineering / reviewed merge, disruptive MacBook reboot and dedicated local accounts | Both runners use dedicated unprivileged accounts; both return online after a zero-login reboot; scheduled and release-candidate runs execute create/join, Helm, image sync, backup/restore, digest upgrade, simultaneous recovery, stable IP, deep-doctor, NetworkPolicy and idempotent cleanup without leaking private data |
| A2. PF reboot and negative-peer proof | **Partial** | Install and privileged status passed on both test Macs; a headless Mac mini reboot reconstructed its exact anchor/reference. MacBook reboot, rejection of an unlisted peer and uninstall rollback remain unproved. Tracked by [#2](https://github.com/buberlo/apple-pod-control/issues/2) | Host operations / disruptive MacBook reboot plus an independent non-peer device | After reboot, both root LaunchDaemons reconstruct only the expected anchors and references; the listed peer succeeds, an unlisted host cannot reach any cluster port, and uninstall removes the rules cleanly |
| A3. Authenticated host overlay | **Open** | APC has an overlay preflight, but Tailscale is absent and no overlay live gate has run. Tracked by [#2](https://github.com/buberlo/apple-pod-control/issues/2) | User/account-bound network setup / macOS approval and interactive overlay authentication | Cluster publish, advertise and PF identities use the authenticated overlay; identity, peer and route checks, deep doctor and NetworkPolicy pass over it; credential rotation and rollback are demonstrated without APC receiving an auth key |
| A4. Three physical HA failure domains | **Partial** | Three local ARM64 K3s/etcd VMs proved quorum behavior and one-member loss on one Mac. Tracked by [#3](https://github.com/buberlo/apple-pod-control/issues/3) | Hardware / three independent Apple-Silicon hosts | One server runs on each of three physical hosts, and API plus existing workloads remain available during loss of any one host, leader failure, reboot and the accepted network-partition cases; rejoin, rolling upgrade and failed-readiness rollback preserve quorum |
| A5. Public egress support decision | **Open** | Host-mediated image distribution works, but public HTTPS from the MacBook Apple VM is still unreliable and current final doctors skip that probe. Tracked by [#6](https://github.com/buberlo/apple-pod-control/issues/6) | APC engineering plus product policy / Apple VM networking | Either public DNS and HTTPS pass from every node with doctor run without `--skip-egress`, or a tested air-gapped/prefetch support profile explicitly removes public egress from the product contract and proves all supported workload flows without it |
| A6. Replacement-host and off-host recovery | **Open** | Exact empty-host recovery on the same physical Mac passes. A replacement-Mac drill validated the package/digest and reset the seed member, but peers could not route to the saved stable address through the target vmnet; APC failed closed and target resources were cleaned. Tracked by [#9](https://github.com/buberlo/apple-pod-control/issues/9) | APC engineering plus security operations / portable member addressing, replacement hardware, encrypted external storage and recovery policy | A protected off-host package and independent trust anchor restore the cluster onto a replacement Mac; token/key escrow, rotation, retention, RPO and RTO are defined and tested |
| A7. APC unattended reboot supervision | **Partial** | The root LaunchDaemon design has deterministic coverage for exact `launchctl asuser` + `sudo -n` + `env -i` privilege drop, `/dev/null` launchd streams, a protected bounded non-root log, running/PID/exact-argv status and root-ancestor/allow-ACL refusal. A repeated live APC zero-login reboot has not passed | Host operations / dedicated service accounts and disruptive reboot window | On both Macs, the APC service runs under a dedicated non-root account, survives a zero-login reboot, passes exact status, returns the selected Kubernetes role to Ready, keeps bounded protected logs and uninstalls without residue |

The usable-alpha tier passes only when A1-A7 all pass. Until then, the supported
description remains “development and trusted-LAN lab,” with the exact
limitations above.

## Tier 3: production readiness

Production readiness requires every alpha gate plus the following independent
product and operational gates. Reusing an alpha result is allowed only where
its evidence covers the stronger production completion condition.

| Gate | State on 2026-07-20 | Current evidence | Owner / dependency category | Objective completion condition |
|---|---|---|---|---|
| P1. Alpha baseline | **Open** | A1-A7 are not all complete | Cross-functional / all alpha dependencies | Every usable-alpha gate above is **Pass** against a release candidate |
| P2. Authenticated and encrypted cross-host transport | **Open** | K3s authenticates its API and node join, and PF restricts trusted-LAN peers, but Flannel VXLAN is unencrypted and the authenticated host-overlay gate has not run | APC networking and security operations / authenticated overlay | Every supported cross-host control and Pod-network path uses authenticated encryption; identity, route and credential rotation are tested and plaintext trusted-LAN mode is explicitly excluded from production |
| P3. Kubernetes secret and backup lifecycle | **Open** | Workloads use native Kubernetes Secrets, while protected APC token/snapshot files have strict modes. Encryption-at-rest, key rotation and encrypted off-host snapshot operations are not yet a proved product profile | Kubernetes security and operator key management | Kubernetes secrets and every backup credential are encrypted at rest, never emitted in logs or process arguments, support rotation/revocation and remain protected through restore and sanitized diagnostics |
| P4. Recovery objectives and fault matrix | **Open** | Local in-place rollback, same-Mac exact empty-host recovery and one-VM quorum behavior passed. A replacement-host exercise failed safely at peer routing, and no approved production RPO/RTO or protected off-host escrow exists | Site reliability / portable addressing, three hosts, external backup and failure injection | Documented RPO/RTO are met in timed restore, full host-loss, leader-loss, partition, rolling-upgrade and rollback exercises using production-equivalent failure domains |
| P5. Release operations and support envelope | **Open** | Regular CI is green, but hardware runs are not scheduled and the egress/runtime dependency boundary is not final | Release engineering and product owner / hardware CI and support policy | Scheduled and release-candidate hardware suites are mandatory and green; supported macOS, Apple container, K3s, kubectl/Helm and network/egress profiles have upgrade, rollback and deprecation policies |

No production claim may be made until P1-P5 all pass for the same release
candidate and the evidence links are recorded here or in a dated validation
report.

## Execution order

1. Finish M5, obtain review, remove draft state and merge PR #5 as a foundation
   change without changing the documented support tier.
2. Complete engineering-only work that does not require account or hardware
   access: egress/support-profile decisions, portable HA member addressing,
   off-host recovery/escrow design, Kubernetes secret encryption and expanded
   deterministic test coverage.
3. After reviewed merge, run and expand A1 from the trusted default branch on
   the two registered, isolated hardware runners.
4. Replace the lab administrator runner identities with dedicated accounts,
   then reboot the MacBook headlessly and verify both runner system services.
5. Install the APC unattended profile for dedicated non-root accounts and run
   the two-Mac zero-login reboot/readiness/log/uninstall gate for A7.
6. Complete the remaining MacBook PF reboot, negative-peer and firewall
   rollback tests for A2.
7. Perform the interactive overlay setup and authenticated network matrix for
   A3.
8. Add a third physical Apple-Silicon host and execute A4, then complete the
   external-backup and replacement-host exercises for A6.
9. Run P1-P5 together against one release candidate before changing the
   project's support statement from alpha to production.
