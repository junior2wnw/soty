# Personal TRANSIT access and Soty Agent

## Public personal profiles (30 October 2026)

The first-party TRANSIT menu/search entry and `/#internet` use authenticated
Soty account access, independently of any owner's computer. The former Transit
card opens the same personal-profile controller. `Получить профиль sing-box`
claims a distinct preinstalled traffic identity; repeat requests and another
device on that Soty account receive the same profile. Existing imported access
is preserved. The download is a portable TUN JSON without a Windows interface name.

Root's `transit.access.profile` extension accepts only the current Connect actor
and matching expected account. It signs a short-lived request to the private
TRANSIT profile issuer; no arbitrary account selector, URL, SSH or financial
authority is accepted. Revocation and account changes are checked after I/O.
Configuration is an optional owner-only `DATA_DIR/transit-access.json` (or the
explicit `SOTY_TRANSIT_ACCESS_CONFIG_FILE`); existing Env and mounts need no change.
The issuer verifies a signed encrypted pool and persists unique account/slot
claims in its own SQLite volume. Capacity exhaustion denies new claims while
existing profiles remain downloadable. This free best-effort path enables no
payments, rewards, interception or claim of unlimited network capacity.

Deployment and pool/rollback procedures live in TRANSIT's
`services/profile-access/README.md`. Agent networking was inspected and tested,
and its existing implementation/bundle remains unchanged. The following sections
describe that existing Agent path and its separate device limitations.

Scope: an existing owner's TRANSIT profile, Windows x64 client ON/OFF, and a
sing-box file for that owner's phone. It does not issue, reset or revoke a
TRANSIT account, open the closed Portal release, or implement a VPN protocol.
The separate Xray computer-sharing route remains a separate feature.

## Installation and ownership

`Меню → Интернет` opens the TRANSIT dialog in the current Soty world. The account
vault holds the access profile and per-host controller capabilities using existing
Connect encryption and revision checks. Other vault data is preserved. Account
replacement closes the dialog and invalidates pending writes.

`Разрешить управление этим компьютером` downloads the normal Machine installer.
Only the SHA-256 of its 256-bit controller capability goes into that installer;
the capability stays in the encrypted account vault. After Windows elevation,
the installer persists that hash and prepares the pinned official sing-box
1.13.15 archive. Installation does not start a system tunnel. A download failure
defers the core and leaves Agent installation available; activation retries it.
Normal upgrades preserve the controller hash. RuntimeOnly maintenance's separate
1.4.8 admission contract was not weakened.

Private profiles and process records live below the Agent data directory's
`transit-client`. Its NTFS inheritance is restricted before any access credential
is written; failure to establish the ACL prevents installation/import/start.
Imported controllers/listeners, direct outbounds and insecure TLS are rejected.
The local profile parser checks a staged file before replacing a saved profile.

## Local API and lifecycle

`GET /operator/transit/client` returns redacted state and the host ID. POST uses
the same path with `{deviceId, controlToken, action}`. Actions are `authorize`,
`import` (plus `profile`), `start`, `stop`, and `export`. Device ID, public health
and installer hash alone grant no control or export. Existing trusted-origin
checks remain in force.

Start checks each entry using temporary loopback SOCKS listeners bound to the
physical Ethernet/Wi-Fi interface. It requires public-IP and real HTTPS checks;
an entry canary alone is insufficient. Three bounded workers test entries; the
runtime copy excludes failed entries from selector/urltest groups. Existing
credentials, peer pins and the saved phone profile are preserved. There is no
fallback to an unencrypted direct outbound.

Only after this check does start create `SotyTransit`. Connected state additionally
requires system routes and public egress matching one of the checked exits.
Double starts serialize and reuse one owned process. Stop confirms that process
exit and IPv4/IPv6 system-route removal both occurred. Failed or unverifiable
stop is UNKNOWN. After an Agent restart, process recovery checks the exact core
path, private configuration path, PID and process creation instant. A reused PID
is not killed. Recovery makes the previous process stoppable and does not claim
that its connectivity was rechecked.

Local operator commands, using the same bundled Agent and data directory:

```text
node soty-connector.mjs transit-prepare
node soty-connector.mjs transit-check <existing-profile.json>
node soty-connector.mjs transit-stop
```

`transit-check` only uses temporary SOCKS probes and a parser; it does not import
the supplied access into a persistent profile or create a TUN. Commands emit
metadata and fixed error codes, not profiles or credentials.

## Phone and current network

Import the existing access once through the owner's dialog. On a phone signed
into that same Soty account, `Скачать профиль sing-box` downloads `transit.json`.
Phone UI does not call the computer controller. Existing sing-box profiles remain
available. The Windows inbound is adapted locally, without credential conversion
services. A downloaded profile is a credential and must not enter public logs.

The current local operator package and signed existing phone profile are under
`D:/roy/.transit/staged-network`; their contents must never enter Git or a report.
The old tracked READINESS file does not describe all of this existing runtime.
The observed Windows native preflight passed 5 of 6 entries. A TLS failure on one
VLESS entry remains a network fault; runtime filtering works around it without
changing the signed phone profile or claiming the entry was repaired.

Legacy Xray sharing still issues XHTTP URIs. The sing-box converter explicitly
rejects XHTTP rather than changing its transport silently. It supports TLS with
TCP/WS. Native TRANSIT uses the existing signed VLESS/Hysteria2 profile and does
not depend on that converter. Sharing now prefers `SotyTransit` when present and
blocks private/link-local/loopback destinations at routing and final egress.

## Release and actual switch

This source is prepared separately from the frozen Root release. Integrate its
exact commits, rebuild the full panel and publish Agent assets together through
Root's established release path. There is no second deployment from this branch.

After that release, pair the current computer through its normal installer and
import the existing owner profile. Run the read-only check before activation.
Keep the working Amnezia installation and phone profile. The requested live
transition happens after the new panel is available and requires real elevated
TUN ON/OFF, IPv4/IPv6/DNS, upload/download, transport interruption and Agent
restart checks on the computer. These were not simulated as a successful live
migration. No Amnezia removal or system-tunnel activation occurred during source
qualification.

Current limits: activation is explicit; a new Agent session recovers ownership
as UNKNOWN until the user stops/restarts it. There is no independently verified
OS firewall kill switch, automatic reconnect after every network change, or
automatic reconnect after reboot. This is not a claim of an always-on,
leak-proof production VPN or open public enrollment.

## Checks

The implementation's tests cover authority, vault/account fences, profile
continuity, repeated start/stop, failed preflight, signal termination, orphan
routes, and process recovery. The private-network regression uses a real pinned
Xray and a loopback HTTP canary: routing-only and final-egress-only guards each
block it; removing both restores access. Set `SOTY_XRAY_BINARY` for that check.
Agent release/update self-tests cover the generated assets. Browser checks use
the real Connect HTTP service with an in-memory account and mock only the Agent
network operations. Real Windows SOCKS and process-recovery checks qualify the
exact bundle independently of those UI fixtures.
