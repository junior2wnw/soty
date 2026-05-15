# soty.online

A small PWA surface for a long-lived byte tunnel between counterparties.

The browser side stays deliberately simple:

- an installed PWA owns a local device name;
- an empty device shows a QR code in the honeycomb field;
- scanning the QR creates an incoming request on the device that shows the QR;
- after approval, the tunnel remains until a counterparty closes it;
- the upper field contains counterparties as movable hex cells;
- the lower field is one shared text surface;
- every character, deletion, line break, command result, and file moves through the same tunnel;
- direct WebRTC data channels are used when the devices and networks allow it;
- the relay remains the reliable path for signaling, reconnects, history, and fallback delivery;
- file transfer works from the counterparty menu, drag and drop, and agent-initiated device file publishing;
- the bell icon sends one small wake pulse through the existing tunnel;
- the remote icon grants one-way command access to the selected counterparty;
- command access uses a local companion agent that the PWA can detect on `127.0.0.1:49424`;
- local operators can use `sotyctl` to list remote targets and run commands through an opened PWA bridge;
- long remote jobs can be staged as temporary scripts and launched without visible terminal windows;
- the installed companion agent starts with the OS and updates itself from `/agent/manifest.json`;
- when Chrome/Edge blocks direct loopback access, the app can pair the companion with a secret server relay and keep agent chat working without browser local-network permission;
- Windows maintenance work uses the same PWA channel plus a machine-scope Soty Worker when admin/SYSTEM actions are required.

## Boundary

Soty is the application shell: UI, install flows, relay wiring, companion-agent
handoff, and user-facing behavior live here.

The technology core lives in the public `trustlink-kernel` repository
(`junior2wnw/4-2-rf`). Protocol, crypto, room secrets, byte envelopes,
permission/session logic, discovery, recovery, path ranking, audit, and other
generic reliability primitives belong there. Soty should depend on that kernel
instead of turning those primitives into a separate legal or technology surface.

## Layout

- `src/main.ts` wires the app lifecycle.
- `src/trustlink/*` keeps the app adapters for runtime, local storage, device records, QR joining, room records, and encrypted payload calls.
- `src/sync.ts` owns realtime text, files, pairing, remote intents, snapshots, and reconnect transport.
- `src/ui/*` contains UI primitives such as the honeycomb field and counterparty menu.
- `src/features/*` contains optional tunnel features.
- `src/core/*` contains tiny shared helpers.
- `server/*` is the relay: HTTP shell, room store, validators, and WebSocket routing.
- `public/*` is the PWA manifest, service worker, and icon.
- `trustlink-kernel` is the separate SDK for room secrets, compact join codes, byte encoding, and web crypto primitives.

## Run

```bash
pnpm install
pnpm run typecheck
pnpm run build
pnpm start
```

Default port: `8080`.

## Local Agent

The PWA never runs OS commands by itself. For normal use, open the counterparty menu and press the remote icon. If the local companion agent is absent, the PWA shows the installer control. On Windows it downloads the single admin installer `install-soty-agent-machine.cmd`; the script elevates once, installs the managed machine agent, brings portable Node.js when needed, seeds the relay/device config for the user-session companion, starts the agent, registers OS autostart, and keeps auto-update enabled. On macOS and Linux, `install-macos-linux.sh` installs the same managed agent, verifies release hashes from `/agent/manifest.json`, resumes interrupted downloads, installs Codex locally when needed, and starts from LaunchAgent, systemd user service, or desktop autostart. The machine scope uses LaunchDaemon on macOS and systemd system service on Linux. After the installer runs once, the agent starts with the OS and updates itself.

For development, run:

```bash
pnpm run agent
```

Then open the counterparty menu in the PWA and toggle the remote icon. Commands typed by the granted counterparty are bridged through the local loopback agent on `127.0.0.1:49424`. Agent chat first tries that loopback bridge. If the local companion has no Codex CLI or the browser blocks loopback access, chat falls back to a server Codex executor channel marked with `SOTY_SERVER_CODEX_RELAY_ID`, `SOTY_AGENT_RELAY_ID`, `SOTY_AGENT_SCOPE=Server`, or a `srv_codex_` relay id. The server executor may think for many client relays, but device commands are routed back through the requesting `sourceRelayId + deviceId`; it must not auto-switch to a global "latest" user agent from another device.

Remote command transcripts are part of the encrypted tunnel document, not page-local scratch state. Reloading one browser tab must not clear another device's command window, and a refreshed tab should recover the terminal transcript from the server snapshot.

Long agent tasks are expected to survive ordinary install/download/repair waits. The browser and server-side Codex turn wait through long jobs, the server relay keeps source jobs through that window, and Soty MCP tools accept `timeoutMs` up to `86400000` for long scripts instead of cutting work at the old five-to-ten-minute ceiling.

On Windows the agent uses PowerShell by default. Use `SOTY_AGENT_SHELL=cmd` when a device should run commands through `cmd.exe`.

Agent chat uses the stock Codex CLI with native OpenAI/Codex tools enabled where the CLI supports them, plus one Soty MCP server. OpenAI built-ins such as web search and image generation remain native tools; Soty MCP exposes the selected user's computer as `computer` and does not reimplement or shadow built-in tool names. For a generated wallpaper, Codex generates with native image generation, then uses `computer` to transfer/apply/verify the exact artifact on the selected device. For a file that already lives on a granted device, Codex uses `computer` file `action=download`/`publish`; the granted device streams the exact bytes into the encrypted Soty room file rail. Public upload services, temporary local HTTP servers, pasted base64, and "download this from a third-party link" are not the normal file-transfer route. To route that stock CLI through a network proxy, set `SOTY_CODEX_PROXY_URL` before install, or pass `-CodexProxyUrl` on Windows / `--codex-proxy-url` on macOS and Linux. The installer stores it as a local `proxy.env` secret and the agent expands it only into the child Codex process as `HTTPS_PROXY`, `HTTP_PROXY`, and `ALL_PROXY`; it does not switch Codex into an OpenAI-compatible API provider mode. `/health` reports only `codexProxy: true` and the proxy scheme.

Installed operator bridge:

```bash
# Windows
%LOCALAPPDATA%\soty-agent\sotyctl.cmd list
%LOCALAPPDATA%\soty-agent\sotyctl.cmd run Phone "ping ya.ru"
%LOCALAPPDATA%\soty-agent\sotyctl.cmd script Phone .\job.ps1 powershell
%LOCALAPPDATA%\soty-agent\sotyctl.cmd access Phone
%LOCALAPPDATA%\soty-agent\sotyctl.cmd install-machine Phone
%LOCALAPPDATA%\soty-agent\sotyctl.cmd machine-status Phone
%LOCALAPPDATA%\soty-agent\sotyctl.cmd say Phone "Пишу как живой оператор."
%LOCALAPPDATA%\soty-agent\sotyctl.cmd say --fast Phone "Короткий статус."
%LOCALAPPDATA%\soty-agent\sotyctl.cmd read Агент
%LOCALAPPDATA%\soty-agent\sotyctl.cmd listen Агент
%LOCALAPPDATA%\soty-agent\sotyctl.cmd export soty-backup.json
%LOCALAPPDATA%\soty-agent\sotyctl.cmd import soty-backup.json

# macOS / Linux
~/.soty-agent/sotyctl list
~/.soty-agent/sotyctl run Phone "ping ya.ru"
~/.soty-agent/sotyctl script Phone ./job.sh sh
~/.soty-agent/sotyctl access Phone
~/.soty-agent/sotyctl install-machine Phone
~/.soty-agent/sotyctl machine-status Phone
~/.soty-agent/sotyctl say Phone "Пишу как живой оператор."
~/.soty-agent/sotyctl say --fast Phone "Короткий статус."
~/.soty-agent/sotyctl read Агент
~/.soty-agent/sotyctl listen Агент
~/.soty-agent/sotyctl export soty-backup.json
~/.soty-agent/sotyctl import soty-backup.json
```

The bridge works when the PWA is open on the controlling device. `run` and `script` also require remote access to the named counterparty.
Use `script` for larger jobs: the agent writes a temporary file on the remote device, runs it hidden, streams output back, and removes the temporary file.
Use `say` to queue live typing into the shared text surface through the PWA; it returns after the message is queued so long operator notes do not hold the HTTP request open. Use `read` to fetch messages sent from the PWA after Enter, or `listen` to long-poll them as JSON lines for an IDE-side assistant loop. Use `computer` file `action=download`/`publish` from the agent dialog to pull a granted device file into the room file rail. Use `export` to save a local JSON backup of the PWA-visible device metadata, tunnel records, selected room, current shared text, and file metadata. Use `import` on a fresh PWA to create a new local device from that backup and restore rooms/text snapshots. Remote command grants are session-only and are intentionally not backed up or restored.

Emergency local repair:

```text
https://xn--n1afe0b.online/?pwa=1&reset-local=1
```

Open that URL on a damaged device to clear only Soty browser-origin state, service-worker cache, and local room/device data, then pair it again. Use this after a browser profile copy, reinstall restore, or visibly inverted remote-access state.
The same local repair is also available as a service gesture: click or tap the QR code canvas ten times within a few seconds.

Managed recovery uses the same companion and the same loopback port, but installs it for the whole machine. The PWA remains the primary control plane; the browser itself cannot click or approve UAC/admin prompts because operating systems intentionally keep those prompts outside the browser sandbox. One explicit admin grant is still required before the user-scope worker can hand off to the machine-scope worker. After that grant, the worker must report `scope: "Machine"`, `system: true`, and `maintenance: true` from `/health`, and can handle admin tasks through the same PWA tunnel. On Windows the worker runs as `SYSTEM`; on macOS/Linux it runs as root through LaunchDaemon/systemd.

```powershell
powershell -ExecutionPolicy Bypass -File .\install-windows.ps1 -Scope Machine -LaunchAppAtLogon
sh install-macos-linux.sh --scope machine
```

From an operator workstation, `sotyctl install-machine <target>` asks the target desktop for that one admin grant and `sotyctl machine-status <target>` verifies that the active loopback worker is the machine worker. The installer stops current-user Soty agent processes before starting the machine task so the machine worker owns `127.0.0.1:49424`; there should not be two active agents competing for the port.

This keeps the PWA as the primary operator channel after reinstall: the local loopback worker starts at boot, and Windows opens the Soty PWA at user logon.

## Wake Pulse

The bell control and typing wake use the already-open tunnel. They can vibrate a hidden PWA once while the unread mark is active. A fully closed PWA cannot receive live browser events without Web Push.
