# soty.online

A small PWA surface for a long-lived byte tunnel between counterparties.

The browser side stays deliberately simple:

- an installed PWA owns a local device name;
- an empty device shows a QR code in the honeycomb field;
- scanning the QR creates an incoming request on the device that shows the QR;
- after approval, the tunnel remains until a counterparty closes it;
- the upper field contains counterparties as movable hex cells;
- the lower field is one shared text surface;
- every character, deletion, line break, and file moves through the same tunnel;
- direct WebRTC data channels are used when the devices and networks allow it;
- the relay remains the reliable path for signaling, reconnects, history, and fallback delivery;
- file transfer works from the counterparty menu and drag and drop;
- the bell icon sends one small wake pulse through the existing tunnel;
- the remote icon grants one-way command access to the selected counterparty;
- command access uses a local companion agent that the PWA can detect on `127.0.0.1:49424`;
- local operators can use `sotyctl` to list remote targets and run commands through an opened PWA bridge;
- long remote jobs can be staged as temporary scripts and launched without visible terminal windows;
- the installed companion agent starts with the OS and updates itself from `/agent/manifest.json`;
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

The PWA never runs OS commands by itself. For normal use, open the counterparty menu and press the remote icon. If the local companion agent is absent, the PWA shows the installer control. On Windows it downloads `install-soty-agent.cmd`; the script installs the agent into the user profile, brings portable Node.js when needed, starts the agent, and registers autostart through the task scheduler, the current-user Run key, or the user Startup folder. On macOS and Linux, `install-macos-linux.sh` does the same with LaunchAgent, systemd user service, or desktop autostart. After the installer runs once, the agent starts with the OS and updates itself.

For development, run:

```bash
pnpm run agent
```

Then open the counterparty menu in the PWA and toggle the remote icon. Commands typed by the granted counterparty are bridged through the local loopback agent on `127.0.0.1:49424`.

On Windows the agent uses PowerShell by default. Use `SOTY_AGENT_SHELL=cmd` when a device should run commands through `cmd.exe`.

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
~/.soty-agent/sotyctl say Phone "Пишу как живой оператор."
~/.soty-agent/sotyctl say --fast Phone "Короткий статус."
~/.soty-agent/sotyctl read Агент
~/.soty-agent/sotyctl listen Агент
~/.soty-agent/sotyctl export soty-backup.json
~/.soty-agent/sotyctl import soty-backup.json
```

The bridge works when the PWA is open on the controlling device. `run` and `script` also require remote access to the named counterparty.
Use `script` for larger jobs: the agent writes a temporary file on the remote device, runs it hidden, streams output back, and removes the temporary file.
Use `say` to queue live typing into the shared text surface through the PWA; it returns after the message is queued so long operator notes do not hold the HTTP request open. Use `read` to fetch messages sent from the PWA after Enter, or `listen` to long-poll them as JSON lines for an IDE-side assistant loop. Use `export` to save a local JSON backup of the PWA-visible device metadata, tunnel records, selected room, current shared text, and file metadata. Use `import` on a fresh PWA to create a new local device from that backup and restore rooms/text snapshots. Remote command grants are session-only and are intentionally not backed up or restored.

Emergency local repair:

```text
https://xn--n1afe0b.online/?pwa=1&reset-local=1
```

Open that URL on a damaged device to clear only Soty browser-origin state, service-worker cache, and local room/device data, then pair it again. Use this after a browser profile copy, reinstall restore, or visibly inverted remote-access state.
The same local repair is also available as a service gesture: click or tap the QR code canvas ten times within a few seconds.

Managed Windows recovery uses the same companion and the same loopback port, but installs it for the whole machine. The PWA remains the primary control plane; the browser itself cannot click or approve UAC because Windows intentionally keeps UAC outside the browser sandbox. One explicit UAC/admin grant is still required before the user-scope worker can hand off to the machine-scope worker. After that grant, the worker must run as `SYSTEM`, report `scope: "Machine"`, `system: true`, and `maintenance: true` from `/health`, and can handle admin tasks such as backups, boot-media preparation, and reinstall staging through the same PWA tunnel.

```powershell
powershell -ExecutionPolicy Bypass -File .\install-windows.ps1 -Scope Machine -LaunchAppAtLogon
```

From an operator workstation, `sotyctl install-machine <target>` asks the target Windows desktop for that one UAC grant and `sotyctl machine-status <target>` verifies that the active loopback worker is the machine worker. The installer stops current-user Soty agent processes before starting the machine task so the machine worker owns `127.0.0.1:49424`; there should not be two active agents competing for the port.

This keeps the PWA as the primary operator channel after reinstall: the local loopback worker starts at boot, and Windows opens the Soty PWA at user logon.

## Wake Pulse

The bell control and typing wake use the already-open tunnel. They can vibrate a hidden PWA once while the unread mark is active. A fully closed PWA cannot receive live browser events without Web Push.
