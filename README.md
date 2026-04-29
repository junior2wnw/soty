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
- the remote icon grants one-way command access to the selected counterparty;
- command access uses a local companion agent that the PWA can detect on `127.0.0.1:49424`;
- the installed companion agent starts with the OS and updates itself from `/agent/manifest.json`.

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

The PWA never runs OS commands by itself. For normal use, open the counterparty menu and press the remote icon. If the local companion agent is absent, the PWA shows the installer control. After the installer runs once, the agent starts with the OS and updates itself.

For development, run:

```bash
pnpm run agent
```

Then open the counterparty menu in the PWA and toggle the remote icon. Commands typed by the granted counterparty are bridged through the local loopback agent on `127.0.0.1:49424`.

On Windows the agent uses PowerShell by default. Use `SOTY_AGENT_SHELL=cmd` when a device should run commands through `cmd.exe`.
