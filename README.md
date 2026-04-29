# соты.online

A small PWA surface for a long-lived byte tunnel between counterparties.

The browser side stays deliberately simple:

- an installed PWA owns a local device name;
- an empty device shows a QR code in the honeycomb field;
- scanning the QR creates an incoming request on the device that shows the QR;
- after approval, the tunnel remains until a counterparty closes it;
- the upper field contains counterparties as movable hex cells;
- the lower field is one shared text surface;
- every character, deletion, line break, and file moves through the same tunnel;
- file transfer works from the counterparty menu and drag and drop;
- the remote icon grants one-way command access to the selected counterparty;
- optional local command execution is provided by `npm run agent` on the device that grants access.

## Layout

- `src/main.ts` wires the app lifecycle.
- `src/trustlink.ts` keeps local identity, tunnel records, QR joining, and tunnel payload helpers.
- `src/sync.ts` owns realtime text, files, pairing, remote intents, snapshots, and reconnect transport.
- `src/ui/*` contains UI primitives such as the honeycomb field and counterparty menu.
- `src/features/*` contains optional tunnel features.
- `src/core/*` contains tiny shared helpers.
- `server/*` is the relay: HTTP shell, room store, validators, and WebSocket routing.
- `public/*` is the PWA manifest, service worker, and icon.

## Run

```bash
pnpm install
pnpm run typecheck
pnpm run build
pnpm start
```

Default port: `8080`.

## Local Agent

The PWA never runs OS commands by itself. On a device that should be controlled, run:

```bash
npm run agent
```

Then open the counterparty menu in the PWA and toggle the remote icon. Commands typed by the granted counterparty are bridged through the local loopback agent on `127.0.0.1:49424`.
