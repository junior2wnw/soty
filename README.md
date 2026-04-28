# соты.online

Minimal PWA tunnel for trusted counterparties.

The repo is intentionally small:

- `src/main.ts` wires the app lifecycle.
- `src/trustlink.ts` keeps local identity, tunnel records, pairing crypto, and tunnel encryption.
- `src/sync.ts` owns realtime text, file, pairing, remote-intent, ack, snapshot, and reconnect transport.
- `src/ui/*` contains reusable UI primitives such as the hex field and counterparty menu.
- `src/core/*` contains tiny shared helpers.
- `server/*` is the relay: HTTP shell, room store, validators, and WebSocket routing are separate modules.
- `public/*` is the PWA manifest, service worker, and icon.

The relay stores encrypted Yjs updates, encrypted files, encrypted snapshots, and minimal routing metadata. The browser PWA cannot execute OS commands; remote access is deliberately a separate permissioned protocol hook for a future native agent.

## Run

```bash
pnpm install
pnpm run typecheck
pnpm run build
pnpm start
```

Default port: `8080`.

## Design Rules

- Installed PWA first; normal browser scans are routed to the install surface.
- QR carries only a join request target, never the room key.
- A tunnel lives until a counterparty closes it.
- Enter is a line break.
- Files travel through the same encrypted tunnel.
- Context menu is one gesture: right click or long hold on a counterparty.
- Runtime dependencies stay boring: Express, ws, Yjs, QRCode.
