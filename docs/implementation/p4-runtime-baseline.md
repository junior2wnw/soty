# P4 runtime baseline and WebSocket EOF correction

30.09.2026. Root integration, starting from `6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e`. This checkpoint changes no storage format or production deployment.

## Runtime and provenance

SQLite documents a WAL-reset race fixed in 3.51.3 and later, with separate maintenance backports. The local system Node24.13.1 embeds SQLite3.51.2. No corruption was observed in this task; the documented race is the reason to require a patched runtime for forthcoming storage work. [SQLite WAL-reset bug](https://www.sqlite.org/wal.html#the_wal_reset_bug), [Node24.15.0 release](https://nodejs.org/en/blog/release/v24.15.0).

Root downloaded the official Windows x64 Node24.21.0 into ignored `var/toolchains/node-v24.21.0-win-x64/node.exe`. Its SHA256 `ba4e6d110e8c1592a1ecd390f6b05f3da124b13871a5be62b341a07a853c6c32` matched the official release checksums; Windows Authenticode is Valid, signer OpenJS Foundation. An actual in-memory SQL query reports Node24.21.0 / SQLite3.53.4. Test commands prepend this directory to their own PATH so child processes also use it. System Node, user PATH and production remain unchanged. [Official release](https://nodejs.org/en/blog/release/v24.21.0), [checksums](https://nodejs.org/dist/v24.21.0/SHASUMS256.txt).

The root and standalone Connect engines/documentation now require Node24.15.0+, with the actual SQLite version checked for a particular build. Historical verification receipts retain the versions actually tested.

Read-only Docker Registry inspection of the existing pin `sha256:4f2b45e32dc7d2caf66b6dbd59fac50e32f8077769efe0ef4d4c3f114672537d` resolves the linux/amd64 manifest `sha256:c70f2d9b9dcd1f95d51b1f2d9c000637f203dbe2cbeaf06680780584518ca5c3`, whose image config reports NODE_VERSION24.15.0. This is metadata, **not execution evidence of Linux SQLite/FTS5**. The runtime pin was not changed. Exact Linux application/helper execution remains a release gate.

## Regression found and corrected

The first complete new-runtime suite reproduced a real WebSocket capacity issue: after the client terminated, its replacement was refused with429. Even waiting for an actual source cancellation timed out; merely waiting for local client close did not establish remote cleanup. The gateway's async socket reader forwarded an `end` message but retained the stream while an upgraded TCP socket could remain half-open. That depended on later peer activity or heartbeat expiry.

On terminal TCP input EOF the WebSocket path now calls the existing idempotent `closeStream`, releasing only that stream's two maps, pending waits, timer and source. It does not alter HTTP uploads, raw CONNECT tunnels, frame handling, control-frame timing or admission capacity. The capacity test waits for the exact remote cancellation before opening its replacement, without sleeps/retries. A diagnostic refusal includes its synthetic request path.

WebSocket Close control frames still pass through the relay. Two additional real gateway/connector/upstream tests check client-initiated and source-initiated Close: both endpoints retain code1000/reason`finished`, the closed slot is freed and a neighboring socket still exchanges data. Bare TCP termination is not represented as a clean WebSocket close. [RFC6455 §7.1](https://www.rfc-editor.org/rfc/rfc6455#section-7.1.1).

A subsequent whole-suite run exposed an independent flaky privacy assertion: a random opaque discussion cursor happened to contain `bio`. The test now rejects exact JSON field names; its separate assertions against actual private profile/message values remain. No product projection or opaque cursor was changed.

## Executed evidence

All artifacts below are under ignored `output/implementation-20260930/`.

| Gate | Result | Log |
|---|---|---|
| Capabilities, discovery HTTP/executor and Connect on unchanged6cd |176/176 pass;7.727s |`p4-b1-node2421-capabilities-connect-baseline.log` |
| Initial world baseline |856 total,850 pass,1 failure,5 opt-in skips |`p4-b1-node2421-world-baseline.log` |
| Isolated capacity diagnostic |Same `/public-replacement`429 reproduced; subsequent remote-cancel wait also failed |`p4-b1-node2421-capacity-{isolated,diagnostic,corrected}.log` |
| Capacity after EOF fix |1/1 pass |`p4-b1-node2421-capacity-eof-fixed.log` |
| Integration after EOF fix plus two Close cases |858 total,852 pass,1 unrelated random-cursor assertion failure,5 opt-in skips |`p4-b1-node2421-world-eof-fixed.log` |
| Final world integration |**858 total,853 pass,0 failures,5 pre-existing opt-in skips;94.081s** |`p4-b1-node2421-world-final.log` |
| TypeScript, connector release build and Vite build |PASS on Node24.21.0 |`p4-b1-runtime-types.log`, `p4-b1-runtime-build.log` |

The five skips remain installed connector bundle,130-second default WebSocket run, real OpenCode job, connector recovery and512MiB file transfer; older separate evidence is not a new execution here. No new frontend design, physical phone, installed PWA, Linux image, schema migration or production claim follows from this runtime checkpoint. Source review of the narrow EOF/Close/privacy changes was independently performed by `whole_product_critic`.
