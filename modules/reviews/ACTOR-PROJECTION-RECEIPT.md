# Managed port authority/projection follow-up — 2026-10-07

This follow-up is based on Root f9d761a plus the frozen6ec924 managed port packet (new worktree f934309). Frozen Source705b150 JSON2 and its patch/bundle bytes are preserved. Original Source and production are untouched. The previous two mixed fixtures used an earlier plain owner projection; they alone were not a Root7 branded-proof gate.

The managed host API now requires `withRootActorAuthority(originalActor, callbackVerifiedOwner)`. It preserves the original Connect/capability proof reference and captures owner metadata only within the explicit current host fence. Proof families remain separate. The port rechecks after credential resolution and Source work; persistent callback poisoning prevents swallowed late callbacks from restoring authority. A private Source actor callback is single-entry and cannot redispatch after completion.

Native list output now uses a closed recursive review DTO and current Source placement context. Nested identity/private metadata/accessors/unknown fields and a foreign object/subject fail502. Known undefined native redactions are omitted, preserving the in-process Source adapter and unchanged network `{items,nextCursor}` shape. Native reply parent/root/depth fields are retained.

Validation with bundled Node24.19.0:

- Root reviews suite25/25 pass,0skip. Includes real signed HTTP Connect7 original actor versus JSON copies/foreign/revoked actors before Source dispatch; native DTO negatives; repeated/late/async Root/Apps/Source callbacks; bounded ignored-abort slots. The standalone brand test labels its Source as synthetic.
- Mixed actual Root/Source gates3/3 pass,0skip: maintained Human approval/JOSE/fresh userinfo; actual native Source JSON/HTTP/current grants; actual signed Apps and original capability credentials; in-process native list projection; lost ACK after a native review commit; Source key revoke denies the same request; four actual ignored-timeout Source promises retain slots until settlement. No public reviews, production users/configuration or tokens are used.
- Frozen Source client build passes in the isolated copy; Source runtime files and frozen packet remain unchanged.

There is still no default managed HTTP/MCP dispatch, production RP loader, PostgreSQL managed admission or distributed transaction claim. Normal durable capability budget/replay/unknown plumbing, native Source permissions and publication UI remain required. Current public-read1.1 and nested widget bytes are unchanged.
