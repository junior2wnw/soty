# P2 — recently opened Notes without a connection

Date: 2026-09-30. Scope: `modules/notes/browser/*`, new cache tests, `src/world/notes.ts/.css`. Server, Connect, PWA worker and unrelated World components were not changed. This receipt distinguishes automated checks from the root-owned real-browser gate.

## Problem and result

Previously, a successful server save retired its clean draft branch. After a server outage and reload, only unsent drafts were recoverable; an already saved note opened as an empty landing view. The outbox was working, but no separate read cache existed.

Recently opened or successfully saved own notes now receive an independent clean snapshot. A failed network request can open that snapshot with **«Копия на устройстве»**, its last verification time and an explicit stale notice. The offline list says **«Без сети · Недавно открытые»**; it is not represented as the complete account catalogue. Search and buckets filter this bounded local set. No full account download is performed.

Edits remain ordinary draft branches and go through the existing durable outbox, mutation receipt and expected-revision CAS. A stale snapshot cannot overwrite a newer server revision or resurrect a purged note. Conflicting local text remains recoverable; saving a separate copy is still an explicit action.

## Storage and authority contract

- `createNoteCache(accountId, projectId, options)` uses its own `soty-notes-cache-v1` IndexedDB database. Keys include origin, project and account. The World host already passes project `soty`, matching Connect; the default is only for callers which omit a project.
- Limits are **20 snapshots / 2 MiB per scope**, **60 snapshots / 6 MiB per origin**, and **320 KiB per entry**. Bytes count the UTF-8 JSON record with a conservative metadata allowance; this is a payload budget, not a claim about a browser's physical storage overhead. LRU eviction affects only acknowledged snapshots. Drafts/outbox remain in their original separate database and are never eviction candidates.
- Only a validated successful `notes.get` or the exact document acknowledged by `notes.put` enters this cache. A write ACK for generation A cannot mark later text B as acknowledged. The server creation timestamp is reconstructed from the first save ACK, which has the same timestamp as the server's newly created document.
- Revision admission is monotonic: an older snapshot cannot replace a newer one; conflicting content at the same revision is rejected. Cache quotas/failures do not roll back an already received server ACK or turn it into an unsaved network mutation. UI distinguishes a server save whose offline copy could not be retained.
- `readNoteWithCache` falls back only on the actual Connect `NETWORK_ERROR` / `NETWORK_TIMEOUT` family without an HTTP response status. An HTTP server error with the same textual code cannot masquerade as a transport failure. Authentication, revocation, not-found/deleted, malformed server response, rate limit and arbitrary exceptions do not become offline success.
- Confirmed missing/deleted/purged notes remove their snapshot. An authentication/access failure clears the current scope; other accounts and dirty branches are preserved. A single durable, bounded epoch fences older in-flight cache reads/writes across instances. A late read after a newer invalidation is not applied to the UI.
- Cache is an availability feature, not an authorization credential or encrypted vault. Like the existing draft store, it contains local plaintext inside this browser origin. A fully offline device cannot learn about a remote revocation instantly. The next authoritative denial clears the relevant cache. Browser storage failures/eviction and remote erasure are not promised away; a failed cleanup blocks further cache recovery in the current instance.

## UI and lifecycle

`mountNotes` creates the cache beside the existing draft store. Successful explicit opens populate it; the list itself never downloads note bodies. A saved deep link can recover its snapshot after network failure, while a dirty branch takes precedence as the user's recoverable text.

Reconnection refreshes a clean cached editor from the server. A draft being edited is never replaced by that refresh: dirty checks and the current session identity gate late responses. Dirty sessions retry the existing outbox. Account/screen disposal removes visible bodies and lists, closes an owned delete dialog and prevents late callbacks from applying content to a new screen.

The restored draft's old card is hidden only for its exact source branch and saved timestamp. A branch modified independently by another tab remains visible and is not deleted for cosmetic reasons.

## Automated evidence

- `node --test modules/notes/test/*.test.mjs`: **35/35 PASS** at this implementation checkpoint. Existing signed HTTP ownership/revocation, service CAS, receipt/restart, quotas, search and browser-outbox tests remain green.
- The 17 new `offline-cache.test.mjs` scenarios cover clean-save recovery, server-read population, monotonic revisions, origin/project/account isolation, LRU/count/bytes, global bounds, network-only fallback, missing/revoked invalidation, delayed responses, cache-failure ACK preservation, A/B generation races, stale-edit CAS conflict and no resurrection after purge. Final-fence regressions cover failed storage after same-instance clear, individual remove and another-instance clear, plus online-only reads when cache storage was unavailable from the start.
- Composition tests use a transactional memory cache-storage adapter and the real Notes SQLite service. They do not claim to prove the browser's IndexedDB implementation.
- `npx tsc -p tsconfig.json --noEmit`: PASS. Scoped Git whitespace check: PASS.
- After the final HTTP-status classification guard, all 17 cache tests were rerun and passed; the existing policy test now also rejects network-named errors carrying HTTP 500/404.

## Browser and independent review gate

`modules/notes/test/offline-cache.test.html` exercises the **real IndexedDB driver** with a separate random fixture database. It checks committed reopening, LRU, three scope dimensions, durable invalidation, monotonic revision, network-only recovery and scoped clear. It closes/deletes its fixture database and does not modify the working cache or outbox. **Root verified all 11 checks passed in the real browser.**

Root also verified the actual production-build/PWA update on preview port 5360: open a clean note online → stop the server process → reload the same `#notes/<id>` → the full text, **«Копия на устройстве»** and verification timestamp remain → edit offline → reload retains the entire dirty text → restart the server → explicit retry reaches **«Сохранено»** → another reload shows the new text in the server list. At the 320 px emulated browser viewport there was no horizontal overflow. Evidence: `output/implementation-20260930/p2-notes-clean-offline-320.png`.

This is a desktop browser with an emulated narrow viewport, not a physical-phone claim. The real browser sequence verifies clean-cache recovery and ordinary resynchronization; concurrent CAS conflict, denied access and no resurrection after purge are covered by the automated service/composition tests, not asserted as additional manual browser steps.

After that browser run, the final read-fence storage-failure branch was strengthened and covered by the added regressions above. A read which captured a non-null epoch must prove that epoch is still current; if that final check is unavailable, its result is not applied. This includes invalidation by another instance, which does not set the original instance's local blocked flag. Initial cache unavailability still allows an authoritative online read, and acknowledged writes remain independent of cache success. Ordinary snapshot storage, UI and successful recovery flow were not changed by that final adjustment.

Independent source review by `/root/whole_product_critic` found the cross-instance final-fence failure path; it was fixed and covered by regressions. The reviewer independently reran **35/35 Notes tests** and found no remaining consequential blocker in the bounded offline Notes scope. The final HTTP-status guard was then communicated for review and verified by the focused cache run above. No production deployment or browser actions were performed by this subagent; the observed browser sequence above was supplied by root.
