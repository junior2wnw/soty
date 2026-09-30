# P2 — access control and room workspace

Status: implementation complete; Access component checks and integrated room browser checks passed within the boundaries below. No production deployment or external execution is claimed here.

## Access control

`mountAccessPanel(host, { api, accountId, accountLabel?, availability? })` returns synchronous `dispose()`, asynchronous `flush()`, and `hasUnsavedChanges()`.

- Signed owner operations list principals, grants, actions and audit events with bounded pagination. Invocation count limits are labelled as actions, not money.
- New connection is closed by default. Availability must supply an enabled Notes handler and a fixed canonical audience. It is rechecked on opening the form, before principal/grant creation and before credential issuance.
- Consent is limited to creating new private Notes for the selected account, with an explicit expiry and action limit. No old-note read, publication, delegation, or runtime permissions are implied.
- Credential is displayed once, initially masked. It exists only in the mounted dialog closure/input; it is not stored in browser storage, URLs or logs. Closing or disposing removes it. Copy success appears only after clipboard acknowledgement.
- Revocation first reads its impact and then requires the actual matching server acknowledgement. A lost acknowledgement does not render a revoked state. Existing changes are not described as rolled back and running work is not described as stopped.
- Account-invalid errors erase content. Disposed requests cannot repaint the old account or a late credential. Partial connection creation has a separate explicit cleanup action.

Browser checks ran in `src/world/access-panel.test.html`, an isolated development fixture with synthetic in-memory API data, never a production service token. The fixture makes its status visible and is not imported by the production entrypoint.

Verified: successful and lost revocation acknowledgements; unavailable handler; masked credential/copy/close; lost issuance acknowledgement and cleanup; dispose during pending credential and list responses; dark/light at a 320 px nested browsing viewport; Escape; arrow-key tab navigation. The mobile content measured `clientWidth === scrollWidth === 305` within the 320 px frame including its scrollbar; the dialog also had equal client and scroll widths. Access checks did not change the browser viewport. These are component checks, not proof of external MCP/OAuth or live handler execution.

## Room workspace

`rooms.css` replaces the role of `style.css`; it does not import it or append overrides to it. `tools-shell.css` contains only shared navigation and the native choice dialog.

- Desktop navigation is the shared 80 px rail and 56 px header, with Apps, Chats and Assistant. On mobile the same three destinations form the bottom navigation.
- Rooms retain the existing controller hooks. The existing profile/device button is moved into the global header before `main.ts` attaches its handler; there is no second identity or profile implementation.
- The existing geometry module still owns hex aspect ratio, safe content area and coordinate placement. The new theme supplies colors, emphasis and spacing only.
- Rooms, conversation and utilities form three areas on wide displays. On narrower displays the utilities become a horizontal shelf rather than disappearing; room permissions, files, chess and commands stay reachable.
- Chat search, pinned messages, reply/edit, voice/listening, draft/thinking, delivery, drop target, terminal host/controller/collapsed, chess/legal/capture/promotion, QR scanning, account phrase, install and traffic states keep their existing selectors.
- Terminal content remains plain text. Chess colors preserve square/piece meaning; other surfaces use the shared neutral tokens. Motion follows both the preference and reduced-motion media query. Native hidden and forced-colors behavior is preserved.
- Readable room labels, state and helper text have a 12 px minimum. Mobile composer controls retain 44 px targets with matching grid tracks, including at 320 px.
- On phone and tablet, the chess board and statistics stack in a scrollable column with non-shrinking children. This avoids the clipped board/overlapping statistics found during the real browser check.

Static verification: TypeScript passed; Git whitespace check passed; Vite's PostCSS parser accepted both files (518 room rules and 64 navigation rules at the final checkpoint). The legacy selector inventory was compared against the replacement: the 11 classes without a dedicated rule are button variants covered by the new base or parent rules; no visibility/state selector is omitted.

### Integrated browser checks

The root-owned `main.ts` import now loads `rooms.css`. Observed stylesheet sources confirmed that `src/style.css` was absent and the independent Connect stylesheet remained loaded. Checks used a separate Chrome test tab at `http://127.0.0.1:5300`, never the root's tab.

- Desktop 1920 × 911: page, shell, conversation and utility areas had no horizontal overflow. The shared hex measured 82 × 71 px. The board was 540 × 540 px with 64 equally sized cells. Space selected e2 and Enter moved the pawn to e4; the local chess engine responded and the move history persisted.
- Phone 320 × 740: page and chat/composer tracks had equal client and scroll widths. Three composer controls measured 44 × 44 px. The complete board measured 271 × 271 px after reserving the scroll bar; additional statistics remained below it in the same scroll region without overlap. The file dialog measured 286 px wide with equal client and scroll widths.
- Tablet 768 × 900: no page overflow; the final board measured 395 × 395 px, followed by readable statistics. Screenshots were visually reviewed at all three widths. Temporary viewport overrides were reset to 1920 × 911 after testing.
- The existing profile button opened the actual Connect profile dialog; Escape closed it and restored focus. The file dialog and the terminal choice dialog closed through Escape. With no authorized device, the terminal choice offered only connecting a computer, without pretending commands were ready.
- The original QR overlay failed Escape and focus restoration. After the root's native-dialog migration, it was an open named `DIALOG`, initially focused Close, and Escape removed it and restored the QR opener. The actions dialog similarly focused its labelled search and restored the opener on Escape. A complete cross-browser Tab cycle was not asserted by this check.

Only a local chess move and transient presentation state were changed. No file was uploaded or deleted, no shell command or quick action was executed, no camera was opened, and no permission grant or connection acceptance was performed. QR content was not read, decoded or copied.

## Independent review provided to root

The assistant/server review found an actual duplicate-admission path after a lost acknowledgement followed by changing inference readiness. It was reproduced with the real store and extension in a disposable temporary directory: original accepted, same request returned `app_model_unavailable`, a new request ID created a second job. Root owns the fix and regression tests. Additional findings sent to root: a throwing real device resolver can break the whole history page; result projection still included an internal session identifier; assistant account invalidation needed synchronous erasure; concurrent continuations require serialized admission or an explicit branch model.

The independent assistant findings remain owned by root; this receipt does not claim their fixes have been independently retested. The room checks above verify the loaded UI and specified scenarios, not every remote transport or permission path.
