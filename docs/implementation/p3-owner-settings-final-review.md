# P3-C1 — independent final view review

2026-09-30. Scope: `src/world/app-settings.ts` / `.css`, the native address disclosure and its narrow mobile width correction. This is a source and saved-image review, not a second browser run. Account transition, Escape integration and live browser flows have separate reviewers. Backend and pure client-state acceptance sources were not changed.

## Result and corrected findings

**Source review accepted after two narrow corrections by the root.** No additional server authority, receipt, source-readiness or publication mechanism is requested by this review. The reviewer changed only this document. The keyboard browser check is separately attributed below; the retired-label correction is source/API evidence, not a browser execution claim.

1. **Keyboard access to trailing native disclosures.** `createDialog` builds its wrap-around sequence from `button, input, textarea, select, a[href], [tabindex]`. The settings form's native `summary` elements initially have no explicit `tabindex`. With groups and danger sections collapsed, the last counted control is the publication save button; Tab wraps to the header before the following groups and danger summaries. These owner functions therefore cannot be reached through the normal forward/backward sequence. Minimal correction: include native summary in the common trap or give each settings summary an explicit `tabIndex=0`. Browser acceptance: from the publication button, Tab reaches groups, then the danger disclosure; Enter/Space toggles them; the modal boundary still wraps.

2. **A retired address retains an enabled hidden input.** The alias row hides its checkbox for a tombstone but initially disables it only when `canPublish` is false. Its surrounding label remains visible. Label activation can therefore toggle the non-rendered checkbox and add a retired ID to the unsaved publication choice, causing an unintended conflict. This is a draft/interaction defect, not a server permission bypass. Minimal correction: disable every non-bound checkbox and reject a change against a current non-bound alias. Browser acceptance: clicking the retired address text does not alter the draft or create a conflict; a bound alias still changes only the unsaved selection until explicit publication.

The root corrected both findings. The common dialog selector now includes native `summary` while retaining visibility, disabled and tab-index filters. Retired inputs are explicitly disabled; their change handler also rereads current `canPublish` and the exact alias's `bound` state before changing the draft. A permanent retirement was deliberately not performed in the browser merely to test the label; the UI correction and existing API/state rejection of retired IDs are the evidence for that boundary.

Root's actual Tab run confirmed that groups and danger summaries became reachable, but found one further part of the keyboard defect: controls inside collapsed details could still report nonzero layout boxes. They incorrectly remained the calculated last control, so the next native Tab escaped to BODY. The final root correction traverses **every** ancestor up to the dialog. For each closed details, a candidate must be inside its first direct summary. This reviewer read the final algorithm against nested cases: an inner first summary remains reachable inside an open outer section; an outer closed section still excludes inner content whether the inner section is open or closed; other content/second summaries of a closed section are excluded. Focusable children of the actual first summary remain eligible. This closes the source defect without changing settings model or action guards.

**Root-attributed final browser evidence, 667×375: PASS.** Actual key presses and DOM focus after each step showed publication save → `SUMMARY` “Выбранные люди и группы · 0” → `SUMMARY` “Закрыть приложение в Сотах” → header `BUTTON` with accessible name “Закрыть”. Shift+Tab from that header returned to the final summary. Closed details content was excluded; input events or API clicks were not substituted. This is the root's live browser proof for this concrete form and viewport, combined with this reviewer's independent nested-ancestor source analysis; it is not a claim of testing every browser or arbitrary nested DOM.

No further material blocker was found in the assigned view/fold/width/semantics boundary after that source recheck.

## Verified source boundary

- The address creation form is built once inside a native `details`. It opens automatically only at the first inspection when there are no aliases; later inspection does not replace nodes, reset the owner's disclosure choice, or clear a newly typed slug. Acknowledged claim clears only the matching slug. Claim and activation remain separate commands.
- The scoped `width:100%` rule at `max-width:760px` matches the existing bottom-sheet breakpoint. Root and descendants use border-box; long names and origins have wrapping/min-width protection. Desktop keeps the 720px cap. No global desktop selector was changed by this polish.
- Current alias activation text and unsaved checkbox choices are separate. Publication requires semantic changes, usable chosen IDs and, for anyone, explicit consent to the entire pinned source device/port including pages and APIs. Group grants do not claim to restrict an already public URL.
- Copy reads permanent links from inspection, never a one-time boot ticket. It acknowledges clipboard success only after fulfillment; failure offers a read-only full link. Private hash routes remain inspection-provided shell launch links. Preview is a distinct deliberate action.
- Retire/revoke/abandon/reset/leave have explicit distinct confirmation text. Retire uses the captured domain ID/revision; abandonment first reads current server state and explicitly does not promise cancellation. Prepared requests remain durable after a deliberate close.
- Refresh retains edited name/grants/publication CAS bases; new observations cannot erase newer input. Freshness uses the bounded server timing helper and a monotonic deadline; source response explicitly does not imply functional correctness or DNS readiness.
- Busy/disabled buttons are functionally blocked by the host capture handler, while native form submit handlers additionally check their disabled state. Error/notice regions are semantic alerts/statuses. Stable controls and keyed rows avoid focus loss from wholesale rerender.

## Saved visual evidence read independently

- `output/implementation-20260930/p3-settings-320.png`: 320px-wide sheet, wrapped title/origin, readable labels and visible address controls. The screenshot alone does not prove offscreen reachability or keyboard behavior.
- `output/implementation-20260930/p3-settings-landscape-667.png`: 667px-wide sheet, compact collapsed address disclosure and visible audience/whole-project consent. The content continues in the dialog's existing inner scroller; actual full scroll/focus measurements belong to the root browser receipt.

The root separately reported actual claim → inactive → activation → copy → exact preview, two-window CAS, pending rejection → reload → retry, Back cancel/discard and 320/667 browser checks. Those reports are not relabelled here as this reviewer's own browser execution.

## Initial reviewed hashes

| File | SHA-256 |
| --- | --- |
| `src/world/app-settings.ts` | `8ff4d39f08ded06762b8acd2e0a8a7a774a28dbff60d73eea9f9bf40f11e4a94` |
| `src/world/app-settings.css` | `583953067276119e9060cb4e67c386cf19fac1ec51f2a1d830968ba6dbf391fe` |
| unchanged `src/world/app-settings-state.mjs` | `9ebb0e2db8191c3f84e83e8a14c4f489fd95c415ceda34c5189805ce843299d7` |

## Final source recheck

| File | SHA-256 |
| --- | --- |
| `src/world/app-settings.ts` after retired-input correction | `2f42b1affb8933b7930b4f1056a4772ba77c19188fbe09afa6c93410ba05f003` |
| unchanged `src/world/app-settings.css` | `583953067276119e9060cb4e67c386cf19fac1ec51f2a1d830968ba6dbf391fe` |
| `src/world/dialogs.ts` after native summary and closed-ancestor filtering | `47aff39d97e9576871464c189c88d0b1b032740bc0199ea31396a6a7e62f5dc3` |
| unchanged independent backend test | `9ae95442e0005dbf56204d49df7ab230eb24ea51e0fe26dd8d66ba81b9b16862` |
| unchanged independent client test | `5cba3bcfa4c1434f0c00f0ca7d063fefef3c4387cd6478375b9486b6b81150b7` |

The already accepted 14 backend / 13 pure-state independent cases were not rerun to inflate this narrow UI gate. Their source hashes and the pure-state implementation hash are unchanged. Document diff check passed; the final product type/build/browser checks remain with the root's integrated checkpoint.

No production settings, test fixtures, browser state or previously rejected temporary cleanup were touched. C2 source replacement, all browsers/assistive technologies and production publication remain outside this narrow review.
