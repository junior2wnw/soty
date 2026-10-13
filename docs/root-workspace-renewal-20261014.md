# Root workspace renewal without remounting the Native editor

Base: ee08358f55c5de36aae99017c42c71e7db8357b7 (approved Source entry fix).

The reactive frame refresh always set shortSessionHandoff, which is the RP-only carrier branch. Root workspace2 SDK ACKs correctly omit shortSession, so the unchanged Apps service rejects that flag with app_scoped_short_session_required. The client also incorrectly demanded an RP short ACK. The existing Root workspace branch already acquires a fresh finite signed identity and requires a fresh private Source ACK.

The client now selects the existing Root branch for the exact approved workspace2 pin. RP handoffs and all server/SDK fences remain unchanged. Separately, Root wake/known-expiry recovery calls the same renewal.refresh instead of launchRuntime: ready, unknown and denied results never remount the visible Native editor. No automatic write replay occurs. Manual Refresh remains explicit.

Evidence: regression fails on the prior client. 84 focused tests pass (Root lifecycle, actual Apps/gateway HTTP, Human issuer witness, frame fetch). The actual Apps test expires the old slot by 301 seconds, confirms old query denial, renews through the real private connector ACK path and reads the same main frame locator; widget isolation and revoked actor denial remain. That connector is a controlled fixture, not a Native service. The separate actual Perem SDK fixture already proves fresh Root reference preserves its Native selection and scoped data. Actual Stage callback execution confirms zero full reloads on ready or failed renewal. TypeScript check passes. No Live operations.

Custody: existing gateway expiry keeps only the bounded attempt, and Apps retains the original session family frame locator; fresh current actor/target/Human/Native confirmation is still required. Explicit revoke/close and server RAM loss deny. After the existing 24-hour attempt deadline or server restart, manual Refresh can still be required. Five-minute real browser acceptance is a parent gate, not implied by these local tests.
