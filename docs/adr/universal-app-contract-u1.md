# U1: closed declaration and local admission reference

Status: accepted U1 declaration decision. The ephemeral reference described here
is the original prototype; it is not the current durable runtime receipt. The
implementation now also has Source-fenced durable registration/admission and
mandatory feedback provisioning. See the [current delivery matrix](../implementation/universal-delivery-matrix-20261007.md)
and [verified Apps7 image/cold-restore receipt](../implementation/universal-apps7-image-canary-20261007.md)
for implemented, tested and remaining work. The production Soty image has not
been updated by this implementation task; Source integration and author self-service
must not be inferred from this ADR.

Keep .soty/app.json as the current deployment contract. Introduce the independent
soty.app-agent.v1 declaration validated by modules/app-contract. Authors describe
typed capability/resource/skill/document pins and mandatory feedback intent.
They do not issue actors, source ownership, grants, handler authority or ready state.

Simple onboarding uses a second closed declaration, soty.app-author-draft.v1:
only title and an optional known schema. A trusted materializer supplies all app
scope/source/auth/visibility metadata and one explicitly named registered
authorProfile {id,version,digest,feedback}. No first-provider fallback exists.
The profile is an immutable host pin with history and current authority checks.
The simple path has empty capabilities/skills/docs and disabled reviews;
advanced capabilities still require reviewed bindings, never automatic grants.
The advanced factory fills only absent schema/digest, rejecting explicit
unsupported versions and stale semantic pins.

Host context is a trusted code handle scoped by registry/tenant/app/environment;
it is never created from the network Host header. Bindings must match the exact
source and accepted semantic capability contract. Read-only public reviews
remain separate from managed placement and moderation; only verified public
projections may be read anonymously. Human auth is a declaration with a registered
profile reference; actual shared login/old account linking belongs to U2.
Managed placements pin an exact provider ref in addition to scope, subject and
rights; a different approved provider cannot inherit that placement authority.

Use existing pure validation primitives and builtins. The bounded canonical
format is explicitly documented, not advertised as JCS. Semantic capability
pins exclude cosmetic prose, documents, handler version and deployment metadata.
The full descriptor digest still binds exact intent/replay. Notes@1 and canonical
Identity remain their existing semantic wires; no route or server config changes.

Provide a reference private head/CAS and receipt brand to test readiness and
stale callbacks. Successful registry upgrade retires the previous handle.
It is intentionally ephemeral; JSON state is presentation, not restorable
authority. UI fixture readiness never enables agent/local invocation or real
production admission. U3 requires durable registry history, transactional
admission/outbox, verified provider commits, current ACL and independent grants.

Bound the retained fixture admission heads at128 without automatic eviction.
Exact replay remains available at saturation. Explicit close disposes the local
handle/maps; it is not production history deletion or a ledger-reset mechanism.
Compile immutable lookup indexes and cache authority digests once per generation.
No discovery server is introduced: U3 supplies permission-filtered pagination.
CLI keeps safe error codes and exit semantics and adds only static diagnostic
stages/messages/hints; author/host values and input paths are never reflected.

A separate Python author sample independently hashes Unicode metadata and
passes the same actual Node CLI without core patches. This demonstrates a
language-neutral declaration, not a deployed SDK/SSO/media/MCP/inbox service.
