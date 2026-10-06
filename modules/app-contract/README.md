# Universal app contract — portable U1 core

Pure, local conformance core originally prototyped on historical base 5a854855.
The companion [durable registration service](server/README.md) now persists
admission and creates a real feedback inbox through the host composition.
This pure entrypoint itself installs no route, transport or executor.
A valid descriptor is metadata, not a grant.
All plans explicitly have productionAdmission:false; only synthetic feedback
confirmation can produce fixture-ready. UI, agent and local gates are independent.

Node builtins and the existing pure Capabilities validation helpers are used.
The package selftest works on Node24.13.1 / Python3.11.9; the full Soty server
still requires its existing Node>=24.15.0 engine. No compatibility claim for a
new MCP protocol follows from these tests.

## Developer entry

В уже проверенном контексте платформы автору достаточно названия — без scope,
ссылок на провайдеров и 64hex pins. Подтверждение source ownership и аккаунта
остаётся обязанностью host integration:

    node modules/app-contract/cli.mjs draft --title "Мой проект"

Результат — закрытый soty.app-author-draft.v1 с одним title. Это заявка с
metadata; она не является готовым приложением и не получает прав. Автор не
выбирает tenant, источник, host или исполнителя.

Короткая локальная демонстрация:

    node modules/app-contract/examples/author.mjs

Или создать **новую** папку только с синтетическими файлами:

    node modules/app-contract/examples/author.mjs output/u1-simple-author
    node modules/app-contract/cli.mjs materialize output/u1-simple-author/.soty/author.json --fixture-host output/u1-simple-author/fixture-host.json

--fixture-host — операторский тестовый файл, а не доказательство владения
сервером/аккаунтом. Материализация берёт app ID/namespace/source/auth/visibility
из доверенного host context и один явно одобренный named authorProfile.
Платформа подставляет его автоматически из настроенного registry; оператор
настраивает доверенные profiles, а не вручную собирает pins для каждого автора.
Без такого профиля возвращается author_profile_required: выбор «первого
провайдера» отсутствует. Нынешний пример использует private/members preset;
сгенерированный descriptor содержит обязательный feedback, пустые
capabilities/skills/docs и disabled reviews. Никакого выполнения он не включает.

В коде автора:

    import { createAuthorDraft } from './modules/app-contract/sdk.mjs';
    const draft = createAuthorDraft({ title: 'Мой проект' });

В доверенном операторском коде:

    import { materializeAuthorDraft } from './modules/app-contract/sdk.mjs';
    const descriptor = materializeAuthorDraft(trustedHost, draft);

Registration API реализован в companion `server/registration.mjs` и подключён
к подписанному Connect через `server/universal-apps.js`. Реальный feedback
проверяется отдельным provider и после commit; неизвестный ответ не создаёт
второй inbox. Сам переносимый core по-прежнему не исполняет приложения.
Host-only registered adapters и общий ledger находятся в `modules/capabilities`;
их конкретный source, текущие права и допуск проверяются отдельно.
Для продвинутой интеграции ниже сохраняется строгий createDescriptor с проверенными ссылками.

Keep deployment's strict .soty/app.json unchanged. Write a separate
.soty/agent.json using createDescriptor from sdk.mjs (see examples/fixture.mjs).
The advanced factory fills absent schema, capabilities/skills/docs, disabled
reviews and absent semantic capability digests. Explicit unsupported schema,
null defaults or stale supplied digest are rejected, never silently normalized.
It does not generate trusted pins.
Capabilities may be empty for a UI-only application. IDs are stable host-issued
references; names and source URLs are not identity or authorization.

Root fields are exactly schema, app, capabilities, skills, docs, feedback, reviews.
App contains id, namespace, title, visibility, source and auth.
Source is {id,revision,digest}. A reference is exactly {id,version,digest};
digest is lower-case SHA256 hex, version is a positive safe integer <=1000000.
Capabilities contain id/version/digest, inputSchema/outputSchema, resources,
effects, recipients and binding; title/description are optional prose.
Resource, capability, skill, document and binding IDs are namespaced
namespace:name/path. Capability/resource IDs must belong to app.namespace.
Skills/docs are immutable pins, not instructions or executable packages loaded here.
No structured field accepts a secret, endpoint, command, owner proof or readiness.
Known credential-like strings are conservatively rejected, not a complete DLP scan.

Human auth is discriminated: {mode:'public'}, or
{mode:'linked-existing'|'shared-soty',profile:ref}. The latter require a matching
host-registered auth profile. Declaration does not prove a working login or SSO.

Feedback is mandatory: mode:'required', provider/captureProfile/retentionProfile
refs, submitAudience:'members'|'public', ticketVisibility:'reporter-and-support'.
A private app cannot declare public submission. This does not grant support
access or create an actual tenant inbox; U3 supplies ACL/storage/media adapters.

Reviews are discriminated:

- disabled: only mode;
- public-read: provider and subjects (references). No publisher proof or managed
  placement is required. Host supplies only previously verified public subject
  projections; private references cannot become public by declaration;
- managed: provider and placements with id/version/digest/subjectId/rights.
  The host placement additionally pins the exact approved reviews provider ref.
  Matching provider, host-owned scope and rights are required; display does not imply
  collect/reply/moderate-origin. No managed grant is issued by planning.

## Trust and reference lifecycle

createAdmissionHost takes trusted server configuration, never HTTP Host or
request fields. context includes scope {registryId,tenantId,appId,environmentId},
namespace, ownerId, authorityRevision, visibility, source, auth.
An explicit personal tenant is still required; null/global tenant is unsupported.
Optional trusted authorProfile is exactly {id,version,digest,feedback}, with the
full required feedback shape above. Its pins must match approved registries and
its audience must match the current context. Trusted platform code selects an
explicitly approved profile from the configured registry; the incoming draft
never selects it. It is included in the authority digest and the
immutable version history, including removal and re-add. Profile upgrade retires
old hosts/receipts; it never auto-registers capabilities or review placements.
Bindings pin scope, source and accepted capability id/version/semantic digest.
Each managed placement contains its provider ref; approval of another reviews
provider cannot reuse the existing placement/subject/rights.
providers/profiles/skills/docs/placements/publicSubjects are immutable registries.
These contain only refs and policy projections, no callable function or endpoint.
See fixtureConfiguration for the complete local host shape.

This boundary is an in-process private handle, not production crypto or proof of
source ownership. Arbitrary trusted host code can construct it. Untrusted ingress
must pass bounded JSON bytes. Object snapshotting rejects getters/symbols/custom
prototypes; it does not sandbox arbitrary Proxy traps or hostile JavaScript code.

planAdmission creates a content-free proposal and mandatory feedback intent.
The provisioning key is stable across source updates/owner changes within the
same registry/tenant/app/environment/provider ID; it is not an access token.
Owner/source visibility and auth are checked against the trusted host.
No metadata field changes tenant, owner or grants.

beginAdmission maintains one private in-memory head per request ID. Same intent
replays its current head; changed intent conflicts. holdFeedback fences older
callbacks. createFeedbackReceipt is for a trusted adapter after its own provider
verification; copied JSON is not a receipt. confirmFeedback checks the current
authority/head/revision and cannot fork a request. Identical lost-ACK replay
returns the prior committed synthetic receipt; a different receipt conflicts.
At most128requests are retained per host. Saturation rejects only a new request
with admission_request_limit; exact replays and conflict checks remain available.
No live request, receipt or idempotency record is evicted automatically.
closeAdmissionHost releases this ephemeral fixture's maps and retires its handle;
all retained callbacks fail host_closed. It is fixture disposal, not production
deletion or permission to start a fresh ledger to bypass the cap.

upgradeAdmissionHost requires a higher authorityRevision, unchanged scope,
immutable same-version registry records and semantic capability pins. It retires
the old handle, so a stale callback cannot keep using it. Changed source uses a
new binding version; semantic changes require a new capability version. Removed
refs are permitted for revocation. Private version history retains their pins,
so removing and later re-adding a version cannot silently rewrite it.
U3 must persist that history and tombstones across processes/restores.
Approved active registries are indexed once; the immutable authority digest is
cached once. Limits are published in LIMITS:128entries per registry and4096
historical pins/contracts, in addition to shared64KiB/depth/node limits.
The host configuration is one app-scoped view, not the complete portfolio.
U1 exports bounded admission plans; no discovery/list-all server is added here.
U3 must retrieve current permitted catalog pages before building that host view.

State/receipt handles are not restorable from JSON. There is no persistent CAS,
transactional outbox, provider retry/reconciliation or real feedback runtime.
U3 must replace the reference head with durable transactions/fencing and verify
provider receipts/current ownership. Global exactly-once is not promised.

## Deterministic format

This is the bounded U1 canonical format, not RFC8785/JCS. ASCII object keys are
sorted lexicographically; Unicode scalar strings keep their exact code points
(no normalization) and use JSON escaping, preserving non-ASCII characters.
Numbers are safe integers only; floats/exponents, -0, unsafe integers, accessors,
duplicate decoded keys and unpaired surrogates are rejected. Object limits:
64KiB input/canonical bytes, depth18,4096nodes, arrays<=128; capabilities<=32.
Schemas are a closed draft2020-12 subset: object/array/string/integer/boolean/null,
closed properties, finite string/array bounds, integer bounds and scalar enum.
Remote refs, schema code and automatic imports are unsupported.

Descriptor arrays preserve order. Semantic capability hashing sorts resources
and recipients by ASCII ID and sorts effects; schemas retain array ordering,
including required/enum. The semantic digest covers id/version, schemas,
resource/recipient contract pins and effects. Prose, skills/docs, binding and
source/deployment pins are separate. The full descriptor digest includes them
and binds exact request replay. Existing Notes semantic wire/digest and canonical
Identity contracts are not renamed, re-hashed or consumed by this package.

## Conformance commands

From the repository root:

    node --test modules/app-contract/test/*.test.mjs
    node modules/app-contract/examples/fixture.mjs
    python modules/app-contract/examples/descriptor.py output/u1-author-example
    node modules/app-contract/cli.mjs validate output/u1-author-example/.soty/agent.json
    node modules/app-contract/cli.mjs plan output/u1-author-example/.soty/agent.json --host output/u1-author-example/fixture-host.json --request-id fixture.request

Choose a new empty output directory: the Python example creates files exclusively.
It independently computes Python stdlib hashes, invokes the actual Node CLI,
compares descriptor/capability digests, and writes admission-proposal.json.
All identities and provider refs are invented; no network calls occur.
CLI errors expose only stable codes. It reads at most65537bytes per input and
rejects oversize input without printing input, credentials or file contents.
Errors retain ok:false/error and exit1, with static Russian message/hint and a
fixed stage. Missing files, malformed JSON, unsupported schemas and missing
bindings have distinct diagnostics; paths, input values and stacks are not echoed.
Use --help for exact command forms. Successful legacy validate/plan outputs retain
their shape; materialize adds prototype/productionAdmission:false and explicitly
labels its trusted local fixture input.

The fixture demonstrates pending-feedback -> feedback-held -> fixture-ready,
current-head replay, changed-intent conflict, zero provider calls and zero
executed handlers. Tests include stale host/CAS callbacks, public/private
reviews, tenant/environment scope, semantic escalation, malformed JSON, safe
errors, independent Python conformance and unchanged legacy contract vectors.
