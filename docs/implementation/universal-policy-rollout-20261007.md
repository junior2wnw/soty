# Закрытый Universal/Human rollout delta

Статус: source implementation и локальные fixtures; production не включён.
Модуль `deploy/connector/universal-policy.mjs` не вызывает Docker, SSH, сеть или
shell, не генерирует production ключи и не изменяет существующий контейнер.
Kvartal policy остаётся отдельной и сохраняет свой прежний допуск.

## Разрешённое намерение

`prepareUniversalPolicy(plan, { shellOrigins, ownerUid?, fixtureRoot? })`
принимает только локальное доверенное операторское намерение:

```js
{
  schema: 'soty.universal-rollout-policy.v1',
  phase: 'features', // либо legacy-baseline с human:null и reviews:null
  human: { issuer: 'https://SHELL/human-identity', source: '/private/exact-human.json' },
  reviews: { source: '/private/exact-reviews.json', sha256: APPROVED_NONSECRET_FILE_HASH }
}
```

`human` и `reviews` могут быть `null`. Фаза `legacy-baseline` ничего не добавляет
в Env/mounts. Фаза `features` допускает только:

- `SOTY_UNIVERSAL_APPS_ENABLED=true`;
- `SOTY_HUMAN_IDENTITY_ENABLED=1` и точные issuer/key pathname при Human, иначе `0`;
- приватный Human file bind в `/run/secrets/soty-human-identity.json`, read-only;
- при явно одобренном файле reviews — pathname и read-only bind в
  `/run/config/soty-reviews-bindings.json`.

Здесь нет client self-registration, account linking, источников из HTTP body,
предлагаемых URLs/credentials/commands или автоматического выбора файла. Review
pins проверяет тот же `createReviewsService`: отдельные scoped app/project/person,
точные provider/subject refs и только `public-read`. Это не probe публичного
провайдера, не managed rights и не подтверждение его доступности.

Private Human JSON имеет ровно существующий loader shape:
`clients`, `jwks`, `cookieKeys`, `artifactKey`, `artifactKeyId`. Реальный host profile
проверяет issuer против `shellOrigins`, фиксированные HTTPS callbacks, RSA/JWK,
cookie/client/key bounds. Непроверенные значения не становятся частью host.
Размер каждого файла ≤64 KiB; UTF-8 и duplicate JSON keys проверяются строго.

## Файлы и секреты

Production preflight поддерживает Linux POSIX filesystem. Проверяются точный
absolute normalized source, readable regular file, один hardlink, все ancestors
без symlink/junction и group/world write, разрешённые UID, final `O_NOFOLLOW`,
fstat до/после чтения, размер и повторное разрешение пути. Private file не имеет
group/other доступа или executable bits; nonsecret reviews также не допускает
group/world write или execute. По умолчанию owner — UID процесса оператора;
иной `ownerUid` является отдельным доверенным host input. Namespace ACL/SELinux,
фактическая readable доступность из container User и Docker file-bind semantics
требуют отдельного реального Linux boot.

Windows допускается только с явным task-owned temporary `fixtureRoot`.
Такая квитанция имеет `fixtureOnly:true`; обычная readiness acceptance отказывается
считать её production. Для Linux fixtures проверка ограничивается их собственным
закрытым root; production проверяет цепочку до `/`.

Handle хранит exact private bytes только в закрытом WeakMap. Повторная проверка
`assertUniversalPolicyCurrent(handle)` сравнивает identity/ancestors и байты в
памяти. Любое изменение навсегда закрывает прежний handle; замена файла теми же
байтами требует новой проверки и approval. `disposeUniversalPolicy(handle)`
закрывает handle и очищает удержанные буферы. Проверка не обещает невозможность
последующей записи доверенным владельцем/administrator: её надо повторять перед
CREATE, STOP, START и перед admission/leave.

`publicUniversalPolicy(handle)` содержит phase, разрешённые env/pathnames,
публичные protocol/client/JWKS projections, closed reviews digest и
`policyDigest` только этих несекретных полей. Нет private file hash, private key,
cookie key, client secret, artifact key или file body. Публичный digest специально
не меняется от одной смены секретов; он не заменяет private witness.

## Допуск между prepare и promote

Opaque handle не восстанавливается из JSON или публичной квитанции. Для
двухпроцессного CLI используется отдельный приватный encrypted operator artifact:

```js
const approved = await prepareUniversalPolicy(plan, filesystemOptions);
const sealed = await sealUniversalPolicy(approved, { key: OPERATOR_CUSTODY_KEY, keyId });
// packet сохранить локально приватно, atomically/exclusive и с mode0600.
// В reviewed receipt — случайный sealed.witnessId и public policyDigest.
const restored = await restoreUniversalPolicy(plan, packet, filesystemOptions, {
  key: OPERATOR_CUSTODY_KEY, keyId, expectedWitnessId: reviewedReceipt.witnessId
});
```

AES-256-GCM использует свежий random nonce, отдельный supplied 32-byte operator
custody key и AAD со schema/witnessId/keyId/public policyDigest. Внутри шифрования
находятся exact file fingerprints и filesystem evidence. Packet нельзя публиковать
как обычную квитанцию, передавать через client API или печатать. Custody key не
берётся из нового Human artifact key и не генерируется этим модулем; хранение/
чтение ключа, atomic exclusive запись encrypted packet и exact reviewed witnessId
принадлежат операторскому CLI. Отсутствующий ключ/пакет или неизвестный результат
записи — fail-closed, без нового незаметного approval и без regeneration ключей.

Restore вновь проверяет actual files, public policy и закрытый encrypted witness.
Подмена packet/AAD, неверный ключ, иной reviewed witnessId, изменение только
cookie/client/artifact key или byte-identical новая inode отвергаются безопасным
кодом. Сам packet/witness не запускает процесс и не даёт пользователю новые права.

## Tiny wiring seams для владельца rollout

Существующие `rollout.mjs`, `cli.mjs`, `runtime.mjs`, server composition здесь
не изменены. До использования требуется последовательно подключить:

1. Опциональный trusted `universalPolicy` handle в rollout args. На prepare это
   новый проверенный handle; на promote — только восстановленный с exact
   reviewed witnessId. Journal хранит public policy/witnessId, а не private
   body/hash/keys. Публичный digest без witness не разрешает promote.
2. В `createConfig` после сохранения исходных Env/mounts/networks и отдельного
   Kvartal delta вызвать `applyUniversalPolicy(config, handle)`. Функция возвращает
   clone, сохраняет остальные поля и отказывается заменить прежнее другое
   значение Env или shadowing file/ancestor mount. Exact duplicate уже
   применённого delta идемпотентен. Existing flags с другим значением требуют
   отдельного явно проверенного изменения; этот first-enablement slice их
   скрыто не перезаписывает.
3. В exact image guard вызвать `assertUniversalImagePrerequisites(handle,
   { candidateImage, originalImage })` с actual immutable image inspect.
   Dockerfile имеет `io.soty.universal.legacy=${LEGACY_UNIVERSAL_MODE}`;
   container label не заменяет image label. Storage start guard остаётся
   обязательным и продолжает проверять actual data непосредственно перед START.
4. Повторять `assertUniversalPolicyCurrent` перед CREATE/STOP/START и после
   actual startup до leave. Старый original никогда не получает новые
   env/mounts, и для проверки candidate preservation digest повторный exact
   delta не создаёт дополнительных mounts.
5. После создания actual services и Human HTTP adapter вызвать host-only
   `captureUniversalPreparedness({ compiledLegacyMode, universalConfigured,
   reviewsConfigured, humanProfile, humanHttpEnabled, reviewsConfiguration })`.
   Здесь bools берутся из actual instantiated objects, `humanProfile` — из того
   же замкнутого host profile, а config — из actual captured loader value.
   Перед leave вызвать `assertUniversalPreparedness(handle, runtimeDto)`.
   Не подставлять plan/body или значение `enabled` вместо actual objects.

Preparedness проверяет compiled mode, Universal/Reviews objects, Human HTTP,
issuer/protocol, публичные client/JWKS pins и captured reviews config. Она не
принимает один `storageReady:true`, другие issuer/clients или partial services.
Это trusted factory measurement; DTO не является криптографической аттестацией
процесса или приватных ключей. Exact secret approval обеспечивается private
witness плюс повторными file checks и actual mount/immutable-image boot gate.
Этот DTO следует получить через частный операторский/in-namespace порт,
не публикуя глобальные review bindings или конфигурацию в browser `/health`.

## Два этапа и проверка отката

Первый immutable image собирается с `LEGACY_UNIVERSAL_MODE=1`, семью readers v5
и прежними параметрами. Он должен реально обслуживать старые данные, не читая
новые secret files и не создавая Reg/Feedback/Human stores. Пока они пусты,
read-only probe продолжает exact format.v3; наличие только Reg/Feedback даёт v4,
Human — v5. После успешного isolated boot/rollback rehearsal этот image
становится exact serving original для второго этапа.

Второй image имеет legacy label0/readers v5, явный closed delta и actual
preparedness. Его fallback — первый v5 baseline, не исходный v3. После появления
новых stores v3 не допускается к START/rollback helper/автоматическому restart;
guard сохраняет данные и выдаёт recovery_required. Возврат compatible v5 baseline
выключает новые функции, сохраняя их stores; это не удаление аккаунтов или
гарантия выхода из уже выданных offline JWT. ID token residual window ≤60s,
а immediate session revocation требует fresh issuer checks.

Обязательный следующий gate: два exact Linux images, encrypted coherent all-store
backup, isolated восстановление, actual old/baseline/feature/baseline cold boots,
config/data preservation и signed login/revoke/public health. Dockerfile label,
fixtures, unit PASS и HTTP200 не заменяют этот gate. Реальные Docker/SSH mutation,
image build и production публикация в данной работе не выполнялись.

Локальные проверки: policy suite20 tests/19 PASS/0 FAIL/1 Windows file-symlink
SKIP; directory junction и hardlink cases выполнены. Отдельный Node процесс
реально восстановил encrypted approval с task-owned private custody file.
Actual server factories создали временные Human/Universal SQLite и HTTP adapter;
read-only probe выдал v5 и отверг исходный v3 reader. Это не Linux/Docker boot:
POSIX UID/mode ветка и реальный file-bind gate остаются непроверенными на Windows.
