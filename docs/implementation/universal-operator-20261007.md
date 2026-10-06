# Private runtime measurement — 07.10.2026

`modules/app-contract/universal-preparedness.mjs` предоставляет bounded pure
capture actual host services/profile/HTTP. Runtime не импортирует deploy.
`deploy/connector/universal-policy.mjs` использует тот же capture и сохраняет
старый export для callers/tests. Measurement не является authority, доказательством
ключа, готовности remote provider или заменой storage/cold-boot gate.

Tiny Root hooks после успешной factory construction:

```js
app.locals.captureUniversalPreparedness = () => captureUniversalPreparedness({
  compiledLegacyMode: forcedLegacyMode,
  universalConfigured: Boolean(universal),
  reviewsConfigured: Boolean(universal?.reviews),
  humanProfile,
  humanHttpEnabled: app.locals.humanIdentityStatus.enabled,
  reviewsPreparedness: reviews?.preparedness(),
});
const operator = await startUniversalOperator({
  capture: app.locals.captureUniversalPreparedness,
});
// Shutdown: close operator before disposing the services used by capture.
await operator.close();
```

Listener запускается Root index только при явно одобренном
`SOTY_UNIVERSAL_OPERATOR_ENABLED=1`; `''`/`0` не запускают его. Default child/test
processes не занимают фиксированный socket. Обе rollout-фазы добавляют именно
этот один approved flag; baseline не включает Human/Reviews и не читает их keys.

Root имеет actual closed humanProfile в private constructor closure. Reviews
`.preparedness()` отдаёт immutable `{configurationDigest,providerCount,bindingCount}`
из собственного captured config, не из позже изменённого original object.
Дефолтные пустые отзывы сохраняют прежний digest. Measurement не содержит
tenant/app/person/subject bindings, token/secret bodies или secret fingerprint.
Human projection содержит public issuer/protocol/client/signing-public-key pins.
Optional renewal projection отдельно измеряет eligible clients и operational
admissionOn/Off. Смена cookie/artifact/clientSecret при сохранённых публичных
pins не меняет measurement; same-file custody проверяется private policy witness.

`server/universal-operator.js` даёт fixed Unix port:
`/tmp/soty-operator/universal.sock`. Directory0700/socket0600, ownercurrentUID,
canonical no-symlink paths. Windows возвращает supported:false и не создаёт
listener; actual image gate остаётся Unix/Linux. Нет публичного HTTP route,
bearer, actor, конфигурационного тела или mutation protocol.

Trusted Docker-exec reader импортирует `readUniversalOperator()` без аргументов.
Client half-closes запись после exact ASCII
`soty.universal-preparedness.v1\n`; server отвечает одной закрытой JSON measurement
и newline. Request≤128 bytes, response≤64KiB, не более8 accepted sockets,
absolute timeout1000ms. Только полный exact request вызывает synchronous capture;
extra fields/commands, неизвестный формат, Promise/throw/credentials дают безопасный
отказ. Ошибки не выводят исходный input/cause/key или host callback result.

Существующий live listener не заменяется. Stale owned0600 socket убирается только
после ECONNREFUSED и повторной проверки inode/owner/mode/parent. Сокет публикуется
через exclusive hardlink от короткоживущего private listener name; private name
сразу удаляется. Это не позволяет автоматическому unlink libuv на server.close
удалить подменённый fixed pathname. Shutdown удаляет fixed name лишь при совпадении
его inode, уничтожает pending connections и закрывает native listener.
[Первичный libuv source](https://github.com/libuv/libuv/blob/v1.51.0/src/unix/pipe.c#L167).
Directory принадлежит runtime UID; процесс с тем же UID/root остаётся trusted
operator boundary. Изменение прав/пути во время работы прекращает measurement.

Локально проверены pure capture/default/v1/v2 admissionOff, private rotation,
immutable reviews projection, closed output и Windowsunsupported constructor.
`server/test/universal-operator.test.mjs` содержит настоящие Unix tests: native
roundtrip/modes, request/connection/time limits, callback errors, active/stale
listener, substituted file preservation, symlink/unsafe modes и shutdown.
На Windows они явно skipped; их нельзя считать выполненным Linux gate. Перед
rollout нужен exact reviewed image с Root lifecycle hooks и Docker-exec read,
проверенный в выделенном canary namespace; publication здесь не выполнялась.
