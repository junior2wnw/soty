# Контракт размещения

Код команд находится в актуальном репозитории Сот:
scripts/soty-app-release.mjs и deploy/apps/release.mjs.
Не копировать их старую реализацию в другой skill.

## Манифест приложения

    {"schema":"soty.local-app.v1","name":"Project","port":8111,"entryPath":"/"}

Только эти поля, не больше 4096 байт. Порт 1024–65535, не заблокированный
коннектором. Путь локальный, не //host, не служебный /_soty. Манифест не запускает
процесс и не является фактом публикации.

## Экспорт настроек

Схема soty.app-deployment.v1 — whitelist текущего apps.inspect:

- checkedAt и точный shellOrigin;
- app: id, name, state;
- addresses: revision, canonical {id,origin} или null, aliases {id,origin,state,active};
- publication: policyEpoch, launchPolicy, listed, activeTargetRevision;
- source: port, entryPath, revision, digest, profile.

Никаких owner IDs, grants, device IDs, connector IDs, cookies или credentials.
Оригинальный apps.inspect нельзя просто записать вместо этой схемы:
использовать exportAppDeployment(snapshot, shellOrigin) из
src/world/app-deployment.mjs. Экспорт не подписанный grant; применение всё равно
проверяется реестром Сот и конкретным поручением.

## Команды

- init: идемпотентен для одинакового манифеста. Другой файл не перезаписывает.
- plan: свежесть экспорта десять минут; future clock tolerance 30 секунд;
  enabled app, source revision текущей публикации, точный port/path манифеста,
  включённый bound alias. При нескольких aliases требуются --domain-id.
- isolated — стандартный режим. native требует отдельного решения о доверии.
  Gateway port по умолчанию 18182; переопределять только по живой конфигурации.
- output plan — новый каталог. native-ingress.caddy имеет SHA в release-plan.json.
  Ни DNS, ни Caddy, ни registry эти команды сами не меняют.
- verify — публичные HTTP/TLS проверки. Дополнительные probes: массив
  {path,status,contains?}, до 32 вместе с обязательными; без секретов и внешних URLs.
  Redirect отключён, response для contains ограничен 2 MiB.
  Restricted publication требует отдельной браузерной проверки.
- zone — только snippet единого HTTPS catch-all с host expression и leaf TLS,
  не настройка DNS и не reload Caddy. --retain-origin сохраняет дополнительные
  именные/canonical зоны. Не дублировать уже существующий catch-all.

HTTP receipt содержит httpOnly=true и browserAndDataChecksRequired=true.
Не исправлять результат проверок переписыванием этих флагов.

## Подписанные операции

apps.register требует принадлежащего текущему владельцу hostDeviceId/connectorId;
name/port/entryPath/grants. apps.domains.claim требует appId, slug, requestId,
expectedDomainsRevision. Publication: expectedPolicyEpoch,
expectedTargetRevision, launchPolicy, listed, activeDomainIds; для anyone —
exposureAck текущего whole-port revision/digest/profile.

При смене источника: свежая подготовка/observation → review конкретного target →
apps.source.promote с requestId и ожидаемыми revisions. Старые app/domain IDs
сохраняются. Подробные аргументы брать из modules/apps/README.md и свежего UI,
а не угадывать. Упоминание имени компании не включает корпоративный bridge.
