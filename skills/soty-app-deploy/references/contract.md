# Контракт размещения

Код команд находится в актуальном репозитории Сот:
scripts/soty-app-release.mjs и deploy/apps/release.mjs.
Не копировать их старую реализацию в другой skill.

## Манифест приложения

    {"schema":"soty.local-app.v1","name":"Project","port":8111,"entryPath":"/"}

Только эти поля, не больше 4096 байт. Порт 1024–65535, не заблокированный
коннектором. Путь локальный, не //host, не служебный /_soty. Манифест не запускает
процесс и не является фактом публикации.

## Незавершённая работа внутри Сот

Для приложения под своим управлением включи обычное автосохранение и подключи
`createSotyAppWork` из `@soty/source-app/browser` до того, как станет доступен
ввод. Используй точный доверенный `parentOrigin`, `isDirty` по реальному состоянию
и `save`, который завершается только после устойчивого сохранения. Вызывай
`work.changed()` при изменении состояния и `work.dispose()` при удалении экрана.
Проверь закрытие, возврат и обновление Сот с изменёнными данными, а также отказ
или потерю ответа сохранения: уход не должен объявляться успешным. Подробный API
и ограничения — в `modules/source-app/README.md` текущего SDK.

Старое или стороннее приложение без этого интерфейса остаётся доступным, но Соты
не могут подтвердить сохранение его черновика. Самостоятельная навигация iframe
и закрытие браузера также требуют автосохранения со стороны приложения. Это не
является общим входом и не передаёт личность или права.

## Экспорт настроек

Схема soty.app-deployment.v1 — whitelist текущего apps.inspect:

- checkedAt и точный shellOrigin;
- app: id, name, state;
- addresses: revision, canonical {id,origin} или null, aliases {id,origin,state,active};
- publication: policyEpoch, launchPolicy, listed, activeTargetRevision;
- source: port, entryPath, revision, digest, profile.

Для profile soty.selected-human-embed.v2 используется soty.app-deployment.v2:
к source добавляется launchEntry:null|{domainId,origin,path}, наверху —
nativeEntry:null|{origin,domainId,target,committedPolicyEpoch,admissionKeyHash}.
target содержит только revision,digest,profile,port,entryPath; Native profile —
soty.relay-restricted.v1. Origins и порты пары различаются. Экспорт сохраняет
и закрытое состояние пары для диагностики: restricted, retired Native alias или
отсутствующий Source entry не превращаются в разрешение размещения.

Никаких owner IDs, grants, device IDs, connector IDs, cookies или credentials.
Оригинальный apps.inspect нельзя просто записать вместо этой схемы:
использовать exportAppDeployment(snapshot, shellOrigin) из
src/world/app-deployment.mjs. Экспорт не подписанный grant; применение всё равно
проверяется реестром Сот и конкретным поручением.

## Команды

- init: идемпотентен для одинакового манифеста. Другой файл не перезаписывает.
- plan: свежесть экспорта десять минут; future clock tolerance 30 секунд;
  enabled app, source revision текущей публикации, точный port/path манифеста,
  включённый bound alias. При нескольких aliases обычного проекта нужен --domain-id.
  Source v2 выбирает точный source.launchEntry, пара с --mode native — Native alias;
  несовпадающий явный domain ID отвергается. У пары требуются anyone, оба active
  bound alias и Source entry. Native манифест совпадает с nativeEntry.target,
  а ссылка Сот сохраняет Source path. Source-only Native план запрещён.
- isolated — стандартный режим. native требует отдельного решения о доверии.
  Gateway port по умолчанию 18182; переопределять только по живой конфигурации.
- output plan — новый каталог. native-ingress.caddy имеет SHA в release-plan.json.
  У пары schema soty.app-release.v2, source сохраняет primary Source pin,
  nativeEntry — утверждённый Native target. nativeGuard содержит rootCheckUrl,
  appId, origin, primaryTargetDigest,nativeTargetDigest для Native SDK;
  это настройки проверки, не grant и не признак runtimeReady.
  Ни DNS, ни Caddy, ни registry эти команды сами не меняют.
- verify — публичные HTTP/TLS проверки. Дополнительные probes: массив
  {path,status,contains?}, до 32 вместе с обязательными; без секретов и внешних URLs.
  Redirect отключён, response для contains ограничен 2 MiB.
  Для пары обязательны entry/query 200 и exact JSON policy GET 200 (не HTML),
  до 4096 байт, все текущие pin/epoch/admission поля и lease 1–1000 ms.
  Отчёт nativePolicyChecked не подтверждает Native Human login, SDK guard или данные.
  Старый v1 receipt тоже не объявляет Native guard готовым.
  Restricted publication требует отдельной браузерной проверки.
- zone — только snippet единого HTTPS catch-all с host expression и leaf TLS,
  не настройка DNS и не reload Caddy. --retain-origin сохраняет дополнительные
  именные/canonical зоны. Не дублировать уже существующий catch-all.
- doctor --observations FILE --output NEW_FILE — только закрытые наблюдения
  без URL/logs/credentials. Тот же код, что Root MCP apps_diagnose. Ответ
  liveVerified=false/releaseReadiness=not_assessed, без команд или grants.
- auth-guide [--mode soty|human-oidc|private-owner|agent] --output NEW_FILE — тот же
  guide, что apps_auth_guide. Не включает issuer и не регистрирует клиентов.

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
