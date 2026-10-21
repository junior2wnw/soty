# Selected Source: ссылки на документы и восстановление входа

Дата: 21.10.2026. Изолированная ветка
`codex/soty-peremetrika-links-current-20261021` от
`cbeb42586ff501899652b2010188415ce5b68d16` (World6, Apps14).
Исходный checkout и параллельный production release не изменяются этой веткой.

## Пользовательский результат

Ссылка `/#app/APP?path=encode('/embed#document=pag_x&block=hero&invite=…')`
открывает тот же документ после входа, отдельного запуска и обновления сессии.
Переход сам по себе не даёт прав на приложение или документ. Source проверяет
действующего пользователя, приглашение и ACL отдельно.

Активный именованный домен не обязательно является допущенным Source origin.
При ошибке такого входа восстановление ведёт через текущую запись приложения
в Сотах. Несовпадающий origin по-прежнему отклоняется, pins и аудитория не меняются.

## Границы контракта

- Только буквальный `/embed` и необязательный fragment до 2048 UTF-16 units.
  Query перед `#`, внешний/нормализованный/кодированный путь, backslash, второй
  raw `#`, raw whitespace, controls, неправильный percent/UTF-8 и surrogate
  отклоняются. Прежний предел boot URL 8192 сохраняется.
- Полный путь живёт в RAM ticket/session и защищённом ответе его погашения.
  Публичный bootstrap URL содержит только `path=/embed`: private document/block/
  invite не переносятся в HTTP query или access log. Invite не заменяет ticket,
  frame selector, приложение или полномочие.
- Серверный createScopedLaunchPath и клиентский launcher согласованы:
  различие публичного `/embed` и полного пути допустимо только для двух известных
  scoped profiles. App/domain/origin/source/slot/ticket проверяются как прежде.
  Обычные приложения сохраняют полное сравнение путей.
- Boot проверяет полученный полный путь тем же валидатором. При наличии frame
  selector адрес имеет вид `/embed?sotyFrame=FRAME#document=…`. Root удаляет свой
  selector перед relay: реальный Source получает ровно `/embed`.
- Renewal берёт полный путь из исходной сессии, сохраняет его в идемпотентном
  результате и не позволяет выбрать другой документ. Hidden renewal сообщает
  прежний ready ACK, не заменяя видимое окно. Signed authority context и Source
  target не получают данные document/block/invite.
- Восстановление выбранного Source требует действующего scoped admission и
  доверенного profile.parentOrigin. Текущий допущенный bound-домен выбирается
  по Source embedOrigin; #launch/APP/DOMAIN не повторяет неверный alias/canonical. Для обычных
  приложений exact-domain recovery сохраняется. Ошибки frame/source для
  top-level GET могут перенаправлять в Соты; внутри iframe доступна конечная
  понятная ссылка восстановления. Повторный public reset Source отключён.

## Проверки и выпуск

Предыдущий navigation fixture проверил 257 сценариев, включая настоящий Root
HTTP, signed Root/Connect SDK/connector/Source-proof, Native consent/OIDC/BFF и
renewal. Отдельная финальная server/client privacy-проверка дала 144 PASS.
Это доказательства исходной правки, а не приёмка нового production image.
Повторные проверки точного текущего порта и полная Linux-сборка сохраняются в
output/peremetrika-links-20261021/ отдельно от Git.

В production параллельно выполняется отдельный переход World3→World6.
Ни receipt старого World3, ни совместимый тогда image не разрешают текущий
выпуск. Не переносить Dockerfile.world3, старые планы, leases или fallback.
Использовать полный текущий корневой Dockerfile, все штатные build gates и
реальные labels точного конечного image.

Действующий release-контракт: [app-release.md](deployment/app-release.md) и
[guarded maintenance rollout](../deploy/connector/README.md). До live prepare:
дождаться закрытия параллельной операции, заново установить serving tuple,
форматы всех stores, полный config/mount fingerprint, отсутствие другого writer
и отдельное окно выпуска. Требуются fresh drain, независимый review, encrypted
backup всего volume и конфигурации, isolated restore и фактический cold-start
точных candidate/fallback images. Секреты остаются в RAM/private stdin;
конфигурацию и данные не добавлять в Git или открытые артефакты.

После admission не восстанавливать старые снимки данных. При неоднозначном
результате сначала readback точной операции, не повторять START/promote вслепую.
После успешного code release отдельно проверить настоящий document/block/invite
в браузере: initial, reload, external, renewal, истечение и отзыв, права другого
пользователя. Доступ owner-only не расширять ради демонстрации.

Merge только обычным fast-forward после повторного fetch и проверки изменений
параллельной ветки; task branch не перезаписывает Root main целиком.
