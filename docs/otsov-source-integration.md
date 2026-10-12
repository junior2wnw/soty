# OTsov в каталоге помощника Сот

`createSelectedOtsovSourceApplications(scope)` из `server/otsov-source.js`
возвращает три существующих Source capability: текущий контекст, список своих
проектов и создание приложения. Это host code для уже одобренного
`appId/target/resource/sourceProfile`; factory не регистрирует приложение,
не выдаёт grant и не создаёт вход. Нужны `soty.workspace.v1` и точный
`soty.standard-workspace@2` с digest
`23061fc95610e3011ec42b3ecf229308bce44b0a1268408ca9b6ee138c98f7e9`.

Передайте строки factory в существующий `externalApplications`. Штатные
`captureExternalApplications/composeExternalApplications` сохраняют target,
Root/Source contract hashes, текущего человека, opaque handle и свежий
`contextRef`. В личного помощника передаются captured contract references
и индивидуальные credentials уже разрешённых Source capability.
Общий owner key не используется; аккаунт определяется Native SDK по
подписанному текущему Root witness.

Минимальные Native inputs, без скрытых wrapper полей:

```json
{"operation":"otsov.projects.list","representation":"compact","offset":0,"limit":64}
{"operation":"otsov.project.create","payload":{"name":"Мой сайт","files":[{"path":"index.html","content":"<!doctype html><title>Мой сайт</title><h1>Привет</h1>"}]}}
```

Список возвращает `requestId,operation,projects,nextOffset`; `nextOffset=-1`
завершает обход. Названия при чтении сохраняются целиком в пределах существующего
Native бюджета 60 000 байт JSON; создание по-прежнему ограничивает имя 120 символами.
Create принимает 1–16 текстовых файлов; Native проверяет
суммарный пакет до 48 KiB, безопасные относительные пути и отсутствие повторов,
затем передаёт их существующему OTsov ProjectStore как карту файлов.
Root сохраняет исходный input digest. Его write proof получен чтением
исходной durable receipt с `contextAfter/workspaceReceipt`; потерянный ACK
проверяется тем же requestId, без автоматического нового create.

Статический skill `otsov-app-builder` прикреплён только к discovery/create.
Он объясняет выбор OTsov для нового приложения; предметные действия в уже
подключённых приложениях выполняются их собственными MCP/API. Сохранение
исходников не подтверждает запуск, публикацию или подключение к каталогу.
Полные snapshot/export/file operations пока не выставлены в Root MVP:
их произвольный JSON не объявлен поддерживаемой bounded схемой.

Согласованный Native implementation:
`D:\otsov\server\soty-workspace.mjs`;
его wire reference: `D:\otsov\docs\soty-tool-contracts.md`.
Root composition/startup/live в этой работе не изменялись. Восемь focused
тестов проверяют реальные factories/schema/bindings/guidance/receipt;
Native transport там явно synthetic. Отдельная compatibility проверка использует
существующую тестовую среду OTsov: Root factory → настоящий SDK HTTP → encrypted
Source SQLite → ProjectStore → исходная receipt → Root readProof. Создание,
повтор прежнего запроса, отказ при другом payload, изоляция двух людей, отзыв
доступа и сохранность прежнего хранилища прошли. Root issuer, visitors и Apps
handoff в этой проверке synthetic; боевой Root HTTP не проверялся.
Действующее размещение и вход OTsov из Сот требуют отдельной проверки
установленного соединения.

