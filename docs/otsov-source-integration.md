# OTsov в каталоге помощника Сот

`createSelectedOtsovSourceApplications(scope)` из `server/otsov-source.js`
возвращает пять ограниченных Source capability: текущий контекст, список своих
проектов, создание приложения, окно текстового исходника и CAS-замену файлов. Это host code для уже одобренного
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
Полные snapshot/export, произвольные file operations и preview-report не выставлены:
поддерживаются только описанные ниже bounded context и text file put.

`otsov.project.context` принимает `projectId`, один путь в `paths`, необязательные
`branch` и `from`. Возвращает один штатный Native текстовый диапазон до 16 384 UTF-16
единиц с revision/contentHash, file version/hash и признаком truncated. Продолжение
читается с `from=file.to`; для согласованного полного файла проверяйте неизменность
version/hash между окнами. Binary не объявляется текстом; actor/tokens/visual nodes
не попадают в ответ инструмента. Схема не позволяет выбирать чужой аккаунт или
изменять видимый проект.

`otsov.project.operations` принимает `projectId`, необязательную `payload.branch`
и 1–16 `payload.fileOps` вида `{kind:"put",path,expectedVersion,content}`. Пакет
fileOps целиком не больше 48 KiB; одинаковые пути запрещены. Версия берётся из
свежего чтения; 0 означает только никогда не существовавший файл. Операция идёт
через прежний Native ProjectStore: CAS, safe paths, leases, pause, квоты и durable
receipt остаются его обязанностью. Запись не подменяется собственным хранилищем.
Для этих capability нужен отдельный явный signed grant с read/update в действующем
установленном соединении; factory не расширяет старое разрешение create.

Сохранённый код и Native preview-report не доказывают запуск UI. Видимый редактор
выбирает проект через свой существующий `soty.context.select`, а Root Source этот
переход намеренно не предоставляет. В текущем UI помощника нет проверенного typed
handoff с идентификатором проекта и свежим намерением человека. URL проекта не
выдумывается; фоновые Source-вызовы не переключают экран.

Согласованный Native implementation:
`D:\otsov\server\soty-workspace.mjs`;
его wire reference: `D:\otsov\docs\soty-tool-contracts.md`.
Root composition/startup/live в этой работе не изменялись. Focused
тесты проверяют реальные factories/schema/bindings/guidance/receipt;
Native transport там явно synthetic. Отдельная compatibility проверка использует
существующую тестовую среду OTsov для прежнего create: Root factory → настоящий SDK HTTP → encrypted
Source SQLite → ProjectStore → исходная receipt → Root readProof. Создание,
повтор прежнего запроса, отказ при другом payload, изоляция двух людей, отзыв
доступа и сохранность прежнего хранилища прошли. Root issuer, visitors и Apps
handoff в этой проверке synthetic; боевой Root HTTP не проверялся.
Новые context/put unit-случаи и signed create → context → put → fresh context
readback с Native CAS/отзывом update grant подготовлены как proposal; до отдельного
прогона они не объявляются проверенными. Runtime-прогон выполняет Root.
Действующее размещение и вход OTsov из Сот требуют отдельной проверки
установленного соединения.
