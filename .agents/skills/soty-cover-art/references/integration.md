# Контракты Сот для обложек

Проверено по публичным исходникам `D:/глава/ПРОЕКТЫ/soty-field-context-lod` для переданного root HEAD `dede3a2d4f7ee28519a1fb641b90549ec0a23756`, 21.10.2026. HEAD передан владельцем задачи; этот исследователь не выполнял Git. Ниже отделены действующие функции от предложенного расширения.

## Действует: операторский арт

- `scripts/app-art/pipeline.mjs`: `prepareArtwork`, `importArtwork`, `bindArtwork`, `rollbackArtwork`, `validateArtwork`, `validatePublicArtwork`, `queueArtwork`.
- `scripts/app-art/cli.mjs`: CLI `prepare/import/bind/profile/icon/rollback/validate/list/queue`. Сетевого генератора в CLI нет.
- `docs/app-art-pipeline.md`: схема определения, field/card profiles, безопасные команды.
- `src/world/app-art.mjs`, `src/world/field-art.mjs`: явный stable app ID binding/coverKey; названия и внешние URL не выбирают изображение. `resolveFieldAppArt` предпочитает ID binding.
- Принятые профили: `public/app-art/manifest.json` и `field-profile.json` с равными source-копиями. В production нужны rebuild/release, замена одного JSON не обновит bundle.
- Registry, prompts, jobs, original и provenance находятся только в игнорируемых `scripts/app-art/` и `output/app-art-history/`. Не печатай их полное содержимое и не добавляй в Git/публичный пакет.
- Field input: локальный still PNG/JPEG/WebP, квадрат с допуском .98–1.02, от 960×960. Card input: от 960×640, aspect 1.2–2.0. До 32 MiB и 32 млн input pixels, EXIF orientation должна быть нормализована. Не AVIF/HEIC/SVG/GIF/APNG/animated WebP.
- Field recipe: 160/320/640/960 px, лимиты 20/60/140/250 KB; card: 320/640/960/1440, 60/140/250/420 KB. Без upscale; метаданные не копируются. Прозрачный field арт допустим.
- Immutable versions и SHA, visual-approved import, lock, сохранение старого указателя до записи файлов, откат без удаления уже существуют.
- **Проверка версии:** первоначальный `importArtwork` всегда фиксировал `generation.provider=codex-builtin-image-gen` и `review.noTextOrUi=true`. Расширение P4a добавляет операторский `prepare --origin upload` и обязательный приём `import --public-approved`: upload job/receipt v2 не содержат prompt/provider/noTextOrUi; старые generated receipts v1 не переписываются. Перед применением проверь, что это расширение действительно установлено. Пользовательский private runtime upload API этим не создаётся.
- Публичная статическая обложка скачивается любым знающим URL. Opaque ключ/хеш ID не является ACL. Секретный арт и личные фотографии сюда автоматически не помещаются.

Безопасные примеры действующего пути (подставляй реальный app ID, cwd — выбранный checkout):

```powershell
node scripts/app-art/cli.mjs prepare --definition ./selected-field-art.json
# Затем реальная генерация image_gen и осмотр её результата в настоящей маске.
node scripts/app-art/cli.mjs import --key selected-art --source 'C:\local\selected-square.png' --visual-approved --reviewer codex
node scripts/app-art/cli.mjs bind --profile field --app-id app-<32-hex> --key selected-art
node scripts/app-art/cli.mjs validate
```

Это команды для root-исполнителя с разрешённой Node lane. Не выполняй их параллельно с другими владельцами native/Node/Git. `validate --public` проверяет cold checkout без private operator history; не притворяется provenance-проверкой. Если история потеряна, не инициализируй пустой registry поверх готового каталога.

Для выбранного общего собственного арта после установки P4a используйте `prepare --definition ./selected-field-art.json --origin upload`, затем точный `--job` и `import --source ... --visual-approved --public-approved`. Собственный текст/логотип допустим; происхождение останется upload. `--public-approved` разрешает статическую публикацию именно выбранной картинки и не подходит для личного аватара. Без установленного режима эти команды должны быть недоступны, а старый generated importer не заменяет их.

## Действует: личный аватар

- `src/world/app.ts:2531` открывает профиль: Фото → локальный preview → Сохранить; удаления и цвета поддерживаются. Crop пока центральный, пользовательского crop editor нет.
- `src/world/avatars.ts:88` принимает PNG/JPEG/WebP до 20 MiB, декодирует в canvas, делает центрированный квадрат, выдаёт WebP до 512 px/96 KiB и 192 px/8 KiB. `image-codec.mjs` удаляет метаданные; текущий профиль намеренно непрозрачный.
- Signed операции в `modules/world/server/avatars.mjs`: `world.profile.avatar.set {expectedRevision,avatarUrl,thumbnailUrl}`, `world.profile.avatar.read {profileId,communityId?}`, `world.profile.avatars {profileIds,communityId?}`. Actor задаёт проверенный Connect host, не аргумент клиента.
- Set только для текущего `actor.accountId`, CAS профильной revision; приём PNG/JPEG/WebP с проверкой container/signature/dimensions/metadata. До 12 записей/минуту. Контейнерная проверка — не полноценный decoder/malware scan.
- Текущий set не имеет долговечного client request ID/истории версий/undo. Хранится одна image+thumbnail пара в World SQLite. Для безопасного replay/undo нужен новый контракт, нельзя приписывать его существующему set.
- Full avatar читается с текущим `canViewFullProfile`; thumbnail batch (до 24) имеет минимальное разрешение по активной комнате/историческому автору. Ordinary profile/discovery/chat DTO не содержат image bytes.
- `AvatarHydrator` ленивый, revision-scoped, кеш только в памяти до 256, сбрасывается при context change; `safeImageUrl` принимает только bounded raster data URL.

## Действует: границы агента и доставки

- `modules/apps/server/index.mjs` не содержит `coverKey`/cover upload операций в register/update. Владение и runtime-доступ проверяет Apps; не расширяй `apps.update` произвольным URL.
- `server/http-app.js:125`: `img-src 'self' blob: data:`; не ослабляй CSP для arbitrary image hosts. Versioned static WebP имеет годовой immutable cache.
- `modules/connect/server/index.mjs`: signed envelope до 2 MiB; HTTP handler имеет небольшой дополнительный envelope overhead.
- `modules/capabilities/server/index.mjs`: capability input до 256 KiB. Полные фото/base64 не должны путешествовать через модель/MCP; агент передаёт выбранный assetRef/jobRef и crop.
- `modules/capabilities/server/catalog.mjs`, registered adapters и существующий invocation/receipt протокол — путь для агентных действий. В исследованном Source image generation/cover capability отсутствует. Наличие встроенного `image_gen` у Codex не даёт production агенту provider API.

## Предложено: единый runtime workflow (ещё не существующий API)

Один cover editor для profile/app, три исходных выбора «Своя картинка / Сгенерировать / Без картинки», одна квадратная композиция с настоящими hex/card previews. Сохранять crop/scale отдельно от исходника; HTML-подписи и управление остаются поверх. Стандартный field preview не должен превращаться в новый отдельный app store.

Общая обработка изображения, но отдельные authority adapters: профиль остаётся в World, приложение в Apps. Не вводить публичный static каталог для личных картинок и не выполнять cross-DB commit под видом атомарного.

Минимальные proposed actions:

1. `cover.prepare` принимает только выбранный bounded канонический upload либо brief generation; получает authenticated actor, target kind/ID и expected cover revision, возвращает owner-scoped draft/assetRef. Никакой записи в active pointer.
2. `cover.job.get/cancel` читает/останавливает именно этот draft. Generation имеет явную разрешённую provider binding, число вариантов, бюджет и durable provider request key; unknown не повторяет charge.
3. `cover.commit` принимает requestId, target, expected cover revision, prepared assetRef и crop; повторно проверяет actor/ownership/target/asset scope, сохраняет immutable version+receipt и CAS pointer в **одном** authoritative store. Для профиля wrapper поддерживает настоящий `avatar.set` только как отдельный adapter, без ложной атомарности с соседним store.
4. `cover.get/versions` проверяет текущий доступ к target на каждом чтении; отдаёт bounded rendition и metadata без original/prompt. Закрытые bytes не попадают в public caches или discovery DTO. Agent получает IDs/metadata, не image bytes автоматически.
5. `cover.restore` — отдельный явный commit прежнего assetRef с текущей revision. Старый receipt не перезаписывает поздний выбор. `remove` очищает active pointer, не включает генерацию.

Названия выше — проект интерфейса, а не зарегистрированные инструменты. В реализации предпочти минимум операций по существующему execution/receipt протоколу. Пока они не добавлены и не проверены, skill работает через фактические operator/profile пути и точно описывает недоступную часть.

Достаточный первый transport: UI локально rasterize в 960 px WebP <=250 KB, signed Connect принимает только этот выбранный объект, сервер реально декодирует с pixel/byte/concurrency limits и заново кодирует renditions. Локальный original не отправляется автоматически. Для agent generation/upload использовать уже выбранный owner assetRef, не filesystem path от модели, не внешний URL и не подписание чужого файла. Если нужен оригинал для дальнейших правок, это отдельный явный private retention выбор.

Generation profile default manual; periodic mode остаётся выключенным, пока нет реально поддерживаемых cadence+budget+expiry+stop+notification полей. Crop, тёмная тема, повторный вход и новое название не генерируют новые изображения.

## Значимые проверки расширения

- Чужой actor, revoked app/device, account switch между prepare/commit, устаревший crop/target revision: отказ без смены active pointer.
- Receipt replay после более поздней смены картинки возвращает историю операции и текущий pointer, не возвращает картинку назад.
- Crash между rendition write/commit, повтор того же запроса, network unknown, provider accepted-but-timeout: не новая charge/версия; old pointer остаётся usable.
- Avatar permission change немедленно закрывает full read/кеш при смене контекста; thumbnails сохраняют только установленное минимальное room author исключение.
- Реальные decoder fixtures: MIME spoof, EXIF/GPS, huge dimensions, multi-frame/alpha policy, AVIF/HEIC unsupported, corrupt/truncated, output budget. Для личного upload не ставится `generated` provider.
- Upload → crop → cancel; upload → save → reload → restore; generate → preview → cancel; manual fallback; same flow with keyboard/screenreader, 320/390 px и обеими темами. Pinch/drag всегда имеют видимую альтернативу.
- Agent prepare/commit через тот же granted adapter; каталог без permission не выполняет действие; model не видит raw local image или закрытый brief без выбора пользователя.

Нынешние поддерживаемые suites: `scripts/app-art/pipeline.test.mjs`, `scripts/app-art/field-profile.test.mjs`, `src/world/app-art.test.mjs`, `src/world/field-art.test.mjs`, `src/world/unified-field-art.test.mjs`, `src/world/image-codec.test.mjs`, `modules/world/test/avatars.test.mjs`, `server/test/app-art-cache.test.mjs`. Их наличие не означает, что этот исследователь их запускал.
