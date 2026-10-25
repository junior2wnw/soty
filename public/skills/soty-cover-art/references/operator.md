# Общий стандартный арт

Используй существующий `scripts/app-art/cli.mjs` в рабочем checkout Сот. CLI готовит brief, оптимизирует, версионирует и связывает изображения; внешний генератор он не вызывает. Это операторский путь для публичного общего арта, не пользовательское private хранилище.

Перед изменением восстанови действующие приватные `scripts/app-art/registry.json` и `output/app-art-history/` и выполни `validate`. Чистый checkout умеет `validate --public`; пустой registry поверх готового каталога не создавай.

Инвентаризацию веди по настоящим ID и типам объектов нужного живого поля, включая доступные приложения других владельцев и публичный каталог. Не ограничивай проверку одним owner и не делай вывод о типе по одинаковому названию. Профили людей, сообщества и устройства не являются приложениями: не подставляй их ID в `appId` и не присваивай им чужую обложку через app binding.

Определение нового field арта:

```json
{
  "key": "field-selected-app",
  "appId": "app-00000000000000000000000000000000",
  "name": "Замените на настоящее приложение",
  "purpose": "Настоящее назначение",
  "subject": "One distinctive compact sculptural object expressing the function",
  "alt": "Обложка приложения",
  "kind": "app",
  "profile": "field",
  "palette": {"base":"#2C302D","accent":"#CEDAC2","ink":"#F5F3EB"},
  "focalPoint": {"x":0.5,"y":0.42},
  "compactFocalPoint": {"x":0.5,"y":0.42}
}
```

Подставь настоящий stable ID; пример не является существующим приложением. Для нескольких приложений `prepare --catalog` принимает разрешённый снимок `{apps:[{appId,name,description,art:{profile:"field",subject,...}}]}`. Бери только нужные поля, без конфигурации, токенов или личных данных.

```text
node scripts/app-art/cli.mjs prepare --definition selected-art.json
```

Вызови доступный генератор с точным `job.prompt`. Для Codex обычный путь — встроенный `image_gen`, один вызов на отдельный мастер. Сохрани результат в проекте. Просмотри его с настоящим `roundedHexPath(1,.12)` из `src/geometry/hex.mjs` и CSS/HTML подписью поля. После визуальной проверки:

```text
node scripts/app-art/cli.mjs import --key field-selected-app --source selected-square.png --job scripts/app-art/jobs/field-selected-app/job-<hash>.json --visual-approved --reviewer selected-reviewer
node scripts/app-art/cli.mjs bind --profile field --app-id <actual-app-id> --key field-selected-app
node scripts/app-art/cli.mjs validate
```

Для готовой выбранной картинки вместо генерации: `prepare --definition selected-art.json --origin upload`, затем точный upload job и `import --visual-approved --public-approved`. Такой receipt сохраняет origin upload и не приписывает вызов генератора.

Профиль выпускает WebP 160/320/640/960 px с бюджетами 20/60/140/250 KB, без upscale и metadata. Private ID связывается через SHA lookup, для новых приложений URL использует случайный publicKey. Это минимизация метаданных, а не ACL: любой знающий URL прочитает static изображение.

`rollback --key <key> --version <n>` возвращает сохранённую принятую версию без новой генерации и удаления истории. `public/app-art/field-profile.json` и `src/world/app-art-field-profile.json` должны совпадать. Одной замены JSON недостаточно: нужен обычный проверенный UI build/release и reload. Старые immutable URL сохраняются. Сети, серверы, другие приложения и backend этим pipeline не изменяются.
