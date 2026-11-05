# Список чтения: настоящий Native Source и компактный виджет

Один `/embed` работает в небольшом окне и в полном фокусе. Это самостоятельное приложение с личным списком в SQLite, а не Root-шаблон или картинка. Домен взят без изменений из проверенного `@soty/source-app` preview.21. SDK владеет проверкой Root/Source, текущего человека, сессиями, personal storage, транзакцией и квитанцией.

UI использует общую CSS-композицию `glass-ui__widget` из Volumetric Glass UI. Оптическая сцена не создаётся: честный CSS static material. Размер задаёт iframe, содержимое прокручивается, кнопки остаются не меньше 44 px. При смене размеров форма и её DOM не заменяются. Произвольная маска принадлежит полотну; экстремально узкая маска не обещает рабочую форму без перехода в фокус.

Режим задаёт точный parent через общий `bindGlassWidgetContext`: explicit `glance` оставляет итог/последнюю запись, `interactive` показывает редактор и действия. Размер окна не считается признаком режима. Старый/неизвестный host остаётся interactive. Переключение только меняет UI-атрибут: не создаёт Native authority, не делает API-вызовов и не заменяет input. Общий helper и Source script объединяются в прежнем `ui.script`; четвёртый маршрут не появляется. Dispose выполняется при teardown.

## Подготовить переносимый проект

Из checkout Сот, Node >=24.15:

```powershell
node modules/canvas-widgets/examples/reading-list/prepare-project.mjs --out D:\MyReadingSource --parent-origin http://127.0.0.1:5194 --port 53248
```

Каталог должен быть новым. Скрипт проверяет frozen release, извлекает его в `.soty-sdk`, копирует канонический домен и собственные три UI-ассета. Общий Glass CSS включается в `source/assets/ui.css`: дополнительный HTTP-маршрут не требуется. `consumer-provenance.json` фиксирует версии/хеши. Никаких ключей, grant, регистрации или подключения эта команда не создаёт.

В подготовленном каталоге установите **точные** зависимости frozen SDK из его shrinkwrap, затем заполните `operator.private.json` по `operator.workspace.template.json`:

```powershell
npm ci --prefix .soty-sdk --ignore-scripts --omit=dev
node .soty-sdk/dist/install.mjs check --config operator.private.json
node .soty-sdk/dist/install.mjs init --config operator.private.json
node .soty-sdk/dist/install.mjs serve --config operator.private.json
```

До `init` создайте отдельный private `storage.directory` и соседний с config каталог `secrets`. SDK требует их существования: на Linux каталоги 0700, config/ключи 0600; на Windows ограничьте ACL владельцем. `init` выполняется один раз на новом хранилище; повторная инициализация отказывает. Restart использует тот же каталог, ключи и exact approved profile.

## Операторское подключение

1. Root/владелец штатно создаёт приложение и connector, получает **current approved** selected-human profile. Metadata `.soty/app.json`/`author.json` не даёт прав сама по себе. Реестр Native profiles в текущем Root startup-static: новый профиль должен пройти существующую операторскую admission, а не угадываться Source или добавляться браузером.
2. В private config: `profile` — точный выданный профиль; `listener={host:'127.0.0.1',port:53248}`; `connectorPort` — отдельный **Source authority IPC** уже настроенного connector, а не этот listener и не Root API. Файлы ключей находятся в соседнем с config каталоге `secrets/`: `transport.key` и `cipher.key`; config хранит только их имена. Transport key должен совпадать с разрешённой записью connector. Не публикуйте config/ключи. Точный appId, connector tuple, target revision/digest, Source profile и origins обязаны совпадать с Root.
3. `native.domainModule` = абсолютный `source/domain.mjs`; `native.uiDirectory` = абсолютный `source/assets`; `storage.directory` = отдельный абсолютный private каталог; `cipherKeyFile`/`keyId` — сохранённые ключи сессий; `policy.personalWorkspaces=true` включает существующую явную политику автоматического личного workspace. По умолчанию в SDK template она false: менять только как осознанное операторское решение.
4. Через обычный connector подключите loopback listener, подготовьте/утвердите существующий Native Source target. Entry **`/embed`**. Разместите приложение штатным «Окно»; текущий Source pin и права реального посетителя остаются обязательными. Декларация или CSS не создают launch grant. Не меняйте `/embed`, Native transport или renewal ради виджета.

Профиль не включает произвольный чужой адрес. Не копируйте browser identity keys, Root cookie или Source секреты в UI. Для другого компьютера используйте его собственный штатный connector и согласованный профиль. Пример не выполняет production deploy.

Для named gateway используйте существующий **локальный HTTPS/TLS mount** и согласованный alias: workspace допускает `nativeOrigin === embedOrigin`. HTTP на произвольном `*.localhost` не принимается. Frozen installer дополнительно требует для HTTP exact `localhost`/`127.0.0.1` тот же port, что у Source listener; внешний HTTP proxy origin с другим портом его не обходит. Прямое открытие listener не создаёт Root proof. Локальный тестовый сертификат не равен проверенному production TLS; новый DNS/домен не требуется.

Root конфигурирует существующий `SOTY_SELECTED_EMBED_REGISTRY_FILE` (`soty.selected-embed-registry.v1`, exact profiles), connector — `SOTY_SELECTED_EMBED_HOST_FILE` (`soty.selected-embed-hosts.v1`, profile + private key). Подготовка/продвижение Source остаются `apps.source.prepare` → `apps.source.promote` с текущим target, alias и явными consent/exposure acknowledgements. Это операторский admission, который пример не автоматизирует и не обходит.

## Работа и границы

Добавление — `reading.add` с новым request ID. SDK возвращает durable workspace receipt; повтор/неизвестный сетевой результат проверяется по прежней квитанции, не отправляется как новое добавление. Список читает `reading.list`. Кнопка «Ещё» использует существующую пагинацию; `100+` означает загруженную часть, а не выдуманный общий размер.

Введённый текст и ссылка на незавершённую операцию сохраняются в sessionStorage **после** получения непрозрачного личного storage scope. Это UI namespace, не principal/permission. Пример не является password-vault; не храните в нём секреты, требующие отдельного шифрования локального черновика. Текст, набранный во время сохранения предыдущей строки, не стирается её подтверждением. Стандартный `createSotyAppWork` сообщает Root о грязном вводе и сохраняет его по запросу; закрытие/resize не внедряет новый протокол.

Factory `createReadingListServer` сохраняет все поддержанные `ui.document/script/style/sdk` overrides. `ui.style` — **полная замена** style-ассета; чтобы расширить default Glass CSS, добавляйте свой CSS к `readingListAssets(...).style`. Optional `widgetContextScript` позволяет передать те же approved library bytes явно; по умолчанию factory читает canonical vendor helper. Portable exporter уже включает их в script и пишет SHA в provenance.

Тесты этого каталога используют отдельные синтетические Root proofs/keys, временные SQLite и порты. Они не доказывают операторскую admission или реальные устройства пользователя; Root browser gate проводится отдельно.
