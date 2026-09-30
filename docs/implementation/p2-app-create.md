# P2 — создание приложения без повторной задачи после потери ответа

30 сентября 2026. Локальная реализация и проверка production-сборки на preview; production-развёртывание и реальные команды не выполнялись.

## Найденный дефект

В `local-apps.ts` request ID и fingerprint формы «Создать с ИИ» существовали только в замыкании modal. Сервер мог принять задачу, а подтверждение потеряться. После закрытия/reload новая форма создавала новый ID и вторую задачу. Сохранённый после ACK job не защищал промежуток перед ACK. Путь постоянного Assistant уже имел другой устойчивый механизм и не закрывал эту отдельную кнопку.

## Контракт исправления

- `app-create-state.mjs` хранит один account-scoped draft, immutable pending и accepted job в одной записи localStorage. Ключ содержит account ID; payload содержит `expectedAccountId`, точные device/connector, prompt, cwd и request ID. Данные браузера не являются серверным разрешением.
- Каждое read/modify/write получает один account Web Lock; сеть не держит lock. Две вкладки либо повторяют одинаковый ID/intent, либо получают конфликт незавершённой отправки. Без Web Locks черновик доступен, но новый dispatch закрыт: localStorage не подменяет cross-tab CAS.
- `prepare` проверяет и записывает payload до каждого dispatch, включая повтор. Ошибка чтения/записи запрещает сетевой вызов. Неизвестный ответ оставляет pending. Новая попытка доступна лишь после точной квитанции `admission.status='rejected'` либо принятой задачи и явного завершения её просмотра.
- Accepted receipt сравнивает целый intent, account, request ID и device/connector. Draft revision предотвращает очистку более новых правок, даже если человек вернул прежний текст. Поздний старый ACK/reject не меняет следующую отправку.
- Выбор устройства основан на `JSON.stringify([hostDeviceId, connectorId])`; двоеточие внутри ID не создаёт коллизию. При reorder или отсутствии выбранного устройства другой target не подставляется. Pending поля read-only; «Проверить отправку» сохраняется доступной при недоступной модели, чтобы получить уже существующий receipt.
- До и после сетевых await проверяется текущий аккаунт; server args всегда содержат expected account. Точный accepted ACK после закрытия modal может сохранить known job, если аккаунт не поменялся, но не возвращает закрытый DOM. После смены/отзыва аккаунта прежний DOM закрывается.
- Введённый текст синхронно попадает в volatile draft до первого identity await. Generation защищает от обратного порядка ответов и старой отложенной записи. При quota/ошибке сохранения черновик остаётся в памяти контроллера. Escape/header-close и native beforeunload защищают несохранённые изменения; человек может повторить сохранение, скачать текст или явно закрыть без этих локальных изменений. Discard не удаляет pending, server job или сохранённую правку другой вкладки и инвалидирует старую ожидающую запись. PWA update guard ждёт сохранения.
- Обычный route/Back не уничтожает контроллер создания. При смене аккаунта `resetAccount()` закрывает прежний DOM и очищает прежнее локальное состояние, сохраняя жизненный цикл guards для новых форм; `destroy()` снимает их только при финальном unload. Прежний вызов destroy при account switch оставлял повторно используемые callbacks без защиты и исправлен отдельно.
- Прежний recalled job читается и импортируется; просмотр из Assistant history и добавление valid proposal сохранены. Создание не открывает приложению аудиторию автоматически.
- Статусы distinguish execution uncertainty, cancel requested и terminal cancelled; не обещаются остановка дерева процессов и rollback. `succeeded` означает завершение исполнения, а не доказанный работающий сайт. Полный текст результата ограничен 4 млн символов/500 страницами и строго растущим cursor.

## Проверки

`node --test src/platform/app-create-state.test.mjs`: 14 случаев — lost ACK/remount, concurrent tabs, changed target, exact receipt, newer draft, rejected receipt, storage failure, corrupt/foreign scope, legacy known result, no-Web-Locks dispatch refusal, read failure, explicit volatile discard, immediate-input guard, delayed-save ordering/update flush. Typecheck и whitespace check проходят. Серверный durable rejection/replay принадлежит интегратору и проверяется `server/test/apps-jobs.test.mjs`; P2 не превращает недоступный model status в доказательство отсутствия уже принятой задачи.

`src/platform/app-create.test.html` — отдельная dev-only fixture. Она использует случайные QA account keys, настоящее DOM-составление/create state/Web Locks, но **синтетический API и нулевые настоящие эффекты**. Кнопка проверяет потерянный ACK → remount → reorder A/B → model-off проверка того же ID; late accepted ACK после закрытия; смену аккаунта во время ответа; повторное использование actions после resetAccount, мгновенный beforeunload и Escape recovery при quota, затем сохранение после восстановления storage. Перехват записи касается только собственного случайного QA ключа и снимается в finally; после проверки удаляются только QA keys. Production entrypoint fixture не импортирует. Интегратор независимо получил PASS в настоящем IAB после input-staging и повторно после resetAccount-исправления. Наличие fixture не заменяет реальный inference.

Дополнительная настоящая интеграция выявила, что прежний strict apps service не принимал expectedAccountId в devices/register. На общей границе добавлено сравнение с authenticated actor до операции и удаление только этого поля перед неизменённым exact schema. Независимые `modules/apps/test/apps.test.mjs` + `server/test/capabilities-connect.test.mjs`: **12/12 PASS**; `server/test/apps-jobs.test.mjs` + `server/test/jobs-runtime.test.mjs`: **10/10 PASS**. Клиентский guard не снят ради прохождения тестов.

Границы: реальный inference/device, runtime isolation и расход — P5; полный installed PWA/new-human gate не доказан unit/component тестами. Не заявляется, что localStorage хранит секреты исполнения: OpenCode session ID остаётся на сервере.

## Уточнение после P2 checkpoint

Интегратор воспроизвёл: Escape сразу после ввода попадал между volatile staging и успешной записью, ошибочно показывал recovery как при quota и оставлял устаревшее сообщение после ACK. Закрытие теперь ждёт `flush` с коротким «Сохраняем черновик…» и завершает первоначальное действие только после подтверждённой записи. Export/discard появляются только при настоящей ошибке. Новая правка инвалидирует прежний close intent; обычный ввод не исчезает из-за поздно завершившегося закрытия. Переход «Подключить компьютер» проходит тот же путь.

14 state tests и TypeScript проходят. Dev fixture дополнена нормальным immediate Escape, задержанным Web Lock с новым вводом и quota recovery; интегратор повторно получил **PASS в настоящем IAB** для всего нового набора. Это synthetic transport с нулём настоящих эффектов. Интегратор также подтвердил build PASS и повторил обычную форму на production-preview: ввод с немедленным Escape закрывает форму после сохранения, повторное открытие возвращает точный новый текст без ошибки. Штатная кнопка PWA «Обновить» сохранила черновик Assistant. Эти browser-проверки не подтверждают installed PWA на физическом телефоне или реальный inference.
