# P3-D3 — компоненты сохранённых и обсуждения

Дата: 30 сентября 2026. Исходная точка: D2 `4ff4359`, рабочая копия `soty-platform`. Это авторская квитанция интерфейсного среза, а не production или полный D3 signoff.

## Срез и статус

Реализованы `app-saved.ts`, `app-library.ts`, `app-discussion.ts`, общий `app-engagement.css`; в `apps-home.ts` добавлена отдельная линза «Сохранённые» с внешним mount/cleanup, в `application-card.ts` локальный pin назван «Закрепить здесь». Другие production-файлы автор этого среза не изменял. Маршруты, toolbar, iframe, safe launch entry и stage принадлежат root; хранение и ограниченные feed-модели — publishing architecture.

Первый авторский freeze: TypeScript `node node_modules/typescript/bin/tsc --noEmit` — **PASS**, exit0; `git diff --check` — **PASS**, без whitespace errors. Проверка типов выполнялась одна, без параллельных тяжёлых наборов. Исходники переданы независимому reviewer. Реальные browser, геометрия, focus/keyboard и полный mounted acceptance на момент этой записи **ещё не приняты**. Синтетические либо модельные результаты других авторов не присваиваются этому UI как собственные browser-доказательства.

## Компонентные границы

| Компонент | Вход и handle | Поведение |
| --- | --- | --- |
| `mountAppSaved` | api/accountId/exact entry/isCurrent; optional storage/locks/onChanged. Handle dispose/refresh | Один toolbar control; `aria-pressed` только от server current. Другая сохранённая точка входа требует явного «Заменить вход». |
| `mountAppLibrary` | api/accountId/isCurrent/openEntry/discussEntry; optional storage/locks/onChanged. Handle dispose/refresh/focus | Account library с cursor, общим pending, точным domain/origin/path. Недоступный вход показывает собственный сохранённый label и удаление, без fallback на другой адрес. |
| `mountAppDiscussion` | api/accountId/appId/entry/isCurrent, optional initialConversationId/administrative/retainedEntry/storage/locks/onConversationChange/onClose | Handle dispose/refresh/focus/flush/hasUnsavedChanges/setVisible/updateSelection/updateEntry. У компонента нет доступа к iframe или URL мини-апки. |

`entry.name` у save — необязательная подсказка. Серверный snapshot после сохранения остаётся источником label. Из библиотеки наружу передаются ровно четыре поля входа. `retainedEntry` служит только фильтром своего локального реестра: он не разрешает сетевую историю, отправку или запуск. `updateEntry` допускает только однократное заполнение null точным проверенным входом того же приложения; повтор того же значения идемпотентен, другой origin/domain/path отвергается.

`onConversationChange({conversationId?,administrative?})` вызывается только после явного выбора пользователя. Фоновый опрос или ответ сервера не создаёт маршрут. `updateSelection` сначала сохраняет локальный ввод и отказывает при оставшемся volatile/conflict, не меняя его аудиторию. Все созданные scoped draft handles живут до dispose; архив или закрытие панели их не уничтожает. `flush()` не бросает при отказе локального сохранения: видимый статус и `hasUnsavedChanges() === true` позволяют родительскому controller остановить переход. Durable unsent draft сам по себе не считается несохранённой правкой.

## Сценарии и решения

- **Сохранение:** ответ на старый request подтверждает его историю, но кнопка смотрит на `response.current.entry`. Общий account pending виден как конкретное прежнее действие. Retry захватывает показанный pending до любых render/await и передаёт его helper под тем же lock. Снятие ожидания не заявляет отмену серверного эффекта.
- **Возврат:** home mount имеет cleanup; Notes, «Все», «Мои проекты», «Вместе», карточки и поле остаются своими представлениями. Линза saved не смешивает account bookmarks с локальными pins. Row availability относится к сохранённому exact entry.
- **Обсуждение:** current/archive/admin отделены серверным context. При переходе аудитории прежний текст остаётся отдельным readonly черновиком. Новая форма получает только черновик своего account/app/conversation/exact-entry scope. Ни один action не копирует текст в новый разговор автоматически.
- **Отправка:** Enter создаёт строку; Ctrl/Cmd+Enter или кнопка отправляют. Новый intent проходит helper `beforeCreate`, persistent admission и current/canPost. Retry относится только к immutable pending. ACK не вставляет pending body в историю: helper принимает только проверенный контекст, новые сообщения приходят из cursor feed, сохраняя порядок.
- **Ответ:** draft хранит только reply ID. UI не переносит private body/snippet в другой контекст. Удалённое сообщение остаётся tombstone; подтверждённое удаление очищает body сразу, до следующего polling response, без выдуманного времени удаления.
- **Два окна:** remote draft conflict требует явного выбора точной версии. Сравнение и отказ от локального ожидания используют native dialog с первоначальным фокусом на безопасном действии. Immutable pending snapshots не перечитываются под видом прежнего клика.
- **Поздние ответы:** account/current и generation guards стоят на success/error/finally. Hidden прекращает poll и сетевую проекцию; ACK всё ещё может подтвердить свой durable record, но не рисует данные в скрытой или чужой панели. Опрос не создаёт новый разговор и не переводит архив в writable.
- **История:** explicit «Раньше» и «К последним» используют bounded helper window. Poll не меняет textarea, не заменяет её DOM и не переставляет existing message rows без изменения порядка. Не показываются скрытые счётчики или даты архивов, которых нет в API.
- **Локальная потеря доступа:** собственные retained drafts можно прочитать/скопировать; отдельное удаление захватывает их opaque revision. Наличие такого текста не открывает сетевую историю. Полная авторская metadata и профиль не запрашиваются.

## Внешний вид и доступность

Только существующие нейтральные `--sw-*` tokens и icon/hex SSOT; classic stylesheet не импортируется. Все основные controls имеют минимум44px; длинный текст переносится. Сообщения выводятся исключительно через textContent. Textarea, details и keyed rows стабильны; scoped drafts не remount при polling. У save постоянное accessible name, текущее состояние — отдельно в aria-pressed. Начальная история не озвучивается как поток новых сообщений; используется отдельный ограниченный status. Reduced motion и forced colors учтены CSS, но этот авторский этап не является проверкой screen reader или заявлением WCAG compliance.

Root layout hosts: `.se-saved`, `.se-library`, `.se-discussion` (height100%, min-height0). Toolbar compress/layout принадлежит `app-stage.css`; компонент не меняет frame DOM. Основные fixture selectors — `data-engagement-key`: save/save-retry/save-replace; library-list/open/remove/discuss/retry/refresh/latest/more; discussion-input/send/retry/status/audience/messages/current/archives/archive/older/latest/retained/retained-text/retained-retry/remove.

## Первый freeze SHA-256

```text
app-saved.ts         34d0b45bc0edb80f7aa4eb8ddf966fede0119d5c2fef9d213dc021e30e79265a
app-library.ts       eecf77b384f98829a985eaaa1080eab86a1bd555fee1eedd625e518b71d6de1c
app-discussion.ts    a18b50e0ba843b3907bb5829c4bb5d6401b279bfdae073d6bcedac1f6dd43b94
app-engagement.css   2bbf471c5021b585be15ac7fb29a34aa952bc507fd6200e02855df75912da73b
apps-home.ts         abf1e584c8686832969451914cb38a07a035c0ea4485543f0137fc0884f87152
application-card.ts  01968250d8efd460b3817119847771d404b79b8534c2541998595d37fdfcf56c
```

## Остающийся gate

Независимый mounted component: unknown pending A→B, held ACK/new text, аудитория и архив, cursor reset, storage failure/retained text, admin→normal hydration, account ABA, hidden/dispose. Root: actual browser320/667/desktop, keyboard/focus, library/exact entry, работающий iframe без remount при панели/Back, два окна. Нового человека, физический телефон, screen reader, production и публичный пилот эти проверки не обещают. До результатов нельзя считать весь D3 завершённым.

## Разрешённая малая поправка после первого freeze

Root принял обнаруженную автором неточность empty copy: текущий разговор без `canPost` (в том числе administrative) теперь говорит «Пока нет сообщений», а не предлагает начать обсуждение. Изменена только эта строка; `app-discussion.ts` refreeze SHA-256 `af7372cef23f685381d91bbaf136de13325d219443e84b06ad2c84f5a0284e0d`. Проверка фокуса при read refresh остаётся отдельным независимым сценарием; DOM identity сама по себе не доказывает, что ввод остаётся видимым.

## Независимый mounted RED и исправление выбора current

Root сообщил результат настоящей browser fixture reviewer: **13/14 PASS, 1 FAIL**. Проваленный сценарий менял серверную аудиторию после сохранения private draft, затем вызывал `updateSelection({})`. UI ошибочно доверял прежнему `isCurrent` и не запрашивал новый текущий разговор. Остальные13 включали admin hydration, Back и held-refresh focus/selection; это атрибутированный результат root/reviewer, не собственная browser-проверка автора.

Исправление отделяет `requestedConversationId` (только явный выбор архива) от `resolvedConversationId` (последний подтверждённый разговор, только для собственного локального draft во время чтения). Current `refresh` всегда отправляет context read без conversationId; explicit archive всегда сохраняет ID. `updateSelection({})` больше не использует cached isCurrent как причину пропустить чтение. Poll проверяет загруженный разговор и может сделать его readonly; он не переносит текст в новую аудиторию и не меняет маршрут. Старые scoped draft models остаются живыми.

Новый SHA-256 `app-discussion.ts`: `1dcce82acb24db6360aa08f868e8e81ea00aac4c36308eda2834d3861fef6a8e`. TypeScript после исправления без диагностик. Независимый scenario6 оставлен без ослабления; полный browser повтор14 ожидается отдельно. До него RED не объявляется закрытым.

Повтор root при1280×720 снова дал13/14: audience scenario прошёл, зато реальный40ms context gap проявил потерю input focus/selection. Исправление сохраняет прежний собственный scoped draft в том же видимом textarea **readonly** на время `context=null && loading`. В этот момент server history очищена, `canPost` не присваивается и отправка недоступна. Новый подтверждённый conversation получает только свой draft; при отказе чтения остаётся отдельный retained local текст. Асинхронное `focus()` или восстановление selection не используется. Дублирующая карточка того же текста во время этой короткой проверки не создаётся. После этого TypeScript — PASS, настоящий exit0. Refreeze `app-discussion.ts`: `bc0b6fca3637e6c94431f9d74c9e25883b2140c5af657a846470096a570f4468`. Независимый held-refresh assertion не изменён; следующий root browser повтор остаётся обязательным.
