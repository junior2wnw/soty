# P2: постоянный помощник и сохранность отправки

Маршрут `#assistant` использует текущий Connect-аккаунт, собственные устройства и уже существующий долговечный job store. Создание приложения остаётся отдельным явным действием. Произвольный shell не опубликован внешним клиентам; текущий исполнитель владельца работает с правами его пользователя, о чём интерфейс сообщает рядом с вводом.

## Контракт

- `apps.assistant.history`: подписанный owner запрос с `expectedAccountId`, `limit` 1–50 и непрозрачным account-bound cursor. Фильтрация действующих прав на устройство предшествует пагинации. Короткие DTO не возвращают вывод/секреты/session ID.
- `apps.assistant.send`: `expectedAccountId`, `hostDeviceId`, `connectorId`, `requestId`, `conversationId`, текст до 16000 символов, полный путь `cwd` до 2000, optional `previousJobId`. Произвольный OpenCode session ID от браузера не принимается. Продолжение берёт только собственную завершённую сессию того же разговора, устройства и папки, без неизвестного исхода.
- Тот же нормализованный intent и request ID сначала ищется в устойчивом ledger — до mutable readiness/continuation. Другой intent даёт конфликт, старое непроверяемое/retired состояние не создаёт новую задачу.
- Известный отказ admission фиксируется до ответа `{admission:{status:'rejected',requestId,reason}}`; эта отправка больше не может стать исполняемой. Клиент снимает pending только по совпадающей квитанции. Ошибка сети или смена доступности сами по себе не означают, что эффекта не было.
- Одновременные продолжения сериализуются внутри job-store mutation: существующая работа блокирует следующую, устаревший predecessor не открывает параллельную сессию.
- `apps.agent.read/cancel/result` сохраняют owner/device guard. Read возвращает безопасный `task` и признак возможности продолжения; session ID остаётся внутренним. Отмена не означает откат изменений или уже подтверждённое прекращение дерева процессов.

## Клиентская устойчивость

`assistant-state.mjs` хранит отдельную запись каждого разговора в localStorage, текущий разговор вкладки — в sessionStorage. Pending содержит неизменяемый payload и request ID, сохраняется до отправки и сериализуется через Web Locks. Потеря подтверждения, повторное открытие и смена порядка устройств не меняют получателя. Старый пункт списка восстанавливает свежую запись, а не старую копию target. При ошибке хранения отправка блокируется, введённое остаётся в окне.

Недоступное устройство остаётся явно выбранным; другое не подставляется автоматически. Недоступная задача закрывает её вывод и запрещает продолжение, сохраняя черновик в прежнем разговоре. Только явный «Новый разговор» создаёт другой контекст. Поздний ответ после dispose/account switch не возвращает содержание прежнего аккаунта. Запросы истории/устройств имеют generation guards; незавершённая отправка входит в PWA update guard.

На телефоне первичный ввод расположен перед примерами, пустой блок истории скрыт, а порядок DOM совпадает с визуальным. Иконка нового разговора сохраняет доступное имя. У native dialogs фон недоступен, Escape закрывает окно с возвратом фокуса.

## Проверка

- 8 server jobs tests: ownership, signed HTTP, persisted replay/reopen, concurrent continuation, output bounds, revoked devices, model unavailable, durable rejection.
- 5 авторских + 8 независимых state tests: lost ACK, target/account spoof, late receipt, cross-tab remount, stale list target, pending-only recovery, corrupt/quota/partial-write failure, clone isolation.
- 2 command file parser tests: ACK backpressure, split protocol lines, exact binary bytes, empty/malformed/out-of-order/failure.
- Реальный UI пустого аккаунта: новый разговор, возврат черновика, уход/возврат, мобильные 320px/светлая тема. Signed backend tests не требуют настоящего inference.
- Отдельная dev-only `assistant.test.html`: synthetic lost ACK, A/B reorder, remount, exactly one synthetic accepted request, denied job сохранение draft+disabled Send, account change clearing. Никаких команд/API в fixture не выполняется. Production bundle этот HTML не включает.

Не доказаны этой приёмкой: живой inference на реальном подключённом устройстве, OS sandbox, обещание жёсткого денежного лимита, остановка всех дочерних процессов. Эти проверки относятся к P5 и внешним integration gates.

Основание доступности: [W3C Reflow](https://www.w3.org/WAI/WCAG22/Understanding/reflow.html) — оболочка проверяется на 320 CSS px, двумерное поле ограничено своей областью; [W3C Name, Role, Value](https://www.w3.org/WAI/WCAG22/Understanding/name-role-value.html) — иконки сохраняют имя и состояние при скрытии подписи. Это критерии нашей проверки, не заявление о полной сертификации WCAG.
