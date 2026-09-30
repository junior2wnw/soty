# P2-I — возврат фокуса после цепочки окон приложения

30.09.2026. Read-only preflight после D4 `588ec65` / plan `05c8ba3`. Production не изменён, heavy suites и новая браузерная сессия не запускались. Основание — root actual320: «Связи и доступ: app» → «Название и доступ» → изменение/закрытие оставляет `document.activeElement === document.body` после обновления главной.

Это исторический preflight. Принятая реализация, hashes и границы её проверки: [P2-I implementation receipt](p2-dialog-focus-implementation.md).

## Подтверждённая цепочка

1. `src/world/dialogs.ts:5` запоминает единственный DOM node, активный при открытии. Его `close` listener удаляет dialog, возвращает фокус на ещё connected node и лишь затем вызывает `onClose`.
2. `app.ts:899` заменяет окно связей окном настроек через `dialog.close(); openAppSettings(app)`. Логическое место возврата не передаётся следующему окну. По текущему браузерному порядку captured node может оказаться прежней кнопкой карточки или элементом уходящего modal; устойчивого контракта нет.
3. В `openAppSettings` отдельный, позже зарегистрированный `close` listener выполняет dispose и, когда metadata изменились, `renderPersonal()`/`renderGroup()`. Он может удалить уже сфокусированную кнопкой общего listener карточку. Проверка `returnFocus.isConnected` до этого render недостаточна.
4. `dom.ts` только создаёт controls и click listeners, lifecycle/focus-return там нет. У launch карточки есть стабильный `data-entity-id=app:<id>`, но кнопка «Связи и доступ» не имеет стабильного action key. Искать по тексту нельзя: название может измениться.
5. `apps-home.ts` имеет отложенный rAF для собственного `homeState.focusControl/focusId`. Новый возврат из modal не следует реализовывать записью в этот канал: иначе поздний frame может перехватить уже перемещённый пользователем фокус.

Это source diagnosis в дополнение к root browser RED, а не отдельно воспроизведённый мной browser результат.

## Первичные основания

[WAI-ARIA APG, Dialog Modal Pattern](https://www.w3.org/WAI/ARIA/apg/patterns/dialog-modal/), проверено 30.09.2026: обычно возврат направляется к invoking element; если он исчез, выбирается логическое продолжение процесса. APG не требует возвращаться именно к уже удалённой DOM identity. Это guidance по взаимодействию, не доказательство соответствия всему WCAG.

[WHATWG HTML, close the dialog](https://html.spec.whatwg.org/multipage/interactive-elements.html#close-the-dialog), Living Standard, проверено 30.09.2026: нативный close сначала выполняет предусмотренный браузером возврат к previously focused element, а событие `close` ставит в очередь. Поэтому прикладной listener — не единственный участник восстановления фокуса; простое добавление ещё одного focus/rAF не решает порядок lifecycle.

## Предлагаемый узкий контракт

**Один управляемый finalizer плюс явное логическое место возврата.** Это предложение для согласования, не принятая реализация.

- Shared `createDialog` получает дополнительный необязательный return target: `isCurrent(): boolean` и `resolve(): HTMLElement | null`. Старые вызовы остаются допустимы. `WorldDialog.close({restoreFocus?: false})` позволяет явно передать управление следующему окну/маршруту. Не создавать общий focus manager для всех экранов.
- Finalizer выполняется ровно один раз. Для управляемого `WorldDialog.close`: native close → synchronous dispose/onClose/render → разрешение focus target. Нативное queued `close` после этого не повторяет callback/фокус. Header close пользуется тем же методом. Для нативного закрытия остаётся guarded fallback; существующий `cancel.preventDefault` и unsaved confirmation нельзя обойти. Settings X/Escape уже идут через свой `requestClose` и в итоге `options.onClose`, их поведение сохраняется.
- Существующий settings cleanup/render переводится из независимого позднего listener в этот lifecycle hook. Нельзя ограничиться перестановкой focus и listener: `dialog.close(); otherControl.focus()` не должен потом уничтожить выбранный control поздним settings render. Preview, route continuation и forced close должны явно передавать управление, а не возвращать его предыдущей карточке.
- `inspectApplication` создаёт один account/screen-scoped return target и передаёт его дальше в `openAppSettings`. Intermediate inspect close подавляет дополнительный custom return. При обычном закрытии settings: surviving original opener → заново найденная кнопка «Связи и доступ» того же app/action → launch того же app → существующий `#soty-main` (`tabIndex=-1`) в том же экране. Никакого выбора произвольной первой кнопки или другого приложения.
- Минимальный устойчивый action key добавляется на inspect button в `application-card.ts` (ID, не label). Resolver ищет только внутри текущего root/main, требует connected/visible/not-disabled/not-inert и допускает fallback лишь при совпадении account ID **и account generation**, screen/route и живого mount. Same-account ABA не считается тем же разрешением. Settings над runtime сохраняют живую toolbar-кнопку; iframe и textarea не пересоздаются ради фокуса.
- Custom return отменяется, если управление уже передано другому modal, маршруту, аккаунту, или после начала закрытия возник новый пользовательский focus/action. Capture/restore ticket живёт только в этом close lifecycle; не оставлять timer, повторный observer или rAF retry. При нативном queued fallback нужна та же проверка, а не безусловное «если сейчас BODY, focus старому app». Нативный краткий возврат внутри самого `.close()` отделён от custom focus и не выдаётся за управляемый этим флагом браузерный алгоритм.

### Узкие файлы следующего подплана

`src/world/dialogs.ts` — additive lifecycle/return contract; `src/world/app.ts` — конкретная цепочка, metadata-close render и identity fences; `src/world/application-card.ts` — стабильный action key. `dom.ts` не требует изменения по найденной причине. `apps-home.ts` менять только если actual negative покажет вмешательство его существующего rAF; сначала не расширять scope. Любое изменение формы `requestClose` ради явного route handoff согласовать отдельно; guards/draft модель не переписывать. Новые mounted tests — отдельная reviewer зона.

## Обязательная следующая приёмка

1. Actual mounted main320: связанная цепочка, rename/publication readback, Close и Escape → правильная пересозданная inspect button того же app; дальнейший Tab продолжает рядом. Без изменений metadata — surviving opener.
2. Settings из живого app toolbar → тот же control, неизменные iframe identity, внутренний ввод и WS. Не remount ради return.
3. Внутренняя unsaved confirmation: Cancel оставляет поле/selection; подтверждённый выход возвращает к app. Native cancel нельзя закрыть в обход этого решения.
4. Close → немедленный новый focus или второй modal до доставки queued close/следующего frame: старый callback не возвращает фокус и не удаляет новую выбранную цель. Проверить также текущий home rAF после финального return.
5. Navigation, Preview, account A→B→A, destroy во время закрытия: ни фокуса старой карточке, ни late rerender прежнего экрана. URL и active dialog принадлежат новому действию.
6. App скрыт/удалён из текущего списка: return к `#soty-main` того же screen, без угадывания другой карточки. Новое имя не ломает поиск по ID.
7. Shared helper: два обычных существующих modal сценария и native close fallback, idempotent cleanup без двойного callback. Typecheck плюс focused tests последовательно; общего тяжёлого suite на стадии preflight не требуется.

После actual mounted gate root повторяет настоящие keyboard Enter/Space/Tab/Shift+Tab/Escape на320 и desktop. Физическое устройство, installed PWA, IME/screen reader combinations остаются отдельными внешними проверками. Этот документ ничего из них не объявляет пройденным.
