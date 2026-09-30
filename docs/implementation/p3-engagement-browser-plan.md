# P3-D4 — общий сценарий и визуальная приёмка

Основание:D3 `0644023` и принятый R `e00f3a0`. Локальные проверки D4 выполнены; доказательства и границы — в p3-engagement-browser-evidence.md. Публичный rollout остаётся отдельным gate. Этот подплан уточняет [D0–D4](p3-engagement-plan.md), не заменяет остальные этапы master plan.

## Последовательность

- [x] **D4-A. Читаемый draft.** На реальных320×760 textarea обсуждения48px при scrollHeight66px обрезает вторую строку. Подобрать общий bounded autosize, использовать существующий механизм если он уже есть; сохранить focus/selection, scoped persistence и мобильный предел высоты. Проверить исходный текст после remount, ввод, удаление, width/orientation и длинный текст; никаких remount ради размера. Автор:publishing_architecture — только UI/controller, styles и при необходимости узкий reusable helper. Независимый critic — отдельная DOM/regression область.
- [x] **D4-B. Реальный пользовательский цикл.** На существующем изолированном API/connector выполнить save→home/library→return→discussion, открыть и закрыть панель без потери runtime input, сохранить собственный draft через reload/Back. Два независимо enrolled Connect actor/client states; второе устройство одного QA-account через настоящее enrollment. Где UI, а где signed HTTP — указать точно, без обещания физического телефона.
- [x] **D4-C. Права и точный вход.** Public alias/private canonical с path/query/hash, audience transition, недоступный/retired сохранённый адрес, offline source, запрет повторного post после потери прав, доступные owner archives. Данные и оригинальные drafts не перемещать между аудиториями; не заменять закрытый вход другим alias.
- [x] **D4-D. Визуальный/клавиатурный аудит.** Actual320×760,667×375,desktop; длинные сообщения/имена, Tab/Shift+Tab/Enter/Space/Back, panel/menu/archive focus, отсутствие x-overflow/старого CSS. Измерять настоящую вкладку перед screenshot, не только заданный viewport. Исправить реальные findings и повторить затронутые сценарии.
- [ ] **D4-E. Checkpoint.** Сверить два независимых взгляда, выполнить необходимые UI/state/signed HTTP regression и typecheck/build, сохранить доказательства, commit/push. Следующий этап не закрывает оставшиеся external gates.

Root владеет интеграцией, QA harness, браузером, evidence и этим журналом; автор и reviewer не меняют общие файлы одновременно. Не более3субагентов. P4 read-only contract подготовлен отдельно; production-функции P4 ещё не включены.

## Неизменяемые условия

Нельзя сделать input видимым удалением текста, увеличением всего экрана за пределы viewport, скрытием кнопки отправки или отключением failed assertion. Draft/pending/account/entry/conversation identity из D3 сохраняется. Autogrow — свойство отображения, не новая модель данных. Ошибка local persistence продолжает блокировать уход. Длинный текст после достижения предела прокручивается внутри поля, а не вытесняет header/history/действия полностью.

Отдельная ошибка MutationObserver воспроизведена вне приложения на static script-free iframe; её не подавляем и не выдаём за product defect. Настоящие errors/неверное состояние/потери данных остаются блокерами. Физический телефон, installed PWA, новый независимый участник, отдельный домен/DNS/TLS, Linux fallback и фактический backup restore по-прежнему самостоятельные внешние gates.
