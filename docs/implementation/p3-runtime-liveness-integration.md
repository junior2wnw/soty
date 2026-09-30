# P3-R — принятая локальная интеграция

Дата:30.09.2026. Основание:D3 `0644023`, [подплан R0–R4](p3-runtime-liveness-plan.md). Локальный checkpoint закрывает живость платформенного WS и восстановление отдельного sample. Он не закрывает весь P3, общую D4 или production.

## Результат

Тихое приложение получает прозрачную проверку обоих концов WebSocket; живой внешний connector/ACK не маскирует мёртвый внутренний endpoint. Собственные Ping/Pong не становятся бизнес-сообщениями. Пределы buffers/message/capacity и сроки прав сохраняются. Gateway не требует heartbeat-кода от каждого автора и не повторяет действия пользователя.

Пример «Покупки» сохраняет ввод при обрыве и предлагает явное подключение. Pending/unknown запись отдельна от нового draft. Неизвестный результат требует нового read-only подключения и проверки человеком; автоматического POST нет. Подтверждённый ACK разблокирует изменение только после соответствующей версии WS того же server instance. Старый/чужой snapshot не считается подтверждением собственного эффекта. Versioned sample URLs `?snapshot=1` не меняют Apps wire; прежние array/echo URLs сохранены.

## Что поймал аудит

1. Root нашёл линейное удержание Promise reactions у долгого соединения. Исправлено одной отменяемой текущей записью на pump; independent open-relay GC probe стал6/6/6 вместо1008/9008/41008.
2. Независимый reviewer воспроизвёл чужой precommit WS до/после собственного ACK. Счётчик поступлений заменён явной causal version/instance; две реальные HTTP/WS регрессии прошли RED→GREEN.
3. Root в браузере подтвердил потерю фокуса при временном native disabled. Синхронные guards и aria-disabled сохранили недоступность и клавиатурный фокус. Позднего принудительного focus нет.
4. Полный regression выявил прежнее ожидание одного WS error code при двух конкурирующих30s deadlines. Причина подтверждена отдельным сетевым опытом, production таймеры ради теста не менялись. Усиленный default тест одновременно проверяет HTTP head timeout, самостоятельный HTTP ACK timeout после настоящих head/body и bounded WS write/ACK,21живого соседа и повторное занятие всех трёх освобождённых public slots. Неожиданные error codes не допускаются.

## Проверки окончательного среза

| Проверка | Доказанный результат |
| --- | --- |
| Root world |815всего /810PASS /0FAIL /5явных skip;85.02s; `p3-r-world-root-final.log` |
| Connect |70/70PASS; `p3-r-connect-root.log`; Connect-код не менялся |
| R1 автор |26/26: parser/relay/lifecycle |
| R3 независимый |17сетевых+memory PASS; default opt-in отдельно1/1PASS130.27s на исправленном relay |
| R4 независимый final |22/22PASS:20авторских и2новых независимых real HTTP/WS causal cases |
| Default head/ACK/write composition |1/1PASS30.26s перед общим повтором; `p3-r-ack-deadline-root-final.log` |
| TypeScript/build |PASS; `p3-r-typecheck-root.log`, `p3-r-build-root.log` |

Пять пропусков общего world — четыре прежних opt-in и новый отдельный long test; его фактический запуск указан выше. Initial world808/802PASS/1FAIL/5skip и два диагностических timeout runs сохранены, не выданы за успешную финальную приёмку. Все логи — `output/implementation-20260930/`. Полный world повторён после causal/focus исправлений; изменённые assertions не выключались.

SHA256 relay `e57d674d3fe5f21fa55bb6353c2305666fb643eecfe910b5b384837b667d1e9d`, sample `8a81acb7c3f564ab6bb9e7f09051f45a51dcee26cf897e6baee04b81053d3d6b`. Остальные pin и точные методы — в [R3 independent](p3-runtime-liveness-independent.md), [sample author](p3-sample-recovery-implementation.md), [sample independent](p3-sample-recovery-independent.md). Connector1.4.0/wire и storage formats не менялись; frontend build не изменил отслеживаемый release bundle.

## Настоящий браузер на окончательном sample

Root использовал loopback UI5420/API5421, настоящий connector58350 и отдельный source B56323. Измерены фактические320×760 и667×375; viewport control применяется к выбранной вкладке, поэтому DOM размер проверен перед итоговым снимком.

- Reconnect Enter возвращает готовность с фокусом на том же button. Checkbox Space→ACK сохраняет focus на том же checkbox, а checked соответствует новому snapshot.
- Test-only wrapper перехватил `res.end` после настоящего commit/broadcast. Один normal toggle + один lost-response add дали **writes2/dropped1**; после read-only reconnect осталосьwrites2. Attempt A отдельно виден рядом с новым draft B. `p3-r4-final-counter.json` сохраняет счётчики без headers/credentials.
- Readonly attempted textarea→Tab приводит к «Я проверил список»; Enter возвращает фокус в сохранившийся draft. В landscape следующий Tab показывает целиком действие отправки. Кнопки44px+, горизонтального overflow нет (inner viewport305/652 из-за scrollbar).
- После отдельной остановки source focus/value/selection32:32 сохранились; в offline поле дописан текст. Перезапуск источника и explicit reconnect сохранили его в том же iframe. Новому server instance пришли GET+WS, **writes0** (`p3-r4-final-restart-counter.json`). In-memory список самого sample после process restart закономерно начальный; это не обещание его durable backend storage.

Снимки: `p3-r4-final-unknown-320.png`, `p3-r4-final-recovered-667.png`; focus proof `p3-r4-final-focus.json`. Попытку повторно нажать aria-disabled Add инструмент отклонил; это не называется фактическим browser double-Enter. Реальные handler guards отдельно проверены executable tests.

## Оставшиеся задачи

D4: полный save→return→discussion с независимыми actor states, exact aliases/private paths, denial/retire/archives, общий focus/visual/long-text audit. На320px найдено обрезание второй строки chat draft из-за fixed48px textarea; это отдельная следующая UI-задача, не спрятанная под успешным R4 sample. [Диагностика среды](p3-browser-environment-audit.md) отдельно объясняет воспроизводимую script-free MutationObserver error.

P3-E/production: отдельная подтверждённая domain zone, DNS/TLS, реальные проекты, совместимый Linux reader/fallback и фактическое изолированное восстановление свежей зашифрованной копии. Gate браузера на физическом телефоне/установленной PWA не заменён desktop viewport. P4 готовится по своему утверждённому плану; рабочий внешний create/OAuth/MCP ещё не заявлен.
