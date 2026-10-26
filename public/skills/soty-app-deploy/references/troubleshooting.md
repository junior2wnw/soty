# Диагностика запуска

При наличии checkout подробные gates находятся в `docs/deployment/app-troubleshooting.md`. Порядок:
документ/Network timing → boot/admission/cookie → app transport/CSP → identity →
данные/reload. HTTP 200, iframe.src и событие load сами не подтверждают приложение.
Boot проверяй GET: неподдерживаемый HEAD или ожидание HEAD body не измеряют его
загрузку. Parent frame-src и child frame-ancestors/XFO проверяются отдельно.
Cross-origin AX about:blank не устанавливает actual document URL. Один человек
с двумя наблюдениями в разных браузерах — не два участника и не общий диагноз
Root auth; точный профиль, persistence и другие приложения проверяются отдельно.

| Наблюдение | Проверить следующим | Корректный статус |
|---|---|---|
| Документ не загружен | Origin/route, DNS/TLS, CSP и transport, затем влияние инструмента | Подтверждённая причина или unknown, без диагноза по одному пустому окну |
| Документ есть, приложение ждёт входа | Capability/profile, issuer/client/callback и текущая app session | Рабочая сессия либо необходимый consent/отказ |
| Интерфейс есть, список пуст | Фильтр, область человека, свежесть Source/API/WS и реальный payload | «Нет результатов» отличается от «данные не поступают» |
| ACK записи потерян | Receipt/history/readback того же намерения | Confirmed либо pending/unknown; новый requestId не создаётся вслепую |
| Роль/доступ не подходят | Текущий grant, владелец, source pin и frame binding | Нормальный отказ; аудитория автоматически не расширяется |
| Долго ждёт поле | Navigation/resources → entry/shell → данные поля → приложение | Сопоставимый trace по фазам/версии, а не выдуманное ускорение |
| Operator падает до работы с DATA | Qualified Node24 и настоящий Linux CLI/module graph, включая TLA cycle; safe exit/stdout/stderr | Ошибка инструмента; потерянный stderr оставляет причину выводом |
| Изолированный reader получает EACCES | Сначала public code mode/UID, затем отдельно DAC own-copy DATA; только finite observer role/RO exception | Не расширять права production и не менять DATA ownership |
| Docker list превышает bound | Exact label и volume filters, all=true, inspect всех peers | Не снимать bound глобального inventory; stopped peer тоже удерживает том |
| CREATE preservation mismatch | Exact prospective/full diff; у QA network:none пустой верхнеуровневый /MacAddress может быть omitted | Исправить только доказанный input; не нормализовать все конфигурации |
| Holder STOP истёк по времени | Тот же intent, actual stopped state и terminal helper; SIGTERM handler собственного PID1 | Не повторять STOP/операцию по одному transport timeout |
| Storage probe exit0 без JSON | Независимый probe с `SOTY_STORAGE_PROBE=1`, явный World и v7 gate | START запрещён до настоящего результата |
| Mount CAS отличается | Детерминированный порядок полных Mount records, затем все поля | Перестановка отличается от изменения mount; поля не выбрасываются |
| UI publisher потерял ответ | Тот же plan/receipt, readonly inspect, exact process/disk/active | Unknown до сверки; execute/transaction не создаются заново |
| `app_native_entry_domain_required` | Оба точных origin: включённый прежний Native alias и подходящий Source alias; свежий inspect | Отказ может относиться к любому из двух адресов. Новый Source alias не публиковать через прежний порт |
| `app_source_entry_domain_required` | Точный approved Source origin, OWN bound alias и capability `sourceEntryActivation`; выбрать выключенный alias в Source editor | Закрепить имя, оставить выключенным до атомарного переключения; не публиковать прежний порт ради CHECK |
| `invalid_source_entry_action` / `app_source_entry_action_mismatch` | Closed activate/domainId, текущий профиль, подготовленный origin и исходный intent | Новая явная проверка при изменении черновика; unknown отправленный запрос не заменяется |
| Capability активации отсутствует | Обновление Root, затем свежий inspect | Выбранная новая активация блокируется; уже включённый адрес без action сохраняет старый контракт |

«Сохранено на устройстве», pending и server-confirmed требуют подтверждения
соответствующего хранилища. Не обещай «всё сохранено» общей фразой об ошибке.
При unknown сохрани исходное намерение и используй readonly recovery своего
контракта: Source `recoverReceipt`, Human `checkRevoke`, UI publisher `inspect`.
Они имеют разные результаты и не заменяют друг друга. Отказ local storage
не является «черновик сохранён»; отсутствие записи в другом профиле не доказывает
нулевой эффект. См. [аутентификацию](authentication.md) и [операции](operations.md).
Доступность интерфейса и открытый WS не доказывают свежесть данных приложения.
Проверяй источник/подписку, фильтры и результат под текущим человеком.

Root phase trace — partial, до 24 событий/120s, без child DOM и URL. root_load
не равен boot_document_ack; отсутствие ACK не доказывает сбой. Сохраняй только
закрытые состояния и безопасные коды. Не выгружай cookies, токены, auth headers,
полные boot URLs, device keys, watch IDs или приватные документы.

Если standalone работает, а embedded document stalled **до DNS/connect** и
debugger attached, проверь влияние инструмента. Воспроизводимый случай был
20.10.2026: штатная отмена task debugger и новый launch восстановили iframe.
Не ставь этот диагноз по одному пустому окну. Пользуйся разрешённым каналом,
не отключай browser security и не переподключай блокирующий инструмент ради QA.
Этот reference сам не разрешает альтернативную технологию управления браузером.

Если network/CSP подтверждает запрет parent — проверь exact frame-ancestors и
X-Frame-Options. Если isolated запрещает нужный nested chart — review native
конкретного приложения, сохранив общий sandbox. WebSocket 400 в auth — проверь
Upgrade headers только в auth subrequest; 101 + message echo и отрицательные
Origin/ACL/pin checks обязательны. См. operations.md.

Expired boot → один свежий launch из текущей карточки. Unsupported readiness
после load → нейтральное loaded; не fake authenticated ready. Denied/unknown
scoped readiness не превращаются в успех. Lost persistence → active volume,
single writer, schema до optional workers и стабильный account mapping.

Общий помощник принимает только enum observations:

```json
{"embedded":"blank","standalone":"works","document":"stalled_before_connection","debugger":"attached"}
```

CLI: `node scripts/soty-app-release.mjs doctor --observations FILE --output NEW_FILE`.
Root MCP: `apps_diagnose({observations:{...}})`. Полная схема доступна в tools/list.
Ответ различает observed/hypothesis/insufficient_evidence, отмечен liveVerified=false
и releaseReadiness=not_assessed. Он не запускает проверок, не авторизует и не
меняет политики. Если живой Root ещё старый, не выдумывай наличие нового MCP;
используй проверенную CLI из актуального repo.
