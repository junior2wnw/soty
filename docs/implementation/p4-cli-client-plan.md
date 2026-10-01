# P4-C2c — два настоящих клиента

GO: C2b `2103d755c081f03cdad98959fe593c6151ea2eec` committed/pushed, точный remote SHA подтверждён. Master P0–P8 и D1/D2 сохраняются.

Статус C2c: **принят локально**. [Root result](p4-cli-client-result.md): OpenCode COMPLETE из fresh5521 и Codex COMPLETE из отдельного fresh5523, семь stages на каждый; immutable source/dist/store/negative evidence независимо проверены. Исторический Codex PARTIAL сохраняется. Ниже — последовательный подплан и отдельные неудачные подготовительные попытки; D1/D2/master не завершены.

## Последовательный подплан

1. Проверить поддерживаемые интерфейсы выбранных Codex0.153.4 и OpenCode1.18.15. Domain author владеет только новым Codex contract; UI author — только новым OpenCode contract; независимый critic — только новым acceptance/harness review. Root — общая композиция actual AS/PWA и operator сценарий. Source fixes только по выявленному причинному дефекту и отдельному назначению файлов.
2. Создать один свежий loopback stand с actual Connect/Notes2/Capabilities3/OAuth/MCP и текущей принятой PWA. Аккаунт и согласие создаются настоящим owner UI. Ни seeded approval, ни token injection, ни SDK substitute. Каждый CLI получает собственный новый config/home без личных ключей или keyring; выбранные binaries pinned SHA.
3. Перед запуском reviewer читает harness. Он сохраняет только разрешённые версии/method/tool/status/IDs/counts/digests; raw OAuth URL/state/code/verifier/token/headers и Note body не попадают в outputs/receipt. Точные callback/resource/S256/cardinalities проверяются локально. CLI stdout/stderr bounded memory-only, cleanup только exact owned child/listeners.
4. Сначала Codex: actual authorize→signed consent→callback/code exchange→initialize/list→catalog RU/EN/get→create/get; открыть результат и редактировать в PWA, exact replay, owner revoke, отказ с сохранённой запиской. Затем тот же путь OpenCode с отдельной connection и input/key. Negotiated revision берётся из actual wire, не версии библиотеки.
5. Если CLI не предлагает поддерживаемый прямой tool call, ограниченный deterministic loopback provider fixture может запускать **настоящий CLI tool loop**. Такое доказательство относится только к transport/tool integration; D1 требует настоящей модели и всего фиксированного RU/EN корпуса. Fixture не выдаёт за модельный интеллект, production provider или OS sandbox. Выбор подтверждается contracts/reviewer до исполнения.
6. Root сверяет RO metadata actual stores, результат owner UI, количество effects/расход, revoke и сохранённую правку. Независимый audit проверяет pins/trace/границы утверждений, устраняет findings; затем commit/push. Дальше D1 matrix, D2 внешние HTTPS/backup/restore/release и master P5–P8.

## Ограничения и простота

Один существующий capability/Invocation/root budget и один AccessPanel. Никакого нового реестра агентов, панели, второго job lifecycle или auto retry неизвестной выдачи. Тестовый provider — только собственный finite QA endpoint. Не включать arbitrary shell, personal credentials, DCR, generic execution или production AS этим stand. Поддерживаемые client commands не заменяют наблюдение настоящего результата.

## Первый actual startup и узкое исправление

Fresh run `e187b4e6183f8b46f2911cf5ef844e92`, host PID49928: PWA сама создала synthetic owner; Codex login PID14508 завершился exit1 до authorize. Wire requestStarts/authorizations/tokenPosts0, connections/Notes/proofs/Invocations/spent/reserved0; exact child exited, registry пуст. Owner control завершил клиента как `completed:false`; failed receipt SHA256 `35f9b0e5b3a4ebf4925bd1257003b7a66e3176b7b1caf429eca0db4c28125b36`. Затем остановлен exact owned host PID; graceful stand shutdown этим запуском не заявлен.

Read-only config diagnostic в этом же ещё не авторизованном профиле подтвердил два конкретных pre-authorize несоответствия: pinned CLI отвергает `--strict-config` для `codex mcp`, а semantic loader отвергает пустой model catalog. Исправляются только login argv и одна полноценная static metadata запись; app-server strict flag сохраняется. Явный `project_root_markers=[]` ограничивает project config discovery собственной пустой cwd. `--strict-config` сам по себе не является изоляцией слоёв. Presence-only проверка Windows managed config/requirements показала отсутствие обоих файлов; их политики не обходятся.

Model metadata не вызывает модель: direct tool RPC по-прежнему без `turn/start`, а no-model endpoint обязан сохранить observed0. Failed run не перезапускается автоматически и не смешивается со следующим fresh OAuth/effect сценарием. Для нормального завершения следующего owned exec session используется короткая stdin-команда `stop`, вызывающая тот же graceful close, что SIGINT/SIGTERM; raw CLI/secret outputs по-прежнему отсутствуют.

Последующий actual config diagnostic обнаружил custom deserializer rule: static ModelInfo обязан содержать `base_instructions` либо `model_messages.instructions_template`. Добавлена одна static instructions строка, не модельный ответ. Итоговый **настоящий `mcp list --json`: exit0**, ровно один server `soty`, собственный URL совпал; OAuth этим не проверялся. Новый exact loopback host5519 отделяет origin IndexedDB от failed5517; fallback порта отсутствует. Stop alias имеет один latch, чтобы повторные сигналы не опередили cleanup.
