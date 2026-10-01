# P4-D1.1 — корпус и manifest подготовки

Статус 2026-10-01: **source-only, not_run**. Созданы только три файла в ignored `var/p4-real-model/` и этот документ. Загрузчик, manifest, tests, CLI, hosts и модели автором не запускались; личные config/auth не читались. Здесь нет результата D1, разрешения расходов, paid runner, proxy или нового ledger.

Источник запросов — [исправленный D1 план](p4-real-model-plan.md), §2 и §4: raw worktree SHA-256 `d8d63a0a394abda43be57bd4492034509b7b082be76d7d86f685407d83faf5f1`. Сохранены все 24 запроса/12 RU–EN пар. `\n` из таблицы становится LF в строке JSON после декодирования; Unicode не нормализуется. Общая инструкция и два разрешённых follow-up скопированы дословно. `expectations` — отдельная часть файла для будущего oracle, её renderer не возвращает модели; для 03–04 `title:null` означает выбор заголовка моделью, не пустой title.

| Файл | Bytes | SHA-256 exact worktree bytes |
|---|---:|---|
| `var/p4-real-model/corpus24.json` | 6681 | `7673bb2b8b3d0428435e875be8aec802599497455ae7e4dfbe44cc2bcdb4ad01` |
| `var/p4-real-model/corpus.mjs` | 6661 | `99f700bb0b6813bfcd9fac5b2ed18e455e6573d6d8cd16c223d8f08ab1e806db` |
| `var/p4-real-model/manifest.mjs` | 9301 | `5cb8743e25ce3d9372cbb849752b01ba198056eec55ece45ea471526fcc72ebb` |

Это исходники, а не executable/result receipts. SHA файлов посчитаны чтением; API semantics и синтаксис ещё должен проверить root. Хеши Markdown в будущем manifest относятся к прочитанным worktree bytes, включая EOL; они не называются Git-blob hashes. C2c checkpoint `7a30c77ec7f2442675a082fc2efd60acff7ded4b` и версии/hash двух binaries взяты из принятой квитанции/контрактов, без повторного чтения executable или запуска CLI.

## Узкий API

- `loadCorpus()` читает только соседний `corpus24.json`; `parseCorpus(Uint8Array)` нужен для проверки точных bytes. Предел 32 KiB, обязательный frozen SHA, затем строгая форма 24 последовательных ID/языков/ожиданий. Изменение корпуса требует явного нового review/pin. Возвращается глубоко замороженный объект; renderer принимает только объект, полученный этим загрузчиком.
- `renderTask(corpus, id, substitutions?)` возвращает только `{id, language, instruction, prompt}`. Подстановки разрешены ровно для `{ownerNoteId}` и `{foreignAccountId}`; нет форматирования произвольных переменных, template evaluation, getters или неявного stringify. Публичные ID ограничены скалярной native syntax/длиной; это не проверка существования/ownership. Missing required slot, лишний ключ, неверный тип или control/template injection отвергаются. Текст ограничен 4 KiB UTF-8. JSON roles не навязываются: Codex/OpenCode должны получить общую инструкцию своим штатным способом.
- `renderFollowUp(corpus, id)` доступен только для 21/22. Возвращает заранее фиксированный текст без key/tool/ID. Сам helper не исполняет follow-up и не ведёт счётчик; будущий runner обязан допустить его максимум один раз после unknown model turn в том же контексте.
- `createPreparationManifest(substitutions?)` в `manifest.mjs` без аргумента описывает unbound templates; с явными **обоими** IDs добавляет rendered prompt hashes. Manifest содержит только hashes входов/ожиданий/подстановок, source pins, записанные CLI pins и policy; значения synthetic IDs и Note bodies в stdout не выводятся. Документы читаются только по фиксированному списку, максимум 256 KiB каждый. Перед выдачей проверяется совпадение 24 prompts/инструкции/follow-up с D1 Markdown.

CLI manifest принимает либо ноль аргументов, либо по одному `--owner-note-id` и `--foreign-account-id`. Не читает env/config и ничего не записывает. Неизвестные/повторные options отвергаются; stderr содержит только allowlisted static error code. Пример будущей **локальной** проверки root, не выполненный автором:

```text
var/toolchains/node-v24.21.0-win-x64/node.exe var/p4-real-model/manifest.mjs
```

Выход ≤64 KiB, без timestamp/random/run ID. Хеши объектов используют JSON с рекурсивной сортировкой ключей; `manifestSha256` исключает собственное поле. Одинаковые source bytes и IDs дают одинаковый manifest. Источники читаются последовательно, это не атомарный filesystem snapshot: freeze исходников перед приёмкой остаётся обязанностью будущего runner/reviewer.

## Что manifest фиксирует и чего не разрешает

`status:not_run`, `modelCalls:0`, `executionAuthorized:false`, `results:null`; выбранные provider/model/native settings отсутствуют, расходы pending. Ноль означает отсутствие исполнения этим preparation helper, не исторический runtime trace и не моделирование успешных задач.

Policy сохраняет cap11/expected10, отдельные ready пустые контексты 23/24 **до** единственного owner revoke, ноль model calls при их подготовке, typed current-attempt denial и terminal model answer вместо quota/timeout PASS. Включены согласуемые денежные и технические caps плана, unknown reservation и остановка платного допуска после failed/unknown request. Эти значения описывают будущие условия, helper их не реализует и не гарантирует spend bound.

Codex 0.153.4 закреплён как Responses + настоящий `turn/start`; прямой C2c MCP RPC не модельный run. OpenCode 1.18.15 — настоящий session loop + Chat Completions; finite provider не модель. Ни endpoint, ни model ID, ни token, ни новая config не выбираются. Существующий Notes create-only scope сохраняется; ни правильный tool, ни key, ни receipt не добавляются к corpus prompts.

## Нужные проверки root до принятия

1. Реальная загрузка и независимое сравнение всех 24 строк с §4 плана; чётность языков, точные emoji/LF, пустые bodies 07/08, model-chosen title 03/04. Эти сравнения не подменяются одним успешным JSON parse.
2. Отклонение превышения 32 KiB и любой подмены frozen bytes: prompt/unknown placeholder/ID order/duplicate key/invalid UTF-8. Не менять pin ради отрицательного fixture.
3. Renderer: missing slot; extra/symbol/prototype key; accessor без его вызова; array/boxed/non-string ID; newline/braces/URL/overlength. Valid synthetic IDs заменяют только положенные места, UTF-8 остального текста неизменен. Clone/mutated corpus не обходит frozen loader.
4. Ни expectations, ни tool/key hints в model-facing return; follow-up только для 21/22, точно по плану и без подстановок. Сам факт выдачи строки не объявлять lost-ACK test.
5. Два одинаковых manifest вызова дают byte-identical JSON и digest; перестановка ключей bindings не меняет hash, изменение одного synthetic ID меняет соответствующие rendered hashes. Без bindings всё остаётся явно unbound/not_run.
6. CLI unknown/duplicate/missing args возвращают bounded static error и nonzero exit, без stdout partial manifest, значений IDs или файловых путей в диагностике. Source/plan mismatch также не выдаёт success manifest.
7. Проверить cap11/expected10, native protocol differences, pending expense/provider и отсутствие results/сетевых/process/write imports. Никакой test не засчитывается в 24/24, D1/D2 либо P5.

Исполнительных проверок и generated manifests этим source-only срезом не создано. Нужные данные нового owner stand, окончательные provider/model/tariff/settings, расходы, actual model execution и человеческая PWA проверка остаются отдельными следующими gates.

## Последующая root offline приёмка

Root после independent source GO выполнил отдельный `var/p4-real-model/corpus-root.test.mjs`: **6/6 PASS, 0skip, 528.7477ms**, `output/implementation-20260930/p4-d1-corpus-root-first.log`. Реально загружены/сверены все24 строки с планом, точные LF/emoji/пустые bodies, frozen-byte tamper/truncation/size refusals, опасные/missing substitutions без getter/string-conversion вызова, отсутствие expectations в renderer, только21/22 follow-ups, детерминизм manifest/source hashes и отдельные subprocess вызовы **offline manifest CLI** с normal/duplicate/missing/invalid flags. Это не запуск Codex/OpenCode или модели и не lost-ACK/model gate. Исторический source-only отчёт автора выше сохранён.

Подготовительный manifest фиксирует только inputs/pins/proposed policy, `not_run`, `modelCalls:0`, `results:null`; никакие48 исходов не созданы. Provider/model/расходы и actual D1 остаются pending. Дальнейшие domain/build tests не повторялись: production не менялся.

Root отдельным offline вызовом проверил одинаковый manifest при перестановке ключей synthetic bindings и записал create-new `var/p4-real-model/preparation-unbound.json`, **13823 B**, byte SHA256 **`b1494c3c638d775b28596588b0280703264f259a0f86727a451f116a7643c4b4`**, внутренний `manifestSha256` **`b114006f1acf42642517cd19b337d022f8d7879537ee95ec65e7946daf7a2457`**. Все24 inputs unbound, modelCalls0/executionAuthorizedfalse/resultsnull; это immutable preparation artifact, не result receipt. Никаких provider/host/client вызовов при его создании не было. Root test source SHA256 `993e8177becc4b514865f33dd024c52bf4b0f814af575262ca973f6e0d75374b`; первый PASS log SHA256 `77b8f7b26d20053c5e4e3b930eb8f22f6d79131db6e22aea28fd6dd381838b3f`.
