# Навигация документов на serving69dd / World3

Ветка `codex/soty-document-navigation-world3-20261021`, worktree
`C:/Users/Junio/.codex/worktrees/soty-document-navigation-world3-20261021`.
Точная база: `69dd1b9747240b65474e3fca593a6d441783521a`.
Ни `cbeb4258`, ни World6/Apps14 сюда не перенесены.

Из `111d876e` перенесены только три production-файла
`modules/apps/server/{launch-path,index,runtime-pages}.mjs` и три тестовых файла.
`apps.launch` допускает буквальный `/embed` и fragment до 2048 UTF-16 code units,
с прежним пределом boot URL 8192. Query перед fragment, другой/encoded/reserved
путь, внешний target, raw `#` внутри fragment, backslash, controls, неверные
percent/UTF-8 и surrogate отклоняются. Навигация хранится в RAM ticket/session;
renewal берёт её из исходной сессии, без нового клиентского селектора.

Boot URL содержит только `path=/embed`; document/block/invite остаются в RAM
ticket/session и в защищённом ответе после обмена ticket. Они не попадают в
bootstrap query и URL журналов. Собственный hash boot остаётся одноразовым
ticket. После проверки cookie boot открывает
`/embed?sotyFrame=FRAME#document=…&block=…&invite=…`. Root удаляет свой selector
перед relay: настоящий Source получает только `/embed`. Hidden renewal сохраняет
путь в своём результате, сообщает прежний ready ACK и не перемещает видимое окно.
Invite не становится Root ticket/frame/правом доступа. Source SDK, profiles,
pins, authority DTO, schema/migrations и конфигурация не менялись.

Для дополнительного именного адреса Source сохраняет точный отказ Origin.
Страница отказа ведёт через доверенный parentOrigin в карточку текущего
app (`#app/…`) и свежий `apps.entry.get({appId,current:true})`, вместо повторного
запуска заведомо неподходящего domain ID. Exact-domain launch остаётся закрытым;
для обычных приложений прежний recovery с конкретным domain ID сохранён.

Проверено на этой базе: **167/167 PASS, 0 SKIP**, включая реальный Root/Connect
SDK/connector/Source verifier и обычный Source с Native consent/OIDC/BFF;
boot script в VM, launcher, external, renewal и отрицательные проверки.
`pnpm run typecheck` и `pnpm run build` прошли на существующем Node24.13.1 с
честным engines warning >=24.15.0. Новый Node не устанавливался.
Лог: `output/document-navigation-world3-20261021/navigation-tests.log`.

## Полная сборка и действительный reader manifest

Git-Dockerfile **69dd** объявляет v6, Apps1..8, без World. Это не точный manifest
предоставленного serving image `sha256:3a1fc98bcab5ebb9ddb36eac998a294e7f70c78a1bfb8c3dc39e627ffb27c4a2`.
Локальные release-артефакты объясняют разницу: OLD3 bridge меняет только label
старого image, сохраняя filesystem и приложение; его v7 имеет World **[3]**.
Основание: `Dockerfile.bridge` / `ROOT-EXPERIMENT.md` в
`C:/Users/Junio/.codex/tmp/soty-old69dd-rollback-bridge-source-plan-20261021-71697df92c0c`,
и `D:/глава/РАБОТА/2026-10-21-soty-unified-experience/CHECKPOINT-SERVICE-RESTORED-20261021.md`.
Это прочитанные локальные доказательства; новый live readback не выполнялся.

Для воспроизводимого полного build подготовлен
`deploy/connector/Dockerfile.world3`. Он строит **весь Source**, а не overlay
над production image. Базовые Node/Alpine/Xray pins, pnpm lock и все CI-гейты
исходного Dockerfile69dd сохранены. Отличия: ранний отказ при Source World !=3,
закреплённый Caddy2.11.2 в build-stage для обязательного выполнения двух Caddy gates,
копирование используемого feedback asset в финальный runtime, import smoke
финального startup после prune и truthful label:

```json
{"version":7,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6,7,8],"notes":[1,2],"capabilities":[1,2,3],"appRegistration":[1],"feedback":[1],"humanIdentity":[1,2,3],"connect":[3,4],"world":[3]}}
```

`world3-candidate.test.mjs`: **2/2 PASS**. Реальные migrators создали World3 с
field1, populated profile и Apps8; cold reopen сохранил версию, layout и данные.
Отдельно собран точный финальный COPY-набор рецепта и проверены настоящие startup
imports/feedback asset. Это локальное доказательство Source/package closure,
**не** Linux Docker/image/restore qualification. Host guard v6 из Git69dd не
принимает v7; в выпуске использовать уже квалифицированный v7 operator/probe,
а не старый `deploy/connector/cli.mjs` из этого checkout.

Расширенные package/storage/app-release проверки: **56 PASS, 0 FAIL, 2 SKIP**.
Два SKIP — реальные Caddy acceptance tests: локальный Caddy не установлен.
В полном Linux recipe pinned Caddy и `SOTY_CADDY_BIN` устраняют этот skip;
фактическое выполнение остаётся image gate, а не заявленным здесь PASS.

Существующий qualified v7 guard/probe из public Source packet
`soty-rollback-bridge-controller-v6-20261021-092363b4ab89` отдельно проверен на
genuine SQLite, созданных actual migrators: World3/field1/Apps8 принят, genuine
World6 отвергнут с `storage_reader_incompatible`. Guard/probe не менялись.
Фиксированные SHA и результат:
`output/document-navigation-world3-20261021/qualified-v7-source-check.json`.
Это проверка Source-композиции, без Docker/production START.

Полный рецепт выполняет: planner fixture npm ci/build → maintained-source check
и production build → typecheck/app-release/connect/platform/world/dev suites →
identity/inference selftests → connector/connect/private-attach tests → durable
protocol/persistence selftests → production prune → final runtime import smoke.
Не отключать гейты и не подменять результат готовым dist.

## План для main/release owner — ещё не выполнен

1. Сохранить source release именно на этой ветке от 69dd. Для development main
   можно отдельно cherry-pick только navigation commit после fetch/review;
   не сбрасывать main на 69dd и не переносить World3 recipe на World6 Source.
   Рабочие checkout и чужие dirty-файлы не менять. Push сейчас не выполнялся.
2. Из точного committed release Source сделать `git archive` и новый build
   context. Проверить существующие серверные Node24/Docker/BuildKit tools и
   реальную minor version; recipe требует Node24 >=24.15. Ничего не устанавливать
   и не менять runtime/config ради подготовки.
3. На разрешённом Linux builder, без production mounts, собрать:

   ```sh
   docker build --build-arg REVISION="$ROOT_REV" --build-arg LEGACY_UNIVERSAL_MODE=0 \
     --iidfile "$NEW_BUILD_DIR/candidate.iid" -f deploy/connector/Dockerfile.world3 \
     -t "soty-document-navigation-world3:$ROOT_REV" .
   ```

   Проверить exact image revision/digest и reader v7/World3/Apps1..8, final
   runtime/installed resources, текущие approved compiled mode/features.
   Если любой штатный gate падает, сохранить ошибку; image не квалифицирован.
4. Существующим qualified v7 guard/probe/controller на НОВЫХ disposable volumes
   проверить genuine World3/field1/Apps8: START/health, cold reopen и логические
   данные. На genuine World6 проверить `storage_reader_incompatible` **до START**.
   Старые receipts квалифицируют инструменты, но не новый image.
5. Проверить новый candidate на отдельно authenticated restored clone через
   существующие encrypted backup/custody/isolated restore инструменты. Сравнить
   logical SQLite rows/IDs/ACL/targets/metadata до и после cold start; byte equality
   DB/WAL после нормального открытия не подменяет logical preservation.
6. Лишь в разрешённом новом release-транзакте перечитать actual serving ID/image,
   features/pins/mounts/config fingerprint и writer/queue. Старый container ID из
   снимка не является будущим CAS. Переиспользовать qualified createRuntime,
   cold-jobs, v7 storage guard, offline executor и custody bodies. Нужны новый
   coherent backup checkpoint/receipts и совместимый World3 rollback; не resume
   failed migration/recovery session. World/Apps migration здесь не требуется.
7. После отдельно разрешённого cutover проверить Root document/block/invite link,
   Source reload, external, renewal и отказ неавторизованного пользователя в
   настоящем браузере. Root live, config, keys и данные в этой задаче не менялись.

Локально Docker CLI отсутствует; Linux builder/runtime version, full Docker CI,
fresh image/cold restore и live browser остаются явными гейтами владельца выпуска.
Навигация готова в исходниках; выпуск ещё не выполнен.
