# Selected Source: навигация к документу, 21.10.2026

Исправление подготовлено в отдельном worktree
`C:/Users/Junio/.codex/worktrees/soty-document-navigation-20261021`, ветка
`codex/soty-document-navigation-20261021`. По уточнению владельца база обновлена
с `d4a288b88c66ed06986d49f3ac8e8264ada9fc47` до
`cbeb42586ff501899652b2010188415ce5b68d16`. Git подтвердил, что serving revision
`69dd1b9747240b65474e3fca593a6d441783521a` входит в новую базу.
Исходный dirty checkout не изменялся. Push, SSH-подключение к серверу,
проверка live credentials/config и операции с Root production не выполнялись.

## Контракт и исправление

Ссылка `/#app/APP?path=encode('/embed#document=pag_x&block=hero&invite=…')`
использует прежние `apps.launch`, launcher и Connect/Source SDK. Новый протокол,
поля полномочий, profiles, pins и изменения схемы хранения не вводятся.

- `launch-path.mjs`: буквальный `/embed` и необязательный fragment до 2048
  UTF-16 code units. Дополнительно действует прежний предел boot URL 8192.
  Query перед `#`, иной/нормализованный/кодированный путь, external target,
  reserved endpoint, raw второй `#`, backslash, raw whitespace, ASCII controls,
  некорректный percent/UTF-8 и surrogate отклоняются. Текст URL внутри fragment
  остаётся данными того же документа; Root не следует ему.
- `index.mjs`: навигация хранится в RAM ticket/session. Renewal берёт её из
  исходной сессии и сохраняет в идемпотентном результате. Клиент не может выбрать
  новый путь через renewal. Signed authority context и Source target не получают
  document/block/invite. Проверки actor, Source и доступа остаются действующими.
- `runtime-pages.mjs`: тот же валидатор проверяет ответ boot; адрес имеет вид
  `/embed?sotyFrame=FRAME#document=…`. Root удаляет свой frame selector до relay,
  поэтому настоящий Source получает ровно `/embed`. Initial/external boot
  выполняет навигацию; hidden renewal сообщает прежний ready ACK и не заменяет
  видимое окно. Invite не становится ticket, frame или правом на приложение.

## Проверка

257 тестов, 257 PASS, 0 FAIL, 0 SKIP: новый валидатор, реальный boot-скрипт в VM,
selected-source promotion, shell lifecycle, launcher и renewal, installed Root
workspaces, новый signed Root/Connect SDK/connector/Source-proof сценарий,
обычный selected Source с настоящим Native consent/OIDC/BFF и renewal.
Новые тесты: `modules/apps/test/launch-path.test.mjs`,
`modules/source-app/test/scoped-fragment-navigation.test.mjs`; дополнены
`modules/apps/test/runtime-pages.test.mjs`.

Лог: `output/document-navigation-20261021/navigation-tests.log`.
`pnpm run typecheck`, `pnpm run build` и `git diff --check` прошли.
Отдельно `node --test deploy/connector/storage-world-v7.test.mjs`: 21 PASS.
Внешний Peremetrika Native fixture здесь не подключался; live browser/TLS и
Docker image не проверялись. Локальный Node v24.13.1 ниже engines >=24.15.0;
для квалифицированного image требуется штатный Linux/Node24 build.

## Интеграция без перезаписи параллельной работы

Брать только commit этой правки. В НОВОМ чистом integration worktree от свежего
`origin/main` выполнить cherry-pick; не переносить HEAD целиком поверх main:

```powershell
git fetch origin
git worktree add -b codex/soty-document-navigation-integration-20261021 C:/Users/Junio/.codex/worktrees/soty-document-navigation-integration-20261021 origin/main
# В новом integration worktree; FIX_COMMIT заменить точным commit этой ветки:
git merge-base --is-ancestor 69dd1b9747240b65474e3fca593a6d441783521a HEAD
git cherry-pick FIX_COMMIT
pnpm install --frozen-lockfile
pnpm run typecheck
pnpm run build
```

Повторить перечисленные navigation tests и штатные release checks. Перед push
снова fetch/проверка remote main; применять обычный fast-forward push без force.
При движении main перечитать изменения и пересобрать точную итоговую revision.

## Безопасная подготовка Root release

Действующий путь: [app-release.md](deployment/app-release.md), раздел выпуска
самих Сот → [guarded maintenance rollout](../deploy/connector/README.md),
реальные `deploy/connector/cli.mjs`, `rollout.mjs`, `storage-guard.mjs` и полный
корневой `Dockerfile`. Исторический overlay и traffic rollout не подходят.
Текст старого README о reader3 не заменяет текущий v7 guard/Dockerfile.

Локально обнаружены SSH alias `dev` и файл identity (только `ssh -G`, без
соединения); Docker CLI отсутствует. Состояние Linux builder не проверялось.
Полученные от владельца container/image/revision служат снимком, а не CAS для
будущего переключения. Предыдущие receipts привязаны к прежнему image и не
квалифицируют этот новый commit автоматически.

**Гейт:** текущая база объявляет Apps readers 1..14 и World [6]; полученный от
владельца serving manifest объявляет Apps 1..8 и World [3]. 21 реальный локальный
storage-тест подтверждает отказ current World6 image на непустом World3/5 до
START. Фактический production storage здесь не читался. Если он World3, текущий
rollout заблокирован: нужен отдельно квалифицированный переход с совместимым
reader/fallback и encrypted backup/isolated restore. Успешная сборка не снимает
этот гейт. Не менять labels, guards, маркеры или использовать recovery для обхода.
После новых Apps9..14/World6 writes прежний image не является доказанным откатом.

Кандидатные команды для Linux release owner, **сейчас не выполнялись**:

```sh
# Чистый context из точного integrated commit, включая полный shared/modules.
docker build --build-arg REVISION="$ROOT_REV" --build-arg LEGACY_UNIVERSAL_MODE=0 -t "soty-root:$ROOT_REV" .

# IDs/digests и приватные пути только из свежего qualified preflight.
node deploy/connector/cli.mjs prepare \
  --original-id "$OLD_ID" --original-image "$OLD_IMAGE" \
  --candidate-image "$CANDIDATE_IMAGE" --storage-probe-image "$PROBE_IMAGE" \
  --revision "$ROOT_REV" --transaction "$TRANSACTION" --journal "$JOURNAL" \
  --universal-plan "$CURRENT_PLAN" --universal-custody-file "$CUSTODY" \
  --universal-witness-file "$NEW_WITNESS" --universal-shell-origins "$SHELL_ORIGINS"

node deploy/connector/cli.mjs promote \
  --original-id "$OLD_ID" --original-image "$OLD_IMAGE" \
  --candidate-image "$CANDIDATE_IMAGE" --storage-probe-image "$PROBE_IMAGE" \
  --revision "$ROOT_REV" --transaction "$TRANSACTION" --journal "$JOURNAL" \
  --universal-plan "$CURRENT_PLAN" --universal-custody-file "$CUSTODY" \
  --universal-witness-file "$NEW_WITNESS" --universal-shell-origins "$SHELL_ORIGINS" \
  --reviewed-receipt "$FRESH_REVIEW" --health-origin http://127.0.0.1:18182
```

Это шаблон аргументов существующего CLI, не готовый live plan. Сохранить текущие
approved features/pins/config и их фактическую preparedness, полный mounts/config
fingerprint, один writer, fresh drain и очередь по действующему контракту.
Нужны image digest/reader validation, disposable Linux cold-start/fault tests,
encrypted backup и isolated restore с проверкой mount isolation/inventory,
совместимый откат. Backup CLI: `node deploy/connect/gpg-release-backup.mjs --plan
PRIVATE_PLAN`, ключ только через существующий приватный stdin; backup требует
уже остановленного writer в согласованной release-паузе. Новые ключи и планы
конфигурации эта задача не создаёт. После разрешённого выпуска проверить снаружи
и в браузере настоящий document/block/invite link, reload, external и renewal.
