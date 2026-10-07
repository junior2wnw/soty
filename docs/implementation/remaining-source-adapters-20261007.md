# Следующие Source adapters

Код оригинальных проектов сохранён. Общий U1/feedback/agent contract позволяет подключать интерфейс отдельно от Source SSO и write capabilities; карточка работающего приложения сама не разрешает его приватные API. Текущие факты ниже получены чтением исходников и состояния Git, не входом в пользовательские очереди.

## Переметрика

Свежий original `D:/peremetrika` — clean `aa83a392017ff26bbdbf0a9bb63fe0fc58af526b`. В этом проходе не менялся. Существует другой dirty native-entry worktree; он не принадлежит текущей команде и сохранён без изменений. Его разрешение iframe/других адресов не считается общим входом Сот.

Действующий код имеет независимые owner, participant и delegated-agent границы. `server/page-auth.mjs` поддерживает optional owner password, ограниченные Source-owned participant sessions и read-only viewer. `server/agent-access.mjs` выдаёт текущий Source grant по явно выбранным страницам, квоте создания, сроку и разрешению публикации. Этот agent token может разрешать изменения: `readOnlyHint` у MCP-инструмента не превращает credential в право только чтения. `server/mcp-http.mjs` не выдаёт browser cookie за MCP credential и повторяет Source checks на API. OAuth здесь не объявлен реализованным.

Первый adapter принимает точную live schema и минимальный разрешённый read profile, читает конкретный документ или bounded workspace summary через текущий Native grant. Следующий уровень соединяет maintained Source RP с прежним Native participant/owner только по двум независимым подтверждениям, не по имени/email. Write pipeline сохраняет document revision, reviewed change set, responsible reviewer и Native receipt. Draft/version/preview/publish/sign остаются разными состояниями; создание документа не публикует его. Root-owner не становится владельцем всех документов.

Перед фактическим обновлением применяется собственный current deployment-v2: exact Source archive, совместимый читатель, шифрованная копия всего shared, проверка неизменности конфигурации/данных, внешняя и браузерная проверка. Native project data не включаются в release archive. Этот документ не разрешает публикацию документов или изменение продакшена.

## TRANSIT

Original `D:/roy` — `e6c08393`; существующие untracked файлы сохранены. `AGENTS.md` и `docs/READINESS.md` прочитаны. Source отделяет Portal identity, Access Pass, recovery, Node capabilities и внешнее подтверждение TON wallet. Общий вход Сот может связывать человека с уже подтверждённым Native account; он не заменяет fresh userVerification для чувствительного действия и не получает payout seed или подпись кошелька.

Первый adapter — отдельная карточка, разрешённый обзор состояния и локальный dev workspace через проверенный connector. Он не открывает public admin, не устанавливает узел, не создаёт сетевую экспозицию/расходы и не включает реальные продажи/выплаты. Runtime/node readiness и business/payment readiness проверяются отдельно. Последующие локальные действия используют signed pinned package, plan/явное разрешение нужного эффекта, self-test/drain/recovery и сохранённые receipts. Soty auth, Account session и Access Pass нельзя взаимозаменять.

## Остальные приложения

Тавыш сохраняет device/local-first identity, медиа и воспроизведение; новый профиль связывается явно. Квартал, Scope, Agent Platform и другие Source начинают с точного current inventory, владельца/аудитории, ресурса, минимальных API/grants и своего fallback. Название приложения или наличие MCP не выдаёт права. Один approved adapter template и SDK повторно используются разными авторами; scopes, Native grant, feedback inbox и receipts остаются отдельными.

Достаточный результат adapter — actual signed Source login/current permission → конкретный Native resource → разрешённое read/write → видимый результат → отдельная очередь обращения → restart/revoke/unknown-ACK/fallback. Source snapshots, синтетические fixtures и готовые descriptor-файлы не заменяют этот сценарий.
