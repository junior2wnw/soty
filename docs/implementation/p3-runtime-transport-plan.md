# P3-B2/B3 — последовательность допуска и реального открытия

2026-09-30. Подплан реализации после независимой приёмки B1. Статус: **план, не выполненный runtime**. Основание — [контракт публикации](p3-publication-contract.md).

## B2.1 — точный адрес, ticket и session

- Разрешать named route только для конкретного активного bound alias и действующего app. Inactive новый claim сохраняет status-only, tombstone410, неизвестный host404; shell/channel на managed app origin не появляются.
- `apps.launch` выбирает canonical по умолчанию либо точный domainId этого app. Проверяет действующего actor и policy, возвращает30s одноразовый ticket с branded decision. Локальный path проверяется отдельно; origin/returnUrl от клиента не принимаются.
- Обмен ticket повторно читает текущие права после `await` body. Другой alias, expired/replayed ticket, изменённый epoch/target/basis или actor не создают session.
- Account session имеет абсолютный срок1h, без автоматического продления. Cookie host-only `__Host-`, Secure, HttpOnly, Path=/, SameSite=None, Partitioned. Один alias/cookie не действует на другом.
- Boot удаляет ticket из fragment до сетевой работы и проверяет, что cookie действительно принимается браузером. Успешный POST сам по себе не доказывает работающую cookie-сессию.
- Не допускается скрытый fallback из недействительной account cookie в anonymous в том же запросе/потоке. Новый публичный вход — отдельное решение без владельца и credentials.

Граница наблюдаемости: если браузер вообще перестал присылать cookie, следующий новый запрос неотличим от первого анонимного посещения. Запрет fallback относится к предъявленному session и уже открытому stream, а не к обещанию узнавать посетителя после удаления cookie.

Gate: два адреса одного app; чужой app; delayed body с concurrent grants/revoke/retire; повтор ticket; session deadline; запрещённые заголовки и отсутствие утечки metadata.

## B2.2 — непрерывный HTTP/WS допуск

- Сессия и каждый stream используют внутренний branded decision. Проверка — до open, отправки очередного chunk/end, после await ACK/write, до обработки входящих head/data/end/ack. Уже доставленные байты нельзя вернуть.
- Public decision имеет30s lease. Его можно обновить до истечения только через проверку тех же pins и первоначального accessBasis; просроченный допуск не оживает. Account deadline остаётся абсолютным.
- Ошибка права/TTL закрывает конкретный stream. Она не должна terminate общий connector и оборвать соседние допустимые streams. Нарушение протокола/auth самого connector остаётся отдельной причиной прекращения канала.
- Membership events проверяют связанные допуски сразу; периодический аудит проверяет внешние изменения при отсутствии event. Верхняя граница и поведение при задержке event loop измеряются, не объявляются мгновенными.
- Public-basis потоки ограничены24 внутри общего32 на connector: резерв8 для grant-basis. Зарегистрированный посетитель без grant расходует public quota. Освобождение на timeout/error/disconnect должно быть ровно один раз.
- Unsafe HTTP требует точного Origin; GET/HEAD могут не иметь его. WS всегда требует точный Origin. Missing/null/foreign/duplicate значения не являются разрешением. Origin не удостоверяет внешнего AI-клиента.
- Источник stream выбирается из единственного current RuntimeTarget. Старый connector v1 не подтверждает revision ACK, поэтому не обещается неизменный код/attested execution.

Gate: revoke в каждом async boundary, public24 + разрешённый private, TTL одного посетителя при живом соседнем stream, quota без утечек, missing/wrong Origin, прежние HTTP/assets/WS/headers/limits.

## B3 — понятный вход и браузерная приёмка

- Private direct link использует только заданный trusted shell route с app/domain ID и локальным path. Автоматический переход допустим лишь для верхней GET-навигации; asset/fetch/HEAD/unsafe/iframe получают честный статус. Fetch Metadata влияет на представление, а не на права.
- Shell получает свой обычный actor, проверяет app/domain/path на сервере и открывает новый ticket. Произвольный origin из URL не принимается. Недоступная private ссылка не раскрывает название, владельца или участников.
- В iframe shell не пытается открываться внутри самого app: это заблокировано существующим sandbox/frame-ancestors. Ошибка cookie должна оставить понятный статус и внешнее действие «Открыть отдельно», которое получает новый ticket; sandbox не ослабляется.
- Проверяются private owner/granted, public без аккаунта, signed public-basis, unlisted/listed как отдельное обнаружение, недоступный источник, отзыв и восстановление соединения. Настройка публикации и fresh source observations расширяются в C после этого gate.
- CSP оболочки отдельно допускает проверенные named iframe origins; CSP/HSTS на app используют его точный origin, не требуют существующего legacy template. Named-only конфигурация имеет собственный тест.

Независимый critic пересматривает async и пользовательские границы, отдельный acceptance file не копирует author fixture. Root проходит реальные браузерные сценарии, повторяет связанную suite и фиксирует checkpoint. DNS/TLS, фактический Linux Apps image, restore/fallback и публичный пилот остаются внешними условиями P3-E/release.
