# HIVE: фактическое восстановление отдельного приложения

Принято 08.10.2026 (проверка завершена 07.10, 19:00 UTC). Source `4ab98937332362f5198ed82599e563f43568413d`, exact image `sha256:4366f23939abf93bf1d58b5c58739421c5aa4f73a8390a5fc0146100c87457bc`. Cold helpers `929ec9e2d129e9304bd1526b64d5dbda728b76b9`, новый nonce `9ef61d6a7c584506905bd86a50a03b7e`.

Это приёмка настоящего Native runtime, SQL, прав и сохранности в изолированном Linux-стенде. Root/OIDC вход и естественный пользовательский сценарий принимаются отдельно. Использованы пять новых синтетических аккаунтов; реальные пользовательские данные не обрабатывались.

| Проверка | Результат |
| --- | --- |
| Запуск приложения | Настоящий production entrypoint/vinext, Node24.15, UID1000, фиксированный SQLite path; 12 Native migrations |
| Работа с проектом | Device ECDSA registration; приватный проект, исходный документ, геометрия и Native viewer permissions |
| Обратная связь | PNG+Opus, reporter-private ticket, owner support reply, ready_to_check, отдельное подтверждение reporter |
| Агентский доступ | Реальный readonly feedback MCP credential; отсутствие raw media в ограниченной проекции и отказ записи |
| Отзыв устройства | Тот же actor/device сначала имеет session/project доступ, затем Native DELETE именно его устройства закрывает доступ; другие actors сохраняют доступ |
| Истечение | Отдельный реально выданный Native session сначала действует, только его synthetic row ограничена deadline3s; после настоящего ожидания session expired и private access denied |
| Копия | RSA3072/AES-GCM: database, configuration и witness; временная plaintext backup удалена |
| Восстановление | Новый пустой physical volume, private config/witness восстановлены из ciphertext; полный Native SQL/cipher inventory сохранён |
| Совместимость | Независимые literal readers; прежний selected reader отклонён до старта; compatible feature-off читает прежний проект и сохраняет отказы revoked/expired |
| Завершение | 14 точных собственных containers: все stopped/exit0/noOOM; два отдельных physical mountpoints сохранены |

Независимая проверка после полного run подтвердила три аутентифицированных encrypted leaves, равенство закрытых config/witness, отсутствие plaintext backup и полное совпадение Native SQL/cipher проекции. После запуска восстановленной базы SQLite меняет четыре байта заголовка journal mode; тело после первых100 bytes совпадает. Это не объявляется побайтовым равенством всей базы после запуска.

Предыдущие отказы `070` и `8b` сохранены. В первом revoke actor не имел before-revoke доступа; во втором fixture ошибочно ожидала один и тот же HTTP отказ для удалённого и истёкшего session. Настоящий callee различает expired retained cookie (`401/account_session_expired`) и deleted session/anonymous private resource (`404/project_not_found`). Исправлена проверка, правила приложения и TTL не ослаблены.

Архив `HIVECOLD1` — отдельный ограниченный synthetic fixture protocol. Он не объявляется production backup API или универсальным `SOTYBAK1`. Seeded shared identity rows проверены только как storage/negative samples; Root authentication здесь false.

Артефакты:

- [Полный actual run](D:/соты/output/soty-universal-platform-implementation-20261006/hive-cold929-actual-public.json).
- [Независимая SQL/crypto/physical проверка](D:/соты/output/soty-universal-platform-implementation-20261006/hive-cold929-independent-public.json).

Отдельный текущий B02/4ab browser уже подтвердил двухминутную запись, RootPlay→Use→SourcePlay и Voice Send с одной Native receipt. PNG import и единичный Source401/UI paused ещё расследуются; полный browser/restart/privacy/revoke путь остаётся открытым. Production не обновлялся.
