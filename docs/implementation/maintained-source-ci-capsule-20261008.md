# Настоящий Source в тестовой сборке Root

Первый Linux build кандидата3496 остановился на13 scoped-session-promotion тестах: обязательный maintained Source package отсутствовал. Ошибка и журнал сохранены; тестовые требования не выключены и не заменены пропусками.

`test-fixtures/planner-source-ci` содержит86 точных Git blobs Планировщика из `6c614feb489767266ce23676b2bc0df6342f8370`: server/shared/src, package+lock, tsconfig, Vite entry, scripts и инструкции. Содержимое не включает базы, конфигурацию, `.env`, секреты и node_modules. `SOURCE-PINS.json` связывает каждый исходник с SHA256; отдельная build проверка требует точный набор и байты. LF фиксируется через `.gitattributes`.

В отдельном Docker stage `planner-ci` зависимости ставятся по неизменному package-lock через `npm ci --ignore-scripts`, затем собирается настоящий Native frontend. Только build stage получает этот пакет. World и Platform используют явные Source paths для Gateway/Proof/adapter lanes. Финальный образ Сот fixture не копирует, действующее Source приложение и его данные не обновляются. Общий root lockfile и Native код не изменены.

Дополнительный фокус без Native frontend прошёл11/12: единственный отказ —503 на `/embed`, поскольку Source читает собственный `dist` и не имеет override. Прежний отказ сохранён; вместо подмены HTML добавлены точные frontend исходники и их настоящая сборка.

На Windows exact capsule с существующими Source dependencies прошла13/13 обязательных session-promotion тестов без пропусков, включая реальный30-секундный срок кандидата, Source ACK, отзыв устройства/профиля, закрытие и потерянный ответ. Это source proof; отдельно требуется actual Linux build с заново установленными pinned dependencies и общий холодный/браузерный комплект.
