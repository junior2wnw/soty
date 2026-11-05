# Документ полотна: v1 и v2

Полотно хранит только стабильные shortcut IDs, source refs, contexts и геометрию. Оно не хранит копии каталога, launch URL, UI-код, пароль, ACL, обложку или содержимое provider. Несколько размещений одного ref имеют разные shortcutId и независимую геометрию; ref не выдаёт право доступа.

Прежние FIELD_SCHEMA, FieldDocument/FieldShortcut, createFieldDocument и validateFieldDocument остаются v1-only. Они сохраняют старые bytes/hash и failclosed на v2. Новые AnyFieldDocument/AnyFieldShortcut обозначают настоящую union; validateAnyFieldDocument читает обе версии.

V2 shortcut:

```js
{
  shortcutId: 'stable-instance',
  entity: {kind:'app', id:'registered-source-id'},
  contextId: 'personal',
  placement: {kind:'free', x:120, y:80, width:640, height:360}
  // alternative: placement:{kind:'hex',slot:[q,r]}
}
```

x/y — центр в world pixels относительно context.x/y. Width/height16..8192 — границы хранения; provider задаёт более строгий удобный диапазон. Все числа конечны; весь free bounding rectangle остаётся внутри ±1,000,000 по каждой оси. Отрицательный ноль нормализуется. Free overlap допустим; одинаковые hex slots в одном context запрещены. Два источника геометрии в одном shortcut не принимаются. Форма/маска описывается отдельно, не меняет размещение и не является произвольным CSS/HTML.

Существующие ограничения сохраняются:24 contexts,256 shortcuts,128KiB UTF8. Неизвестные поля и metadata в refs отклоняются. fieldShortcutPlacement возвращает detached union для уже проверенного shortcut; renderer сам переводит hex в пиксели по своим метрикам.

convertFieldDocumentToV2 — явное преобразование после действия человека. Оно сохраняет ID, refs, context и hex slots. createFieldDocumentV2 создаёт пустую v2. Обычное чтение, boot и повтор старого receipt ничего не конвертируют.

## Сохранение и совместимость

World использует прежние таблицы/immutable receipts/CAS/account fence. Первый PUT с явным document.schema=soty.field.v2 меняет world_meta.field_schema_version1→2 в одной транзакции с документом и receipt. Ошибка любого write откатывает и epoch, и геометрию. Данные других аккаунтов не преобразуются.

Старый idempotent PUT v1 возвращает прежний receipt и current v2. Новый PUT v1 поверх v2 отклоняется field_document_downgrade_forbidden. Старый reader1 после epoch2 отказывает field_schema_unsupported без изменений данных. Автоматического обратного преобразования free geometry нет. Rollback требует pre-v2 snapshot и явного решения о последующих записях.

Frozen support/contract-v1.mjs и field-store-v1.mjs происходят из HEAD450204c317e56d1c092046ca9864a05fc985149b. Во втором изменены только два import path для изолированного чтения старого контракта/валидации.

Production storage-probe/readers/release guard должны явно объявить поддержку field epoch2 до выпуска. Изменения этого пакета не являются production migration или готовым resize UI.

## Проверка

Node24.22.1: старые contract tests без изменений, новые contract-v2.test, test/storage-v2.test, test/http-v2.test и World field-directory/field-migration. Реальный HTTP проверяет подпись Connect, два аккаунта, v1→free resize→Root restart+новый клиент, старый receipt, запрет downgrade. SQL fault при записи receipt — отрицательный контроль: до удаления fault нет ни epoch2, ни новой геометрии; идентичный повтор проходит после удаления fault.
