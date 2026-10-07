const text = maxLength => ({ type: 'string', maxLength });
const integer = { type: 'integer', minimum: 1, maximum: 1000000 };
const object = (properties, required = Object.keys(properties)) => ({ type: 'object', properties, required, additionalProperties: false });
/** The existing bounded Core schema subset. Native arbitrary block data is
 * exact JSON text, not executed HTML and not an open author-supplied schema. */
export function peremetrikaReadCatalog({ capabilityId, appId, resourceId, binding, kind }) {
  const selection = kind === 'page' ? object({ kind: { ...text(30), enum: ['page'] }, pageId: text(80) })
    : object({ kind: { ...text(30), enum: ['library-version'] }, itemId: text(80), version: integer, sourcePageId: text(80) });
  const outlineBlock = object({ sheetId: text(80), sheetTitle: text(300), sectionId: text(80), sectionLabel: text(300), id: text(80), type: text(80), title: text(1000) });
  const properties = { resourceId: text(160), selection,
    metadata: object({ pageId: text(80), revision: { type: 'integer', minimum: 1 }, specHash: text(64), title: text(300), status: text(80) }),
    content: object({ outline: object({ schemaVersion: { ...text(10), enum: ['1.0', '2.0'] }, title: text(300),
      blocks: { type: 'array', items: outlineBlock, maxItems: 128 } }), blockJson: text(65536) }, ['outline']),
    ...(kind === 'library-version' ? { selectedVersion: object({ itemId: text(80), kind: { ...text(20), enum: ['template', 'module'] }, version: integer,
      title: text(300), sourcePageId: text(80), sourceRevision: { type: 'integer', minimum: 1 }, specHash: text(64), versionHash: text(64) }) } : {}) };
  return { capabilityId, version: 1, appId, title: kind === 'page' ? 'Прочитать выбранную страницу' : 'Прочитать выбранную версию шаблона',
    description: 'Текущие права проверяет Переметрика. Структура до 128 блоков и точные данные одного выбранного блока как JSON; без записи, HTML-экспорта и публикации.',
    visibility: 'private', executionEnabled: true,
    inputSchema: object({ blockId: text(80) }, []), outputSchema: object(properties),
    resources: [resourceId], effects: [], recipients: [resourceId],
    executionBinding: { kind: 'registered', handler: capabilityId, version: 1, binding } };
}
