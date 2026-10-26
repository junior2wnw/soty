/** Registry entries are displayed without creating or changing field shortcuts. */
export function unplacedOwnApps(apps, accountId, shortcuts) {
  if (!Array.isArray(apps) || !Array.isArray(shortcuts)) throw new Error('invalid_apps_overview');
  const placed = new Set(shortcuts.filter(shortcut => shortcut.entity?.kind === 'app').map(shortcut => shortcut.entity.id));
  const seen = new Set();
  return apps.flatMap((app, index) => {
    if (!app || app.ownerAccountId !== accountId || typeof app.id !== 'string' || !/^app-[a-f0-9]{32}$/u.test(app.id)
      || typeof app.name !== 'string' || !app.name.trim() || app.name.length > 64 || /[\u0000-\u001f\u007f]/u.test(app.name)
      || !['ready', 'starting', 'offline', 'stopped'].includes(app.state) || placed.has(app.id) || seen.has(app.id)) return [];
    seen.add(app.id);
    return [{ id: app.id, name: app.name, state: app.state, createdAt: Number.isSafeInteger(app.createdAt) ? app.createdAt : 0, index }];
  }).sort((left, right) => right.createdAt - left.createdAt || right.index - left.index)
    .map(({ index, ...app }) => app);
}
