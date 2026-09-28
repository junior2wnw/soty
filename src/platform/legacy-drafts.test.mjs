import test from 'node:test';
import assert from 'node:assert/strict';
import { createLegacyDraftStore } from './legacy-drafts.mjs';
function storage() { const items = new Map(); return { getItem: key => items.get(key), setItem: (key, value) => items.set(key, value), removeItem: key => items.delete(key) }; }

test('legacy drafts survive a full tools navigation and remain isolated by device', () => {
  const disk = storage(); let device = 'device-a';
  const drafts = createLegacyDraftStore(disk, () => device);
  drafts.set('room', 'Не отправленная мысль\nВторая строка'); drafts.set('other-room', 'Другой разговор');
  const reopened = createLegacyDraftStore(disk, () => device);
  assert.equal(reopened.get('room'), 'Не отправленная мысль\nВторая строка');
  device = 'device-b'; assert.equal(reopened.get('room'), undefined); reopened.set('room', 'Личная запись Б');
  device = 'device-a'; assert.equal(reopened.get('room'), 'Не отправленная мысль\nВторая строка');
  assert.equal(reopened.get('other-room'), 'Другой разговор');
});
test('legacy storage denial keeps live text and a guard until it is durable', () => {
  let deny = true; const disk = storage();
  const drafts = createLegacyDraftStore({ ...disk, setItem: (key, value) => { if (deny) throw new Error('quota'); disk.setItem(key, value); } }, () => 'device');
  drafts.set('room', 'Сохранить точно'); assert.equal(drafts.get('room'), 'Сохранить точно'); assert.equal(drafts.hasUnsavedChanges(), true);
  assert.equal(drafts.flush(), false); deny = false; assert.equal(drafts.flush(), true);
  assert.equal(createLegacyDraftStore(disk, () => 'device').get('room'), 'Сохранить точно');
});
test('a failed legacy deletion cannot resurrect sent text in the current session', () => {
  let deny = false; const disk = storage();
  const drafts = createLegacyDraftStore({ ...disk, removeItem: key => { if (deny) throw new Error('storage'); disk.removeItem(key); } }, () => 'device');
  drafts.set('room', 'Отправлено'); deny = true; drafts.delete('room');
  assert.equal(drafts.get('room'), undefined); assert.equal(drafts.hasUnsavedChanges(), true);
  deny = false; assert.equal(drafts.flush(), true); assert.equal(createLegacyDraftStore(disk, () => 'device').get('room'), undefined);
});
