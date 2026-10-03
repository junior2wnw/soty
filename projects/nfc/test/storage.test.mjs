import test from 'node:test';
import assert from 'node:assert/strict';
import { makeRecord } from '../src/records.mjs';
import { restoreLocalDraft, saveLocal, loadTemplates, saveTemplates, recoveryFile, storageKeys } from '../src/storage.mjs';

const template = (name = 'Метка') => ({ schema: 'soty.nfc-template.v1', name, records: [{ ...makeRecord('text'), values: { text: 'Текст' } }] });
function memory(entries = {}, fail = () => false) {
  const data = new Map(Object.entries(entries));
  return { getItem: key => data.get(key) ?? null, setItem: (key, value) => { if (fail(key)) throw new Error('Quota'); data.set(key, value); } };
}

test('incomplete drafts restore every typed field; malformed fields are not silently erased', () => {
  const draft = { ...template(), records: [makeRecord('contact')] };
  draft.records[0].values.phone = '+7 (';
  assert.deepEqual(restoreLocalDraft(draft).records[0].values, draft.records[0].values);
  draft.records[0].values.name = 123;
  assert.throws(() => restoreLocalDraft(draft));
  assert.throws(() => restoreLocalDraft({ ...template(), records: [{ values: { url: 'example.com' } }] }));
});

test('editing after corruption preserves the exact original in a downloadable local recovery', () => {
  const raw = '{partially damaged data', storage = memory({ [storageKeys.draft]: raw });
  assert.equal(recoveryFile(storage).items[storageKeys.draft], raw);
  assert.equal(saveLocal(storage, storageKeys.draft, template()), true);
  assert.equal(storage.getItem(storageKeys.draft + '.recovery'), raw);
  assert.equal(recoveryFile(storage).items[storageKeys.draft + '.recovery'], raw);
  assert.equal(JSON.parse(storage.getItem(storageKeys.draft)).name, 'Метка');
});

test('failed or already occupied recovery storage never permits overwriting the original', () => {
  for (const storage of [memory({ [storageKeys.draft]: '{bad' }, key => key.endsWith('.recovery')),
    memory({ [storageKeys.draft]: '{bad', [storageKeys.draft + '.recovery']: 'older data' })]) {
    assert.equal(saveLocal(storage, storageKeys.draft, template()), false);
    assert.equal(storage.getItem(storageKeys.draft), '{bad');
  }
});

test('saving usable templates preserves rejected entries rather than replacing their only copy', () => {
  const raw = JSON.stringify([template(), { schema: 'future-format', records: ['keep me'] }]);
  const storage = memory({ [storageKeys.templates]: raw });
  const values = loadTemplates(storage);
  assert.equal(saveTemplates(storage, [template('Новая'), ...values]), true);
  assert.equal(storage.getItem(storageKeys.templates + '.recovery'), raw);
  assert.equal(loadTemplates(storage).length, 2);
});

test('the 41st template and oversized collection are refused without losing any earlier template', () => {
  const values = Array.from({ length: 40 }, (_, index) => template(String(index)));
  const raw = JSON.stringify(values), storage = memory({ [storageKeys.templates]: raw });
  assert.equal(saveTemplates(storage, [template('41'), ...values]), false);
  assert.equal(storage.getItem(storageKeys.templates), raw);
  for (const value of values) value.records[0].values.text = 'A'.repeat(14_000);
  assert.equal(saveTemplates(storage, values), false);
  assert.equal(storage.getItem(storageKeys.templates), raw);
});

test('legacy template identities survive another load so removing one preserves its neighbour', () => {
  const storage = memory({ [storageKeys.templates]: JSON.stringify([template('Первая'), template('Вторая')]) });
  const first = loadTemplates(storage)[0];
  assert.equal(saveTemplates(storage, loadTemplates(storage).filter(value => value.key !== first.key)), true);
  assert.deepEqual(loadTemplates(storage).map(value => value.name), ['Вторая']);
});
