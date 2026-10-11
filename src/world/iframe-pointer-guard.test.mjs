import test from 'node:test';
import assert from 'node:assert/strict';
import { holdIframePointers } from './iframe-pointer-guard.ts';

const documentPort = () => {
  const children = [], existing = { type: 'existing stylesheet' }; children.push(existing);
  return { children, existing, head: { append(node) { children.push(node); } },
    createElement(tag) { assert.equal(tag, 'style'); return { dataset: {}, textContent: '', remove() { children.splice(children.indexOf(this), 1); } }; } };
};
test('overlapping shell drags hold one document guard; duplicate or stale releases cannot unshield a newer drag', () => {
  const doc = documentPort(), first = holdIframePointers(doc), second = holdIframePointers(doc);
  assert.equal(doc.children.length, 2); assert.match(doc.children[1].textContent, /iframe\s*\{\s*pointer-events:\s*none\s*!important/);
  first(); first(); assert.equal(doc.children.length, 2); second(); assert.deepEqual(doc.children, [doc.existing]);
  const newer = holdIframePointers(doc); second(); assert.equal(doc.children.length, 2); newer(); assert.deepEqual(doc.children, [doc.existing]);
});
test('iframe pointer guards are isolated by document', () => {
  const a = documentPort(), b = documentPort(), releaseA = holdIframePointers(a), releaseB = holdIframePointers(b);
  releaseA(); assert.deepEqual(a.children, [a.existing]); assert.equal(b.children.length, 2); releaseB(); assert.deepEqual(b.children, [b.existing]);
});
