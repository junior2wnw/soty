import test from 'node:test';
import assert from 'node:assert/strict';
import { createFieldSearchPlacement } from './unified-field-search.mjs';
const app = index => ({ entity: { kind: 'app', id: `app-${index}` }, title: `App ${index}` });
const community = { entity: { kind: 'community', id: 'group' }, title: 'Known group' };
const positions = document => new Map(document.shortcuts.map(shortcut => [shortcut.entity.id, shortcut]));

test('append across bucket boundaries preserves selected identity, slot and context world position', () => {
 const placement = createFieldSearchPlacement(), firstItems = [...Array.from({length:7},(_,index)=>app(index)),community];
 const first = placement.update('scope', firstItems, firstItems), secondItems = [...firstItems,...Array.from({length:12},(_,index)=>app(index+7))];
 const second = placement.update('scope', secondItems, secondItems), next = positions(second);
 for (const [id,node] of positions(first)) assert.deepEqual(next.get(id),node);
 for (const context of first.contexts) assert.deepEqual(second.contexts.find(value=>value.contextId===context.contextId),context);
});
test('permission removal drops the entire old community island, while rename updates current title without moving it', () => {
 const placement=createFieldSearchPlacement(), first=placement.update('scope',[community],[community]);
 const renamed={...community,title:'Current group'}, second=placement.update('scope',[renamed],[renamed]);
 assert.equal(second.contexts[0].title,renamed.title); assert.equal(second.contexts[0].contextId,first.contexts[0].contextId);
 const removed=placement.update('scope',[app(1)],[community,app(1)]);
 assert.equal(removed.contexts.some(value=>value.contextId===first.contexts[0].contextId),false);
 assert.equal(placement.contextEntity(first.contexts[0].contextId),null);
});
test('query reset detaches placements and returned mutation cannot corrupt remembered positions', () => {
 const placement=createFieldSearchPlacement(), first=placement.update('one',[app(1)],[app(1)]), id=first.shortcuts[0].shortcutId;
 first.shortcuts[0].slot[0]=999; first.contexts[0].x=999;
 const same=placement.update('one',[app(1)],[app(1)]); assert.equal(same.shortcuts[0].shortcutId,id); assert.notEqual(same.shortcuts[0].slot[0],999);
 assert.notEqual(same.contexts[0].x,999); assert.notEqual(placement.update('two',[app(1)],[app(1)]).shortcuts[0].shortcutId,id);
});
test('duplicate and oversized result batches stay bounded and cannot create duplicate shortcuts', () => {
 const placement=createFieldSearchPlacement(), items=Array.from({length:300},(_,index)=>app(index));
 const document=placement.update('scope',[items[0],items[0],...items],items);
 assert.ok(document.shortcuts.length<=256); assert.equal(new Set(document.shortcuts.map(value=>value.entity.id)).size,document.shortcuts.length);
});
test('a temporarily unavailable result returns to its old reserved slot without colliding with a newcomer', () => {
 const placement=createFieldSearchPlacement(), a=app(1), b=app(2), first=placement.update('scope',[a],[a]);
 placement.update('scope',[b],[b]); const restored=placement.update('scope',[a,b],[a,b]);
 assert.deepEqual(positions(restored).get(a.entity.id),positions(first).get(a.entity.id));
 assert.notDeepEqual(positions(restored).get(a.entity.id).slot,positions(restored).get(b.entity.id).slot);
});
