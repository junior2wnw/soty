import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import { normalizeFieldCamera, zoomFieldCamera, fieldScreenToWorld } from './unified-field-camera.mjs';
import { fieldPointToSlot } from './unified-field-layout.mjs';
import {fieldResizeDimensions} from './unified-field-presentation.mjs';

// Execute production handlers and their lifecycle helpers. The ports replace
// DOM/render/persistence, not the pointer state machine or camera calculations.
// A read-only source override lets release review prove regressions on main.
const source = readFileSync(process.env.SOTY_REVIEW_FIELD_SOURCE || new URL('./unified-field.ts', import.meta.url), 'utf8');
const parsed = ts.createSourceFile('unified-field.ts', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
const printer = ts.createPrinter(), print = node => printer.printNode(ts.EmitHint.Unspecified, node, parsed);
const engine = parsed.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'createUnifiedField');
const functions = names => engine.body.statements.filter(node => ts.isFunctionDeclaration(node) && names.includes(node.name?.text)).map(print).join('\n');
const listener = (owner, name, optional=false) => {
  const statement = engine.body.statements.find(node => ts.isExpressionStatement(node) && ts.isCallExpression(node.expression)
    && node.expression.expression.getText(parsed) === `${owner}.addEventListener` && node.expression.arguments[0]?.text === name);
  if(!statement&&optional)return '()=>{}';
  assert.ok(statement, `${owner} ${name} production listener`); return print(statement.expression.arguments[1]);
};
const compiled = ts.transpileModule(`function fixture() {
  const state = { camera: { x: 0, y: 0, scale: 1 }, changes: 0, commits: 0, focused: '', fit: 0, rejectCapture: false, starts:0, inspections:[], previews:[] };
  let gesture = null, pinch = null, pendingPointer = null, inPointerFrame = false, moving = null, contextPreview = null,
    suppressClickUntil = 0, destroyed = false, arranging = false, spacePanning=false,lastInspection=null;
  const pointers = new Map(), captures = new Set(), timers = new Map(); let timer = 0;
  const viewport = { clientWidth: 390, clientHeight: 620,dataset:{},
    getBoundingClientRect: () => ({ left: 0, top: 0 }),
    hasPointerCapture: id => captures.has(id),
    setPointerCapture(id) { if (state.rejectCapture) throw new Error('inactive pointer'); captures.add(id); },
    releasePointerCapture: id => captures.delete(id), contains: node => node === viewport };
  const camera = () => state.camera, viewportSize = () => ({ width: 390, height: 620 });
  const setCamera = next => { state.camera = normalizeFieldCamera(next); state.changes++; };
  const render = () => {}, emit = () => {}, schedule = () => {}, announce = () => {};
  const mode = 'mine', options = { commit: ()=>{},onInspect:()=>{} }, selectedId = 'first', nodeElements = new Map(),metrics={};
  const fieldDocument={contexts:[{contextId:'personal',x:0,y:0}],shortcuts:[{shortcutId:'first'}]};
  const setTimeout = callback => { timers.set(++timer, callback); return timer; }, clearTimeout = id => timers.delete(id);
  const perform = () => {}, commitMove = () => { state.commits++; return Promise.resolve(); }, execute = commitMove;
  const beginMove = id => {state.starts++;moving={shortcutId:id,preview:{valid:true,target:state.object?.placement?.kind==='free'?{contextId:'personal',placement:{...state.object.placement}}:{contextId:'personal',slot:[0,0]}}};};
  const previewMove = target => {state.previews.push(target);moving.preview.target=target;};
  const available = id => state.object??({shortcutId:id,contextId:'personal',x:-100,y:-120,width:200,height:240});
  const activeNode=available,inspectShortcut=id=>{state.inspections.push(id);return true;};
  const layout = { nodes: [{ shortcutId: 'next' }],contexts:[{contextId:'personal',x:0,y:0,body:{left:-1000,right:1000,top:-1000,bottom:1000}}] }, nearestFieldNode = () => layout.nodes[0];
  const focusNode = id => { state.focused = id; }, fitOverview = () => { state.fit++; }, setArrange = value => { arranging = value; };
  const undo = () => { state.commits++; return Promise.resolve(); };
  ${functions(['localPoint', 'beginResize','flushPointerSample', 'finishGesture', 'cancel', 'resetPointers', 'capturePointer'])}
  const handlers = {
    down: ${listener('viewport', 'pointerdown')}, move: ${listener('viewport', 'pointermove')},
    up: ${listener('viewport', 'pointerup')}, cancel: ${listener('viewport', 'pointercancel')},
    lost: ${listener('viewport', 'lostpointercapture')}, outsideUp: ${listener('window', 'pointerup')},
    blur: ${listener('window', 'blur')}, key: ${listener('plane', 'keydown')},windowKey:${listener('window','keydown',true)},keyup:${listener('window','keyup',true)},click: ${listener('viewport', 'click')}
  };
  return { state, viewport, handlers, frame: flushPointerSample,
    snapshot: () => ({ pointers: [...pointers.keys()], captures: [...captures], pinch: !!pinch, gesture: gesture?.type ?? null, pending: !!pendingPointer,dragPoint:gesture?.dragPoint }),
    setArrange: value => { arranging = value; },readOnly:()=>{options.commit=null;},object:value=>{state.object=value;},resize:typeof beginResize==='function'?beginResize:null,
    target(id='first'){return{dataset:{shortcutId:id},closest(selector){return selector==='[data-shortcut-id]'?this:null;}};},
    event(type, id = 1, overrides = {}) {
      const event = { pointerId: id, pointerType: 'touch', button: 0, buttons: 1, isPrimary: id === 1,key:'',
        clientX: id * 50, clientY: 100, target: { closest: () => null },
        defaultPrevented: false, preventDefault() { this.defaultPrevented = true; }, stopPropagation() {}, ...overrides };
      handlers[type](event); return event;
    } };
}
exports.fixture = fixture;`, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const fixture = () => {
  const context = vm.createContext({ exports: {}, normalizeFieldCamera, zoomFieldCamera, fieldScreenToWorld,fieldResizeDimensions,fieldPointToSlot:point=>fieldPointToSlot(point),performance });
  new vm.Script(compiled).runInContext(context); return context.exports.fixture();
};
const pinch = f => { f.event('down', 1); f.event('down', 2); assert.equal(f.snapshot().pinch, true); };
const clean = f => { const state = f.snapshot(); assert.equal(state.pointers.length, 0); assert.equal(state.captures.length, 0); assert.equal(state.pinch, false); assert.equal(state.gesture, null); assert.equal(state.pending, false); };

test('ordinary touch pan and two-finger anchored zoom still change only the camera', () => {
  const f = fixture(); f.event('down'); f.event('move', 1, { clientX: 100 }); f.frame();
  assert.equal(f.state.camera.x, -50); assert.equal(f.state.camera.scale, 1); f.event('up'); clean(f);
  const p = fixture(); pinch(p); p.event('move', 2, { clientX: 150 }); p.frame();
  assert.equal(p.state.camera.scale, 2); assert.equal(p.state.commits, 0);
});

test('desktop drag moves a personal object directly without Arrange, preserves the camera and suppresses launch',()=>{
  const f=fixture(),target=f.target();
  f.event('down',1,{pointerType:'mouse',target});f.event('move',1,{pointerType:'mouse',target,clientX:90,clientY:125});f.frame();
  assert.equal(f.snapshot().gesture,'move');assert.equal(f.state.starts,1);assert.equal(f.state.previews.length,1);
  assert.equal(f.snapshot().dragPoint.x,-60);assert.equal(f.snapshot().dragPoint.y,-95,'the cover follows the full pointer delta before snapping at release');
  assert.equal(f.state.camera.x,0);assert.equal(f.state.camera.y,0);assert.equal(f.state.changes,0);
  f.event('up',1,{pointerType:'mouse',clientX:90,clientY:125});assert.equal(f.state.commits,1);
  assert.equal(f.event('click',1,{detail:1}).defaultPrevented,true);
});

test('a free widget follows exact world pixels without hex snapping and keeps its canonical size',()=>{
  const f=fixture(),target=f.target();f.object({shortcutId:'first',contextId:'personal',x:-100,y:-120,width:200,height:240,placement:{kind:'free',x:0,y:0,width:200,height:240}});
  f.event('down',1,{pointerType:'mouse',target});f.event('move',1,{pointerType:'mouse',target,clientX:91,clientY:126});f.frame();
  const placement=f.state.previews.at(-1).placement;assert.equal(placement.kind,'free');assert.equal(placement.x,41);assert.equal(placement.y,26);assert.equal(placement.width,200);assert.equal(placement.height,240);
  f.event('up',1,{pointerType:'mouse',clientX:91,clientY:126});assert.equal(f.state.commits,1);assert.equal(f.state.camera.x,0);
});
test('pointer resize anchors the original top-left and commits bounded free dimensions once; cancel never writes',()=>{
  const f=fixture(),event={button:0,pointerId:1,pointerType:'mouse',clientX:295,clientY:430,preventDefault(){},stopPropagation(){}};
  f.resize(event,'first');f.event('move',1,{pointerType:'mouse',clientX:335,clientY:460});f.frame();
  const placement=f.state.previews.at(-1).placement;assert.equal(placement.kind,'free');assert.equal(placement.width,240);assert.equal(placement.height,270);assert.equal(placement.x-placement.width/2,-100);assert.equal(placement.y-placement.height/2,-120);
  f.event('up',1,{pointerType:'mouse',clientX:335,clientY:460});assert.equal(f.state.commits,1);clean(f);
  const cancelled=fixture();cancelled.resize(event,'first');cancelled.event('move',1,{pointerType:'mouse',clientX:330,clientY:460});cancelled.frame();cancelled.event('cancel');assert.equal(cancelled.state.commits,0);clean(cancelled);
});
test('keyboard free movement uses pixels, with Shift precision, while native controls retain their own keys',()=>{
  const f=fixture();f.object({shortcutId:'first',contextId:'personal',x:-100,y:-120,width:200,height:240,placement:{kind:'free',x:0,y:0,width:200,height:240}});
  f.event('key',1,{key:'m'});f.event('key',1,{key:'ArrowRight'});assert.equal(f.state.previews.at(-1).placement.x,10);
  f.event('key',1,{key:'ArrowDown',shiftKey:true});assert.equal(f.state.previews.at(-1).placement.y,1);
  const count=f.state.previews.length;f.event('key',1,{key:'ArrowRight',target:{closest:selector=>selector.includes('[data-field-control]')?{}:null}});assert.equal(f.state.previews.length,count);
  f.event('key',1,{key:'Enter'});assert.equal(f.state.commits,1);
});

test('the same object swipe pans on touch; a live read-only object offers inspect instead of mutation',()=>{
  const touch=fixture(),target=touch.target();touch.event('down',1,{target});touch.event('move',1,{target,clientX:100});touch.frame();
  assert.equal(touch.snapshot().gesture,'pan');assert.equal(touch.state.starts,0);assert.equal(touch.state.camera.x,-50);
  const live=fixture(),other=live.target('live-app');live.event('down',1,{pointerType:'mouse',target:other});live.event('move',1,{pointerType:'mouse',target:other,clientX:100});live.frame();
  assert.equal(live.snapshot().gesture,'inspect');assert.deepEqual([...live.state.inspections],['live-app']);assert.equal(live.state.commits,0);
});

test('Space and middle button pan over a personal object, then release restores direct manipulation',()=>{
  for(const input of ['space','middle']){
    const f=fixture(),target=f.target();if(input==='space')f.event('key',1,{key:' '});
    const button=input==='middle'?1:0;
    const down=f.event('down',1,{pointerType:'mouse',target,button});
    if(input==='middle')assert.equal(down.defaultPrevented,true);
    f.event('move',1,{pointerType:'mouse',target,button,clientX:100});f.frame();
    assert.equal(f.snapshot().gesture,'pan');assert.equal(f.state.starts,0);
    f.event('up',1,{pointerType:'mouse',button});f.event('keyup',1,{code:'Space',key:' '});
    f.event('down',2,{pointerType:'mouse',target});f.event('move',2,{pointerType:'mouse',target,clientX:150});f.frame();assert.equal(f.snapshot().gesture,'move');
  }
});

test('read-only fields always pan and can never start a placement write',()=>{
  const f=fixture(),target=f.target();f.readOnly();f.event('down',1,{pointerType:'mouse',target});f.event('move',1,{pointerType:'mouse',target,clientX:100});f.frame();f.event('up',1,{pointerType:'mouse',clientX:100});
  assert.equal(f.state.starts,0);assert.equal(f.state.commits,0);assert.equal(f.state.camera.x,-50);
});

test('Space panning works from the canvas region itself, beyond a focused node',()=>{
  const f=fixture(),target=f.target();
  const key=f.event('windowKey',1,{code:'Space',key:' ',target:f.viewport});assert.equal(key.defaultPrevented,true);
  f.event('down',1,{pointerType:'mouse',target});f.event('move',1,{pointerType:'mouse',target,clientX:100});f.frame();
  assert.equal(f.snapshot().gesture,'pan');assert.equal(f.state.starts,0);
});

for (const end of ['cancel', 'lost', 'outsideUp', 'blur']) test(`a ${end} during pinch clears the whole owned sequence and permits a fresh single-finger pan`, () => {
  const f = fixture(); pinch(f); f.event('move', 2, { clientX: 120 });
  f.event(end, 2); clean(f); const before = f.state.changes;
  f.frame(); assert.equal(f.state.changes, before, 'a pending sample cannot run after cancellation');
  f.event('down', 3); assert.equal(f.snapshot().pinch, false); f.event('move', 3, { clientX: 190 }); f.frame();
  assert.equal(f.state.changes, before + 1); assert.equal(f.state.commits, 0); f.event('up', 3); clean(f);
});

test('releasing one finger ends pinch without recycling its remaining finger as a new gesture', () => {
  const f = fixture(); pinch(f); f.event('up', 2); clean(f);
  const before = f.state.changes; f.event('move', 1, { clientX: 190 }); f.frame();
  assert.equal(f.state.changes, before); f.event('up', 1); clean(f);
});

test('a pinch trailing click is suppressed while a new deliberate tap works immediately', () => {
  const f = fixture(); pinch(f); f.event('up', 2);
  assert.equal(f.event('click', 2, { detail: 1 }).defaultPrevented, true);
  f.event('down', 3); f.event('up', 3);
  assert.equal(f.event('click', 3, { detail: 1 }).defaultPrevented, false);
});

for (const end of ['cancel', 'lost']) test(`an untracked ${end} cannot cancel the current press or pan`, () => {
  const f = fixture(); f.event('down', 1); f.event('move', 1, { clientX: 100 }); f.frame();
  f.event(end, 90); assert.equal(f.snapshot().gesture, 'pan');
  const before = f.state.changes; f.event('move', 1, { clientX: 130 }); f.frame(); assert.equal(f.state.changes, before + 1);
  f.event('up', 1); clean(f);
});

test('missing mouse release cancels on a zero-buttons move, while touch does not use mouse button assumptions', () => {
  const f = fixture(); f.event('down', 1, { pointerType: 'mouse' });
  f.event('move', 1, { pointerType: 'mouse', buttons: 0, clientX: 100 }); clean(f); assert.equal(f.state.changes, 0);
  const p = fixture(); p.event('down', 1); p.event('move', 1, { buttons: 0, clientX: 100 }); p.frame();
  assert.equal(p.snapshot().gesture, 'pan'); assert.equal(p.state.changes, 1);
});

test('rejected capture leaves no gesture, pinch, pending sample or partially captured pointer', () => {
  for (const duringPinch of [false, true]) {
    const f = fixture(); f.event('down', 1); f.state.rejectCapture = true;
    assert.doesNotThrow(() => duringPinch ? f.event('down', 2) : f.event('move', 1, { clientX: 100 }));
    clean(f); assert.equal(f.state.commits, 0);
  }
});

test('IME, handled keys and browser modifier navigation leave selection and placement untouched', () => {
  for (const overrides of [{ key: 'ArrowRight', isComposing: true }, { key: 'ArrowRight', defaultPrevented: true },
    { key: 'ArrowLeft', altKey: true }, { key: 'ArrowRight', ctrlKey: true }, { key: 'Home', metaKey: true },
    { key: '+', ctrlKey: true }, { key: '-', metaKey: true }]) {
    const f = fixture(), event = f.event('key', 1, overrides);
    assert.equal(event.defaultPrevented, !!overrides.defaultPrevented); assert.equal(f.state.focused, ''); assert.equal(f.state.commits, 0);
  }
  const f = fixture(); assert.equal(f.event('key', 1, { key: 'ArrowRight' }).defaultPrevented, true); assert.equal(f.state.focused, 'next');
  assert.equal(f.event('key', 1, { key: 'z', metaKey: true }).defaultPrevented, true); assert.equal(f.state.commits, 1);
  const native = fixture(); const input = native.event('key', 1, { key: 'ArrowRight', target: { closest: () => ({ contentEditable: 'plaintext-only' }) } });
  assert.equal(input.defaultPrevented, false); assert.equal(native.state.focused, '');
});
