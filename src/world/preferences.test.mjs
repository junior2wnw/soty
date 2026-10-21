import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
import assert from 'node:assert/strict';
import ts from 'typescript';
import {normalizeTheme} from './theme/palette.mjs';

const source=readFileSync(process.env.SOTY_PREFERENCES_SOURCE??new URL('./preferences.ts',import.meta.url),'utf8');
const code=ts.transpileModule(source,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
const key='soty.world.ui.v1';
function preferences(storage){
  const module={exports:{}};
  vm.runInNewContext(code,{module,exports:module.exports,localStorage:storage,require(name){assert.equal(name,'./theme/palette.mjs');return {normalizeTheme};}},{filename:'actual-world-preferences.ts'});
  return module.exports;
}
function storage(raw=null){
  let current=raw;const reads=[],writes=[];
  return {reads,writes,getItem(name){assert.equal(name,key);reads.push(name);return current;},setItem(name,value){assert.equal(name,key);assert.equal(typeof value,'string');writes.push(value);current=value;}};
}
for(const [name,raw,wanted] of [
  ['fresh device',null,true],
  ['existing preferences without this field','{}',true],
  ['malformed JSON','{not-json',true],
  ['malformed field type',JSON.stringify({assistantShortcut:'false'}),true],
  ['explicit visible choice',JSON.stringify({assistantShortcut:true}),true],
  ['explicit hidden choice',JSON.stringify({assistantShortcut:false}),false],
])test(`assistant shortcut ${name} preserves the expected visibility choice`,()=>{
  const saved=storage(raw),api=preferences(saved),value=api.loadPreferences();
  assert.equal(value.assistantShortcut,wanted);assert.equal(saved.reads.length,1);assert.equal(saved.writes.length,0);
});
test('storage denial keeps the visible default and saving remains usable without an exception',()=>{
  let reads=0,writes=0;const denied={getItem(){reads++;throw Error('synthetic storage denied');},setItem(){writes++;throw Error('synthetic storage denied');}},api=preferences(denied);
  const value=api.loadPreferences();assert.equal(value.assistantShortcut,true);assert.equal(reads,1);assert.doesNotThrow(()=>api.savePreferences(value));assert.equal(writes,1);
});
test('an explicit false and later true survive fresh module reloads without rewriting unrelated preferences',()=>{
  const saved=storage(JSON.stringify({compact:true,scale:1.2,themeMode:'dark',themeBrightness:27})),first=preferences(saved),choice=first.loadPreferences();
  assert.equal(choice.assistantShortcut,true);choice.assistantShortcut=false;first.savePreferences(choice);
  const second=preferences(saved),hidden=second.loadPreferences();assert.equal(hidden.assistantShortcut,false);assert.equal(hidden.compact,true);assert.equal(hidden.scale,1.2);assert.equal(hidden.themeMode,'dark');assert.equal(hidden.themeBrightness,27);
  hidden.assistantShortcut=true;second.savePreferences(hidden);
  const third=preferences(saved).loadPreferences();assert.equal(third.assistantShortcut,true);assert.equal(third.compact,true);assert.equal(third.themeBrightness,27);assert.equal(saved.writes.length,2);
});
