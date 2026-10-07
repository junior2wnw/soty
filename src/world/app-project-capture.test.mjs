import test from 'node:test';
import assert from 'node:assert/strict';
import {captureSourcePin,matchesScopedCaptureContext} from './app-project-capture.mjs';

const source={id:'planner.selected-workspace',version:1,digest:'a'.repeat(64)},appId='app-'+'a'.repeat(32),now=1000000;
const context=()=>({ok:true,ready:true,appId,scopedSource:{...source},target:{revision:2,digest:'b'.repeat(64)},expiresAt:now+300000});
test('approved capture namespace is exact ASCII128 and never derives identity from native DB name, title or extra fields',()=>{
  assert.deepEqual(captureSourcePin(source),source);
  for(const value of [{...source,id:'hive/native-db'},{...source,id:'я'},{...source,id:'a'.repeat(129)},{...source,title:'trusted?'}])assert.equal(captureSourcePin(value),null);
  let touched=false;const accessor={version:1,digest:source.digest};Object.defineProperty(accessor,'id',{enumerable:true,get(){touched=true;return source.id;}});
  assert.equal(captureSourcePin(accessor),null);assert.equal(touched,false);
});
test('signed peer context accepts only current exact source pins and a bounded live target, without private fields',()=>{
  assert.equal(matchesScopedCaptureContext(context(),{appId,source},now),true);
  for(const value of [{...context(),ready:false},{...context(),appId:'app-'+'b'.repeat(32)},
    {...context(),scopedSource:{...source,id:'hive'}},{...context(),scopedSource:{...source,version:2}},
    {...context(),scopedSource:{...source,digest:'c'.repeat(64)}},{...context(),expiresAt:now},
    {...context(),expiresAt:now+310001},{...context(),target:{revision:0,digest:'b'.repeat(64)}},
    {...context(),resources:['private']},{...context(),subject:'private'}])assert.equal(matchesScopedCaptureContext(value,{appId,source},now),false);
});
