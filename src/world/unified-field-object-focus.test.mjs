import test from 'node:test';
import assert from 'node:assert/strict';
import {fieldObjectFocusFrame,fieldObjectNeedsFocus} from './unified-field-object-focus.mjs';

test('reading window stays inside the actually visible viewport across phone, desktop and keyboard-sized areas',()=>{
  for(const width of [320,390,720,1280,1920])for(const height of [240,500,844,1080]){
    const view={width,height};
    for(const visible of [{left:0,top:0,width,height},{left:8,top:30,width:width-16,height:height-80}]){
      const result=fieldObjectFocusFrame(view,visible);
      assert.ok(result.left>=visible.left&&result.left+result.width<=visible.left+visible.width);
      assert.ok(result.barTop>=visible.top&&result.barTop+44<=result.top);
      assert.ok(result.top>=visible.top&&result.top+result.height<=visible.top+visible.height);
      assert.ok(result.width>0&&result.width<=960&&result.height>0&&result.height<=720);
    }
  }
});

test('one zoom threshold cannot flicker between usable controls and a miniature',()=>{
  let miniature=fieldObjectNeedsFocus(.77);assert.equal(miniature,true);
  for(const scale of [.79,.81,.78,.85]){miniature=fieldObjectNeedsFocus(scale,miniature);assert.equal(miniature,true);}
  miniature=fieldObjectNeedsFocus(.87,miniature);assert.equal(miniature,false);
  for(const scale of [.84,.8,.79]){miniature=fieldObjectNeedsFocus(scale,miniature);assert.equal(miniature,false);}
  assert.equal(fieldObjectNeedsFocus(.77,miniature),true);
});
