import test from 'node:test';
import assert from 'node:assert/strict';
import {MessageChannel} from 'node:worker_threads';
import {randomUUID} from 'node:crypto';
import {createOtsovEditorHost} from '../../src/world/otsov-editor-handoff.mjs';
import {fixture,Events,flush} from '../test/support/voice-orb-controller-fixture.mjs';
import {editorArtifact,editorReadback,voiceResult,untilEditor,speak} from '../test/support/voice-editor-fixture.mjs';
import {resolve,join} from 'node:path';
import {pathToFileURL} from 'node:url';
assert.ok(process.env.SOTY_OTSOV_NATIVE_FIXTURE_ROOT,'Provide the maintained OTsov Native checkout only for this explicit external integration gate.');
const nativeFixtureRoot=resolve(process.env.SOTY_OTSOV_NATIVE_FIXTURE_ROOT);
const {mountOtsovEditorReceiver}=await import(pathToFileURL(join(nativeFixtureRoot,'src/v4/otsov-editor-handoff.mjs')).href);
class EditorWindow extends Events {MessageChannel=MessageChannel;crypto={randomUUID};document={visibilityState:'visible'};setTimeout=setTimeout;clearTimeout=clearTimeout;}
function editorPort(t,onOpen){const root=new EditorWindow(),native=new EditorWindow();native.parent={};const slot={},parentOrigin='https://root-editor.invalid',origin='https://native-editor.invalid';let opens=0;
 const peer={postMessage(data,target,ports){assert.equal(target,origin);queueMicrotask(()=>native.emit('message',{data,origin:parentOrigin,source:native.parent,ports}));}};
 const host=createOtsovEditorHost({view:root,timeoutMs:1000,readPeer:()=>({window:peer,origin,appId:editorArtifact.appId,accountId:'alice',generation:1,slot}),async assertPeer(){return true;}});
 const close=mountOtsovEditorReceiver({view:native,appId:editorArtifact.appId,parentOrigin,async prepare(){},async openProject(artifact,owner){opens++;assert.deepEqual(artifact,editorArtifact);assert.equal(owner.isCurrent(),true);onOpen?.();return {...editorReadback};}});
 t.after(()=>{host.close();close();});return{open:(artifact,request)=>host.open(artifact,request),opens:()=>opens};}

test('editor-handoff actual voice controller uses the real MessageChannel receiver, survives navigation and keeps the same session for the next input', {timeout:5000},async t=>{
 let selection=null;const port=editorPort(t,()=>{selection={appId:editorArtifact.appId,handle:'h'.repeat(43)};});
 const f=fixture(t,{openProjectEditor:port.open,readSelection:()=>selection,rpc(op,args){if(op==='voice.orb.turn')return voiceResult(args);}});
 await f.start();speak(f);await untilEditor(()=>f.voice.view().editor?.state==='opened');
 assert.deepEqual(f.voice.view().editor.readback,editorReadback);assert.equal(port.opens(),1);assert.equal(f.voice.view().active,true);
 assert.equal(f.tracks[0].readyState,'live');assert.equal(f.peers[0].closed,0);assert.equal(f.calls.some(row=>row.op==='voice.orb.end'),false);
 f.peers[0].message({type:'response.done',response:{status:'completed',metadata:{soty_input:'editor_input_1'},output:[{type:'function_call',status:'completed',name:'delegate_to_agent',call_id:'editor_call_1',arguments:'{"task":"duplicate"}'}]}});await flush();assert.equal(port.opens(),1);
 speak(f,'editor_call_2','editor_input_2');await untilEditor(()=>port.opens()===2&&f.voice.view().editor?.state==='opened');
 assert.equal(f.counts().peerCount,1);assert.equal(f.calls.filter(row=>row.op==='voice.orb.start').length,1);assert.equal(f.calls.filter(row=>row.op==='voice.orb.turn').length,2);
 assert.equal(f.calls.find(row=>row.op==='voice.orb.turn').args.editorHandoff,undefined);
});
