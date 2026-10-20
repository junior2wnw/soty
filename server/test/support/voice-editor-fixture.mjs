import {deferred,flush} from './voice-orb-controller-fixture.mjs';
const editorArtifact={schema:'soty.otsov-project-artifact.v1',appId:'app-'+'a'.repeat(32),projectId:'11111111-1111-4111-8111-111111111111',branch:'main',revision:2,invocationId:'native_invocation_001'};
const editorReadback={projectId:editorArtifact.projectId,selectedProjectId:editorArtifact.projectId,branch:'main',revision:2,online:true};
const voiceResult=args=>({schema:'soty.voice-orb-turn.v1',sessionId:args.sessionId,callId:args.callId,requestId:'voice_current_owned_turn',answer:'Проект сохранён.',editorHandoff:editorArtifact});
const bounded=async promise=>{let timer;try{return await Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('editor_fixture_timeout')),1800);})]);}finally{clearTimeout(timer);}};
async function untilEditor(predicate){const until=Date.now()+1800;while(!predicate()){if(Date.now()>=until)throw Error('editor_state_missing');await flush();}}
function gate(t){const value={entered:deferred(),release:deferred()};t.after(()=>value.release.resolve());return value;}
function speak(f,callId='editor_call_1',user='editor_input_1',args={task:'Создай проект и покажи редактор'}){
 f.peers[0].message({type:'input_audio_buffer.committed',item_id:user});
 f.peers[0].message({type:'response.done',response:{status:'completed',metadata:{soty_input:user},output:[{type:'function_call',status:'completed',name:'delegate_to_agent',call_id:callId,arguments:JSON.stringify(args)}]}});
}

export {editorArtifact,editorReadback,voiceResult,bounded,untilEditor,gate,speak};
