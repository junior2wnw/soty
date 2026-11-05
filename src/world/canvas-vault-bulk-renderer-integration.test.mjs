import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {createCanvasVaultClient,CANVAS_VAULT_NOTE_TYPE} from './canvas-vault-client.mjs';
import {createCanvasVaultLocalStore,createCanvasVaultMemoryCache,vaultErrorCode} from './canvas-vault-local.mjs';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';
import {nativeFieldSafeArea} from './native-field-safe-area.mjs';
const overview=readFileSync(new URL('./field-object-overview.ts',import.meta.url),'utf8');
const source=readFileSync(new URL('./canvas-vault-renderer.ts',import.meta.url),'utf8'),portSource=readFileSync(new URL('./canvas-field-content.test.mjs',import.meta.url),'utf8'),parsed=ts.createSourceFile('ports.mjs',portSource,ts.ScriptTarget.Latest,true,ts.ScriptKind.JS),declaration=parsed.statements.find(node=>ts.isClassDeclaration(node)&&node.name?.text==='NodePort');assert.ok(declaration);
const flatten=node=>[node,...node.children.flatMap(flatten)],NodePort=vm.runInNewContext(`(${ts.createPrinter().printNode(ts.EmitHint.Unspecified,declaration,parsed)})`,{flatten});
Object.defineProperty(NodePort.prototype,'firstElementChild',{get(){return this.children[0]??null;},configurable:true});
const until=async predicate=>{for(let index=0;index<150;index++){if(predicate())return;await new Promise(resolve=>setImmediate(resolve));}assert.fail('Expected media lifecycle boundary');};
const turns=async()=>{for(let index=0;index<15;index++)await new Promise(resolve=>setImmediate(resolve));};
export function proposedRenderer(value){
  if(value.includes('let readController:AbortController|null=null;'))return value;
  const replacements=[
    ['let passwordInput:HTMLInputElement|null=null;','let passwordInput:HTMLInputElement|null=null;let readController:AbortController|null=null;'],
    ['function clear(){sequence++;','function clear(){sequence++;readController?.abort();readController=null;'],
    ["if(!open()||!player||flight)return;const ticket=sequence;play.disabled=true;","if(!open()||!player||flight)return;const ticket=sequence,controller=new AbortController();readController=controller;play.disabled=true;"],
    ['options.client.readContent(options.objectId);','options.client.readContent(options.objectId,{signal:controller.signal});'],
    ['finally{if(ticket===sequence){flight=null;play.disabled=false;}}','finally{if(readController===controller)readController=null;if(ticket===sequence){flight=null;play.disabled=false;}}'],
  ];
  for(const[from,to]of replacements){assert.equal(value.split(from).length,2);value=value.replace(from,to);}return value;
}
async function fixture(t,code=proposedRenderer(source)){
  const objectId='renderer_bulk_object',accountId='renderer_bulk_owner',size=2*512*1024+71,releases=[],ops=[],urls=[],revoked=[];let holds=0,current=true,ready=true,reads=0;
  const store=createCanvasVaultLocalStore(accountId,'renderer_bulk',{origin:'https://renderer-bulk.example',indexedDB:indexedNativePort().indexedDB});
  const localKeyScope={schema:'soty.vault.local-scope.v1',kind:'placement',id:objectId};
  let expiresAt=0;
  const api={async request(op,args){assert.equal(args.expectedAccountId,accountId);assert.equal(args.objectId,objectId);
    if(op==='canvas.vault.status')return{objectId,protection:{kind:'password'},state:'protected',locked:expiresAt<=Date.now(),revision:1,expiresAt,localKeyScope};
    if(op==='canvas.vault.unlock'){expiresAt=Date.now()+300000;return{objectId,locked:false,revision:1,expiresAt,localKeyScope};}assert.equal(op,'canvas.objects.content');assert.ok(expiresAt>Date.now());reads++;
    if(args.index===1){holds++;await new Promise(resolve=>releases.push(resolve));}
    const bytes=Buffer.alloc(Math.min(512*1024,size-args.index*512*1024),args.index+1);return{contentType:'image/png',bytesBase64:bytes.toString('base64url'),byteLength:size,chunks:3,index:args.index,revision:1};}};
  const client=createCanvasVaultClient({api,accountId,projectId:'renderer_bulk',isCurrent:()=>current,store});await client.unlock(objectId,'Synthetic renderer read password');
  const read=client.readContent;client.readContent=(...args)=>{const entry={options:args[1],done:false};ops.push(entry);return read(...args).then(value=>{entry.done=true;return value;},error=>{entry.done=true;throw error;});};
  class ImagePort extends NodePort{}class MediaPort extends NodePort{}class VideoPort extends MediaPort{}
  const document={activeElement:null,createElement(tag){const C=tag==='img'?ImagePort:tag==='video'?VideoPort:tag==='audio'?MediaPort:NodePort,value=new C(tag);value.ownerDocument=document;return value;}};
  const env=vm.createContext({exports:{},document,HTMLElement:NodePort,HTMLImageElement:ImagePort,HTMLMediaElement:MediaPort,HTMLVideoElement:VideoPort,AbortController,nativeFieldSafeArea,createCanvasVaultMemoryCache,vaultErrorCode,CANVAS_VAULT_NOTE_TYPE,Blob,URL:{createObjectURL:blob=>{const value='blob:bulk-'+urls.length;urls.push({value,blob});return value;},revokeObjectURL:value=>revoked.push(value)}});
  env.icon=()=>document.createElement('svg');
  const compiled=ts.transpileModule(overview+'\n'+code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText.replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,'');vm.runInContext(compiled+'\nexports.create=createCanvasVaultView;exports.text=canvasVaultErrorText;',env);
  const item={entity:{entity:{kind:'object',id:objectId},title:'Protected image'},presentation:{shape:'rect'},width:340,height:300},view=env.exports.create(item,{client,accountId,objectId,isCurrent:()=>current,canReveal:()=>ready,projection:()=>({view:{schema:'soty.canvas-vault-view.v1',kind:'vault',contentType:'image/png',byteLength:size,revision:1,canEdit:false}})});
  t.after(()=>{current=false;view.dispose();for(const release of releases)release();client.dispose();store.close();});
  return{view,client,item,ops,urls,revoked,text:env.exports.text,get holds(){return holds;},get reads(){return reads;},releaseAll(){for(const release of releases.splice(0))release();},ready(value){ready=value;view.update(item);}};
}

async function disposalOracle(t,code=proposedRenderer(source)){
  const f=await fixture(t,code);f.view.setVisible(true);await until(()=>f.holds===1);f.view.dispose();await turns();
  try{assert.equal(f.ops[0].done,true,'Disposing a media card must cancel its read without retiring the shared SDK grant or waiting for the wire');assert.equal(f.client.isUnlocked(f.item.entity.entity.id),true);}
  finally{f.releaseAll();}
  await turns();assert.equal(f.reads,2,'A disposed card must not request the remaining third part');assert.equal(f.urls.length,0);
}
test('proposed protected media disposal cancels its held read and never requests another part or constructs a Blob',disposalOracle);
test('negative control: old renderer sequence hiding alone still wastes the remaining read and fails the cancellation oracle',async t=>{
  const old=source.includes('let readController:AbortController|null=null;')?source.replace('readController?.abort();readController=null;',''):source;
  await assert.rejects(disposalOracle(t,old),/Disposing a media card must cancel its read/);
});

async function replacedLoadOracle(t,code=proposedRenderer(source)){
  const f=await fixture(t,code);f.view.setVisible(true);await until(()=>f.holds===1);
  f.ready(false);f.ready(true);await until(()=>f.holds===2&&f.ops.length===2);await turns();f.view.dispose();await turns();
  try{assert.equal(f.ops[1].done,true,'An old load finally must not erase the controller of the replacement read');assert.equal(f.ops[1].options.signal.aborted,true);}
  finally{f.releaseAll();}await turns();assert.equal(f.urls.length,0);
}
test('an old cancelled load cannot clear a newer media controller before the replacement view closes',replacedLoadOracle);
test('negative control: unconditional controller cleanup fails the same replacement-read cancellation oracle',async t=>{
  const code=proposedRenderer(source),needle='if(readController===controller)readController=null;';assert.equal(code.split(needle).length,2);await assert.rejects(replacedLoadOracle(t,code.replace(needle,'readController=null;')),/An old load finally must not erase the controller/);
});

test('a complete protected image still constructs its Blob only after every part and clears the URL on dispose',async t=>{
  const f=await fixture(t);f.view.setVisible(true);await until(()=>f.holds===1);assert.equal(f.urls.length,0);f.releaseAll();await until(()=>f.urls.length===1);assert.equal(f.reads,3);assert.equal(f.urls[0].blob.size,2*512*1024+71);f.view.dispose();assert.deepEqual(f.revoked,[f.urls[0].value]);
});
