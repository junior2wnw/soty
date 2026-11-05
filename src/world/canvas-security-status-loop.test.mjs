import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {createCanvasObjectSecurityActions} from './canvas-object-security-actions.mjs';

const fixture=ts.createSourceFile('fixture.mjs',readFileSync(new URL('./canvas-create-dialog.test.mjs',import.meta.url),'utf8'),ts.ScriptTarget.Latest,true,ts.ScriptKind.JS),printer=ts.createPrinter();
const nodeClass=fixture.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='harness').body.statements.find(ts.isClassDeclaration);
const compile=code=>ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText.replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,'');
const tick=()=>new Promise(resolve=>setImmediate(resolve));

test('actual settings, security controller and SDK status adapter do not recursively refresh a locked status notification',async()=>{
 const listeners=new Set();let statusCalls=0;
 const object={schema:'soty.canvas-object.v1',objectId:'object_status',ownerId:'account_status',revision:1,accessRevision:1,state:'active',binding:{providerId:'soty.notes',ref:'note_status'},presentation:{mask:{shape:'rect'}},visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'password'}};
 const api={async request(op){if(op==='canvas.objects.get')return{object};if(op==='canvas.pages.get')return{page:null};if(op==='contacts.list')return{contacts:[]};if(op==='world.community.list')return{communities:[]};throw Error('Unexpected request '+op);}};
 const vault={isUnlocked:()=>false,subscribe(listener){listeners.add(listener);return()=>listeners.delete(listener);},async status(){
  statusCalls++;if(statusCalls>6)throw Object.assign(Error('bounded fixture: original feedback loop reaches the same rate limit'),{code:'rate_limited'});
  for(const listener of [...listeners])listener(object.objectId,'locked');
  return{locked:true,protection:{kind:'password'},capabilities:{nativeConversion:true,placementProtection:false}};
 }};
 const transition={remember(){},async liveReconcile(){return{server:'protected',device:'complete',protectionComplete:true};}};
 const actions=createCanvasObjectSecurityActions({api,accountId:object.ownerId,objectId:object.objectId,isCurrent:()=>true,vault,nativeTransition:transition});
 const env=vm.createContext({exports:{},document:{activeElement:null},AbortController,TextEncoder,crypto,CANVAS_VAULT_NOTE_TYPE:'application/vnd.soty.note+json',canvasVaultErrorText:error=>error.code??'error'});
 vm.runInContext(compile(printer.printNode(ts.EmitHint.Unspecified,nodeClass,fixture))+'\nexports.Node=Node;',env);const Node=env.exports.Node;
 Object.assign(env,{el:(tag,cls='',text='')=>new Node(tag,cls,text),button:(text,_icon,cls='',callback)=>{const node=new Node('button',cls,text);if(callback)node.addEventListener('click',callback);return node;},labeledField:(text,input)=>{const label=new Node('label');label.append(new Node('span','',text),input);return label;},pinDialogSubmit:(dialog,_form,save)=>dialog.body.append(save)});
 vm.runInContext(compile(readFileSync(new URL('./canvas-object-security.ts',import.meta.url),'utf8'))+'\nexports.security=mountCanvasObjectSecurity;',env);
 vm.runInContext(compile(readFileSync(new URL('./canvas-object-settings.ts',import.meta.url),'utf8'))+'\nexports.settings=mountCanvasObjectSettings;',env);
 const dialog={body:new Node('section'),close(){}};
 const handle=env.exports.settings(dialog,{api,vault,security:actions,accountId:object.ownerId,objectId:object.objectId,title:'Closed note',projection:null,isCurrent:()=>true,onChanged:async()=>{},onOpen(){}});
 try{await tick();await tick();assert.equal(statusCalls,1,'An own locked status read must not recursively enqueue another status read');}
 finally{handle.dispose();}
});
