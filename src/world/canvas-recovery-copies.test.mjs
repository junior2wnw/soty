import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';

const url=new URL('./canvas-object-settings.ts',import.meta.url),source=readFileSync(url,'utf8'),printer=ts.createPrinter();
const domSource=ts.createSourceFile('fixture.mjs',readFileSync(new URL('./canvas-create-dialog.test.mjs',import.meta.url),'utf8'),ts.ScriptTarget.Latest,true,ts.ScriptKind.JS);
const nodeClass=domSource.statements.find(value=>ts.isFunctionDeclaration(value)&&value.name?.text==='harness').body.statements.find(ts.isClassDeclaration);
const compile=code=>ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText.replace(/^export /gmu,'');
const tick=()=>new Promise(resolve=>setImmediate(resolve));
const copies=[{archiveId:'archive_1',records:[{operationId:'operation_1',kind:'note',sourceIntent:{title:'Old private title',body:'Old private body',items:[{text:'Original task',done:true}]}}]}];
function fixture({code=source,read=async()=>copies}={}){
 let current=true,unlocked=true;const calls={reads:[],clipboard:[],downloads:[],revoked:[],mutations:0},listeners=new Set();
 const document={activeElement:null},env=vm.createContext({exports:{},document,calls:{picker:0},Blob,File,setTimeout:fn=>{return fn;},URL:{createObjectURL:()=>{calls.downloads.push('blob:copy');return'blob:copy';},revokeObjectURL:value=>calls.revoked.push(value)},navigator:{clipboard:{async writeText(value){calls.clipboard.push(value);}}}});
 vm.runInContext(compile(printer.printNode(ts.EmitHint.Unspecified,nodeClass,domSource))+'\nexports.Node=Node;',env);const Node=env.exports.Node;
 Object.defineProperty(Node.prototype,'childElementCount',{get(){return this.children.length;}});
 Object.assign(env,{el:(tag,cls='',text='')=>new Node(tag,cls,text),button:(label,_icon,cls='',callback)=>{const node=new Node('button',cls,label);node.type='button';if(callback)node.addEventListener('click',callback);return node;}});
 const ast=ts.createSourceFile('settings.ts',code,ts.ScriptTarget.Latest,true),declaration=ast.statements.find(value=>ts.isFunctionDeclaration(value)&&value.name?.text==='mountCanvasRecoveryCopies');
 vm.runInContext(compile(printer.printNode(ts.EmitHint.Unspecified,declaration,ast))+'\nexports.mount=mountCanvasRecoveryCopies;',env);
 const vault={isUnlocked:()=>unlocked,subscribe(callback){listeners.add(callback);return()=>listeners.delete(callback);},async creationRecoveries(id,options){calls.reads.push({id,options});return read(id,options);},async restoreCreationRecovery(){calls.mutations++;throw Error('Viewing an old copy must not change the current Source');}};
 const host=new Node('div'),handle=env.exports.mount(host,{vault,objectId:'object_1',isCurrent:()=>current}),all=node=>[node,...node.children.flatMap(all)],nodes=()=>all(host);
 return{calls,handle,host,nodes,find:cls=>nodes().find(value=>value.className===cls),button:label=>nodes().find(value=>value.tagName==='BUTTON'&&value.textContent===label),open(){handle.element.open=true;handle.element.emit('toggle');},lock(){unlocked=false;for(const listener of listeners)listener('object_1','relock');},changeAccount(){current=false;handle.dispose();}};
}
test('an old note is view/copy only; each export obtains a fresh authorized snapshot and never overwrites current Source',async()=>{
 const f=fixture();f.open();await tick();f.button('Посмотреть прежнюю запись').emit('click');await tick();
 assert.match(f.find('co-recovery-preview').value,/Old private title\n\nOld private body\n\n☑ Original task/u);assert.equal(f.find('co-recovery-preview').readOnly,true);
 f.button('Копировать текст').emit('click');await tick();assert.equal(f.calls.reads.length,3);assert.equal(f.calls.clipboard.length,1);assert.match(f.calls.clipboard[0],/Old private body/u);assert.equal(f.calls.mutations,0);
 f.lock();assert.equal(f.find('co-recovery-preview').value,'');assert.equal(f.handle.element.hidden,true);f.handle.dispose();
});
async function lateLock(code=source){
 let release;const f=fixture({code,read:()=>new Promise(resolve=>{release=resolve;})});f.open();await tick();f.lock();release(copies);await tick();
 assert.equal(f.find('co-recovery-list').children.length,0,'a late private result cannot repopulate a retired copy list');assert.equal(f.find('co-recovery-preview').value,'');assert.equal(f.calls.clipboard.length,0);f.handle.dispose();
}
test('relock fences a late copy list and scrubs all current plaintext',()=>lateLock());
test('negative control: ignoring current-generation guards fails the same late private-result oracle',async()=>{
 const needle='if(!current()||ticket!==generation)return;',broken=source.replaceAll(needle,'if(false)return;');assert.notEqual(broken,source);await assert.rejects(lateLock(broken),/late private result cannot repopulate/u);
});
test('account retirement during fresh clipboard authorization cannot export stale text',async()=>{
 let release,count=0;const f=fixture({read:async()=>++count===3?new Promise(resolve=>{release=resolve;}):copies});f.open();await tick();f.button('Посмотреть прежнюю запись').emit('click');await tick();f.button('Копировать текст').emit('click');await tick();f.changeAccount();release(copies);await tick();
 assert.deepEqual(f.calls.clipboard,[]);assert.equal(f.host.children.length,0);assert.equal(f.calls.mutations,0);
});
test('a Source error keeps a real retry, and retries read rather than silently returning no copies',async()=>{
 let count=0;const f=fixture({read:async()=>{if(++count===1)throw Error('unavailable');return copies;}});f.open();await tick();assert.equal(f.button('Повторить').hidden,false);assert.match(f.find('co-status').textContent,/Не удалось открыть копию/u);
 f.button('Повторить').emit('click');await tick();assert.ok(f.button('Посмотреть прежнюю запись'));assert.equal(f.calls.reads.length,2);f.handle.dispose();
});
test('file download is deliberate and its private Blob URL is retired on lock',async()=>{
 const fileCopies=[{archiveId:'archive_file',records:[{operationId:'operation_file',kind:'image',sourceIntent:{name:'Old picture.png',blob:new Blob(['synthetic bytes'],{type:'image/png'})}}]}];const f=fixture({read:async()=>fileCopies});
 f.open();await tick();assert.equal(f.calls.downloads.length,0);f.button('Скачать прежний файл').emit('click');await tick();assert.equal(f.calls.downloads.length,1);assert.equal(f.calls.reads.length,2);f.lock();assert.deepEqual(f.calls.revoked,['blob:copy']);assert.equal(f.calls.mutations,0);f.handle.dispose();
});
