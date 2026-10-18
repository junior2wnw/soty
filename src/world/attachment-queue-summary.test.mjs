import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
const ts=createRequire(import.meta.url)('typescript');
const current=await readFile(new URL('app.ts',import.meta.url),'utf8');

class Element {
 constructor(tag,cls='',text=''){this.tagName=tag;this.className=cls;this.children=[];this.attrs=new Map();this.hidden=false;this.value=text;this.textWrites=0;}
 get textContent(){return this.value||this.children.map(n=>n.textContent).join(' ');}
 set textContent(value){this.value=value;this.textWrites++;}
 setAttribute(name,value){this.attrs.set(name,value);}
 getAttribute(name){return this.attrs.get(name);}
 append(...items){for(const item of items){this.children.push(item);item.parent=this;}}
 replaceChildren(...items){this.children=[];this.append(...items);}
}
function harness(source,rows=[],pending=null){
 // Execute the actual declarations and render prefix. Existing row/network/durable
 // processing below the for-loop is unchanged and is deliberately not invoked.
 const begin=source.indexOf('\n',source.indexOf("const attach = iconButton('Прикрепить файлы'"))+1;
 const declarations=source.slice(begin,source.indexOf("    const error = el('div', 'sw-chat-error')",begin));
 const render=source.slice(source.indexOf('    const sizeLabel = (bytes: number)'),source.indexOf('      for (const item of attachmentRows)'))+'    };\n';
 assert(declarations.includes('const attachmentRail = '));assert(render.includes('const renderAttachments = '));
 const append=source.match(/dock\.append\([^;]+\); chat\.append/)[0].replace(/ chat\.append$/,'');
 const script=`let attachmentRows=rows,attachmentWritable=true;const active=()=>true;
 const outbox=new Proxy({pendingIntent:()=>pending},{get(target,name){if(name!=='pendingIntent')throw Error('unexpected port');return target[name];}});
 const dock=el('div'),reply=el('div'),error=el('div'),composer=el('form');
 ${declarations}\n${render}\n${append}\nrenderAttachments();
 return {summary:typeof attachmentSummary==='undefined'?null:attachmentSummary,rail:attachmentRail,dock,
 update(next,pendingValue=pending){attachmentRows=next;pending=pendingValue;renderAttachments();}};`;
 const compiled=ts.transpileModule(script,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.None}}).outputText;
 return new Function('el','rows','pending',compiled)((tag,cls,text)=>new Element(tag,cls,text),rows,pending);
}
const row=(id,status='ready',size=1024)=>({attachmentId:'public_'+id,name:'PUBLIC-'+id+'.txt',status,size,sent:status==='ready'?size:0,error:null});
const part=(h,index)=>h.summary.children[index];
const writes=h=>h.summary.children.reduce((n,child)=>n+child.textWrites,0);
function desiredWholeBatch(h){assert(h.summary,'whole-batch summary must exist outside the scroll rail');assert.equal(h.summary.hidden,false);assert.equal(part(h,0).textContent,'Готово 4 из 10');assert.equal(part(h,1).textContent,'10 / 10 файлов · 10 КБ');assert(h.dock.children.indexOf(h.summary)<h.dock.children.indexOf(h.rail));assert.equal(h.summary.parent,h.dock);}

test('all ten queue rows remain visible in the summary above the scroll rail',t=>{
 const rows=Array.from({length:10},(_,i)=>row(i,i<4?'ready':'pending')),before=structuredClone(rows);
 const next=harness(current,rows);desiredWholeBatch(next);assert.deepEqual(rows,before);
 assert.equal(next.summary.getAttribute('role'),'status');assert.equal(next.summary.getAttribute('aria-live'),'polite');assert.equal(next.summary.getAttribute('aria-atomic'),'true');
 assert.equal(next.summary.getAttribute('aria-label'),'Подготовка вложений перед отправкой');
});
test('current queue, aggregate bytes and removal drive the visible count; zero rows hide and clear it',()=>{
 const h=harness(current);assert.equal(h.summary.hidden,true);assert(h.summary.children.every(node=>node.textContent===''));
 h.update([row(1,'ready',1537),row(2,'pending',2048)]);assert.equal(part(h,0).textContent,'Готово 1 из 2');assert.equal(part(h,1).textContent,'2 / 10 файлов · 4 КБ');
 h.update([row(2,'pending',2048)]);assert.equal(part(h,0).textContent,'Готово 0 из 1');assert.equal(part(h,1).textContent,'1 / 10 файлов · 2 КБ');
 h.update([]);assert.equal(h.summary.hidden,true);assert(h.summary.children.every(node=>node.textContent===''));
});
test('only ready counts as prepared; cancelled, failed and full-byte-but-uploading rows never become accepted',()=>{
 const rows=[row(1),row(2,'pending'),row(3,'uploading'),{...row(4,'error'),error:'public_failure'},{...row(5,'cancel-pending'),error:'public_cancel_unconfirmed'}];rows[2].sent=rows[2].size;
 const h=harness(current,rows);assert.equal(part(h,0).textContent,'Готово 1 из 5');assert.equal(part(h,2).textContent,'Требуют внимания: 2');
 assert.doesNotMatch(h.summary.textContent,/принят|доставлен|отправлено/iu);
});
test('chunk progress causes no summary text mutations or live-region replacement; readiness changes announce once',()=>{
 const rows=[row(1,'uploading',256000),row(2,'pending',256000)],h=harness(current,rows),before=writes(h),region=h.summary;
 for(const sent of [1,64000,128000,192000,256000])h.update([{...rows[0],sent},rows[1]]);
 assert.equal(h.summary,region);assert.equal(writes(h),before);
 h.update([{...rows[0],status:'ready',sent:256000},rows[1]]);assert.equal(part(h,0).textContent,'Готово 1 из 2');assert.equal(writes(h),before+1);
});
test('unconfirmed send remains explicit while all files are prepared; it never removes rows or claims acceptance',()=>{
 const rows=[row(1),row(2)],h=harness(current,rows,{clientId:'PUBLIC-intent',attachments:rows.map(v=>v.attachmentId)}),before=structuredClone(rows);
 assert.equal(part(h,0).textContent,'Готово 2 из 2');assert.equal(part(h,2).textContent,'Отправка не подтверждена');assert.equal(part(h,2).hidden,false);assert.deepEqual(rows,before);
 h.update(rows,null);assert.equal(part(h,2).textContent,'');assert.equal(part(h,2).hidden,true);assert.equal(part(h,0).textContent,'Готово 2 из 2');
 h.update([]);assert.equal(h.summary.hidden,true);
});
