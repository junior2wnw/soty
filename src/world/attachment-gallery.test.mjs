// Actual TS controller + actual small DOM helpers. Synthetic layout/dialog and
// codec ports cannot prove native focus, browser decoding or mobile geometry.
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { attachmentManifest, ATTACHMENT_LIMITS as L } from '../../modules/world/attachment-contract.mjs';
const ts=createRequire(import.meta.url)('typescript');
const data=text=>'data:text/javascript;base64,'+Buffer.from(text).toString('base64');
const transpile=source=>ts.transpileModule(source,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText;
const dom=await readFile(new URL('./dom.ts',import.meta.url),'utf8');
const helpers=data(transpile("const icon=()=>document.createElement('svg');\n"+dom.slice(dom.indexOf('export function el<'),dom.indexOf('export function heading('))));
const controller=(await readFile(new URL('./attachment-gallery.ts',import.meta.url),'utf8')).replace("from './dom'",'from '+JSON.stringify(helpers)).replace("from './attachment-preview.mjs'",'from '+JSON.stringify(new URL('./attachment-preview.mjs',import.meta.url).href)).replace("from './gallery-swipe.mjs'",'from '+JSON.stringify(new URL('./gallery-swipe.mjs',import.meta.url).href));
const {createChatAttachmentMedia}=await import(data(transpile(controller)));
const png=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/nXcAAAAASUVORK5CYII=','base64'),sha=b=>createHash('sha256').update(b).digest('hex');
const pause=()=>new Promise(r=>setTimeout(r,0));
const until=async predicate=>{for(let i=0;!predicate();i++){assert(i<200,'OWN controller wait');await pause();}};
const descendants=node=>[node,...node.children.flatMap(descendants)];
const find=(node,cls)=>descendants(node).find(n=>n.className.split(' ').includes(cls));
const byLabel=(node,label)=>descendants(node).find(n=>n.getAttribute('aria-label')===label);
class Element extends EventTarget {
  constructor(tag){super();this.tagName=tag.toUpperCase();this.children=[];this.attrs=new Map();this.dataset={};this.className='';this.textContent='';this.hidden=false;this.disabled=false;this.src='';this.classList={add:(...v)=>{this.className=[...new Set([...this.className.split(' '),...v])].join(' ');}};}
  append(...nodes){for(const node of nodes){this.children.push(node);node.parent=this;}}
  setAttribute(k,v){this.attrs.set(k,String(v));}
  getAttribute(k){return this.attrs.get(k)??null;}
  removeAttribute(k){this.attrs.delete(k);if(k==='src')this.src='';}
  click(){if(!this.disabled)this.dispatchEvent(new Event('click'));}
}
function harness(t,{holdStatus=false}={}) {
  const prior={document:globalThis.document,IntersectionObserver:globalThis.IntersectionObserver,createImageBitmap:globalThis.createImageBitmap,OffscreenCanvas:globalThis.OffscreenCanvas,create:URL.createObjectURL,revoke:URL.revokeObjectURL};
  const doc=new EventTarget();doc.visibilityState='visible';doc.createElement=tag=>new Element(tag);globalThis.document=doc;
  let observer;globalThis.IntersectionObserver=class{constructor(callback){this.callback=callback;this.targets=new Set();observer=this;}observe(n){this.targets.add(n);}unobserve(n){this.targets.delete(n);}disconnect(){this.targets.clear();}fire(n,state){this.callback([{target:n,isIntersecting:state}]);}};
  const created=[],revoked=[];URL.createObjectURL=()=>{const v='blob:OWN-UI-'+created.length;created.push(v);return v;};URL.revokeObjectURL=v=>revoked.push(v);
  globalThis.createImageBitmap=async()=>({width:1,height:1,close(){}});globalThis.OffscreenCanvas=class{getContext(){return{drawImage(){}};}async convertToBlob(){return new Blob(['OWN-PIXELS'],{type:'image/webp'});}};
  const refs=['a','b'].map((id,index)=>({attachmentId:'own_photo_'+id,name:`PUBLIC-${id}.png`,mediaType:'application/octet-stream',size:png.length,chunks:1,manifestSha256:sha(attachmentManifest(png.length,[{bytes:png.length,sha256:sha(png)}])),fileSha256:sha(png)}));
  const calls=[],dialogs=[],errors=[],pending=[];let epoch=1;
  const api={request:async(op,args)=>{calls.push(op);const ref=refs.find(v=>v.attachmentId===args.attachmentId);assert(ref);assert.equal(args.expectedAccountId,'own_account');assert.equal(args.communityId,'own_chat');
    if(op==='world.attachment.status'){if(holdStatus)await new Promise(r=>pending.push(r));return{attachment:{...ref,state:'published'}};}
    assert.equal(op,'world.attachment.read');return{index:0,size:png.length,sha256:sha(png),data:png.toString('base64')};}};
  const media=createChatAttachmentMedia({api,accountId:'own_account',communityId:'own_chat',viewport:new Element('section'),current:()=>epoch===1,error:e=>errors.push(e.code),dialog(_title,onClose){const element=new Element('dialog'),body=new Element('div');element.open=true;element.append(body);const value={element,body,closes:0,close(){if(!element.open)return;element.open=false;value.closes++;onClose();}};dialogs.push(value);return value;}});
  const block=media.render('message_own',refs,()=>epoch===1);
  t.after(()=>{media.close();globalThis.document=prior.document;globalThis.IntersectionObserver=prior.IntersectionObserver;globalThis.createImageBitmap=prior.createImageBitmap;globalThis.OffscreenCanvas=prior.OffscreenCanvas;URL.createObjectURL=prior.create;URL.revokeObjectURL=prior.revoke;});
  return{media,block,refs,calls,dialogs,errors,created,revoked,doc,get observer(){return observer;},changeEpoch(){epoch++;},release(){for(const resolve of pending.splice(0))resolve();},key(dialog,key){const event=new Event('keydown',{cancelable:true});Object.defineProperty(event,'key',{value:key});dialog.element.dispatchEvent(event);return event;}};
}

test('actual controller: no eager reads, lazy viewport load then offscreen/dispose revokes each URL',async t=>{
  const h=harness(t),photo=byLabel(h.block,'Открыть фото PUBLIC-a.png');assert(photo);assert.equal(photo.getAttribute('aria-haspopup'),'dialog');assert.equal(h.calls.length,0);
  h.observer.fire(photo,true);await until(()=>h.created.length===1);assert.equal(find(photo,'sw-message-photo-label').hidden,true);
  h.observer.fire(photo,false);assert.deepEqual(h.revoked,h.created);assert.equal(photo.children[0].src,'');
  h.observer.fire(photo,true);await until(()=>h.created.length===2);h.media.remove('message_own');assert.equal(h.observer.targets.size,0);assert.equal(new Set(h.revoked).size,2);assert.equal(h.errors.length,0);
});

test('actual controller: Arrow/Home/End bounded navigation, no boundary reread, Escape disposes gallery',async t=>{
  const h=harness(t);byLabel(h.block,'Открыть фото PUBLIC-a.png').click();const dialog=h.dialogs[0];
  await until(()=>h.created.length===1);await pause();assert.equal(find(dialog.body,'sw-photo-counter').textContent,'1 из 2');
  h.key(dialog,'ArrowRight');await until(()=>h.created.length===2);await pause();assert.equal(find(dialog.body,'sw-photo-counter').textContent,'2 из 2');
  const reads=h.calls.length;h.key(dialog,'ArrowRight');await pause();assert.equal(h.calls.length,reads);
  h.key(dialog,'Home');await until(()=>h.created.length===3);h.key(dialog,'End');await until(()=>h.created.length===4);
  const escape=h.key(dialog,'Escape');assert.equal(escape.defaultPrevented,true);assert.equal(dialog.closes,1);assert.equal(new Set(h.revoked).size,h.created.length);
});

test('actual controller: removed entry and A→B→A epoch discard a late gallery response before pixels',async t=>{
  const h=harness(t,{holdStatus:true});byLabel(h.block,'Открыть фото PUBLIC-a.png').click();assert.equal(h.calls.length,1);
  h.changeEpoch();h.media.remove('message_own');h.release();await pause();await pause();assert.equal(h.dialogs[0].closes,1);assert.equal(h.created.length,0);assert.deepEqual(h.calls,['world.attachment.status']);
});

test('actual controller: hidden page closes gallery/releases pixels; new visible view reauthorizes',async t=>{
  const h=harness(t);const photo=byLabel(h.block,'Открыть фото PUBLIC-a.png');h.observer.fire(photo,true);await until(()=>h.created.length===1);photo.click();await until(()=>h.created.length===2);
  h.doc.visibilityState='hidden';h.doc.dispatchEvent(new Event('visibilitychange'));assert.equal(h.dialogs[0].closes,1);assert.equal(new Set(h.revoked).size,2);
  const count=h.calls.length;h.doc.visibilityState='visible';h.doc.dispatchEvent(new Event('visibilitychange'));h.observer.fire(photo,true);await until(()=>h.created.length===3);assert(h.calls.length>count);h.media.close();assert.equal(new Set(h.revoked).size,3);
});
