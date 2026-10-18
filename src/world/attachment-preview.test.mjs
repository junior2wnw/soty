import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { inspectRaster, isRasterAttachment, createAttachmentPreviewPool, rasterPreview, PREVIEW_LIMITS as P } from './attachment-preview.mjs';
import { attachmentManifest, ATTACHMENT_LIMITS as L } from '../../modules/world/attachment-contract.mjs';
import { attachmentHttpFixture } from '../../modules/world/test/support/attachment-http-fixture.mjs';

const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/nXcAAAAASUVORK5CYII=', 'base64');
const sha = value => createHash('sha256').update(value).digest('hex');
const deferred = () => { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; };
const wait = async predicate => { for (let i = 0; !predicate(); i++) { assert(i < 300, 'OWN bounded wait'); await new Promise(r => setTimeout(r, 2)); } };
function fixture(raw = png, type = 'image/png') {
  const parts = Array.from({ length: Math.ceil(raw.length / L.chunkBytes) }, (_, i) => raw.subarray(i * L.chunkBytes, (i + 1) * L.chunkBytes));
  const ref = { attachmentId: 'public_fixture_image', name: 'PUBLIC-SYNTHETIC.png', size: raw.length, chunks: parts.length, mediaType: type,
    manifestSha256: sha(attachmentManifest(raw.length, parts.map(bytes => ({ bytes: bytes.length, sha256: sha(bytes) })))), fileSha256: sha(raw), state: 'published' };
  const calls = [], urls = [], revoked = [], transforms = [];
  const api = { async request(op, args) {
    assert.equal(args.expectedAccountId, 'account_own'); assert.equal(args.communityId, 'community_own'); assert.equal(args.attachmentId, ref.attachmentId);
    calls.push({ op, index: args.index });
    if (op === 'world.attachment.status') return { attachment: { ...ref } };
    assert.equal(op, 'world.attachment.read'); const bytes = parts[args.index]; return { index: args.index, size: bytes.length, sha256: sha(bytes), data: bytes.toString('base64') };
  } };
  const options = { api, accountId: 'account_own', communityId: 'community_own', urls: { createObjectURL(blob) { assert(['image/webp', 'image/png', 'application/octet-stream'].includes(blob.type)); const id = 'blob:OWN-' + urls.length; urls.push(id); return id; }, revokeObjectURL(url) { revoked.push(url); } },
    transform: async (blob, info, kind) => { transforms.push({ bytes: blob.size, info, kind }); return new Blob(['PUBLIC-REENCODED-PIXELS'], { type: 'image/webp' }); } };
  return { ref, api, options, parts, calls, urls, revoked, transforms };
}

test('raster selection/header gates: name is not authority, mismatched MIME/HTML/SVG and dimension bombs never reach decoder', async () => {
  assert.deepEqual(inspectRaster(png, 'image/png'), { mime: 'image/png', width: 1, height: 1 });
  assert.deepEqual(inspectRaster(png, 'application/octet-stream'), { mime: 'image/png', width: 1, height: 1 });
  const renamed=fixture(Buffer.from('<svg xmlns="http://www.w3.org/2000/svg"/>'),'application/octet-stream');
  assert.equal(isRasterAttachment(renamed.ref),true);const renamedPool=createAttachmentPreviewPool(renamed.options);
  await assert.rejects(renamedPool.load(renamed.ref),e=>e.code==='attachment_preview_format');assert.equal(renamed.transforms.length,0);assert.equal(renamed.urls.length,0);renamedPool.close();
  const jpeg = Buffer.from([255,216,255,192,0,11,8,0,2,0,3,1,1,0,0,255,217]);
  assert.deepEqual(inspectRaster(jpeg, 'image/jpeg'), { mime: 'image/jpeg', width: 3, height: 2 });
  const webp = Buffer.alloc(30); webp.write('RIFF'); webp.writeUInt32LE(22, 4); webp.write('WEBPVP8X', 8); webp.writeUInt32LE(10,16); webp[24]=2;webp[27]=1;
  assert.deepEqual(inspectRaster(webp, 'image/webp'), { mime: 'image/webp', width: 3, height: 2 });
  webp[20] = 2; assert.throws(() => inspectRaster(webp, 'image/webp'), e => e.code === 'attachment_preview_format');
  for (const bytes of [Buffer.from('<svg xmlns="http://www.w3.org/2000/svg"/>'), Buffer.from('<html>PUBLIC</html>'), Buffer.from('%PDF-PUBLIC'), png.subarray(0, 24)])
    assert.throws(() => inspectRaster(bytes, 'image/png'), e => e.code === 'attachment_preview_format');
  assert.throws(() => inspectRaster(png, 'image/jpeg'), e => e.code === 'attachment_preview_format');
  for (const [width, height] of [[P.side + 1, 1], [6000, 5000], [0, 2]]) {
    const bytes = Buffer.from(png); bytes.writeUInt32BE(width,16); bytes.writeUInt32BE(height,20);
    const f = fixture(bytes), pool = createAttachmentPreviewPool(f.options);
    await assert.rejects(pool.load(f.ref), e => ['attachment_preview_dimensions','attachment_preview_format'].includes(e.code));
    assert.equal(f.transforms.length,0);assert.equal(f.urls.length,0);pool.close();
  }
  const f = fixture(png, 'image/svg+xml'), pool = createAttachmentPreviewPool(f.options);
  assert.equal(isRasterAttachment(f.ref), false); await assert.rejects(pool.load(f.ref), e => e.code === 'attachment_preview_format'); assert.equal(f.calls.length,0); pool.close();
});

test('actual signed-reader verification rejects tampered bytes, moved chunk index and changed message ref before URL/decode', async () => {
  for (const kind of ['bytes', 'index', 'manifest']) {
    const f = fixture(), request = f.api.request;
    f.api.request = async (op,args) => { const result = await request(op,args); if(op==='world.attachment.read'&&kind==='bytes')result.data=Buffer.from('PUBLIC-TAMPER').toString('base64'); if(op==='world.attachment.read'&&kind==='index')result.index++;if(op==='world.attachment.status'&&kind==='manifest')result.attachment.manifestSha256='a'.repeat(64);return result; };
    const pool = createAttachmentPreviewPool(f.options);await assert.rejects(pool.load(f.ref),e=>e.code==='attachment_integrity_failed');assert.equal(f.transforms.length,0);assert.equal(f.urls.length,0);pool.close();
  }
});

test('A→B→A epoch and offscreen guards discard post-status/post-chunk responses, without more reads or URL', async () => {
  for(const cut of ['world.attachment.status','world.attachment.read']) {
    const f=fixture();let account='A',epoch=1;const captured=epoch,request=f.api.request;
    f.api.request=async(op,args)=>{const r=await request(op,args);if(op===cut){account='B';epoch++;account='A';epoch++;}return r;};
    const pool=createAttachmentPreviewPool({...f.options,current:()=>account==='A'&&epoch===captured});await assert.rejects(pool.load(f.ref),e=>e.code==='attachment_context_changed');
    assert.equal(f.calls.length,cut==='world.attachment.status'?1:2);assert.equal(f.urls.length,0);assert.equal(f.transforms.length,0);pool.close();
  }
});

test('clear/close during native transformation cannot publish stale pixels; current ref is captured immutably', async () => {
  const f=fixture(),gate=deferred();f.options.transform=async()=>gate.promise;
  const pool=createAttachmentPreviewPool(f.options),pending=pool.load(f.ref);const rejection=assert.rejects(pending,e=>e.code==='attachment_context_changed');
  await wait(()=>f.calls.length===2);pool.clear();gate.resolve(new Blob(['PUBLIC'],{type:'image/webp'}));await rejection;
  assert.equal(f.urls.length,0);assert.equal(pool.snapshot().liveBytes,0);pool.close();await assert.rejects(pool.load(f.ref),e=>e.code==='attachment_context_changed');
  const other=fixture(),hold=deferred(),request=other.api.request;other.api.request=async(op,args)=>{await hold.promise;return request(op,args);};
  const second=createAttachmentPreviewPool(other.options),ref={...other.ref},reading=second.load(ref);ref.manifestSha256='b'.repeat(64);ref.attachmentId='different';hold.resolve();const lease=await reading;assert.equal(other.calls.length,2);lease.release();second.close();
});

test('two reads/one decoder/finite queue are enforced and close cancels queued work without retry', async () => {
  const f=fixture(),gate=deferred(),request=f.api.request;let statuses=0;
  f.api.request=async(op,args)=>{if(op==='world.attachment.status'){statuses++;await gate.promise;}return request(op,args);};
  const pool=createAttachmentPreviewPool(f.options),jobs=[];
  for(let i=0;i<P.reads+P.queue;i++)jobs.push(pool.load(f.ref).then(()=>({ok:true}),error=>({code:error.code})));
  assert.equal(statuses,P.reads);assert.equal(pool.snapshot().queued,P.queue);await assert.rejects(pool.load(f.ref),e=>e.code==='attachment_preview_busy');
  pool.close();gate.resolve();assert((await Promise.all(jobs)).every(v=>v.code==='attachment_context_changed'));assert.equal(statuses,P.reads);assert.equal(f.urls.length,0);
  const second=fixture(),decode=deferred();let active=0,max=0,started=0;second.options.transform=async()=>{active++;max=Math.max(max,active);if(++started===1)await decode.promise;active--;return new Blob(['PIXELS'],{type:'image/webp'});};
  const p=createAttachmentPreviewPool(second.options),a=p.load(second.ref),b=p.load(second.ref);await wait(()=>started===1);assert.equal(p.snapshot().running,2);decode.resolve();(await a).release();(await b).release();assert.equal(max,1);p.close();
});

test('new gallery and download always reauthorize/read; every bounded RAM URL is revoked exactly once', async () => {
  const f=fixture(),pool=createAttachmentPreviewPool(f.options);
  const thumbnail=await pool.load(f.ref),gallery=await pool.load(f.ref,{kind:'gallery'}),download=await pool.load(f.ref,{kind:'download'});
  assert.equal(f.calls.filter(v=>v.op==='world.attachment.status').length,3);assert.equal(f.calls.filter(v=>v.op==='world.attachment.read').length,3);
  assert.deepEqual(f.transforms.map(v=>v.kind),['thumbnail','gallery']);thumbnail.release();thumbnail.release();gallery.release();download.release();
  f.options.transform=async(_blob,_info,kind)=>new Blob([new Uint8Array(kind==='gallery'?P.galleryBytes:P.thumbnailBytes)],{type:'image/webp'});
  // The live pool retains its captured transform; a separate pressure pool uses larger outputs.
  const pressure=createAttachmentPreviewPool(f.options);for(let i=0;i<20;i++)await pressure.load(f.ref);
  assert(pressure.snapshot().liveUrls<=P.liveUrls);assert(pressure.snapshot().liveBytes<=P.liveBytes);
  await pressure.load(f.ref,{kind:'gallery'});await pressure.load(f.ref,{kind:'gallery'});assert(pressure.snapshot().liveBytes<=P.liveBytes);
  pool.close();pressure.close();assert.equal(new Set(f.revoked).size,f.urls.length);assert.equal(f.revoked.length,f.urls.length);
});

test('unsupported transform output cannot become a URL; browser bitmap/canvas resources close on encode failure', async () => {
  for(const output of [new Blob(['<svg/>'],{type:'image/svg+xml'}),new Blob([],{type:'image/png'}),new Blob([new Uint8Array(P.thumbnailBytes+1)],{type:'image/png'})]) {
    const f=fixture();f.options.transform=async()=>output;const pool=createAttachmentPreviewPool(f.options);await assert.rejects(pool.load(f.ref),e=>e.code==='attachment_preview_capacity');assert.equal(f.urls.length,0);pool.close();
  }
  const beforeBitmap=globalThis.createImageBitmap,beforeCanvas=globalThis.OffscreenCanvas;let closed=0,canvas,resize,canvases=0;
  globalThis.createImageBitmap=async(_blob,options)=>{resize=options;return{width:1,height:1,close(){closed++;}};};
  globalThis.OffscreenCanvas=class{constructor(w,h){canvases++;this.width=w;this.height=h;canvas=this;}getContext(){return{drawImage(){}};}async convertToBlob(){throw Object.assign(Error('OWN encoder failure'),{code:'own_encoder_failure'});}};
  try{await assert.rejects(rasterPreview(new Blob([png]),{mime:'image/png',width:1,height:1},'thumbnail'),e=>e.code==='own_encoder_failure');assert.equal(closed,1);assert.equal(canvas.width,1);assert.equal(canvas.height,1);assert.equal(resize,undefined);
    for(const[width,height]of[[P.side+1,1],[5000,5000],['1',1],[1,0]]){
      globalThis.createImageBitmap=async()=>({width,height,close(){closed++;}});
      await assert.rejects(rasterPreview(new Blob([png]),inspectRaster(png,'image/png'),'gallery'),e=>e.code==='attachment_preview_dimensions');
    }
    assert.equal(closed,5);assert.equal(canvases,1);
  }
  finally{globalThis.createImageBitmap=beforeBitmap;globalThis.OffscreenCanvas=beforeCanvas;}
});

async function publishPng(f, bytes, id) {
  const actor=f.actors[0],args={...f.base(actor),attachmentId:id};
  const good=result=>{assert.equal(result.ok,true,result.error?.code);return result;};
  good(await f.call(actor,'world.attachment.begin',{...args,name:'PUBLIC-OWN.png',size:bytes.length,mediaType:'image/png'}));
  const parts=[];for(let p=0,index=0;p<bytes.length;p+=L.chunkBytes,index++){const part=bytes.subarray(p,p+L.chunkBytes);parts.push({bytes:part.length,sha256:sha(part)});good(await f.call(actor,'world.attachment.chunk',{...args,index,sha256:sha(part),data:part.toString('base64')}));}
  good(await f.call(actor,'world.attachment.complete',{...args,manifestSha256:sha(attachmentManifest(bytes.length,parts))}));
  return good(await f.call(actor,'world.chat.send',{...f.base(actor),clientId:'message_'+id,text:'PUBLIC',attachments:[id]})).message.attachments[0];
}

test('genuine signed HTTP/SQLite: second actor previews published raster; revoked membership denies next gallery even after prior view', async t => {
  const f=await attachmentHttpFixture(t),ref=await publishPng(f,png,'own_preview_auth'),actor=f.actors[1],created=[],revoked=[];
  const api={request:async(op,args)=>{const r=await f.call(actor,op,args);if(!r.ok)throw Object.assign(Error('OWN signed denial'),{code:r.error.code});return r;}};
  const pool=createAttachmentPreviewPool({api,accountId:actor.accountId,communityId:f.group.communityId,transform:async()=>new Blob(['PUBLIC-PIXELS'],{type:'image/webp'}),urls:{createObjectURL(){created.push('blob:OWN-'+created.length);return created.at(-1);},revokeObjectURL(v){revoked.push(v);}}});t.after(()=>pool.close());
  const lease=await pool.load(ref);lease.release();const left=await f.call(actor,'world.membership.leave',{communityId:f.group.communityId});assert.equal(left.ok,true);
  await assert.rejects(pool.load(ref,{kind:'gallery'}),e=>e.code==='community_membership_required');assert.equal(created.length,1);assert.deepEqual(revoked,created);
});

test('genuine signed HTTP/SQLite: revoke between chunks stops pixels; wrong account and removed message never decode', async t => {
  const f=await attachmentHttpFixture(t),bytes=Buffer.concat([png,Buffer.alloc(L.chunkBytes)]),ref=await publishPng(f,bytes,'own_preview_chunk_revoke'),actor=f.actors[1];let reads=0,decoded=0;
  const api={request:async(op,args)=>{const r=await f.call(actor,op,args);if(!r.ok)throw Object.assign(Error('OWN signed denial'),{code:r.error.code});if(op==='world.attachment.read'&&++reads===1)assert.equal((await f.call(actor,'world.membership.leave',{communityId:f.group.communityId})).ok,true);return r;}};
  const common={api,accountId:actor.accountId,communityId:f.group.communityId,transform:async()=>{decoded++;return new Blob(['PIXELS'],{type:'image/webp'});},urls:{createObjectURL(){throw Error('OWN must not publish');},revokeObjectURL(){}}};
  const pool=createAttachmentPreviewPool(common);await assert.rejects(pool.load(ref),e=>e.code==='community_membership_required');assert.equal(reads,1);assert.equal(decoded,0);pool.close();
  const wrong=createAttachmentPreviewPool({...common,accountId:f.actors[0].accountId});await assert.rejects(wrong.load(ref),e=>e.code==='attachment_account_mismatch');wrong.close();
  assert.equal((await f.call(actor,'world.membership.join',{communityId:f.group.communityId})).ok,true);
  const listed=await f.call(f.actors[0],'world.chat.list',{communityId:f.group.communityId,readerEpoch:4});const message=listed.messages.find(m=>m.attachments?.some(v=>v.attachmentId===ref.attachmentId));
  assert.equal((await f.call(f.actors[0],'world.chat.remove',{communityId:f.group.communityId,messageId:message.messageId})).ok,true);
  const removed=createAttachmentPreviewPool(common);await assert.rejects(removed.load(ref),e=>e.code==='attachment_unavailable');assert.equal(decoded,0);removed.close();
});
