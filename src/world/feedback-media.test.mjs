import test from 'node:test';
import assert from 'node:assert/strict';
import { attachmentBytes, validateAttachmentBudget, safeRasterSize, rasterSelection, rasterFileDimensions, createFeedbackRecorder, FEEDBACK_MEDIA_LIMITS } from './feedback-media.mjs';

const attachment = bytes => ({ kind: 'image', name: 'screen.png', mimeType: 'image/png', dataBase64: Buffer.alloc(bytes).toString('base64') });
test('media budget counts decoded bytes including base64 padding and refuses combined excess', () => {
  for (const size of [1, 2, 3, 100, 1048576]) assert.equal(attachmentBytes(attachment(size)), size);
  assert.equal(validateAttachmentBudget([attachment(500000), attachment(548576)]), FEEDBACK_MEDIA_LIMITS.totalAttachmentBytes);
  assert.throws(() => validateAttachmentBudget([attachment(500000), attachment(548577)]), /feedback_attachment_bytes/);
  assert.throws(() => validateAttachmentBudget(Array.from({ length: 4 }, () => attachment(1))), /feedback_attachment_count/);
});
test('media rejects malformed base64 and executable formats rather than turning them into preview URLs', () => {
  for (const dataBase64 of ['a', 'aaaa=', 'data:image/png;base64,YQ==', 'YQ==\n', 'Yx==', '💥===', '']) assert.throws(() => attachmentBytes({ dataBase64 }));
  assert.throws(() => validateAttachmentBudget([{ ...attachment(1), mimeType: 'image/svg+xml' }]), /invalid_feedback_attachment/);
  assert.throws(() => validateAttachmentBudget([{ ...attachment(1), kind: 'audio' }]), /invalid_feedback_attachment/);
});
test('raster limits reject oversized decode dimensions and selection exports only bounded actual pixels', () => {
  assert.deepEqual(safeRasterSize(3840, 2160), { width: 1440, height: 810 });
  assert.throws(() => safeRasterSize(8192, 8192), /feedback_image_dimensions/);
  assert.throws(() => safeRasterSize(0, 10), /feedback_image_dimensions/);
  assert.deepEqual(rasterSelection({ x: -20, y: -10, width: 100, height: 70 }, 800, 600), { x: 0, y: 0, width: 80, height: 60 });
  assert.throws(() => rasterSelection({ x: NaN, y: 0, width: 10, height: 10 }, 800, 600), /feedback_image_selection/);
});
test('compressed image headers are bounded before browser decode; MIME cannot relabel HTML as a raster', () => {
  const png = Buffer.alloc(24); Buffer.from([137,80,78,71,13,10,26,10]).copy(png); png.write('IHDR', 12); png.writeUInt32BE(240,16); png.writeUInt32BE(180,20);
  assert.deepEqual(rasterFileDimensions(png, 'image/png'), { width:240, height:180 });
  png.writeUInt32BE(1000000,16); assert.throws(() => rasterFileDimensions(png, 'image/png'), /feedback_image_dimensions/);
  assert.throws(() => rasterFileDimensions(Buffer.from('<html>'), 'image/png'), /feedback_image_format/);
  const jpeg = Buffer.from([255,216,255,192,0,8,8,0,180,0,240,0]);
  assert.deepEqual(rasterFileDimensions(jpeg, 'image/jpeg'), { width:240,height:180 });
  const webp = Buffer.alloc(30); webp.write('RIFF'); webp.write('WEBP',8); webp.write('VP8X',12); webp.writeUInt32LE(10,16); webp[24]=239; webp[27]=179;
  assert.deepEqual(rasterFileDimensions(webp, 'image/webp'), { width:240,height:180 });
});

function mediaGlobals(t, navigator, MediaRecorder) {
  const saved = ['navigator', 'MediaRecorder'].map(name => [name, Object.getOwnPropertyDescriptor(globalThis, name)]);
  Object.defineProperty(globalThis, 'navigator', { configurable:true, value:navigator });
  Object.defineProperty(globalThis, 'MediaRecorder', { configurable:true, value:MediaRecorder });
  t.after(() => { for (const [name, descriptor] of saved) { if (descriptor) Object.defineProperty(globalThis, name, descriptor); else delete globalThis[name]; } });
}

test('unsupported MP4-only recorder never requests microphone or advertises server-invalid media', async t => {
  let requests = 0;
  class Recorder { static isTypeSupported(value) { return value === 'audio/mp4'; } }
  mediaGlobals(t, { mediaDevices: { async getUserMedia() { requests++; throw new Error('unexpected microphone'); } } }, Recorder);
  const recorder = createFeedbackRecorder({ maxBytes: 1048576 });
  await assert.rejects(recorder.start(), /feedback_record_unavailable/);
  assert.equal(requests, 0);
  assert.throws(() => validateAttachmentBudget([{ kind: 'audio', name: 'invalid.mp4', mimeType: 'audio/mp4', dataBase64: 'AQID' }]), /invalid_feedback_attachment/);
});
test('closing while microphone permission is pending stops a late stream and cannot produce an attachment', async t => {
  let release, stopped=0;
  mediaGlobals(t, { mediaDevices:{ getUserMedia:() => new Promise(done => { release=done; }) } }, class { static isTypeSupported(){return true;} });
  const controller=new AbortController(), recorder=createFeedbackRecorder({maxBytes:100,signal:controller.signal});
  const outcome=recorder.start(); controller.abort(); release({getTracks:()=>[{stop(){stopped++;}}]});
  await assert.rejects(outcome, error => error.name==='AbortError'); assert.equal(stopped,1); assert.equal(recorder.active(),false);
});
test('voice exceeding the remaining shared attachment budget stops tracks instead of silently sending a partial recording', async t => {
  let stopped=0;
  class Recorder {
    state='inactive'; static isTypeSupported(){return true;}
    start(){this.state='recording';queueMicrotask(()=>this.ondataavailable({data:new Blob([new Uint8Array(2)])}));}
    stop(){this.state='inactive';queueMicrotask(()=>this.onstop());}
  }
  mediaGlobals(t, { mediaDevices:{async getUserMedia(){return{getTracks:()=>[{stop(){stopped++;}}]};}} }, Recorder);
  const recorder=createFeedbackRecorder({maxBytes:1});
  await assert.rejects(recorder.start(),/feedback_attachment_bytes/); assert.equal(stopped,1); assert.equal(recorder.active(),false);
});

test('a delayed timer uses elapsed monotonic time rather than counting delivered callbacks', async t => {
  let now=0,interval,stops=0;
  const saved=['performance','setInterval','clearInterval','setTimeout','clearTimeout'].map(name=>[name,Object.getOwnPropertyDescriptor(globalThis,name)]);
  Object.defineProperty(globalThis,'performance',{configurable:true,value:{now:()=>now}});
  Object.defineProperty(globalThis,'setInterval',{configurable:true,value:callback=>{interval=callback;return 1;}});
  Object.defineProperty(globalThis,'clearInterval',{configurable:true,value:()=>{}});
  Object.defineProperty(globalThis,'setTimeout',{configurable:true,value:()=>2});
  Object.defineProperty(globalThis,'clearTimeout',{configurable:true,value:()=>{}});
  t.after(()=>{for(const[name,descriptor]of saved)Object.defineProperty(globalThis,name,descriptor);});
  class Recorder {
    state='inactive';static isTypeSupported(){return true;}
    start(){this.state='recording';}
    stop(){stops++;this.state='inactive';queueMicrotask(()=>this.onstop());}
  }
  mediaGlobals(t,{mediaDevices:{async getUserMedia(){return{getTracks:()=>[{stop(){}}]};}}},Recorder);
  const recorder=createFeedbackRecorder({maxBytes:100,maxSeconds:2});
  const outcome=recorder.start();await Promise.resolve();
  now=2500;interval();const stoppedAtDeadline=stops;
  recorder.cancel();await assert.rejects(outcome);
  assert.equal(stoppedAtDeadline,1,'One late callback after the deadline must stop capture, not count as one second');
});

test('blob output assembles the final stop chunk without FileReader; default attachment remains unchanged', async t => {
  let stopped=0,recorder,reads=0;
  const descriptor=Object.getOwnPropertyDescriptor(globalThis,'FileReader');
  class Reader {
    readAsDataURL(blob){reads++;void blob.arrayBuffer().then(bytes=>{this.result=`data:${blob.type};base64,${Buffer.from(bytes).toString('base64')}`;this.onload();});}
  }
  Object.defineProperty(globalThis,'FileReader',{configurable:true,value:Reader});
  t.after(()=>{if(descriptor)Object.defineProperty(globalThis,'FileReader',descriptor);else delete globalThis.FileReader;});
  class Recorder {
    state='inactive';static isTypeSupported(value){return value==='audio/ogg;codecs=opus';}
    constructor(){recorder=this;}
    start(){this.state='recording';this.ondataavailable({data:new Blob([new Uint8Array([1,2])])});}
    stop(){this.state='inactive';queueMicrotask(()=>{this.ondataavailable({data:new Blob([new Uint8Array([3])])});this.onstop();});}
  }
  mediaGlobals(t,{mediaDevices:{async getUserMedia(){return{getTracks:()=>[{stop(){stopped++;}}]};}}},Recorder);
  const blobRecorder=createFeedbackRecorder({maxBytes:3,output:'blob'}),blobOutcome=blobRecorder.start();
  await Promise.resolve();blobRecorder.stop();const blob=await blobOutcome;
  assert.equal(blob.type,'audio/ogg');assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),new Uint8Array([1,2,3]));assert.equal(reads,0);
  const attachmentRecorder=createFeedbackRecorder({maxBytes:3}),attachmentOutcome=attachmentRecorder.start();
  await Promise.resolve();attachmentRecorder.stop();assert.deepEqual(await attachmentOutcome,{kind:'audio',name:'voice.ogg',mimeType:'audio/ogg',dataBase64:'AQID'});
  assert.equal(reads,1);assert.equal(stopped,2);assert.equal(recorder.onstop,null);
});

test('the final stop chunk participates in the byte limit, not only periodic chunks', async t => {
  let stopped=0;
  class Recorder {
    state='inactive';static isTypeSupported(){return true;}
    start(){this.state='recording';this.ondataavailable({data:new Blob([new Uint8Array(1)])});}
    stop(){this.state='inactive';queueMicrotask(()=>{this.ondataavailable?.({data:new Blob([new Uint8Array(2)])});this.onstop?.();});}
  }
  mediaGlobals(t,{mediaDevices:{async getUserMedia(){return{getTracks:()=>[{stop(){stopped++;}}]};}}},Recorder);
  const recorder=createFeedbackRecorder({maxBytes:2,output:'blob'}),outcome=recorder.start();
  await Promise.resolve();recorder.stop();await assert.rejects(outcome,/feedback_attachment_bytes/);assert.equal(stopped,1);
});
