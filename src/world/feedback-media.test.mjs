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
test('closing while microphone permission is pending stops a late stream and cannot produce an attachment', async t => {
  let release, stopped=0;
  mediaGlobals(t, { mediaDevices:{ getUserMedia:() => new Promise(done => { release=done; }) } }, class {});
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
