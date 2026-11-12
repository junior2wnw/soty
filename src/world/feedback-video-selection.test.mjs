import test from 'node:test';
import assert from 'node:assert/strict';
import * as media from './feedback-media.mjs';

const video = size => ({ kind: 'video', name: 'video.webm', mimeType: 'video/webm', dataBase64: Buffer.alloc(size).toString('base64') });
test('feedback video shares the existing total-byte and three-attachment budget', () => {
  assert.equal(media.validateAttachmentBudget([video(200000)]), 200000);
  assert.throws(() => media.validateAttachmentBudget([video(600000), video(500000)]), /feedback_attachment_bytes/);
  assert.throws(() => media.validateAttachmentBudget(Array.from({length:4}, () => video(1))), /feedback_attachment_count/);
  for (const value of [ {...video(1), mimeType:'text/html'}, {...video(1), mimeType:'audio/webm'}, {...video(1), kind:'image'} ]) {
    assert.throws(() => media.validateAttachmentBudget([value]), /invalid_feedback_attachment/);
  }
});

test('video selection rejects an unsupported format, excessive file or cancellation before reading bytes', async () => {
  assert.equal(typeof media.videoFile, 'function');
  let reads=0;
  const file=(type,size)=>({type,size, arrayBuffer(){reads++;throw Error('unexpected read');}});
  await assert.rejects(media.videoFile(file('video/mp4',1),100),/feedback_video_format/);
  await assert.rejects(media.videoFile(file('video/webm',101),100),/feedback_attachment_bytes/);
  await assert.rejects(media.videoFile(file('video/webm',0),100),/feedback_video_format/);
  const controller=new AbortController();controller.abort();
  await assert.rejects(media.videoFile(file('video/webm',1),100,controller.signal),error=>error.name==='AbortError');
  assert.equal(reads,0);
});
