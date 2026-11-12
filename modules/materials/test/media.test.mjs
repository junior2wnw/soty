import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {inspectMaterialMedia,inspectMaterialVideo} from '../server/media.mjs';
const read=extension=>readFileSync(new URL(`./fixtures/tone.${extension}`,import.meta.url));
test('synthetic FFmpeg Opus WebM/Ogg containers are admitted; headers alone are not audio',async()=>{
  const webm=await inspectMaterialMedia(read('webm'),'audio','audio/webm');assert.equal(webm.width,null);assert.equal(webm.durationMs,null);
  const ogg=await inspectMaterialMedia(read('ogg'),'audio','audio/ogg');assert.equal(ogg.durationMs,250);
  for(const [extension,type] of [['webm','audio/webm'],['ogg','audio/ogg']]) {
    const bytes=read(extension);await assert.rejects(inspectMaterialMedia(bytes.subarray(0,32),'audio',type));
    const corrupt=Buffer.from(bytes);corrupt[0]^=1;await assert.rejects(inspectMaterialMedia(corrupt,'audio',type));
  }
  await assert.rejects(inspectMaterialMedia(read('ogg'),'audio','audio/webm'));
  const damaged=Buffer.from(read('ogg'));damaged[damaged.length-1]^=1;
  await assert.rejects(inspectMaterialMedia(damaged,'audio','audio/ogg'),error=>error.code==='materials_media_invalid');
});
test('PCM WAV enforces frame alignment, declared length, channel bounds and duration',async()=>{
  // Build RIFF with known lengths; FFmpeg stdout WAV intentionally has streaming sentinel sizes.
  const data=Buffer.alloc(48000/4*2),bytes=Buffer.alloc(44+data.length);bytes.write('RIFF');bytes.writeUInt32LE(bytes.length-8,4);bytes.write('WAVEfmt ',8);bytes.writeUInt32LE(16,16);bytes.writeUInt16LE(1,20);bytes.writeUInt16LE(1,22);bytes.writeUInt32LE(48000,24);bytes.writeUInt32LE(96000,28);bytes.writeUInt16LE(2,32);bytes.writeUInt16LE(16,34);bytes.write('data',36);bytes.writeUInt32LE(data.length,40);data.copy(bytes,44);
  const wav=await inspectMaterialMedia(bytes,'audio','audio/wav');assert.equal(wav.durationMs,250);
  const bad=Buffer.from(bytes);bad.writeUInt16LE(32,22);await assert.rejects(inspectMaterialMedia(bad,'audio','audio/wav'));
  await assert.rejects(inspectMaterialMedia(bytes,'audio','audio/wav',{audioSeconds:0,fileBytes:25*1024*1024}),error=>error.code==='materials_audio_duration');
  await assert.rejects(inspectMaterialMedia(bytes.subarray(0,bytes.length-1),'audio','audio/wav'));
});
test('scoped VP8/Opus video verifies tracks, dimensions and actual keyframe header; magic-only and mislabeled audio fail',async()=>{
  const bytes=readFileSync(new URL('./fixtures/video.webm',import.meta.url)),result=await inspectMaterialMedia(bytes,'video','video/webm');
  assert.deepEqual(inspectMaterialVideo(bytes,'video/webm'),result);
  assert.equal(typeof inspectMaterialVideo(bytes,'video/webm')?.then,'undefined');
  assert.equal(result.width,96);assert.equal(result.height,64);assert.equal(result.durationMs,null);
  await assert.rejects(inspectMaterialMedia(bytes,'audio','audio/webm'));
  await assert.rejects(inspectMaterialMedia(read('webm'),'video','video/webm'));
  const signature=bytes.indexOf(Buffer.from([0x9d,0x01,0x2a]));assert.ok(signature>0);
  const corrupt=Buffer.from(bytes);corrupt[signature]^=1;await assert.rejects(inspectMaterialMedia(corrupt,'video','video/webm'),error=>error.code==='materials_video_bounds');
  const mismatch=Buffer.from(bytes);mismatch[signature+3]^=1;await assert.rejects(inspectMaterialMedia(mismatch,'video','video/webm'),error=>error.code==='materials_video_bounds');
  await assert.rejects(inspectMaterialMedia(bytes.subarray(0,32),'video','video/webm'));
});
