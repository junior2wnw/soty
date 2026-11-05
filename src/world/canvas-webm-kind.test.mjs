import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {canvasWebmKind,normalizeCanvasWebmFile} from './canvas-webm-kind.mjs';
const audio=new Uint8Array(await readFile(new URL('../../modules/materials/test/fixtures/tone.webm',import.meta.url)));
const video=new Uint8Array(await readFile(new URL('../../modules/materials/test/fixtures/video.webm',import.meta.url)));
const expected={code:'canvas_create_media_type_unsupported'};
test('actual Opus-only and VP8/Opus files are classified by TrackEntry instead of the picker MIME',async()=>{
  assert.equal(canvasWebmKind(audio),'audio');assert.equal(canvasWebmKind(video),'video');
  const file=new File([audio],'voice.webm',{type:'video/webm'}),normalized=await normalizeCanvasWebmFile(file);
  assert.equal(normalized.kind,'audio');assert.equal(normalized.blob.type,'audio/webm');assert.equal(normalized.blob.name,file.name);
  assert.deepEqual(new Uint8Array(await normalized.blob.arrayBuffer()),audio,'normalizing the declared kind never changes original bytes');
});
test('negative control: trusting the MIME fails the exact audio-only picker oracle',async()=>{
  const file=new File([audio],'voice.webm',{type:'video/webm'}),oracle=value=>assert.equal(value.kind,'audio');
  assert.throws(()=>oracle({kind:file.type.startsWith('video/')?'video':'audio'}),assert.AssertionError);
  oracle(await normalizeCanvasWebmFile(file));
});
test('fake codec words outside TrackEntry, truncation and conflicting explicit kind cannot become audio',async()=>{
  assert.throws(()=>canvasWebmKind(new TextEncoder().encode('WebM A_OPUS V_VP8 TrackType=2')),expected);
  assert.throws(()=>canvasWebmKind(audio.subarray(0,24)),expected);
  await assert.rejects(normalizeCanvasWebmFile(new Blob([audio],{type:'video/webm'}),'video'),expected);
  const extra=new Uint8Array(video.length+6);extra.set(video);extra.set(new TextEncoder().encode('A_OPUS'),video.length);assert.equal(canvasWebmKind(extra),'video');
});
