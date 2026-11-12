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

function observedWebm(trackTypes) {
  const size=value=>{let width=1;while(value>=2**(7*width)-1)width++;const bytes=Buffer.alloc(width);let encoded=BigInt(value)|(1n<<BigInt(7*width));for(let i=width-1;i>=0;i--){bytes[i]=Number(encoded&255n);encoded>>=8n;}return bytes;};
  const element=(id,payload)=>Buffer.concat([Buffer.from(id,'hex'),size(payload.length),payload]);
  const head=Buffer.alloc(19);head.write('OpusHead');head[9]=1;
  const entries=trackTypes.map((type,index)=>{
    const number=Buffer.alloc(3);number.writeUIntBE(index+1,0,3);
    return element('ae',Buffer.concat([element('d7',number),element('83',Buffer.from([type])),
      element('86',Buffer.from(type===1?'V_VP8':'A_OPUS')),
      type===1?element('e0',Buffer.concat([element('b0',Buffer.from([1])),element('ba',Buffer.from([1]))])):element('63a2',head)]));
  });
  const bytes=Buffer.concat([element('1a45dfa3',element('4282',Buffer.from('webm'))),
    element('18538067',Buffer.concat([element('1654ae6b',Buffer.concat(entries)),
      element('1f43b675',Buffer.concat([element('e7',Buffer.from([0])),element('a3',Buffer.from([0x81,0,0,0,0]))]))]))]);
  const reads={codecs:0,blocks:0},toString=bytes.toString,readInt16BE=bytes.readInt16BE;
  // Count parser work on this Buffer only; no runtime timing or global instrumentation.
  bytes.toString=function(...args){const value=toString.apply(this,args);if(args[0]==='ascii'&&['V_VP8','A_OPUS'].includes(value))reads.codecs++;return value;};
  bytes.readInt16BE=function(...args){reads.blocks++;return readInt16BE.apply(this,args);};
  return {bytes,reads};
}

for(const kind of ['audio','video'])test(`${kind} WebM rejects 20000 tracks before reading excess track codecs`,async()=>{
  const types=Array(20000).fill(2);if(kind==='video')types[0]=1;
  const {bytes,reads}=observedWebm(types);assert.ok(bytes.length>800000&&bytes.length<1048576);
  await assert.rejects(inspectMaterialMedia(bytes,kind,kind+'/webm'),error=>error.code==='materials_media_invalid');
  assert.equal(reads.codecs,kind==='video'?2:1);assert.equal(reads.blocks,0);
});

for(const type of [1,2])test(`video WebM rejects duplicate track type ${type} before reading any blocks`,()=>{
  const {bytes,reads}=observedWebm([type,type]);
  assert.throws(()=>inspectMaterialVideo(bytes,'video/webm'),error=>error.code==='materials_media_invalid');
  assert.equal(reads.codecs,2);assert.equal(reads.blocks,0);
});
