import test from 'node:test';
import assert from 'node:assert/strict';
import sharp from 'sharp';
import { canonicalizeCover,createCoverDecoder,COVER_RECIPES } from '../server/codec.mjs';
import { image } from './support/fixture.mjs';

test('real Sharp codec strips input metadata, applies actual crop, retains app alpha and respects every budget',async()=>{
  const input=await sharp({create:{width:800,height:600,channels:4,background:{r:70,g:120,b:160,alpha:.4}}}).png().withMetadata({exif:{IFD0:{ImageDescription:'SYNTHETIC_PRIVATE_METADATA'}}}).toBuffer();
  for(const kind of ['app','profile']) {
    const result=await canonicalizeCover({kind,bytes:input,mime:'image/png',crop:{x:.7,y:.4,zoom:2}});
    assert.equal(result.renditions.length,COVER_RECIPES[kind].length);
    for(const [index,r] of result.renditions.entries()) {const meta=await sharp(r.bytes).metadata();assert.equal(meta.format,'webp');assert.equal(meta.width,r.width);assert.equal(meta.height,r.height);assert.equal(r.width,r.height);assert.ok(r.width<=300);assert.ok(r.bytes.length<=COVER_RECIPES[kind][index][2]);for(const key of ['exif','icc','xmp','iptc'])assert.equal(meta[key],undefined);assert.equal(Boolean(meta.hasAlpha),kind==='app');}
  }
});

test('false MIME, unsupported AVIF/SVG, invalid crop, truncation and oversized dimensions fail with bounded errors',async()=>{
  const png=await image();
  for(const request of [
    {bytes:png,mime:'image/jpeg'},
    {bytes:Buffer.from('<svg/>'),mime:'image/svg+xml'},
    {bytes:png,mime:'image/avif'},
    {bytes:png.subarray(0,20),mime:'image/png'},
    {bytes:Buffer.alloc(320*1024+1),mime:'image/png'},
    {bytes:await image('#112233',2048,1024),mime:'image/png'},
    {bytes:png,mime:'image/png',crop:{x:Infinity,y:.5,zoom:1}},
  ])await assert.rejects(canonicalizeCover({kind:'app',...request}),error=>String(error.code).startsWith('cover_'));
});

test('left and right crop actually select different source pixels, rather than only recording metadata',async()=>{
  const pixels=Buffer.alloc(128*64*3);for(let y=0;y<64;y++)for(let x=0;x<128;x++){const at=(y*128+x)*3;pixels[at]=x<64?255:0;pixels[at+2]=x<64?0:255;}
  const bytes=await sharp(pixels,{raw:{width:128,height:64,channels:3}}).png().toBuffer();
  const left=await canonicalizeCover({kind:'app',bytes,mime:'image/png',crop:{x:0,y:.5,zoom:1}}),right=await canonicalizeCover({kind:'app',bytes,mime:'image/png',crop:{x:1,y:.5,zoom:1}});
  const a=await sharp(left.renditions.at(-1).bytes).raw().toBuffer(),b=await sharp(right.renditions.at(-1).bytes).raw().toBuffer();
  assert.ok(a[0]>200&&a[2]<80);assert.ok(b[2]>200&&b[0]<80);
});

test('queue timeout never releases a running native slot, and owners cannot occupy both decoder slots',async()=>{
  const releases=[],started=[];
  const decoder=createCoverDecoder({concurrency:2,queueLimit:3,ownerQueueLimit:1,queueMs:30,decode:input=>new Promise(resolve=>{started.push(input);releases.push(resolve);})});
  const first=decoder.decode('owner_a','a1'),same=decoder.decode('owner_a','a2'),other=decoder.decode('owner_b','b1');
  await Promise.resolve();assert.deepEqual(started,['a1','b1']);assert.equal(decoder.stats().active,2);assert.equal(decoder.stats().queued,1);
  await assert.rejects(decoder.decode('owner_a','a3'),error=>error.code==='cover_decoder_busy');
  await assert.rejects(same,error=>error.code==='cover_decoder_queue_timeout');assert.equal(decoder.stats().active,2);
  releases[0]('done-a');releases[1]('done-b');assert.equal(await first,'done-a');assert.equal(await other,'done-b');await Promise.resolve();assert.equal(decoder.stats().active,0);
});
