import test from 'node:test';
import assert from 'node:assert/strict';
import { resolveObjectURL } from 'node:buffer';
import sharp from 'sharp';
import { createCoverRasterPreparer } from './cover-local-raster.mjs';
function actualRasterPlatform() {
  let closed=0; const prepare=createCoverRasterPreparer({
    async decode(blob){const result=await sharp(Buffer.from(await blob.arrayBuffer())).rotate().png().toBuffer({resolveWithObject:true});return {width:result.info.width,height:result.info.height,bytes:result.data,close(){closed++;this.bytes.fill(0);}};},
    async encode(bitmap,size,quality,kind){let pipeline=sharp(bitmap.bytes).resize(size.width,size.height);if(kind==='profile')pipeline=pipeline.flatten({background:'#f4efe4'});
      return new Blob([await pipeline.webp({quality:Math.round(quality*100)}).toBuffer()],{type:'image/webp'});},
    createUrl:blob=>URL.createObjectURL(blob),revokeUrl:url=>URL.revokeObjectURL(url),
  });return {prepare,get closed(){return closed;}};
}
const file=(bytes,type)=>({size:bytes.length,type,arrayBuffer:async()=>Uint8Array.from(bytes).buffer});
test('actual native JPEG orientation normalizes once and uploaded WebP has no EXIF metadata',async()=>{
  const input=await sharp({create:{width:120,height:80,channels:3,background:'#558899'}}).jpeg().withMetadata({orientation:6}).toBuffer();
  const platform=actualRasterPlatform(),result=await platform.prepare(file(input,'image/jpeg'),()=>true);
  assert.deepEqual([result.width,result.height],[80,120]);const output=await sharp(Buffer.from(result.base64(),'base64')).metadata();assert.equal(output.orientation,undefined);assert.equal(output.exif,undefined);assert.equal(output.width,80);assert.equal(output.height,120);
  const url=result.url;assert.ok(resolveObjectURL(url));result.dispose();assert.equal(resolveObjectURL(url),undefined);assert.equal(platform.closed,1);
});
test('actual alpha WebP admission and profile background match the server recipe without metadata retention',async()=>{
  const input=await sharp({create:{width:50,height:100,channels:4,background:{r:255,g:0,b:0,alpha:0}}}).png().toBuffer();const platform=actualRasterPlatform();
  const app=await platform.prepare(file(input,'image/png'),()=>true,'app'),profile=await platform.prepare(file(input,'image/png'),()=>true,'profile');
  assert.equal((await sharp(Buffer.from(app.base64(),'base64')).metadata()).hasAlpha,true);const encoded=Buffer.from(profile.base64(),'base64'),meta=await sharp(encoded).metadata();assert.equal(meta.hasAlpha,false);
  const pixel=(await sharp(encoded).raw().toBuffer()).subarray(0,3);for(const [i,channel] of [244,239,228].entries())assert.ok(Math.abs(pixel[i]-channel)<=5);
  assert.deepEqual([app.width,app.height],[50,100]);assert.equal(platform.closed,2);app.dispose();profile.dispose();
});
