import test from 'node:test';
import assert from 'node:assert/strict';
import { createCoverRasterPreparer, inspectCoverRaster, coverCropRect, COVER_FILE_LIMIT } from './cover-local-raster.mjs';
function png(width, height, animation = false) {
  const bytes = new Uint8Array(animation ? 65 : 45), view = new DataView(bytes.buffer);
  bytes.set([137,80,78,71,13,10,26,10]); view.setUint32(8,13); bytes.set(Buffer.from('IHDR'),12); view.setUint32(16,width);view.setUint32(20,height);
  let at=33;if(animation){view.setUint32(at,8);bytes.set(Buffer.from('acTL'),at+4);at+=20;}view.setUint32(at,0);bytes.set(Buffer.from('IEND'),at+4);return bytes;
}
const file = (bytes, type='image/png') => ({ size: bytes.length, type, arrayBuffer: async () => bytes.buffer });
test('size/type/dimension/animation checks run before the native decoder', async () => {
  let decoded=0; const prepare=createCoverRasterPreparer({decode:async()=>{decoded++;},encode(){},createUrl(){},revokeUrl(){}});
  for(const input of [file(png(10000,10000)),file(png(8,8,true)),file(png(2,2),'image/svg+xml'),{size:COVER_FILE_LIMIT+1,type:'image/png',arrayBuffer(){throw Error('must not read');}}])
    await assert.rejects(()=>prepare(input,()=>true));
  assert.equal(decoded,0); assert.deepEqual(inspectCoverRaster(png(4000,4000),'image/png'),{width:4000,height:4000});
});
test('local re-encoding preserves aspect, bounds decoded upload and owns exactly its bitmap/URL', async () => {
  let closed=0,revoked=[],encoded=[]; const prepare=createCoverRasterPreparer({decode:async()=>({width:4000,height:2000,close(){closed++;}}),
    encode:async(_,size)=>{encoded.push(size);return new Blob([png(size.width,size.height)],{type:'image/png'});},createUrl:()=> 'blob:owned',revokeUrl:value=>revoked.push(value)});
  const result=await prepare(file(png(4000,2000)),()=>true);assert.deepEqual(encoded,[{width:1024,height:512}]);assert.equal(closed,1);assert.equal(result.width*result.height<=1048576,true);
  assert.ok(result.base64());result.dispose();result.dispose();assert.deepEqual(revoked,['blob:owned']);assert.throws(()=>result.base64(),{code:'cover_context_changed'});
});
test('cancel during a pending decode holds its slot and cleans the late native bitmap', async () => {
  let release,closed=0,current=true,encodes=0;const held=new Promise(r=>release=r);
  const prepare=createCoverRasterPreparer({decode:()=>held,encode(){encodes++;},createUrl(){throw Error('must not allocate');},revokeUrl(){}});
  const pending=prepare(file(png(2,2)),()=>current);await Promise.resolve();current=false;
  await assert.rejects(()=>prepare(file(png(2,2)),()=>true),{code:'cover_decode_busy'});
  release({width:2,height:2,close(){closed++;}});await assert.rejects(pending,{code:'cover_context_changed'});assert.equal(closed,1);assert.equal(encodes,0);
});
test('encode retries are finite, native dimension mismatch and MIME spoof are rejected', async () => {
  let count=0,closed=0;
  const prepare=createCoverRasterPreparer({decode:async()=>({width:1000,height:1000,close(){closed++;}}),encode:async()=>{count++;return new Blob([new Uint8Array(327681)],{type:'image/webp'});},createUrl(){throw Error('must not allocate');},revokeUrl(){}});
  await assert.rejects(()=>prepare(file(png(1000,1000)),()=>true),{code:'cover_input_limit'});assert.equal(count,6);assert.equal(closed,1);
  assert.throws(()=>inspectCoverRaster(png(8,8),'image/webp'),{code:'cover_file_invalid'});
  const mismatch=createCoverRasterPreparer({decode:async()=>({width:8,height:8,close(){}}),encode:async()=>new Blob([png(16,16)],{type:'image/png'}),createUrl(){throw Error('must not allocate');},revokeUrl(){}});
  await assert.rejects(()=>mismatch(file(png(8,8)),()=>true),{code:'cover_encode_invalid'});
});
test('crop preview matches independently calculated server extraction at edges and zoom', () => {
  assert.deepEqual(coverCropRect(800,400,{x:0,y:1,zoom:2}),{left:0,top:200,size:200});
  assert.deepEqual(coverCropRect(333,777,{x:1,y:0,zoom:4}),{left:250,top:0,size:83});
  assert.deepEqual(coverCropRect(801,401),{left:200,top:0,size:401});
});
