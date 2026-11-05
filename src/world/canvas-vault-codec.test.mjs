import test from 'node:test';
import assert from 'node:assert/strict';
import {CanvasVaultError,vaultBase64,vaultBytes} from './canvas-vault-local.mjs';

const invalid=error=>error instanceof CanvasVaultError&&error.name==='CanvasVaultError'&&error.code==='vault_content_invalid';
const legacyBytes=value=>Uint8Array.from(atob(value.replaceAll('-','+').replaceAll('_','/')),char=>char.charCodeAt(0));

test('canonical base64url preserves all bytes, chunk boundaries and subarray views through512KiB',()=>{
  for(const length of[0,1,2,3,12,16,17,31,32,33,255,256,8191,8192,8193,512*1024]){
    const backing=new Uint8Array(length+7);for(let index=0;index<backing.length;index++)backing[index]=(index*137+31)&255;
    const bytes=backing.subarray(3,3+length),before=backing.slice(),encoded=vaultBase64(bytes),decoded=vaultBytes(encoded,length);
    assert.ok(decoded instanceof Uint8Array);assert.equal(decoded.byteOffset,0);assert.equal(decoded.length,length);
    assert.deepEqual(decoded,bytes);assert.deepEqual(decoded,legacyBytes(encoded));assert.equal(vaultBase64(decoded),encoded);
    assert.deepEqual(backing,before);assert.match(encoded,/^[A-Za-z0-9_-]*$/u);
  }
  assert.equal(vaultBase64(new Uint8Array([251,255])),'-_8');
});
test('malformed types, alphabet, whitespace, padding and impossible tail keep the existing error class/code',()=>{
  for(const value of[undefined,null,0,[],{},new String('AA'),'A','AAAAA','+w','/w','AA=','AA==','AA===','=',' A','AA\n','AA\r','A A','AA\t','AA\0','é','𝄞'])assert.throws(()=>vaultBytes(value),invalid);
});
test('one-byte noncanonical overflow bits remain rejected rather than normalized',()=>{
  const alphabet='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';
  assert.deepEqual(vaultBytes('AA',1),new Uint8Array([0]));
  for(let index=1;index<16;index++)assert.throws(()=>vaultBytes('A'+alphabet[index],1),invalid);
});
test('two-byte noncanonical overflow bits remain rejected rather than normalized',()=>{
  assert.deepEqual(vaultBytes('AAA',2),new Uint8Array([0,0]));
  for(const value of['AAB','AAC','AAD'])assert.throws(()=>vaultBytes(value,2),invalid);
});
test('exact decoded limits accept the boundary and refuse oversized input before calling the decoder',t=>{
  for(const length of[0,1,2,3,12,16,32]){
    const encoded=vaultBase64(new Uint8Array(length));assert.equal(vaultBytes(encoded,length).length,length);
    if(length)assert.throws(()=>vaultBytes(encoded,length-1),invalid);
  }
  let calls=0;t.mock.method(globalThis,'atob',()=>{calls++;throw Error('oversize_was_decoded');});
  assert.throws(()=>vaultBytes('AAAA',2),invalid);assert.equal(calls,0);
});
test('decoder failure and an oversized decoded result retain the same fail-closed boundary',t=>{
  const mock=t.mock.method(globalThis,'atob',()=>{throw Error('decoder_failed');});assert.throws(()=>vaultBytes('AA',1),invalid);
  mock.mock.mockImplementation(()=>String.fromCharCode(0,0));assert.throws(()=>vaultBytes('AA',1),invalid);
});
