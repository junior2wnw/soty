import test from 'node:test';
import assert from 'node:assert/strict';
import {sealPasswordContainer,openPasswordContainer,rewrapPasswordContainer} from './password-container.mjs';

const context={ownerId:'owner',objectId:'object_secret',version:1},password='synthetic-test-password',bytes=Buffer.from('secret protected note payload');
const denied=callback=>assert.throws(callback,error=>error.code==='password_container_unavailable');

test('real AES-GCM envelope contains no plaintext and only the correct password/context opens its random DEK',()=>{
 const container=sealPasswordContainer({context,password,bytes}),second=sealPasswordContainer({context,password,bytes});assert.equal(JSON.stringify(container).includes(bytes.toString()),false);assert.notEqual(container.payload.ciphertext,second.payload.ciphertext);assert.notEqual(container.key.ciphertext,second.key.ciphertext);
 const opened=openPasswordContainer({container,context,password});assert.deepEqual(opened,bytes);opened.fill(0);assert.equal(bytes.toString(),'secret protected note payload','encryption never clears the caller-owned input');
 denied(()=>openPasswordContainer({container,context,password:'wrong-password'}));denied(()=>openPasswordContainer({container,context:{...context,ownerId:'another'},password}));denied(()=>openPasswordContainer({container,context:{...context,objectId:'object_elsewhere'},password}));denied(()=>openPasswordContainer({container,context:{...context,version:2},password}));
});

test('tampered payload/key/AAD and a plaintext-only CSS lock fail instead of returning decrypted content',()=>{
 const container=sealPasswordContainer({context,password,bytes});
 for(const section of ['payload','key']){const changed=structuredClone(container);const raw=Buffer.from(changed[section].ciphertext,'base64url');raw[0]^=1;changed[section].ciphertext=raw.toString('base64url');denied(()=>openPasswordContainer({container:changed,context,password}));}
 const badTag=structuredClone(container);const tag=Buffer.from(badTag.payload.tag,'base64url');tag[0]^=1;badTag.payload.tag=tag.toString('base64url');denied(()=>openPasswordContainer({container:badTag,context,password}));
 const changed=structuredClone(container);changed.context.version=2;denied(()=>openPasswordContainer({container:changed,context:{...context,version:2},password}));
 assert.throws(()=>openPasswordContainer({container:{schema:'css-only',locked:true,plaintext:bytes.toString()},context,password}));
 const hostile=structuredClone(container);hostile.key.kdf.N=2**30;assert.throws(()=>openPasswordContainer({container:hostile,context,password}),error=>error.code==='password_container_invalid','untrusted KDF parameters cannot force unbounded work');
});

test('password rotation rewraps only the DEK and rejects the old password on the new envelope',()=>{
 const container=sealPasswordContainer({context,password,bytes}),nextPassword='different-synthetic-password';
 const next=rewrapPasswordContainer({container,context,oldPassword:password,newPassword:nextPassword});assert.deepEqual(next.payload,container.payload);assert.notEqual(next.key.kdf.salt,container.key.kdf.salt);denied(()=>openPasswordContainer({container:next,context,password}));
 const opened=openPasswordContainer({container:next,context,password:nextPassword});assert.deepEqual(opened,bytes);opened.fill(0);denied(()=>rewrapPasswordContainer({container,context,oldPassword:'wrong-password',newPassword:nextPassword}));
});
