import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';
import { canonical, check, jsonCopy } from './wire.mjs';

/** Constructor-only Source custody. A ciphertext/JSON value is not authority. */
export function createSourceSessionCipher({ key, keyId, namespace }) {
  check(Buffer.isBuffer(key) && key.length === 32 && typeof keyId === 'string' && /^[A-Za-z0-9_.-]{1,64}$/u.test(keyId)
    && typeof namespace === 'string' && /^[a-f0-9]{64}$/u.test(namespace), 'source_session_cipher_invalid', 503);
  const secret=Buffer.from(key);let closed=false;
  const aad=id=>Buffer.from(canonical(['soty.source-session-cipher.v1',namespace,keyId,id]));
  const current=id=>check(!closed && typeof id==='string' && /^[a-f0-9]{64}$/u.test(id),'source_session_cipher_invalid',503);
  return Object.freeze({
    seal(id,input){current(id);const iv=randomBytes(12),cipher=createCipheriv('aes-256-gcm',secret,iv);cipher.setAAD(aad(id));
      const bytes=Buffer.concat([cipher.update(JSON.stringify(jsonCopy(input))),cipher.final()]);
      return Buffer.concat([iv,cipher.getAuthTag(),bytes]).toString('base64url');},
    open(id,ciphertext,storedKeyId){current(id);check(storedKeyId===keyId,'source_session_key_unavailable',503);
      try{check(typeof ciphertext==='string' && /^[A-Za-z0-9_-]+$/u.test(ciphertext));const bytes=Buffer.from(ciphertext,'base64url');
        check(bytes.length>=29 && bytes.toString('base64url')===ciphertext);const decipher=createDecipheriv('aes-256-gcm',secret,bytes.subarray(0,12));
        decipher.setAAD(aad(id));decipher.setAuthTag(bytes.subarray(12,28));
        return jsonCopy(JSON.parse(Buffer.concat([decipher.update(bytes.subarray(28)),decipher.final()]).toString('utf8')));
      }catch{check(false,'source_session_storage_corrupt',503);}},
    close(){closed=true;secret.fill(0);},
  });
}
