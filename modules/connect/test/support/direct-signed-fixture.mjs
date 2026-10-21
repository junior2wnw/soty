import assert from 'node:assert/strict';
import { generateKeyPairSync, sign } from 'node:crypto';
import { createConnectService, digestArgs } from '../../server/index.mjs';
export const PROJECT = 'direct-contract-test', ORIGIN = 'https://direct-contract.test';
export function signedFixture(t, extensions = []) {
  let at = 1_800_000_000_000;
  const service = createConnectService({databasePath:':memory:',projectId:PROJECT,allowedOrigins:[ORIGIN],clock:()=>at,extensions});
  t.after(()=>service.close());
  const good = reply => { assert.equal(reply.ok,true,reply.error?.code); return reply; };
  async function raw(actor,op,args={}) {
    const challenge = good(await service.handle({op:'challenge',origin:ORIGIN,args:{operation:op,digest:digestArgs(args)}}));
    return service.handle({op,args,origin:ORIGIN,proof:{challengeId:challenge.challengeId,publicJwk:actor.publicJwk,
      signature:sign('sha256',Buffer.from(challenge.message),{key:actor.privateKey,dsaEncoding:'ieee-p1363'}).toString('base64url')}});
  }
  const call = async(actor,op,args={})=>good(await raw(actor,op,args));
  function device(label) {
    const key = generateKeyPairSync('ec',{namedCurve:'prime256v1'}), encryption = generateKeyPairSync('ec',{namedCurve:'prime256v1'});
    return {privateKey:key.privateKey,publicJwk:key.publicKey.export({format:'jwk'}),encryptionPublicJwk:encryption.publicKey.export({format:'jwk'}),label};
  }
  async function person(label) {
    const actor=device(label);
    const reply = await call(actor,'bootstrap',{label,encryptionPublicJwk:actor.encryptionPublicJwk});
    return {...actor,accountId:reply.accountId,deviceId:reply.deviceId};
  }
  async function enroll(owner,label) {
    const actor=device(label),start=await call(actor,'enrollment.start',{label,encryptionPublicJwk:actor.encryptionPublicJwk});
    await call(owner,'enrollment.approve',{requestId:start.requestId,wrappedKey:{schema:'test.encrypted',ciphertext:'synthetic'}});
    const reply=await call(actor,'enrollment.finish',{requestId:start.requestId,expectedAccountId:owner.accountId});
    return {...actor,accountId:reply.accountId,deviceId:reply.deviceId};
  }
  async function contacts(left,right) {
    const card = await call(right,'card.get'), request = await call(left,'contacts.request',{cardId:card.cardId});
    return (await call(right,'contacts.accept',{requestId:request.requestId})).relationshipId;
  }
  return {service,call,raw,person,enroll,contacts,now:()=>at,advance:()=>{at+=61_000;}};
}
