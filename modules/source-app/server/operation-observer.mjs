import {createHmac,timingSafeEqual,randomBytes} from 'node:crypto';
import {lstatSync,realpathSync,writeFileSync,readFileSync,renameSync,unlinkSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {canonical,check,fields} from './wire.mjs';
import {selectedResourceProfile} from '../../apps/scoped-embed/resource-profile.mjs';
import {parseSourceJson} from '../shared/strict-json.mjs';

const observations=new WeakSet();
// Internal producer, deliberately absent from the public package exports.
// A copied DTO/file is a historical record, never a new live SDK observation.
export function sourceVerifiedOperation(record){observations.add(record);return record;}

/** Historical diagnostics, never an authentication cache or an authorization
 * input. This separate purpose-sealed record contains no human/data hashes. */
export function createSourceReadinessObserver({directory,profile:raw,key}){
  check(Buffer.isBuffer(key)&&key.length===32&&resolve(directory)===directory,'source_readiness_configuration_invalid',503);
  const profile=selectedResourceProfile(raw);let closed=false,writeStatus='not_attempted';
  const dir=lstatSync(directory);check(dir.isDirectory()&&!dir.isSymbolicLink()&&realpathSync(directory)===directory
    &&(process.platform!=='linux'||dir.uid===process.getuid()&&(dir.mode&0o777)===0o700),'source_readiness_directory_denied',503);
  const secret=Buffer.from(key);
  const path=join(directory,'source-readiness.json');
  const mac=record=>createHmac('sha256',secret).update('soty.source-readiness-observation.v1\0').update(canonical(record)).digest('hex');
  function valid(record){const value=fields(record,['schema','appId','profileDigest','target','sourceProfile','observedAt','operation','status']);
    check(value.schema==='soty.source-readiness.v1'&&value.appId===profile.appId&&value.profileDigest===profile.digest
      &&canonical(value.target)===canonical(profile.target)&&canonical(value.sourceProfile)===canonical(profile.sourceProfile)
      &&Number.isSafeInteger(value.observedAt)&&value.observedAt>0,'source_readiness_invalid');
    const statuses={continue:['ready'],read:['context_verified','read_verified'],execute:['commit_verified','read_verified'],readProof:['receipt_verified','read_verified']};
    check(statuses[value.operation]?.includes(value.status),'source_readiness_invalid');return value;
  }
  return Object.freeze({
    observe(input){let temporary;try{
      check(!closed&&observations.has(input),'source_readiness_observation_required');const record=valid(input);
      temporary=path+'.'+process.pid+'.'+randomBytes(8).toString('hex')+'.tmp';
      writeFileSync(temporary,JSON.stringify({schema:'soty.source-readiness-file.v1',record,mac:mac(record)})+'\n',{flag:'wx',mode:0o600});
      renameSync(temporary,path);temporary=undefined;writeStatus='recorded';
    }catch{writeStatus='observation_unavailable';}finally{if(temporary)try{unlinkSync(temporary);}catch{}}},
    inspect(){
      let lastObservation=null,integrity='missing';
      try{check(!closed,'source_readiness_closed');const stat=lstatSync(path);check(stat.isFile()&&!stat.isSymbolicLink()&&stat.size<=4096
        &&(process.platform!=='linux'||stat.uid===process.getuid()&&(stat.mode&0o777)===0o600),'source_readiness_invalid');
        const envelope=fields(parseSourceJson(new TextDecoder('utf-8',{fatal:true}).decode(readFileSync(path)),{bytes:4096}),['schema','record','mac']);
        check(envelope.schema==='soty.source-readiness-file.v1'&&typeof envelope.mac==='string'&&/^[a-f0-9]{64}$/u.test(envelope.mac),'source_readiness_invalid');
        const record=valid(envelope.record);check(timingSafeEqual(Buffer.from(envelope.mac,'hex'),Buffer.from(mac(record),'hex')),'source_readiness_invalid');
        integrity='verified';lastObservation=record;
      }catch(error){if(error.code!=='ENOENT')integrity='invalid';}
      return{schema:'soty.source-doctor.v1',basis:'last_verified_server_observation',integrity,lastObservation,writeStatus,
        currentAuthority:'not_checked',clientAcknowledgement:'not_observed',connected:false,releaseReady:false};
    },
    close(){closed=true;secret.fill(0);},
  });
}
