import {DatabaseSync} from 'node:sqlite';
import {createDecipheriv} from 'node:crypto';
import {check,fields} from '../server/wire.mjs';
import {verifyOrdinaryReader3} from '../examples/ordinary-app/reader3.mjs';

/** Read-only startup key correctness, not Source permission or full integrity.
 * Known Native columns/AAD only; no DDL, healing, token return or network. */
export function probeInstalledSourceKey(input){
  const value=fields(input,['databasePath','realmId','keyId','cipherKey']);
  check(typeof value.databasePath==='string'&&typeof value.realmId==='string'&&typeof value.keyId==='string'
    &&Buffer.isBuffer(value.cipherKey)&&value.cipherKey.length===32,'source_install_key_probe_denied',503);
  const db=new DatabaseSync(value.databasePath,{readOnly:true}),key=Buffer.from(value.cipherKey);let checkedModels=0;
  try{
    db.exec('PRAGMA busy_timeout=5000; BEGIN');
    const reader=verifyOrdinaryReader3(db,value.realmId);
    const models=[['source_interactions','Interaction','id_hash','revision'],['source_sessions','Session','id_hash',null],
      ...(reader.format>=2?[['native_login_proofs','NativeLoginProof','id_hash',null]]:[]),
      ...(reader.format>=3?[['native_processor_grants','ProcessorGrant','id',null],['native_processor_receipts','ProcessorReceipt','job_id',null]]:[])];
    for(const [table,model,id,revision] of models){
      check(!db.prepare('SELECT 1 FROM '+table+' WHERE key_id<>? OR key_id IS NULL LIMIT 1').get(value.keyId),'source_install_key_probe_denied',503);
      // One bounded actual ciphertext per model suffices for this key. Full
      // record integrity continues to be checked by each Native decoder/read.
      const row=db.prepare('SELECT '+id+' AS id,'+(revision??'0')+' AS revision,cipher,key_id FROM '+table+' ORDER BY '+id+' LIMIT 1').get();if(!row)continue;
      check(typeof row.cipher==='string'&&row.cipher.length<=524288&&Number.isSafeInteger(row.revision)&&row.revision>=0,'source_install_key_probe_denied',503);
      let encrypted,plain;try{encrypted=Buffer.from(row.cipher,'base64');check(encrypted.length>28&&encrypted.toString('base64')===row.cipher,'source_install_key_probe_denied',503);
        const decoder=createDecipheriv('aes-256-gcm',key,encrypted.subarray(0,12));decoder.setAAD(Buffer.from(JSON.stringify(['soty.ordinary-source.v1',value.realmId,model,row.id,row.revision,value.keyId])));
        decoder.setAuthTag(encrypted.subarray(12,28));plain=Buffer.concat([decoder.update(encrypted.subarray(28)),decoder.final()]);
        check(plain.length<=262144,'source_install_key_probe_denied',503);JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(plain));checkedModels++;
      }catch{check(false,'source_install_key_probe_denied',503);}finally{encrypted?.fill(0);plain?.fill(0);}
    }
    db.exec('COMMIT');return Object.freeze({schema:'soty.source-key-probe.v1',readOnly:true,keyIdsMatch:true,checkedModels,
      keyCorrectness:checkedModels?'proved':'unproved_empty',permissionProved:false});
  }finally{try{if(db.isTransaction)db.exec('ROLLBACK');}finally{key.fill(0);db.close();}}
}
