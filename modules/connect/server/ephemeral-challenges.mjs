const OPERATION=/^[a-z][a-z0-9]*(?:\.[a-z][a-zA-Z0-9]*){1,4}$/u;
const MAX_TTL=90000;
const fail=code=>{throw Object.assign(new Error(code),{code});};
const check=(value,code='connect_ephemeral_challenge_invalid')=>{if(!value)fail(code);};
const timestamp=value=>{check(Number.isSafeInteger(value)&&value>=0);return value;};
const identifier=value=>typeof value==='string'&&/^[A-Za-z0-9_-]{3,160}$/u.test(value);

/** This runs before the host accepts traffic, outside any Core transaction.
 * secure_delete handles current cells; VACUUM is also required when an older
 * cleanup already moved sensitive cells to the freelist. A successful TRUNCATE
 * checkpoint is mandatory: a busy old reader still retains old WAL frames.
 * This proves erasure in SQLite's current files, not storage-device forensics
 * or copies outside the caller's database. The caller owns DB shutdown on error. */
function eraseLegacy(db,operations){
  let timeout,failed=false;
  try{
    check(!db.isTransaction,'connect_ephemeral_erasure_failed');
    timeout=db.prepare('PRAGMA busy_timeout').get().timeout;
    check(Number.isSafeInteger(timeout)&&timeout>=0&&timeout<=2147483647,'connect_ephemeral_erasure_failed');
    db.exec('PRAGMA busy_timeout=0; PRAGMA secure_delete=ON');
    check(db.prepare('PRAGMA secure_delete').get().secure_delete===1,'connect_ephemeral_erasure_failed');
    const mode=db.prepare('PRAGMA journal_mode').get().journal_mode;
    check(mode==='wal'||mode==='memory','connect_ephemeral_erasure_failed');
    db.exec('BEGIN IMMEDIATE');
    try{
      const remove=db.prepare('DELETE FROM challenges WHERE operation=?');for(const operation of operations)remove.run(operation);
      db.exec('COMMIT');
    }catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}
    db.exec('VACUUM');
    const result=db.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get();
    check(result?.busy===0&&(mode==='wal'?result.log===0&&result.checkpointed===0:result.log===-1&&result.checkpointed===-1),'connect_ephemeral_erasure_failed');
    check(db.prepare('PRAGMA secure_delete').get().secure_delete===1,'connect_ephemeral_erasure_failed');
  }catch{failed=true;}
  finally{
    if(timeout!==undefined)try{db.exec(`PRAGMA busy_timeout=${timeout}`);}catch{failed=true;}
  }
  if(failed)fail('connect_ephemeral_erasure_failed');
}

/** Fixed operation policy, bounded RAM, and no persistent fallback. Challenge
 * contents are never written to SQLite after the startup erasure gate. */
export function createEphemeralChallengeStore({db,operations,maxEntries=4096,clock=Date.now}={}){
  check(db&&typeof db.prepare==='function'&&typeof db.exec==='function'&&operations instanceof Set
    &&operations.size<=4096&&typeof clock==='function'&&Number.isSafeInteger(maxEntries)&&maxEntries>=1&&maxEntries<=65536,'connect_ephemeral_configuration_invalid');
  const policy=new Set(operations);
  check([...policy].every(operation=>typeof operation==='string'&&operation.length<=100&&OPERATION.test(operation)),'connect_ephemeral_configuration_invalid');
  if(policy.size)eraseLegacy(db,policy);
  const rows=new Map();let closed=false;
  const active=()=>check(!closed,'connect_ephemeral_challenge_closed');
  const now=()=>timestamp(clock());
  function cleanup(at){active();timestamp(at);for(const[id,row]of rows)if(row.expires_at<=at)rows.delete(id);}
  return Object.freeze({
    hasOperation:operation=>policy.has(operation),
    issue(row){
      active();const at=now();cleanup(at);
      check(row&&typeof row==='object'&&!Array.isArray(row)&&Object.keys(row).sort().join(',')==='consumed_at,digest,expires_at,id,message,operation,origin'
        &&identifier(row.id)&&policy.has(row.operation)&&typeof row.origin==='string'&&row.origin.length>0&&Buffer.byteLength(row.origin)<=2048
        &&typeof row.digest==='string'&&/^[A-Za-z0-9_-]{43}$/u.test(row.digest)&&Buffer.from(row.digest,'base64url').length===32&&Buffer.from(row.digest,'base64url').toString('base64url')===row.digest
        &&typeof row.message==='string'&&row.message.length>0&&Buffer.byteLength(row.message)<=8192
        &&Number.isSafeInteger(row.expires_at)&&row.expires_at>at&&row.expires_at-at<=MAX_TTL&&row.consumed_at===null);
      check(!rows.has(row.id),'connect_ephemeral_challenge_conflict');check(rows.size<maxEntries,'connect_ephemeral_challenge_capacity');
      rows.set(row.id,{id:row.id,origin:row.origin,operation:row.operation,digest:row.digest,message:row.message,expires_at:row.expires_at,consumed_at:null});
    },
    get(id){active();check(identifier(id));cleanup(now());const row=rows.get(id);return row?{...row}:undefined;},
    consume(id,at){active();check(identifier(id));cleanup(timestamp(at));const row=rows.get(id);if(!row||row.consumed_at!==null)return false;row.consumed_at=at;return true;},
    cleanup,
    close(){if(closed)return;closed=true;rows.clear();},
    readiness:()=>Object.freeze({schema:'soty.connect-ephemeral-challenges.v1',ready:!closed}),
  });
}
