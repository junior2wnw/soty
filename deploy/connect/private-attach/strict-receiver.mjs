// Current Root SOTYBAK1 extractor only. This port grants no host or STOP authority.
import { constants } from 'node:fs';
import { open, readFile, lstat, realpath, readdir, stat } from 'node:fs/promises';
import { PassThrough, Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { dataRecord } from './vendor/warm/wire-protocol.mjs';
import { extractOwnedBackup } from '../restore-backup.mjs';
import { RECEIVER_LIMITS } from './receiver-limits.mjs';

const fail = code => { throw Object.assign(new Error(code), {code}); };
const ID = /^[a-f0-9]{32}$/u, HEX = /^[a-f0-9]{64}$/u;
const ROOTS = ['/owned','/owned/target','/owned/target/data','/owned/target/config'];
const decode = value => value.replace(/\\([0-7]{3})/gu, (_, n) => String.fromCharCode(parseInt(n,8)));
async function pin(path) {
  const handle = await open(path, constants.O_RDONLY|constants.O_DIRECTORY|constants.O_NOFOLLOW);
  try {
    const value = await handle.stat({bigint:true});
    const info = await readFile(`/proc/self/fdinfo/${handle.fd}`, 'utf8');
    const rows = [...info.matchAll(/^mnt_id:\s*([1-9]\d*)$/gmu)];
    if (rows.length!==1 || !Number.isSafeInteger(Number(rows[0][1]))) fail('receiver_target_invalid');
    return {path,dev:value.dev,ino:value.ino,mountId:Number(rows[0][1])};
  } finally { await handle.close(); }
}
export async function receiveStrictBackup(value, payload) {
  if (process.platform!=='linux' || typeof process.geteuid!=='function') fail('receiver_linux_required');
  const control = dataRecord(value,['targetId','expectedManifestSha256','sourceWitness']);
  const witness = dataRecord(control.sourceWitness,['generationId','checkpointSha256','inventorySha256']);
  if (typeof control.targetId!=='string' || typeof control.expectedManifestSha256!=='string'
    || typeof witness.generationId!=='string' || typeof witness.checkpointSha256!=='string' || typeof witness.inventorySha256!=='string'
    || !ID.test(control.targetId) || !HEX.test(control.expectedManifestSha256)
    || !ID.test(witness.generationId) || !HEX.test(witness.checkpointSha256) || !HEX.test(witness.inventorySha256)
    || !(payload instanceof Readable)) fail('receiver_control_invalid');
  const uid = BigInt(process.geteuid());
  for (const path of ROOTS) {
    const info = await lstat(path,{bigint:true});
    if (!info.isDirectory() || info.isSymbolicLink() || info.uid!==uid
      || (info.mode&0o7777n)!==0o700n || await realpath(path)!==path) fail('receiver_target_invalid');
  }
  const rows = (await readFile('/proc/self/mountinfo','utf8')).split('\n').filter(Boolean).map(line => {
    const parts=line.split(' '),at=parts.indexOf('-');
    if (at<6) fail('receiver_topology_invalid');
    return {id:Number(parts[0]),path:decode(parts[4]),options:parts[5].split(','),type:parts[at+1]};
  });
  for (const [path,type] of [['/owned','tmpfs'],['/owned/target/config','tmpfs'],['/owned/target/data',null]]) {
    const found=rows.filter(row=>row.path===path);
    if (found.length!==1 || !found[0].options.includes('rw')
      || (type && (found[0].type!==type || !found[0].options.includes('noexec') || !found[0].options.includes('nosuid')))
      || (!type && ['tmpfs','ramfs','overlay','aufs','rootfs'].includes(found[0].type))) fail('receiver_topology_invalid');
  }
  if ((await readdir('/owned')).sort().join(',')!=='target'
    || (await readdir('/owned/target')).sort().join(',')!=='config,data'
    || (await readdir('/owned/target/data')).length || (await readdir('/owned/target/config')).length)
    fail('receiver_target_contaminated');
  const mnt=await stat('/proc/self/ns/mnt',{bigint:true});
  const target={targetId:control.targetId,mountNamespace:{dev:mnt.dev,ino:mnt.ino},
    namespace:await pin('/owned/target'),dataRoot:await pin('/owned/target/data'),configRoot:await pin('/owned/target/config')};
  const input=new PassThrough({highWaterMark:65536,objectMode:false,autoDestroy:true,emitClose:true});
  const extraction=extractOwnedBackup({input,target,expectedManifestSha256:control.expectedManifestSha256,
    sourceWitness:witness,limits:RECEIVER_LIMITS});
  const feeding=pipeline(payload,input); feeding.catch(()=>{});
  try { const receipt=await extraction; await feeding; return receipt; }
  catch(error) { input.destroy(); await feeding.catch(()=>{}); throw error; }
}
