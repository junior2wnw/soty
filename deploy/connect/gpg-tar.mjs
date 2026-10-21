import { createHash } from 'node:crypto';
import { Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { spawn } from 'node:child_process';
import { constants,createReadStream,createWriteStream } from 'node:fs';
import { open,lstat,mkdtemp,chmod,rm,link,unlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

export const GPG_LIMITS=Object.freeze({cipherBytes:5*1024**3,plainBytes:5*1024**3,fileBytes:1024**3,totalBytes:4*1024**3,entries:100000,pathBytes:8*1024**2,pathDepth:32,wallMs:1200000});
export const bad=code=>{throw Object.assign(new Error(code),{code,stack:'Error: '+code});};
export const sha=value=>createHash('sha256').update(value).digest('hex');
export function limits(input={}){const value={...GPG_LIMITS,...input};if(Object.keys(input).some(k=>!Object.hasOwn(GPG_LIMITS,k))||Object.values(value).some(n=>!Number.isSafeInteger(n)||n<1))bad('backup_limits_invalid');return value;}
export function keyBytes(value){if(!Buffer.isBuffer(value)||value.length!==64||value.some(b=>b>127)||!/^[A-Za-z0-9_-]{64}$/.test(value.toString('ascii')))bad('backup_key_invalid');return value;}
const utf8=new TextDecoder('utf-8',{fatal:true});
const TAR_MAGIC=Buffer.from('ustar\0','ascii'),TAR_VERSION=Buffer.from('00','ascii');
function text(bytes){const end=bytes.indexOf(0);if(end>=0&&!bytes.subarray(end).every(x=>x===0))bad('backup_tar_invalid');try{return utf8.decode(end<0?bytes:bytes.subarray(0,end));}catch{bad('backup_tar_invalid');}}
function number(bytes){if(bytes[0]&128){if(bytes[0]&64)bad('backup_tar_invalid');let n=BigInt(bytes[0]&127);for(const b of bytes.subarray(1))n=(n<<8n)|BigInt(b);if(n>BigInt(Number.MAX_SAFE_INTEGER))bad('backup_tar_invalid');return Number(n);}if(bytes.some(b=>b>127))bad('backup_tar_invalid');const match=/^[ \x00]*([0-7]*)[ \x00]*$/.exec(bytes.toString('ascii'));if(!match)bad('backup_tar_invalid');const n=match[1]?parseInt(match[1],8):0;if(!Number.isSafeInteger(n))bad('backup_tar_invalid');return n;}
function decimal(value){if(!/^(?:0|[1-9][0-9]{0,15})$/.test(value)||!Number.isSafeInteger(Number(value)))bad('backup_tar_invalid');return Number(value);}
function time(value){if(!/^-?[0-9]{1,16}(?:\.[0-9]{1,9})?$/.test(value))bad('backup_tar_invalid');const negative=value.startsWith('-'),[s,f='']=value.replace(/^-/,'').split('.');return ((BigInt(s)*1000000000n+BigInt(f.padEnd(9,'0')))*(negative?-1n:1n)).toString();}
function name(value,directory,cap){if(Buffer.byteLength(value)>4096||/[\u0000-\u001f\u007f-\u009f\\]/u.test(value)||value.startsWith('/')||/^[A-Za-z]:/.test(value)||value.includes('//'))bad('backup_tar_invalid');while(value.startsWith('./'))value=value.slice(2);if(directory&&value.endsWith('/'))value=value.slice(0,-1);if(directory&&value==='.')value='';const parts=value?value.split('/'):[];if(parts.some(p=>!p||p==='.'||p==='..')||(!directory&&!parts.length))bad('backup_tar_invalid');if(parts.length>cap.pathDepth||parts.some(p=>Buffer.byteLength(p)>255))bad('backup_limit_exceeded');return value;}
function pax(bytes){const result=Object.create(null);for(let at=0;at<bytes.length;){const space=bytes.indexOf(32,at),digits=space<0?Buffer.alloc(0):bytes.subarray(at,space);if(!digits.length||digits.length>6||digits[0]===48||!digits.every(b=>b>=48&&b<=57))bad('backup_tar_invalid');const end=at+Number(digits.toString('ascii'));if(end>bytes.length||end<=space+2||bytes[end-1]!==10)bad('backup_tar_invalid');let s;try{s=utf8.decode(bytes.subarray(space+1,end-1));}catch{bad('backup_tar_invalid');}const eq=s.indexOf('='),k=s.slice(0,eq);if(eq<1||!['path','size','uid','gid','mtime','atime','ctime'].includes(k)||Object.hasOwn(result,k))bad('backup_tar_invalid');const value=s.slice(eq+1);if(['size','uid','gid'].includes(k))decimal(value);if(['mtime','atime','ctime'].includes(k))time(value);result[k]=value;at=end;}return result;}

/** Streaming validator only. No path or file content is emitted by receipts. */
export class TarInventory extends Transform{
  constructor(cap={}){super();this.cap=limits(cap);this.pending=Buffer.alloc(0);this.body=0;this.padding=0;this.current=null;this.ext=null;this.zeros=0;this.bytes=0;this.dataBytes=0;this.paths=0;this.headers=0;this.entries=new Map();this.hash=createHash('sha256');}
  _transform(chunk,_encoding,callback){try{this.feed(chunk);callback(null,chunk);}catch(error){callback(error);}}
  feed(chunk){this.bytes+=chunk.length;if(this.bytes>this.cap.plainBytes)bad('backup_limit_exceeded');this.hash.update(chunk);let at=0;
    while(at<chunk.length){
      if(this.body){const n=Math.min(this.body,chunk.length-at),piece=chunk.subarray(at,at+n);if(this.current.type==='x')this.current.pieces.push(Buffer.from(piece));else this.current.digest.update(piece);this.body-=n;at+=n;if(!this.body){if(this.current.type==='x')this.ext=pax(Buffer.concat(this.current.pieces));else this.current.record.sha256=this.current.digest.digest('hex');this.current=null;}continue;}
      if(this.padding){const n=Math.min(this.padding,chunk.length-at);if(!chunk.subarray(at,at+n).every(x=>x===0))bad('backup_tar_invalid');this.padding-=n;at+=n;continue;}
      const n=Math.min(512-this.pending.length,chunk.length-at);this.pending=Buffer.concat([this.pending,chunk.subarray(at,at+n)]);at+=n;if(this.pending.length!==512)continue;
      const h=this.pending;this.pending=Buffer.alloc(0);if(h.every(x=>x===0)){this.zeros++;continue;}if(this.zeros)bad('backup_tar_invalid');
      if(++this.headers>this.cap.entries*2+1)bad('backup_limit_exceeded');let sum=0;for(let i=0;i<512;i++)sum+=i>=148&&i<156?32:h[i];if(number(h.subarray(148,156))!==sum||!h.subarray(257,263).equals(TAR_MAGIC)||!h.subarray(263,265).equals(TAR_VERSION)||!h.subarray(157,257).every(x=>x===0))bad('backup_tar_invalid');
      const type=String.fromCharCode(h[156]||48);let size=number(h.subarray(124,136));
      if(type==='x'){if(this.ext||size<1||size>65536)bad('backup_tar_invalid');this.current={type,pieces:[]};}
      else{
        if(!['0','5'].includes(type))bad('backup_tar_invalid');const e=this.ext??Object.create(null);this.ext=null;
        // An x header's name is not an extracted path. GNU tar may truncate its
        // placeholder inside a UTF8 sequence. A validated PAX path likewise
        // supersedes the following USTAR name/prefix; decode only a name that
        // the extractor actually consumes, while keeping all path checks.
        let actual=e.path;
        if(actual===undefined){const raw=text(h.subarray(0,100)),prefix=text(h.subarray(345,500));actual=prefix?prefix+'/'+raw:raw;}
        const filename=name(actual,type==='5',this.cap),uid=e.uid===undefined?number(h.subarray(108,116)):decimal(e.uid),gid=e.gid===undefined?number(h.subarray(116,124)):decimal(e.gid),mode=number(h.subarray(100,108));
        if(e.size!==undefined)size=decimal(e.size);if(uid>4294967294||gid>4294967294||mode>0o777||type==='5'&&size!==0||size>this.cap.fileBytes)bad('backup_tar_invalid');
        if(this.entries.has(filename)||this.entries.size>=this.cap.entries)bad('backup_tar_invalid');this.paths+=Buffer.byteLength(filename);this.dataBytes+=size;if(this.paths>this.cap.pathBytes||this.dataBytes>this.cap.totalBytes)bad('backup_limit_exceeded');
        const record={path:filename,type:type==='5'?'directory':'file',size,uid,gid,mode,mtime:time(e.mtime??String(number(h.subarray(136,148)))),sha256:type==='5'?null:sha(Buffer.alloc(0))};this.entries.set(filename,record);
        if(size)this.current={type,record,digest:createHash('sha256')};
      }
      this.body=size;this.padding=(512-size%512)%512;
    }
  }
  finish(){if(this.body||this.padding||this.pending.length||this.ext||this.zeros<2||this.entries.get('')?.type!=='directory')bad('backup_tar_invalid');
    const records=[...this.entries.values()].sort((a,b)=>a.path<b.path?-1:a.path>b.path?1:0);const types=new Map(records.map(r=>[r.path,r.type]));
    for(const r of records){const parts=r.path.split('/');while(parts.length>1){parts.pop();if(types.get(parts.join('/'))!=='directory')bad('backup_tar_invalid');}}
    return {entries:records.length,fileBytes:this.dataBytes,plainBytes:this.bytes,plainSha256:this.hash.digest('hex'),inventorySha256:sha(JSON.stringify(records))};}
}

const common=['--no-options','--batch','--yes','--pinentry-mode','loopback','--no-symkey-cache','--passphrase-fd','3','--status-fd','4'];
async function gpg(key,cap,mode,operation){if(process.platform!=='linux')bad('backup_platform_unavailable');keyBytes(key);const home=await mkdtemp(path.join(tmpdir(),'soty-gpg-owned-'));await chmod(home,0o700);const stamp=await lstat(home);let child,timer,status='',statusOverflow=false;
  try{
    const args=[...common,'--homedir',home,...(mode==='encrypt'?['--symmetric','--cipher-algo','AES256','--force-mdc','--compress-algo','none','--s2k-mode','3','--s2k-digest-algo','SHA256','--s2k-count','65011712']:['--decrypt']),'--output','-'];
    child=spawn('/usr/bin/gpg',args,{stdio:['pipe','pipe','ignore','pipe','pipe']});
    const closed=new Promise((resolve,reject)=>{child.once('error',()=>reject(Error('gpg_failed')));child.once('close',code=>code===0?resolve():reject(Error('gpg_failed')));});closed.catch(()=>{});
    child.stdio[4].on('data',chunk=>{if(status.length+chunk.length>65536){statusOverflow=true;child.kill('SIGKILL');}else status+=chunk.toString('ascii');});
    child.stdio[3].on('error',()=>{});child.stdio[3].end(Buffer.concat([key,Buffer.from('\n')]));
    timer=setTimeout(()=>child.kill('SIGKILL'),cap.wallMs);
    let result;try{result=await operation(child);await closed;}catch{child.kill('SIGKILL');await closed.catch(()=>{});bad(mode==='decrypt'?'backup_authentication_failed':'backup_encryption_failed');}
    const tokens=status.split(/\r?\n/).filter(s=>s.startsWith('[GNUPG:] ')).map(s=>s.slice(9));
    if(statusOverflow||mode==='decrypt'&&(!tokens.includes('DECRYPTION_OKAY')||!tokens.includes('GOODMDC')||!tokens.includes('END_DECRYPTION')||!tokens.some(s=>/^DECRYPTION_INFO 2 9(?: 0)?$/.test(s)))||mode==='encrypt'&&!tokens.includes('END_ENCRYPTION'))bad('backup_authentication_failed');
    return result;
  }finally{clearTimeout(timer);if(child&&!child.killed)child.kill('SIGKILL');
    await new Promise(resolve=>{const stop=spawn('/usr/bin/gpgconf',['--homedir',home,'--kill','gpg-agent'],{stdio:'ignore',timeout:5000});stop.once('error',resolve);stop.once('close',resolve);});
    const now=await lstat(home);if(now.isDirectory()&&!now.isSymbolicLink()&&now.ino===stamp.ino&&now.dev===stamp.dev&&path.dirname(home)===tmpdir()&&path.basename(home).startsWith('soty-gpg-owned-'))await rm(home,{recursive:true,force:true});
  }
}
export async function fileDigest(file,cap={}){const value=limits(cap),handle=await open(file,constants.O_RDONLY|constants.O_NOFOLLOW);try{const stat=await handle.stat({bigint:true});if(!stat.isFile()||stat.size>BigInt(value.cipherBytes))bad('backup_artifact_invalid');const h=createHash('sha256');let bytes=0;for await(const chunk of createReadStream(null,{fd:handle.fd,autoClose:false,start:0})){bytes+=chunk.length;if(bytes>value.cipherBytes)bad('backup_limit_exceeded');h.update(chunk);}const after=await handle.stat({bigint:true});if(BigInt(bytes)!==stat.size||stat.size!==after.size||stat.mtimeNs!==after.mtimeNs||stat.ctimeNs!==after.ctimeNs)bad('backup_artifact_changed');return {sha256:h.digest('hex'),bytes};}finally{await handle.close();}}
export async function encryptTar({file,key,input,cap={}}){const value=limits(cap),partial=file+'.partial',inventory=new TarInventory(value);let result;
  try{result=await gpg(key,value,'encrypt',async child=>{let bytes=0;const bound=new Transform({transform(chunk,_encoding,done){bytes+=chunk.length;done(bytes>value.cipherBytes?Error('cipher_limit'):null,chunk);}});const toGpg=pipeline(input,inventory,child.stdin),toDisk=pipeline(child.stdout,bound,createWriteStream(partial,{flags:'wx',mode:0o600}));
      const all=await Promise.allSettled([toGpg,toDisk]);if(all.some(r=>r.status==='rejected'))bad('backup_encryption_failed');return inventory.finish();});
    const f=await open(partial,'r+');await f.sync();await f.close();await link(partial,file);await unlink(partial);const parent=await open(path.dirname(file),'r');try{await parent.sync();}finally{await parent.close();}return {...result,...await fileDigest(file,value),encrypted:true};
  }catch{bad('backup_encryption_failed');}
}
/** First pass never writes plaintext. Optional second-pass sink must be an
 * already-created owned isolated target; caller pins the same ciphertext hash. */
export async function verifyTar({file,key,expectedSha256,cap={},sink}){const value=limits(cap);if(!/^[a-f0-9]{64}$/.test(expectedSha256??''))bad('backup_artifact_invalid');const before=await fileDigest(file,value);if(before.sha256!==expectedSha256)bad('backup_artifact_changed');
  const inventory=new TarInventory(value);const result=await gpg(key,value,'decrypt',async child=>{
    const source=createReadStream(file,{flags:constants.O_RDONLY|constants.O_NOFOLLOW}),cipherHash=createHash('sha256');let cipherBytes=0;
    const tap=new Transform({transform(chunk,_encoding,done){cipherBytes+=chunk.length;if(cipherBytes>value.cipherBytes)return done(Error('cipher_limit'));cipherHash.update(chunk);done(null,chunk);}});
    const input=pipeline(source,tap,child.stdin).then(()=>{if(cipherHash.digest('hex')!==expectedSha256)bad('backup_artifact_changed');});
    const output=(async()=>{for await(const chunk of child.stdout){inventory.feed(chunk);if(sink)await sink(chunk);}return inventory.finish();})();
    const all=await Promise.allSettled([input,output]);if(all.some(r=>r.status==='rejected'))bad('backup_archive_invalid');return all[1].value;});
  const after=await fileDigest(file,value);if(after.sha256!==expectedSha256||after.bytes!==before.bytes)bad('backup_artifact_changed');return {...result,...after,authenticated:true};}
