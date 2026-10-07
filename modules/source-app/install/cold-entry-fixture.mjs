// Fixed Source-image installer bytes. A live listener plus anonymous denial
// is runtime entry evidence only, not current Root/Human/Native permission.
import {spawn} from 'node:child_process';
import {mkdir,writeFile,readFile} from 'node:fs/promises';
import {request} from 'node:http';
import {randomBytes} from 'node:crypto';
const children=[];
function start(args){const child=spawn(process.execPath,args,{stdio:['ignore','pipe','ignore'],env:{PATH:'/usr/bin:/bin'},windowsHide:true});let text='',closed=false;
  const done=new Promise((resolve,reject)=>{child.once('error',()=>reject(Error('source_entry_failed')));child.stdout.on('data',part=>{text+=part;if(text.length>4096)child.kill('SIGKILL');});
    child.once('close',code=>{closed=true;resolve({code,text});});});done.catch(()=>{});
  const item={child,done,text:()=>text,closed:()=>closed};children.push(item);return item;}
async function stop(item){if(!item.closed())item.child.kill('SIGTERM');const timer=setTimeout(()=>{if(!item.closed())item.child.kill('SIGKILL');},1000);try{await item.done;}finally{clearTimeout(timer);}}
const pause=ms=>new Promise(done=>setTimeout(done,ms));
try{
  if(process.platform!=='linux'||process.getuid()!==1000)throw Error('platform');process.umask(0o077);
  const reader=start(['/app/source-app/reader.mjs','/data/native/native.sqlite','cold-synthetic']);const valid=await reader.done;
  if(valid.code!==0||JSON.parse(valid.text).format!==3)throw Error('reader');
  const old=start(['/app/source-app/reader2.mjs','/data/native/native.sqlite','cold-synthetic']);if((await old.done).code!==78)throw Error('old');
  const foreign=start(['/app/source-app/reader.mjs','/data/native/native.sqlite','foreign-cold-realm']);if((await foreign.done).code!==78)throw Error('realm');
  const cfg=JSON.parse(await readFile('/data/install/operator.json','utf8'));
  await mkdir('/tmp/source-negative',{mode:0o700});await mkdir('/tmp/source-negative/secrets',{mode:0o700});
  await writeFile('/tmp/source-negative/operator.json',JSON.stringify(cfg),{mode:0o600});
  for(const file of ['transport.key','client.key'])await writeFile('/tmp/source-negative/secrets/'+file,await readFile('/data/install/secrets/'+file),{mode:0o600});
  const missing=start(['/app/source-app/install.mjs','check','--config','/tmp/source-negative/operator.json']);const refused=await missing.done;
  if(refused.code!==1||JSON.parse(refused.text).ok!==false)throw Error('missing-key');
  await writeFile('/tmp/source-negative/secrets/cipher.key',randomBytes(32).toString('base64url')+'\n',{mode:0o600});
  const wrong=start(['/app/source-app/install.mjs','serve','--config','/tmp/source-negative/operator.json']),wrongResult=await wrong.done;
  if(wrongResult.code!==1||JSON.parse(wrongResult.text).code!=='source_install_key_probe_denied')throw Error('wrong-key');
  const serve=start(['/app/source-app/install.mjs','serve','--config','/data/install/operator.json']);const deadline=Date.now()+5000;
  while(!serve.text().includes('"listening":true')&&Date.now()<deadline&&!serve.closed())await pause(20);
  if(!serve.text().includes('"listening":true'))throw Error('entry');
  const status=await new Promise((resolve,reject)=>{const r=request({hostname:'127.0.0.1',port:5317,path:'/api/embed/context',method:'GET',agent:false,
    headers:{host:'app-'+ 'a'.repeat(32)+'.root.fixture.invalid','x-root-owner':'true','x-root-subject':'synthetic'},signal:AbortSignal.timeout(2000)},response=>{response.resume();response.once('end',()=>resolve(response.statusCode));});r.once('error',reject);r.end();});
  if(![401,403].includes(status))throw Error('anonymous');await stop(serve);
  console.log(JSON.stringify({schema:'soty.source-cold-entry.v1',passed:true,currentReader:true,legacyRefusedBeforeStart:true,foreignRealmDenied:true,
    missingKeyDenied:true,wrongKeyBeforeListenerDenied:true,actualInstallerListening:true,spoofedReadDenied:true,authenticationProved:false,productionReady:false}));
}catch{console.log(JSON.stringify({passed:false,code:'source_cold_entry_failed',productionReady:false}));process.exitCode=1;}
finally{for(const item of children.reverse())await stop(item);}
