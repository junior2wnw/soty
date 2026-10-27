import test from 'node:test';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {createServer} from 'node:http';
import {mkdtemp,mkdir,readFile,rm,writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {basename,dirname,join,resolve} from 'node:path';

const root=resolve(import.meta.dirname,'..');
const expectedVersion=JSON.parse(await readFile(join(root,'public/agent/manifest.json'),'utf8')).version;
async function freePort(){const s=createServer();await new Promise(done=>s.listen(0,'127.0.0.1',done));const p=s.address().port;await new Promise(done=>s.close(done));return p;}
async function run(runtime,dir,port,serverUrl='http://127.0.0.1:1',verify=async()=>{}){
  const systemRoot=process.env.SystemRoot||process.env.WINDIR||'C:\\Windows';
  const env={SystemRoot:systemRoot,WINDIR:systemRoot,PATH:[dirname(process.execPath),join(systemRoot,'System32')].join(process.platform==='win32'?';':':'),
    TEMP:dir,TMP:dir,USERPROFILE:dir,NODE_OPTIONS:'',SOTY_CONNECTOR_DATA_DIR:dir,SOTY_CONNECTOR_PORT:String(port),
    SOTY_CONNECTOR_SCOPE:'Dev',SOTY_CONNECTOR_MANAGED:'0',SOTY_CONNECTOR_AUTO_UPDATE:'0',SOTY_AGENT_AUTO_UPDATE:'0',
    SOTY_OPENCODE_PATH:process.execPath,SOTY_CONNECTOR_SERVER_URL:serverUrl,SOTY_CONNECTOR_UPDATE_URL:'http://127.0.0.1:1/manifest.json'};
  const child=spawn(process.execPath,[runtime],{cwd:dir,env,windowsHide:true,stdio:'ignore'});
  try{
    let health;const end=Date.now()+15000;
    while(Date.now()<end){if(child.exitCode!==null)throw Error(`synthetic_connector_exit_${child.exitCode}`);
      try{const response=await fetch(`http://127.0.0.1:${port}/health`,{signal:AbortSignal.timeout(500)});health=await response.json();if(health?.ok)break;}catch{}
      await new Promise(done=>setTimeout(done,60));}
    assert.equal(health?.connector,true);assert.equal(health.version,expectedVersion);assert.equal(Object.hasOwn(health,'integrations'),false);
    assert.equal((await fetch(`http://127.0.0.1:${port}/integrations/spreadex/v1/status`)).status,404);
    await verify();
  }finally{
    if(child.exitCode===null){const exited=new Promise(done=>child.once('exit',done));child.kill();await exited;}
  }
}

test('source and standalone bundle preserve legacy ML config and files during two starts',{timeout:120000},async()=>{
  const folder=await mkdtemp(join(tmpdir(),'soty-connector-legacy-'));
  const cases=[{name:'object',value:{mode:'off',unknown:{n:4},records:[1,'fixture']}},{name:'null',value:null},{name:'array',value:['fixture',2]},
    {name:'boolean',value:false},{name:'number',value:17},{name:'string',value:'fixture'},{name:'legacy-migration',value:{legacy:true},legacy:true},{name:'absent'}];
  try{
    for(const runtime of [join(root,'scripts/soty-connector.mjs'),join(root,'public/agent/soty-connector.mjs')]){
      for(const item of cases){
        const dir=join(folder,`${runtime.includes('public')?'bundle':'source'}-${item.name}`);await mkdir(dir);
        const config={workspaceRoot:dir,allowedRoots:[dir],deviceId:'synthetic-device',installId:'synthetic-install',
          ...(Object.hasOwn(item,'value')?{spreadexMl:item.value}:{})};
        await writeFile(join(dir,item.legacy?'agent-config.json':'connector-config.json'),JSON.stringify(config));
        const secretFile=join(dir,'spreadex-ml-secrets.json'),markerFile=join(dir,'spreadex-ml','keep.marker');
        await mkdir(dirname(markerFile));await writeFile(secretFile,'synthetic old secret file');await writeFile(markerFile,'synthetic old worker file');
        for(let i=0;i<2;i++){
          await run(runtime,dir,await freePort());
          const saved=JSON.parse(await readFile(join(dir,'connector-config.json'),'utf8'));
          assert.equal(Object.hasOwn(saved,'spreadexMl'),Object.hasOwn(item,'value'));
          if(Object.hasOwn(item,'value'))assert.deepEqual(saved.spreadexMl,item.value);
          assert.equal(await readFile(secretFile,'utf8'),'synthetic old secret file');
          assert.equal(await readFile(markerFile,'utf8'),'synthetic old worker file');
        }
      }
    }
  }finally{
    assert.equal(dirname(resolve(folder)),resolve(tmpdir()));assert.match(basename(folder),/^soty-connector-legacy-/u);
    await rm(folder,{recursive:true,force:true,maxRetries:5,retryDelay:40});
  }
});

test('standalone bundle still registers ordinary connector capabilities',{timeout:30000},async()=>{
  const folder=await mkdtemp(join(tmpdir(),'soty-connector-registration-'));let registration;
  const server=createServer(async(req,res)=>{
    const chunks=[];for await(const chunk of req)chunks.push(chunk);
    if(req.url==='/api/connectors/register'){
      const value=JSON.parse(Buffer.concat(chunks));registration={version:value.version,capabilities:value.capabilities};
    }
    if(req.url?.startsWith('/api/connectors/poll'))await new Promise(done=>setTimeout(done,100));
    res.writeHead(200,{'content-type':'application/json'}).end(JSON.stringify({ok:true,jobs:[]}));
  });
  await new Promise(done=>server.listen(0,'127.0.0.1',done));const serverUrl=`http://127.0.0.1:${server.address().port}`;
  try{
    const legacy={nested:{keep:'fixture'}};
    await writeFile(join(folder,'connector-config.json'),JSON.stringify({workspaceRoot:folder,allowedRoots:[folder],linkId:'i'.repeat(43),spreadexMl:legacy}));
    const connectorPort=await freePort();
    await run(join(root,'public/agent/soty-connector.mjs'),folder,connectorPort,serverUrl,async()=>{
      const end=Date.now()+5000;while(!registration&&Date.now()<end)await new Promise(done=>setTimeout(done,60));
      assert.equal(registration?.version,expectedVersion);assert.ok(registration.capabilities.includes('agent'));
      assert.ok(registration.capabilities.includes('local-apps'));assert.ok(!registration.capabilities.includes('spreadex-ml'));
      const binding=await fetch(`http://127.0.0.1:${connectorPort}/connector/bind`,{method:'POST',headers:{'content-type':'application/json'},
        body:JSON.stringify({linkId:'j'.repeat(43),serverUrl})});assert.equal(binding.status,200);
      assert.deepEqual(JSON.parse(await readFile(join(folder,'connector-config.json'),'utf8')).spreadexMl,legacy);
    });
  }finally{
    server.closeAllConnections();await new Promise(done=>server.close(done));
    assert.equal(dirname(resolve(folder)),resolve(tmpdir()));assert.match(basename(folder),/^soty-connector-registration-/u);
    await rm(folder,{recursive:true,force:true,maxRetries:5,retryDelay:40});
  }
});
