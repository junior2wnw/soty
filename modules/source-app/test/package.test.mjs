import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, mkdir, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { buildSourceAppPackage } from '../scripts/build.mjs';
import { STANDARD_SELECTED_SOURCE } from '../server/standard-profile.mjs';

test('portable server/browser bundles load outside Root checkout with pinned protocol dependency and exact compiled profile', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-source-package-')); t.after(() => rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 30 }));
  const manifest = await buildSourceAppPackage({ outdir: directory }); assert.equal(manifest.protocolDependency.version, '6.8.4');
  assert.ok(manifest.sourceFiles.every(file => !file.path.includes(':') && !file.path.startsWith('/') && /^[a-f0-9]{64}$/u.test(file.sha256)));
  const require = createRequire(import.meta.url), protocolDir = dirname(dirname(require.resolve('openid-client')));
  await mkdir(join(directory, 'node_modules')); await symlink(protocolDir, join(directory, 'node_modules/openid-client'), process.platform === 'win32' ? 'junction' : 'dir');
  const server = await readFile(join(directory, 'server.mjs'), 'utf8'), browser = await readFile(join(directory, 'browser.mjs'), 'utf8');
  assert.equal(/from ['"]\.\.\//u.test(server), false); assert.equal(/node:|openid-client/u.test(browser), false);
  for(const name of ['example','install']) {
    const bundle=await readFile(join(directory,name+'.mjs'),'utf8');
    const imports=[...bundle.matchAll(/import\s*\{([^}]+)\}\s*from\s*['"]\.\/server\.mjs['"]/gu)];assert.equal(imports.length,1);
    assert.deepEqual(imports[0][1].split(',').map(value=>value.trim().split(/\s+/u)[0]),['__sourceSdkOrdinaryAppServer']);
  }
  const script = `import {STANDARD_SELECTED_SOURCE,STANDARD_SELECTED_SOURCE_V2,createSourceNativeAuthorityPort,isSourceNativeAuthorityPort,createSqliteRelayIdentity,createFeedbackProcessorEngine,createFeedbackJobEnforcer} from ${JSON.stringify(pathToFileURL(join(directory, 'server.mjs')).href)};
    import {createRelayIdentityFetch,createSotyAppWork} from ${JSON.stringify(pathToFileURL(join(directory, 'browser.mjs')).href)};
    import{createOrdinaryAppServer}from${JSON.stringify(pathToFileURL(join(directory, 'example.mjs')).href)};
    import{createServer}from'node:http';import{randomBytes}from'node:crypto';import{mkdir,writeFile}from'node:fs/promises';import{join}from'node:path';
    const p=createSourceNativeAuthorityPort({capture:async()=>({}),withCurrent:(_p,_s,f)=>f()});
    if(!isSourceNativeAuthorityPort(p))throw new Error('brand');
    const reserve=createServer();await new Promise(done=>reserve.listen(0,'127.0.0.1',done));const port=reserve.address().port;await new Promise(done=>reserve.close(done));
    const appId='app-'+'a'.repeat(32),parentOrigin='https://fixture.invalid',nativeOrigin='http://localhost:'+port;
    const identity=createSqliteRelayIdentity({issuer:parentOrigin+'/human-identity',appId,databasePath:${JSON.stringify(join(directory, 'relay-nonces.sqlite'))}});
    if(typeof identity.verify!=='function')throw new Error('relay verifier');identity.close();
    const listeners=new Set(),relayParent={postMessage(data,target){if(target!==parentOrigin)throw Error('identity origin');
      queueMicrotask(()=>{for(const listener of listeners)listener({source:relayParent,origin:parentOrigin,
        data:{schema:'soty.relay-window-identity.v1',type:'grant',requestId:data.requestId,grant:'A'.repeat(43)}});});}};
    const relayView={parent:relayParent,crypto:globalThis.crypto,setTimeout,clearTimeout,
      addEventListener(_type,fn){listeners.add(fn);},removeEventListener(_type,fn){listeners.delete(fn);}};
    let requested=false;const identityFetch=createRelayIdentityFetch({origin:'https://app.fixture.invalid',parentOrigin,view:relayView,fetch:async(path,options)=>{
      requested=path==='/api/items'&&options.headers.get('x-soty-identity-request')==='1'
        &&options.headers.get('x-soty-window-grant')==='A'.repeat(43);return new Response();}});
    await identityFetch('/api/items');if(!requested)throw new Error('relay browser helper');
    let received,handler;const parent={postMessage(data,target){if(target!==parentOrigin)throw Error('work origin');received=data;}};
    const work=createSotyAppWork({parentOrigin,isDirty:()=>true,save:async()=>{},view:{parent,addEventListener(_type,fn){handler=fn;},removeEventListener(){}}});
    handler({source:parent,origin:parentOrigin,data:{schema:'soty.app-work.v1',type:'probe',session:'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'}});
    if(received?.type!=='state'||received.dirty!==true)throw Error('portable app work');work.dispose();
    const profile={schema:'soty.selected-human-embed.v2',appId,connector:{linkId:'synthetic-link',hostDeviceId:'synthetic-host',connectorId:'synthetic-connector'},
      target:{revision:1,digest:'a'.repeat(64)},sourceProfile:STANDARD_SELECTED_SOURCE,resource:{registryId:'soty',environmentId:'fixture',tenantId:'synthetic-owner',appId,resourceId:'selected',selection:{kind:'soty.resource.v1',nativeId:'selected',incarnationId:'one'}},
      issuer:parentOrigin+'/human-identity',clientId:'synthetic-client',embedOrigin:'https://'+appId+'.localhost:24444',nativeOrigin,parentOrigin};
    const ref={id:'synthetic.ocr',version:1,digest:'a'.repeat(64)},policyRef={id:'synthetic.policy',version:1,digest:'b'.repeat(64)};
    const budget={wallMs:5000,cpuMs:1000,cleanupMs:1000,scratchBytes:65536,outputBytes:16000,mediaBytes:1024,attachments:1,parallel:1};
    const spec={ref,purposes:['ocr'],localOnly:true,synthetic:true,process:async()=>({kind:'transcript',text:'synthetic'})};
    const engine=createFeedbackProcessorEngine(spec),enforcer=createFeedbackJobEnforcer({engine,platform:'linux',maxBudget:budget,syntheticTestOnly:true,execute:async()=>({kind:'transcript',text:'synthetic'})});
    const processing={policy:{schema:'soty.feedback.processing-policy.v1',ref:policyRef,purposes:['ocr'],localOnly:true,requiresReporterConsent:true,maxBudget:budget},engines:[engine],enforcer};
    const options={profile,transportKey:randomBytes(32),connectorPort:49424,rp:{issuer:profile.issuer,clientId:profile.clientId,clientSecret:'a'.repeat(43),redirectUri:nativeOrigin+'/soty/callback'},
      databasePath:${JSON.stringify(join(directory, 'example.sqlite'))},realmId:'standalone',cipherKey:randomBytes(32),keyId:'synthetic',initialize:true,newResource:{title:'Empty synthetic resource',guestEmpty:true},allowEmptyGuest:true,feedbackProcessing:processing};
    const app=await createOrdinaryAppServer(options);
    try{await app.listen();for(const path of ['/embed','/assets/ordinary-app.js','/assets/ordinary-app.css','/assets/source-sdk.js']){const r=await fetch('http://127.0.0.1:'+port+path);if(r.status!==200)throw new Error('public asset');}
      const denied=await fetch('http://127.0.0.1:'+port+'/api/embed/context');if(denied.status!==403)throw new Error('anonymous read');
    }finally{await app.close();}
    const foreign=await import(${JSON.stringify(pathToFileURL(join(directory, 'server.mjs')).href+'?foreign=1')});
    const foreignEngine=foreign.createFeedbackProcessorEngine(spec);
    let refused=false;try{await createOrdinaryAppServer({...options,databasePath:${JSON.stringify(join(directory, 'foreign.sqlite'))},feedbackProcessing:{...processing,engines:[foreignEngine]}});}catch(error){refused=error.code==='source_feedback_engine_unapproved';}
    if(!refused)throw Error('foreign engine accepted');
    refused=false;try{await createOrdinaryAppServer({...options,databasePath:${JSON.stringify(join(directory, 'forged.sqlite'))},feedbackProcessing:{...processing,enforcer:{...enforcer}}});}catch(error){refused=error.code==='source_feedback_processor_not_ready';}
    if(!refused)throw Error('forged enforcer accepted');
    const install=await import(${JSON.stringify(pathToFileURL(join(directory, 'install.mjs')).href)}),installDir=${JSON.stringify(join(directory,'install-fixture'))};
    await mkdir(installDir);await mkdir(join(installDir,'secrets'),{mode:0o700});await mkdir(join(installDir,'data'),{mode:0o700});
    for(const name of ['cipher.key','transport.key','client.key'])await writeFile(join(installDir,'secrets',name),randomBytes(32).toString('base64url')+'\\n',{mode:0o600});
    const installProfile={...profile,sourceProfile:STANDARD_SELECTED_SOURCE_V2,issuer:'https://soty.example.test/human-identity',
      embedOrigin:'https://'+appId+'.soty.example.test',nativeOrigin:'https://native.example.test',parentOrigin:'https://soty.example.test'};
    const config={schema:'soty.ordinary-source.operator.v1',profile:installProfile,connectorPort:49424,listener:{host:'127.0.0.1',port},
      storage:{directory:join(installDir,'data'),keyId:'synthetic',cipherKeyFile:'cipher.key'},rp:{clientSecretFile:'client.key',transportKeyFile:'transport.key'},
      native:{realmId:'synthetic',resourceTitle:'Synthetic Native resource'},policy:{newEmptyGuest:false,linkedLogin:false}};
    const configPath=join(installDir,'operator.json');await writeFile(configPath,JSON.stringify(config),{mode:0o600});
    let init;const log=console.log;console.log=value=>{init=JSON.parse(value);};try{await install.sourceInstallMain(['init','--config',configPath]);}finally{console.log=log;}
    if(!init?.initialized)throw Error('portable install init failed');
    const handle=await install.loadSourceOperatorConfiguration(configPath);let installed;
    try{installed=await install.startInstalledSource(handle);if(installed.server.address().port!==port)throw Error('portable install did not start');}
    finally{if(installed)await installed.close();install.disposeSourceOperatorConfiguration(handle);}
    process.stdout.write(JSON.stringify(STANDARD_SELECTED_SOURCE));`;
  const result = await new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--input-type=module', '-e', script], { cwd: directory, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '', err = ''; child.stdout.on('data', part => { out += part; }); child.stderr.on('data', part => { err += part; }); child.on('error', reject);
    child.on('exit', code => resolve({ code, out, err }));
  });
  assert.equal(result.code, 0, result.err); assert.deepEqual(JSON.parse(result.out), STANDARD_SELECTED_SOURCE);
});
