import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { lstat, mkdtemp, readFile, readdir, realpath, rm, writeFile } from 'node:fs/promises';
import { join, relative, resolve, sep } from 'node:path';
import { tmpdir } from 'node:os';
import { normalizeTransitClientProfile } from './agent-modules/transit-client.mjs';
import { createTransitCacheProjection, createTransitProbeWorkspace } from './agent-modules/transit-probe.mjs';
const isolateLegacyTransitCache=createTransitCacheProjection({normalize:normalizeTransitClientProfile,hash:bytes=>createHash('sha256').update(bytes).digest('hex')});
const withTransitProbeWorkspace=createTransitProbeWorkspace({lstat,mkdtemp,realpath,rm,writeFile,join,relative,resolve,sep});

const profile = credential => ({outbounds:[{type:'vless',tag:'entry',server:'entry.example',uuid:credential,tls:{enabled:true,insecure:false}}],
  dns:{servers:[{type:'tcp',tag:'dns',server:'1.1.1.1',detour:'entry'}],final:'dns'},route:{final:'entry'},experimental:{cache_file:{enabled:true,store_fakeip:true}}});

test('legacy projection gains stable isolation without changing its source or explicit issuer identity',()=>{
  const original=profile('existing-credential'),before=JSON.stringify(original),a=isolateLegacyTransitCache(original);
  assert.match(a.experimental.cache_file.path,/^transit-cache-[a-f0-9]{64}\.db$/u);
  assert.equal(isolateLegacyTransitCache(original).experimental.cache_file.path,a.experimental.cache_file.path);
  assert.notEqual(isolateLegacyTransitCache(profile('other-credential')).experimental.cache_file.path,a.experimental.cache_file.path);
  const explicit=profile('existing-credential');explicit.experimental.cache_file.path='transit-cache-'+'f'.repeat(64)+'.db';
  assert.equal(isolateLegacyTransitCache(explicit).experimental.cache_file.path,explicit.experimental.cache_file.path);
  assert.deepEqual(a.outbounds,original.outbounds);assert.equal(JSON.stringify(original),before);
});

test('parallel probes never open the persistent same-profile cache or share a working directory',async()=>{
  const root=await mkdtemp(join(tmpdir(),'transit-probe-test-'));const cache='transit-cache-'+'a'.repeat(64)+'.db';
  try{
    await writeFile(join(root,cache),'persistent-user-mapping');const roots=[];
    await Promise.all([0,1,2].map(index=>withTransitProbeWorkspace(root,async()=>{},async workspace=>{
      roots.push(workspace.root);const file=await workspace.write({profile:index,experimental:{cache_file:{path:cache}}});
      await writeFile(join(workspace.root,cache),'probe-'+index);await new Promise(r=>setTimeout(r,5));
      assert.equal(await readFile(join(workspace.root,cache),'utf8'),'probe-'+index);assert.equal(JSON.parse(await readFile(file,'utf8')).profile,index);
    })));
    assert.equal(new Set(roots).size,3);assert.deepEqual(await readdir(root),[cache]);assert.equal(await readFile(join(root,cache),'utf8'),'persistent-user-mapping');
  }finally{await rm(root,{recursive:true});}
});

test('unconfirmed exit retains the exact private probe files for ownership recovery',async()=>{
  const root=await mkdtemp(join(tmpdir(),'transit-probe-test-'));let retained;
  try{
    await assert.rejects(withTransitProbeWorkspace(root,async()=>{},async workspace=>{retained=workspace.root;await workspace.write({private:'fixture'});workspace.retain();throw Error('transit-probe-stop-unconfirmed');}),/transit-probe-stop-unconfirmed/u);
    assert.equal(JSON.parse(await readFile(join(retained,'profile.json'),'utf8')).private,'fixture');
  }finally{await rm(root,{recursive:true});}
});
