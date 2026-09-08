import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,readFile,writeFile,cp,rm,access} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
// Linux-only filesystem/CLI plumbing with TEST engine and TEST rollout modules.
// Actual rollout safety is covered in rollout.test.mjs; no real Docker claim here.
const unix=process.platform!=='win32';
async function fixture(fn){
 const dir=await mkdtemp(path.join(tmpdir(),'soty-resume-cli-'));
 try{
  const code=path.join(dir,'connector');await cp(path.dirname(fileURLToPath(import.meta.url)),code,{recursive:true});
  await writeFile(path.join(code,'docker-api.mjs'),`export class SafeError extends Error {constructor(code){super(code);this.code=code;}} export class DockerApi {async inspect(){return {HostConfig:{PortBindings:{'8080/tcp':[{HostIp:'127.0.0.1',HostPort:'18182'}]}}};}} export async function httpJson(){throw new Error('must not call health');}`);
  await writeFile(path.join(code,'rollout.mjs'),`export class Rollout {constructor({record}){this.record=record;} async resumeAfterStop(args,failed,prepared,approval,binding){if(failed.phase!=='recovery_required'||prepared.phase!=='prepared'||approval.failedJournalSha256!==binding.failedJournalSha256)throw new Error('test_binding_mismatch');this.state={phase:'committed',resumeFrom:binding};await this.record(this.state);}} export function createConfig(){throw new Error('must not call');} export function safeStatus(){throw new Error('must not call');}`);
  const failed=path.join(dir,'failed.json'),prepared=path.join(dir,'prepared.json'),receipt=path.join(dir,'receipt.json'),journal=path.join(dir,'continuation.json');
  const failedBytes=Buffer.from('{"phase":"recovery_required"}\n'),preparedBytes=Buffer.from('{"phase":"prepared"}\n');
  await writeFile(failed,failedBytes);await writeFile(prepared,preparedBytes);
  const sha=x=>createHash('sha256').update(x).digest('hex');const approval={schema:'soty.controller-stop-resume.v1',approved:true,failedJournalSha256:sha(failedBytes),preparedJournalSha256:sha(preparedBytes)};await writeFile(receipt,JSON.stringify(approval));
  const run=(out=journal)=>spawnSync(process.execPath,[path.join(code,'cli.mjs'),'resume-after-stop','--failed-journal',failed,'--prepared-journal',prepared,'--reviewed-receipt',receipt,'--journal',out,'--original-id','a'.repeat(64)],{encoding:'utf8',timeout:10000});
  await fn({dir,failed,prepared,receipt,journal,failedBytes,preparedBytes,approval,run});
 }finally{await rm(dir,{recursive:true,force:true});}
}
test('CLI durable exclusive resume claim preserves failed/prepared bytes and refuses second continuation',{skip:!unix},async()=>fixture(async f=>{
 const result=f.run();assert.equal(result.status,0,result.stdout+result.stderr);assert.equal(JSON.parse(result.stdout).phase,'committed');
 assert.deepEqual(await readFile(f.failed),f.failedBytes);assert.deepEqual(await readFile(f.prepared),f.preparedBytes);assert.deepEqual(await readFile(f.journal+'.failed-source.json'),f.failedBytes);assert.deepEqual(await readFile(f.journal+'.prepared-source.json'),f.preparedBytes);
 const claim=JSON.parse(await readFile(f.failed+'.resume.claim'));assert.equal(claim.failedJournalSha256,f.approval.failedJournalSha256);
 const second=f.run(path.join(f.dir,'second.json'));assert.equal(second.status,1);await assert.rejects(access(path.join(f.dir,'second.json')));
}));
test('CLI stale failed-journal SHA refuses claim and continuation',{skip:!unix},async()=>fixture(async f=>{
 await writeFile(f.failed,'{"phase":"changed"}\n');const result=f.run();assert.equal(result.status,1);assert.equal(JSON.parse(result.stdout).code,'resume_journal_binding');await assert.rejects(access(f.failed+'.resume.claim'));await assert.rejects(access(f.journal));
}));
