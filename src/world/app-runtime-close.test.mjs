import test from 'node:test';import assert from 'node:assert/strict';
import {closeAppRuntime} from './app-runtime-close.mjs';
import {readFile} from 'node:fs/promises';
const held=()=>{let resolve;return{promise:new Promise(yes=>resolve=yes),resolve};};
function fixture(){const state={current:true,unknown:false,dirty:false},events=[];return{state,events,
  options:{stage:{hasUnconfirmedWork:()=>state.unknown,async flush(){events.push('flush');},hasUnsavedChanges:()=>state.dirty},
    isCurrent:()=>state.current,async confirmUnknown(){events.push('confirm');return false;},onFailure(){events.push('failure');},commit(){events.push('commit');}}};}
test('confirmed clean close flushes then commits once without asking or inventing work',async()=>{const f=fixture();assert.equal(await closeAppRuntime(f.options),true);assert.deepEqual(f.events,['flush','commit']);});
test('unknown Keep or Escape neither flushes nor commits',async()=>{for(const choice of ['keep','escape']){const f=fixture();f.state.unknown=true;assert.equal(await closeAppRuntime(f.options),false,choice);assert.deepEqual(f.events,['confirm']);}});
test('explicit unknown discard still flushes shell work and requires current intent',async()=>{const f=fixture();f.state.unknown=true;f.options.confirmUnknown=async current=>{assert.equal(current(),true);f.events.push('dialog-cleanup');return true;};assert.equal(await closeAppRuntime(f.options),true);assert.deepEqual(f.events,['dialog-cleanup','flush','commit']);});
test('failed or dirty save retains runtime and reports failure without discard',async()=>{for(const mode of ['reject','dirty']){const f=fixture();if(mode==='reject')f.options.stage.flush=async()=>{f.events.push('flush');throw Error('save unavailable');};else f.state.dirty=true;assert.equal(await closeAppRuntime(f.options),false);assert.deepEqual(f.events,['flush','failure']);}});
test('retired confirmation and held-save completion cannot destroy new work',async()=>{for(const phase of ['confirm','flush']){const f=fixture(),gate=held();f.state.unknown=phase==='confirm';if(phase==='confirm')f.options.confirmUnknown=()=>gate.promise;else f.options.stage.flush=()=>{f.events.push('flush');return gate.promise;};const result=closeAppRuntime(f.options);f.state.current=false;gate.resolve(true);assert.equal(await result,false);assert.equal(f.events.includes('commit'),false);assert.equal(f.events.includes('failure'),false);}});
test('actual dirty state delivered during save is rechecked after its ACK',async()=>{const f=fixture(),gate=held();f.options.stage.flush=()=>{f.events.push('flush');return gate.promise;};const result=closeAppRuntime(f.options);f.state.dirty=true;gate.resolve();assert.equal(await result,false);assert.deepEqual(f.events,['flush','failure']);});
test('a new unknown save contract after confirmed work cannot inherit discard consent',async()=>{const f=fixture();f.options.stage.flush=async()=>{f.events.push('flush');f.state.unknown=true;};assert.equal(await closeAppRuntime(f.options),false);assert.deepEqual(f.events,['flush','failure']);});
async function preserveUnknown(close){const f=fixture();f.state.unknown=true;assert.equal(await close(f.options),false);assert.deepEqual(f.events,['confirm']);}
async function preserveFailedSave(close){const f=fixture();f.options.stage.flush=async()=>{f.events.push('flush');throw Error('synthetic save failure');};assert.equal(await close(f.options),false);assert.deepEqual(f.events,['flush','failure']);}
test('broken unknown-consent and swallowed-save controls fail unchanged retention criteria',async()=>{
 const source=await readFile(new URL('./app-runtime-close.mjs',import.meta.url),'utf8');
 for(const [anchor,replacement,scenario]of [
  ['if(unknown&&!await confirmUnknown(isCurrent))','if(false&&!await confirmUnknown(isCurrent))',preserveUnknown],
  ['await stage.flush();','try{await stage.flush();}catch{}',preserveFailedSave],
 ]){assert.equal(source.split(anchor).length-1,1);const broken=(await import('data:text/javascript;base64,'+Buffer.from(source.replace(anchor,replacement)).toString('base64'))).closeAppRuntime;
  await assert.rejects(()=>scenario(broken),error=>error.code==='ERR_ASSERTION');}
});
