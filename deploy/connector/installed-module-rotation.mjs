/** Local Host operation for explicitly approved installed CODE; never an HTTP/MCP input. */
import { createHash, randomBytes } from 'node:crypto';
import { open, lstat, rename, unlink } from 'node:fs/promises';
import { constants } from 'node:fs';
import path from 'node:path';
import { SafeError } from './docker-api.mjs';
import { readUniversalLocalFile, writeUniversalWitnessExclusive, assertUniversalPolicyCurrent,
  publicUniversalPolicy, assertInstalledModulePolicyTransition } from './universal-policy.mjs';
import { loadInstalledApplicationsEntry } from '../../server/installed-applications-entry.js';
import { canonicalJson } from '../../modules/capabilities/server/validation.mjs';

const states = new WeakMap(), target = '/run/config/soty-installed-applications.mjs';
const MODULE_MAXIMUM = 131072;
const setting = 'SOTY_INSTALLED_APPLICATIONS_SHA256', moduleSetting = 'SOTY_INSTALLED_APPLICATIONS_MODULE';
const need = (ok, code) => { if (!ok) throw new SafeError(code); };
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const same = (a, b) => canonicalJson(a) === canonicalJson(b);
const mounts = value => structuredClone(value).sort((a,b)=>a.Destination.localeCompare(b.Destination));
const stateFor = handle => { const state = states.get(handle); need(state, 'installed_module_rotation_handle'); return state; };
const environment = row => {
  const values = row.Config?.Env;
  need(Array.isArray(values) && values.every(v=>typeof v==='string' && v.includes('='))
    && new Set(values.map(v=>v.split('=',1)[0])).size===values.length, 'installed_module_rotation_environment');
  return values;
};
async function refs(source, hash) {
  const entry = await loadInstalledApplicationsEntry({ SOTY_INSTALLED_APPLICATIONS_MODULE: source, SOTY_INSTALLED_APPLICATIONS_SHA256: hash });
  try { return structuredClone(entry.sourceReferences); } finally { await entry.close(); }
}
async function replaceModule(state, expected, bytes) {
  const current = await readUniversalLocalFile(state.source, state.options, { privateFile: true, maximum: MODULE_MAXIMUM });
  let file, temporary;
  try {
    need(sha(current.bytes)===expected, 'installed_module_rotation_file_cas');
    const before = await lstat(state.source, { bigint: true });
    temporary=path.join(path.dirname(state.source), '.soty-installed-'+randomBytes(16).toString('hex')+'.tmp');
    file=await open(temporary, constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|(constants.O_NOFOLLOW||0), 0o600);
    await file.writeFile(bytes); await file.sync(); await file.close(); file=null;
    const check=await readUniversalLocalFile(state.source,state.options,{privateFile:true,maximum:MODULE_MAXIMUM});
    try { const after=await lstat(state.source,{bigint:true});need(sha(check.bytes)===expected
      && before.dev===after.dev && before.ino===after.ino && before.mtimeNs===after.mtimeNs && before.ctimeNs===after.ctimeNs,
      'installed_module_rotation_file_cas'); } finally { check.dispose(); }
    await rename(temporary,state.source);temporary=null;
    const directory=await open(path.dirname(state.source),constants.O_RDONLY|(constants.O_DIRECTORY||0));
    try { await directory.sync(); } finally { await directory.close(); }
    const actual=await readUniversalLocalFile(state.source,state.options,{privateFile:true,maximum:MODULE_MAXIMUM});
    try { need(sha(actual.bytes)===sha(bytes),'installed_module_rotation_readback'); } finally { actual.dispose(); }
  } finally { current.dispose();if(file)await file.close();if(temporary)await unlink(temporary); }
}

/** Capture before staging. Existing transport has decrypted/verified the rollback envelope in RAM. */
export async function prepareInstalledModuleRotation({ original, expectedEnvironmentSha256, expectedMountsSha256,
  source, expectedOldSha256, approvedNewSha256, approvedModule, expectedOldReferences, approvedReferences,
  oldPolicy, restoreOldPolicy, encryptedRollback, encryptedRollbackSha256, rollbackFile, options, validateOnly = false,
  approvedReferenceUpgrades = [] }) {
  need(typeof validateOnly==='boolean','installed_module_rotation_validation_mode');
  need(original?.State?.Running===true && !original.State.Paused && /^[a-f0-9]{64}$/.test(original.Id||''), 'installed_module_rotation_original');
  const env=environment(original), actualMounts=mounts(original.Mounts);
  need(sha(Buffer.from(JSON.stringify(env)))===expectedEnvironmentSha256,'installed_module_rotation_environment_cas');
  need(actualMounts.length===7 && new Set(actualMounts.map(m=>m.Destination)).size===7
    && sha(Buffer.from(JSON.stringify(actualMounts)))===expectedMountsSha256,'installed_module_rotation_mounts');
  const bound=actualMounts.filter(m=>m.Destination===target);
  need(bound.length===1 && bound[0].Type==='bind' && bound[0].RW===false && bound[0].Source===source
    && env.includes(moduleSetting+'='+target), 'installed_module_rotation_path');
  need(/^[a-f0-9]{64}$/.test(expectedOldSha256||'') && /^[a-f0-9]{64}$/.test(approvedNewSha256||'')
    && expectedOldSha256!==approvedNewSha256 && env.includes(setting+'='+expectedOldSha256),'installed_module_rotation_old_sha');
  need(approvedModule instanceof Uint8Array && approvedModule.length<=MODULE_MAXIMUM && sha(approvedModule)===approvedNewSha256,'installed_module_rotation_new_sha');
  need(typeof restoreOldPolicy==='function','installed_module_rotation_old_proof');
  await assertUniversalPolicyCurrent(oldPolicy);
  const originalReferences=await refs(source,expectedOldSha256);
  need(same(originalReferences,expectedOldReferences) && Array.isArray(approvedReferences)
    && approvedReferences.length<=64 && new Set(approvedReferences.map(r=>r.capabilityId)).size===approvedReferences.length,
    'installed_module_rotation_refs');
  // Only an explicit Host approval may replace a contract. Grants and receipts remain version-bound.
  need(Array.isArray(approvedReferenceUpgrades) && approvedReferenceUpgrades.length<=64,'installed_module_rotation_refs');
  const upgradedIds=new Set();
  for(const upgrade of approvedReferenceUpgrades){
    need(upgrade && same(Object.keys(upgrade).sort(),['after','before']) && upgrade.before && upgrade.after,
      'installed_module_rotation_refs');
    const {before,after}=upgrade;
    need(typeof before.capabilityId==='string' && before.capabilityId===after.capabilityId
      && !upgradedIds.has(before.capabilityId) && Number.isSafeInteger(before.version) && before.version>0
      && Number.isSafeInteger(after.version) && after.version>before.version
      && /^[a-f0-9]{64}$/.test(after.digest||'') && after.digest!==before.digest
      && same({...before,version:after.version,digest:after.digest},after)
      && originalReferences.some(ref=>same(ref,before)) && approvedReferences.some(ref=>same(ref,after)),
      'installed_module_rotation_refs');
    upgradedIds.add(before.capabilityId);
  }
  need(originalReferences.every(ref=>approvedReferences.some(next=>same(next,ref))
    || approvedReferenceUpgrades.some(upgrade=>same(upgrade.before,ref))), 'installed_module_rotation_refs');
  need(encryptedRollback instanceof Uint8Array && sha(encryptedRollback)===encryptedRollbackSha256,'installed_module_rotation_encrypted_backup');
  let envelope;
  try { envelope=JSON.parse(Buffer.from(encryptedRollback).toString('utf8')); } catch { need(false,'installed_module_rotation_encrypted_backup'); }
  need(envelope?.header?.algorithm==='RSA-OAEP-SHA256/AES-256-GCM'
    && ['soty.root-encrypted-backup.v1','soty.hive-encrypted-backup.v1'].includes(envelope.header.schema)
    && typeof envelope.ciphertext==='string' && envelope.ciphertext.length>0,'installed_module_rotation_encrypted_backup');
  const captured=await readUniversalLocalFile(source,options,{privateFile:true,maximum:MODULE_MAXIMUM});
  try {
    need(sha(captured.bytes)===expectedOldSha256,'installed_module_rotation_old_sha');
    // OPEN preflight executes the exact admission path, without file publication or a mutable handle.
    if(validateOnly)return Object.freeze({validated:true,originalReferences:originalReferences.length,approvedReferences:approvedReferences.length,
      environmentMatched:true,mountsMatched:true,rollbackEnvelopeMatched:true,originalModuleMatched:true});
    await writeUniversalWitnessExclusive(rollbackFile,encryptedRollback,options);
    const handle=Object.freeze(Object.create(null));
    states.set(handle,{source,expectedOldSha256,approvedNewSha256,oldPolicy,restoreOldPolicy,options,
      originalId:original.Id,originalImage:original.Image,original:structuredClone(original),environment:[...env],mounts:actualMounts,
      originalModule:Buffer.from(captured.bytes),approvedModule:Buffer.from(approvedModule),approvedReferences:structuredClone(approvedReferences),phase:'captured'});
    return handle;
  } finally { captured.dispose(); }
}
export async function stageInstalledModuleRotation(handle) {
  const state=stateFor(handle);need(state.phase==='captured','installed_module_rotation_phase');
  need(state.restartSuspended===true,'installed_module_rotation_restart_policy');
  state.phase='staging';
  try { await replaceModule(state,state.expectedOldSha256,state.approvedModule);state.phase='staged'; }
  catch(error) { await restoreInstalledModuleOriginal(handle);throw error; }
}
export async function admitInstalledModuleRotation(handle,candidatePolicy) {
  const state=stateFor(handle);need(state.phase==='staged','installed_module_rotation_phase');
  try {
  await assertUniversalPolicyCurrent(candidatePolicy);
  assertInstalledModulePolicyTransition(state.oldPolicy,candidatePolicy,{source:state.source,expectedOldSha256:state.expectedOldSha256,approvedNewSha256:state.approvedNewSha256});
  need(same(await refs(state.source,state.approvedNewSha256),state.approvedReferences),'installed_module_rotation_refs');
  state.candidatePolicy=candidatePolicy;state.phase='admitted';
  } catch(error) { await restoreInstalledModuleOriginal(handle);throw error; }
}
/** Existing guarded restart-policy operation must finish before replacing CODE. */
export function markInstalledModuleRestartSuspended(handle, observed) {
  const state=stateFor(handle);need(state.phase==='captured','installed_module_rotation_phase');
  need(observed.Id===state.originalId && observed.Image===state.originalImage && observed.State.Running
    && same(observed.Config,state.original.Config),'installed_module_rotation_original');
  const host=structuredClone(observed.HostConfig);
  need(same(host.RestartPolicy,{Name:'no',MaximumRetryCount:0}),'installed_module_rotation_restart_policy');
  host.RestartPolicy=state.original.HostConfig.RestartPolicy;
  need(same(host,state.original.HostConfig) && same(mounts(observed.Mounts),state.mounts),'installed_module_rotation_original');
  state.restartSuspended=true;
}
export function installedModuleOriginalForRollout(handle,observed) {
  const state=stateFor(handle);
  need(observed.Id===state.originalId && observed.Image===state.originalImage,'installed_module_rotation_original');
  const projected=structuredClone(observed), policy=projected.HostConfig.RestartPolicy;
  need(same(policy,state.original.HostConfig.RestartPolicy)
    || state.restartSuspended && same(policy,{Name:'no',MaximumRetryCount:0}),'installed_module_rotation_restart_policy');
  // This projects ONLY the explicitly suspended restart policy, never old Env.
  projected.HostConfig.RestartPolicy=structuredClone(state.original.HostConfig.RestartPolicy);
  need(same(projected.Config,state.original.Config) && same(projected.HostConfig,state.original.HostConfig),
    'installed_module_rotation_original');
  return projected;
}
export function installedModuleCandidateBase(handle,original,policy,candidateImage) {
  const state=stateFor(handle);
  need(state.phase==='admitted' && policy===state.candidatePolicy,'installed_module_rotation_policy');
  need(original.Id===state.originalId && original.Image===state.originalImage,'installed_module_rotation_original');
  need(candidateImage===state.originalImage,'installed_module_rotation_image');
  need(same(environment(original),state.environment),'installed_module_rotation_environment_cas');
  need(same(mounts(original.Mounts),state.mounts),'installed_module_rotation_mounts');
  // This is explicitly the FUTURE candidate Env. The measured original stays OLD.
  const candidateBase=installedModuleOriginalForRollout(handle,original);
  candidateBase.Config.Env=state.environment.map(value=>value===setting+'='+state.expectedOldSha256?setting+'='+state.approvedNewSha256:value);
  return candidateBase;
}
export async function restoreInstalledModuleOriginal(handle) {
  const state=stateFor(handle);if(state.phase==='restored')return;
  need(['captured','staging','staged','admitted'].includes(state.phase),'installed_module_rotation_phase');
  const current=await readUniversalLocalFile(state.source,state.options,{privateFile:true,maximum:MODULE_MAXIMUM});let currentHash;
  try { currentHash=sha(current.bytes); } finally { current.dispose(); }
  if(currentHash===state.approvedNewSha256)await replaceModule(state,state.approvedNewSha256,state.originalModule);
  else need(currentHash===state.expectedOldSha256,'installed_module_rotation_file_cas');
  const oldProof=await state.restoreOldPolicy();
  // Fresh identity after atomic restore is allowed ONLY for byte-exact original module.
  assertInstalledModulePolicyTransition(state.oldPolicy,oldProof,{source:state.source,expectedOldSha256:state.expectedOldSha256,approvedNewSha256:state.expectedOldSha256,rollback:true});
  await assertUniversalPolicyCurrent(oldProof);state.restoredPolicy=oldProof;state.phase='restored';
}
export async function assertInstalledModuleOriginalProof(handle) {
  const state=stateFor(handle);need(state.phase==='restored','installed_module_rotation_old_proof');
  await assertUniversalPolicyCurrent(state.restoredPolicy);
  need(publicUniversalPolicy(state.restoredPolicy).environment.some(item=>item.name===setting && item.value===state.expectedOldSha256),'installed_module_rotation_old_proof');
}
export function disposeInstalledModuleRotation(handle) {const state=stateFor(handle);state.originalModule.fill(0);state.approvedModule.fill(0);states.delete(handle);}
