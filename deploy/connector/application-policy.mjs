import {readFile,realpath,lstat} from 'node:fs/promises';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {SafeError} from './docker-api.mjs';

export const policyTarget='/run/config/soty-application-model-policy.json';
export const policySetting='SOTY_GONKA_APPLICATION_MODEL_POLICY_FILE';
const proposed={schema:'soty.application-model-policy.v1',applications:[{id:'kvartalufa',allowedModels:['deepseek-ai/DeepSeek-V4-Flash-0731','MiniMaxAI/MiniMax-M2.7']}]};
export async function readApprovedPolicy(filePath,sha256){
  if(typeof filePath!=='string'||!path.isAbsolute(filePath)||!/^[a-f0-9]{64}$/.test(sha256||''))throw new SafeError('policy_exact_guard_invalid');
  try{
    const stat=await lstat(filePath);
    if(!stat.isFile()||stat.size>65536||await realpath(filePath)!==path.resolve(filePath))throw new SafeError('policy_file_invalid');
    const bytes=await readFile(filePath);
    if(bytes.length>65536||createHash('sha256').update(bytes).digest('hex')!==sha256)throw new SafeError('policy_hash_mismatch');
    const parsed=JSON.parse(bytes);
    // The generic server supports separate application rules. This rollout
    // admits only the concrete existing-application proposal reviewed here.
    if(JSON.stringify(parsed)!==JSON.stringify(proposed))throw new SafeError('policy_scope_mismatch');
    return {source:path.resolve(filePath),target:policyTarget,sha256};
  }catch(e){if(e instanceof SafeError)throw e;throw new SafeError('policy_file_invalid');}
}
export function addApprovedPolicy(config,policy){
  if(!policy)return;
  if(policy.target!==policyTarget||!path.isAbsolute(policy.source)||!/^[a-f0-9]{64}$/.test(policy.sha256||''))throw new SafeError('policy_exact_guard_invalid');
  if(config.Env?.some(v=>v.startsWith(policySetting+'='))||config.HostConfig.Binds?.some(v=>v.split(':')[1]===policyTarget)||config.HostConfig.Mounts?.some(v=>v.Target===policyTarget)||config.Volumes?.[policyTarget])throw new SafeError('policy_preexisting_configuration');
  config.Env=[...(config.Env||[]),`${policySetting}=${policyTarget}`];
  config.HostConfig.Mounts=[...(config.HostConfig.Mounts||[]),{Type:'bind',Source:policy.source,Target:policyTarget,ReadOnly:true}];
}
