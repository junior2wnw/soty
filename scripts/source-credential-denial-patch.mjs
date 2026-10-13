import { createHash } from 'node:crypto';

export const sourceCredentialDenialBaseSha = '77708dec9002ca16818330ee336ccaba1528ef9000308ad0c0549d5bdf0dd796';
export function patchSourceCredentialDenial(source) {
  if (typeof source !== 'string' || createHash('sha256').update(source).digest('hex') !== sourceCredentialDenialBaseSha) {
    throw new Error('installed_source_beforeimage_mismatch');
  }
  const before = "const deny=()=>{throw new Error('agent_source_unavailable');};";
  const after = "const deny=()=>{const error=new Error('agent_source_unavailable');Object.defineProperty(error,'code',{value:'authorization_required'});throw error;};";
  if (source.split(before).length !== 2) throw new Error('installed_source_callback_boundary_mismatch');
  // In this exact approved source, all 14 calls belong to its two credential callbacks.
  // Construction, registration, schema and code-pinning guards remain unchanged.
  return source.replace(before, after);
}
