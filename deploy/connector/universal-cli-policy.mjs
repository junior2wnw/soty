import { SafeError } from './docker-api.mjs';
import { readUniversalLocalFile, parseUniversalLocalJson, prepareUniversalPolicy, restoreUniversalPolicy,
  publicUniversalPolicy, sealUniversalPolicy, writeUniversalWitnessExclusive, disposeUniversalPolicy } from './universal-policy.mjs';

const require = (ok, code = 'universal_cli_invalid') => { if (!ok) throw new SafeError(code); };
export async function openUniversalCliPolicy({ action, planFile, custodyFile, witnessFile, shellOrigins = [], fixtureRoot, approval }) {
  require(['prepare', 'promote'].includes(action) && typeof planFile === 'string' && typeof custodyFile === 'string' && typeof witnessFile === 'string');
  require(new Set([planFile, custodyFile, witnessFile]).size === 3);
  const context = { shellOrigins, ...(fixtureRoot ? { fixtureRoot } : {}) };
  let plan, custody, key, handle;
  const files = [];
  try {
    const planInput = await readUniversalLocalFile(planFile, context); files.push(planInput); plan = parseUniversalLocalJson(planInput.bytes);
    const keyInput = await readUniversalLocalFile(custodyFile, context, { privateFile: true }); files.push(keyInput); custody = parseUniversalLocalJson(keyInput.bytes);
    require(custody && Object.keys(custody).length === 2 && typeof custody.keyId === 'string' && /^[A-Za-z0-9_-]{1,64}$/u.test(custody.keyId)
      && typeof custody.key === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(custody.key), 'universal_cli_custody_invalid');
    key = Buffer.from(custody.key, 'base64url'); require(key.length === 32 && key.toString('base64url') === custody.key, 'universal_cli_custody_invalid');
    if (action === 'promote') {
      require(approval && /^[A-Za-z0-9_-]{32}$/u.test(approval.universalWitnessId || '') && /^[a-f0-9]{64}$/u.test(approval.universalPolicyDigest || ''), 'supervised_universal_receipt_mismatch');
      const witness = await readUniversalLocalFile(witnessFile, context, { privateFile: true }); files.push(witness);
      handle = await restoreUniversalPolicy(plan, witness.bytes, context, { key, keyId: custody.keyId, expectedWitnessId: approval.universalWitnessId });
      require(publicUniversalPolicy(handle).policyDigest === approval.universalPolicyDigest, 'supervised_universal_receipt_mismatch');
    } else handle = await prepareUniversalPolicy(plan, context);
    let sealed = false;
    return Object.freeze({ handle, policyDigest: publicUniversalPolicy(handle).policyDigest,
      witnessId: action === 'promote' ? approval.universalWitnessId : null,
      /** Call only after Rollout.guard binds the exact private preservation fingerprint. */
      async publishWitness() {
        require(action === 'prepare' && !sealed, 'universal_cli_witness_already_published'); sealed = true;
        const result = await sealUniversalPolicy(handle, { key, keyId: custody.keyId });
        try { await writeUniversalWitnessExclusive(witnessFile, result.packet, context); return { witnessId: result.witnessId, policyDigest: result.policyDigest }; }
        finally { result.packet.fill(0); }
      },
      dispose() { key.fill(0); files.forEach(file => file.dispose()); try { disposeUniversalPolicy(handle); } catch {} },
    });
  } catch (error) { key?.fill(0); files.forEach(file => file.dispose()); if(handle)try { disposeUniversalPolicy(handle); } catch {}
    if (error instanceof SafeError) throw error; throw new SafeError('universal_cli_invalid'); }
}
