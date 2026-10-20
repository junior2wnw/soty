import { validArtifact, copyArtifact, projectId, branchId } from '../modules/otsov-editor-handoff/wire.mjs';

/** Private host ledger, never filled from model output, evidence data or a URL. */
export function createProjectEditorLedger() {
  const committed = new Map(); let chosen = null;
  return Object.freeze({
    record({ detail, input, invocation }) {
      const operation = input?.operation, appId = detail?.appId;
      if (!['otsov.project.create','otsov.project.operations'].includes(operation)
        || detail?.schema !== 'soty.authorized-app-capability.v1' || detail.scope !== 'authorized'
        || detail.reference?.capabilityId !== `app.${appId}:${operation}`
        || invocation?.capabilityId !== detail.reference.capabilityId || invocation.version !== detail.reference.version
        || detail?.binding?.id !== `app.${appId}:selected-native` || detail.binding.version !== 2
        || invocation?.status !== 'succeeded' || invocation.effectState !== 'committed'
        || invocation.receipt?.verificationMethod !== 'domain_read') return null;
      const artifacts = invocation.receipt.artifacts?.filter(a => a.type === 'otsov-project') ?? [];
      if (artifacts.length !== 1) return null;
      const artifact = artifacts[0], branch = input.payload?.branch ?? 'main';
      const effect = invocation.effects?.find(e => e.resourceType === 'otsov-project' && e.resourceId === artifact.id
        && e.revision === artifact.revision && e.kind === (operation === 'otsov.project.create' ? 'created' : 'updated'));
      const value = { schema:'soty.otsov-project-artifact.v1', appId, projectId:artifact.id, branch,
        revision:artifact.revision, invocationId:invocation.invocationId };
      if (!effect || !/^[a-f0-9]{64}$/u.test(artifact.sha256 ?? '') || !validArtifact(value)
        || operation !== 'otsov.project.create' && input.projectId !== value.projectId) return null;
      const key = JSON.stringify([value.projectId, value.branch]), prior = committed.get(key);
      if (prior && (prior.appId !== value.appId || prior.revision > value.revision)) return null;
      committed.set(key, copyArtifact(value)); return copyArtifact(value);
    },
    request(input) {
      if (!input || Object.keys(input).sort().join(',') !== 'branch,projectId' || !projectId(input.projectId) || !branchId(input.branch)) throw Error('editor_target_denied');
      const key = JSON.stringify([input.projectId,input.branch]); if (!committed.has(key)) throw Error('editor_target_denied');
      chosen = key; return copyArtifact(committed.get(key));
    },
    requested() { return chosen ? copyArtifact(committed.get(chosen)) : null; },
  });
}
