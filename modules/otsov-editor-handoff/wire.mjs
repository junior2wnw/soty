export const EDITOR_HANDOFF_SCHEMA = 'soty.otsov-editor-handoff.v1';
export const exact = (v, names) => !!v && typeof v === 'object' && !Array.isArray(v)
  && [Object.prototype, null].includes(Object.getPrototypeOf(v))
  && Object.keys(v).sort().join(',') === [...names].sort().join(',');
export const projectId = v => typeof v === 'string' && /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/u.test(v);
export const branchId = v => typeof v === 'string' && v.length <= 80 && /^[a-zA-Z0-9_]+(?:-[a-zA-Z0-9_]+)*$/u.test(v);
export const requestId = v => typeof v === 'string' && /^[A-Za-z0-9_.:-]{8,160}$/u.test(v);
export const validArtifact = v => exact(v, ['schema','appId','projectId','branch','revision','invocationId'])
  && v.schema === 'soty.otsov-project-artifact.v1' && /^app-[a-f0-9]{32}$/u.test(v.appId)
  && projectId(v.projectId) && branchId(v.branch) && Number.isSafeInteger(v.revision) && v.revision >= 1
  && typeof v.invocationId === 'string' && /^[A-Za-z0-9_.:-]{3,160}$/u.test(v.invocationId);
export const validReadback = (v, artifact) => exact(v, ['projectId','branch','revision','selectedProjectId','online'])
  && v.online === true && v.projectId === artifact.projectId && v.selectedProjectId === artifact.projectId
  && v.branch === artifact.branch && Number.isSafeInteger(v.revision) && v.revision >= artifact.revision;
export function copyArtifact(v) { if (!validArtifact(v)) throw Error('editor_artifact_invalid'); return Object.freeze({ ...v }); }
