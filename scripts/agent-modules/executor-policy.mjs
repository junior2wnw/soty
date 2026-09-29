// Keep the wire kind distinct from future executor versions. A server upgrade
// must never turn an unfamiliar operation into an AI prompt or a shell command.
export function resolveJobExecutor(job) {
  const aliases = { agent: 'agent', chat: 'agent', command: 'command', script: 'script' };
  if (!job || typeof job !== 'object' || Array.isArray(job)
    || typeof job.kind !== 'string' || !Object.hasOwn(aliases, job.kind)) return null;
  const kind = aliases[job.kind];
  if (!job.input || typeof job.input !== 'object' || Array.isArray(job.input)) return null;
  if (job.input.kind !== undefined && (typeof job.input.kind !== 'string'
    || !Object.hasOwn(aliases, job.input.kind) || aliases[job.input.kind] !== kind)) return null;
  return kind;
}
