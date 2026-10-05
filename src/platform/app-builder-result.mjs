/** A finished job is only an app draft when the device supplied a valid,
 * job-bound proposal. Unknown execution outcomes must remain honest. */
export function appBuilderProposal(result, pending) {
  const proposal = result?.job?.result?.appProposal;
  if (result?.job?.status !== 'succeeded' || result.job.executionUncertain || proposal?.schema !== 'soty.local-app.v1' || proposal.sourceJobId !== pending.jobId
    || typeof proposal.name !== 'string' || !proposal.name.trim() || proposal.name.length > 64
    || !Number.isInteger(proposal.port) || proposal.port < 1024 || proposal.port > 65535 || proposal.port === 49424
    || typeof proposal.entryPath !== 'string' || !proposal.entryPath.startsWith('/') || proposal.entryPath.startsWith('//')
    || proposal.entryPath.startsWith('/_soty/') || /[\u0000-\u001f]/u.test(proposal.entryPath)) return null;
  return proposal;
}
/** The requested connector is authority-bound on the server. A queued job has
 * not leased a runtime yet, so its assigned connector is deliberately empty. */
export function appBuilderReceipt(job, payload) {
  return Boolean(job && job.schema === 'soty.connector-job.v4' && job.kind === 'agent'
    && typeof job.id === 'string' && /^[A-Za-z0-9_.:-]{3,180}$/u.test(job.id)
    && job.deviceId === payload.hostDeviceId
    && (job.connectorId === payload.connectorId || job.connectorId === '' && job.status === 'queued' && job.attempts === 0));
}
export function matchingAppBuilderRegistration(apps, pending, proposal, accountId) {
  return apps.find(app => app.ownerAccountId === accountId && app.hostDeviceId === pending.hostDeviceId && app.connectorId === pending.connectorId
    && app.name === proposal.name && app.port === proposal.port && app.entryPath === proposal.entryPath && app.state !== 'revoked') ?? null;
}
export function appBuilderLaunchUrl(value, pageUrl) {
  if (typeof value !== 'string') return null;
  try {
    const page = new URL(pageUrl), url = new URL(value, page);
    if (!['http:', 'https:'].includes(url.protocol) || url.origin === page.origin || url.username || url.password
      || (page.protocol === 'https:' && url.protocol !== 'https:')) return null;
    return url.href;
  } catch { return null; }
}
