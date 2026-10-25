export function deployment(now = Date.now()) {
  return { schema: 'soty.app-deployment.v1', checkedAt: now, shellOrigin: 'https://shell.example',
    app: { id: 'app-' + 'a'.repeat(32), name: 'Project', state: 'enabled' },
    addresses: { revision: 2, canonical: { id: 'dom_' + 'b'.repeat(32), origin: 'https://canonical.example' },
      aliases: [{ id: 'dom_' + 'c'.repeat(32), origin: 'https://project.example', active: true, state: 'bound' }] },
    publication: { policyEpoch: 2, launchPolicy: 'anyone', listed: true, activeTargetRevision: 1 },
    source: { port: 8111, entryPath: '/?project=fixture', revision: 1, digest: 'd'.repeat(64), profile: 'soty.relay-restricted.v1' } };
}
export function manifest() { return { schema: 'soty.local-app.v1', name: 'Project', port: 8111, entryPath: '/?project=fixture' }; }
export function pairedDeployment(now = Date.now()) {
  const value = deployment(now); value.schema = 'soty.app-deployment.v2';
  const source = { id: 'dom_' + 'e'.repeat(32), origin: 'https://source.example', active: true, state: 'bound' };
  value.addresses.aliases.push(source); value.publication.activeTargetRevision = 2;
  value.source = { port: 8112, entryPath: '/embed', revision: 2, digest: 'd'.repeat(64), profile: 'soty.selected-human-embed.v2',
    launchEntry: { domainId: source.id, origin: source.origin, path: '/embed' } };
  value.nativeEntry = { domainId: value.addresses.aliases[0].id, origin: value.addresses.aliases[0].origin,
    target: { revision: 1, digest: 'f'.repeat(64), profile: 'soty.relay-restricted.v1', port: 8111, entryPath: '/?project=fixture' },
    committedPolicyEpoch: 2, admissionKeyHash: '9'.repeat(64) };
  return value;
}
export function nativePolicy(plan) {
  return { schema: 'soty.native-entry-policy.v1', appId: plan.appId, origin: plan.origin,
    primaryTargetRevision: plan.source.revision, primaryTargetDigest: plan.source.digest,
    nativeTargetRevision: plan.nativeEntry?.target.revision ?? plan.source.revision,
    nativeTargetDigest: plan.nativeEntry?.target.digest ?? plan.source.digest,
    policyEpoch: plan.publication.policyEpoch, admissionKeyHash: plan.nativeEntry?.admissionKeyHash ?? '9'.repeat(64), leaseMs: 1000 };
}
