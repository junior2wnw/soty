export function deployment(now = Date.now()) {
  return { schema: 'soty.app-deployment.v1', checkedAt: now, shellOrigin: 'https://shell.example',
    app: { id: 'app-' + 'a'.repeat(32), name: 'Project', state: 'enabled' },
    addresses: { revision: 2, canonical: { id: 'dom_' + 'b'.repeat(32), origin: 'https://canonical.example' },
      aliases: [{ id: 'dom_' + 'c'.repeat(32), origin: 'https://project.example', active: true, state: 'bound' }] },
    publication: { policyEpoch: 2, launchPolicy: 'anyone', listed: true, activeTargetRevision: 1 },
    source: { port: 8111, entryPath: '/?project=fixture', revision: 1, digest: 'd'.repeat(64), profile: 'soty.relay-restricted.v1' } };
}
export function manifest() { return { schema: 'soty.local-app.v1', name: 'Project', port: 8111, entryPath: '/?project=fixture' }; }
