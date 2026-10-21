const schema = 'soty.transit-profile.v1';

export async function loadTransitAccountState(client, accountId) {
  const vault = await client.loadVault(accountId);
  const row = vault.payload?.transitClient;
  if (row && (row.schema !== schema || !row.profile || typeof row.profile !== 'object')) throw new Error('transit-saved-profile-invalid');
  return { profile: row?.profile || null, controllers: vault.payload?.transitControllers || {} };
}

export async function loadTransitAccountProfile(client, accountId) {
  return (await loadTransitAccountState(client, accountId)).profile;
}

async function update(client, accountId, change) {
  const vault = await client.loadVault(accountId);
  if (vault.payload !== null && (typeof vault.payload !== 'object' || Array.isArray(vault.payload))) throw new Error('transit-vault-shape');
  // Connect encrypts the whole vault locally and checks both account and revision.
  // Preserve every other feature's payload and let a concurrent write fail CAS.
  return client.saveVault(change(vault.payload || {}), vault.revision, accountId);
}

export async function saveTransitAccountProfile(client, accountId, profile) {
  return update(client, accountId, payload => ({ ...payload, transitClient: { schema, profile } }));
}

export async function saveTransitController(client, accountId, deviceId, token) {
  if (!/^[A-Za-z0-9_-]{43}$/u.test(token) || !deviceId) throw new Error('transit-control-invalid');
  return update(client, accountId, payload => {
    const previous = payload.transitControllers || {};
    if (typeof previous !== 'object' || Array.isArray(previous)) throw new Error('transit-vault-shape');
    return { ...payload, transitControllers: { ...previous, [deviceId]: token } };
  });
}
