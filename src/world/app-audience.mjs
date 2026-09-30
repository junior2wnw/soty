function publicationView(value) {
  if (!value || !['restricted', 'anyone'].includes(value.launchPolicy)
    || !Number.isSafeInteger(value.activeNamedAddressCount) || value.activeNamedAddressCount < 0) return null;
  return value;
}

/** Publication is a policy for exact named addresses, never for the legacy entry. */
export function describeAppAudience(app, accountId) {
  if (app.status === 'revoked') return { label: 'Доступ закрыт', icon: 'lock', publicNamed: false,
    details: ['Именные ссылки: выключены.', 'Личные допуски: закрыты.'] };
  if (!accountId || app.ownerAccountId !== accountId) return { label: 'Вам доступно', icon: 'app', publicNamed: false, details: [] };
  const publication = publicationView(app.publication);
  const accounts = Boolean(app.grants?.accountIds?.length), communities = Boolean(app.grants?.communityIds?.length);
  const direct = accounts && communities ? 'выбранным людям и сообществам'
    : communities ? 'выбранным сообществам' : accounts ? 'выбранным людям' : 'только вам';
  const directDetail = app.grants ? `Личные допуски: ${direct}.` : 'Личные допуски: состояние не проверено.';
  if (!publication) return { label: 'Ваше приложение', icon: 'app', publicNamed: false,
    details: ['Именные ссылки: состояние не проверено.', directDetail] };
  const publicNamed = publication.launchPolicy === 'anyone' && publication.activeNamedAddressCount > 0;
  return {
    label: publicNamed ? 'Именные ссылки: всем' : communities && accounts ? 'Выбранным участникам'
      : communities ? 'Выбранным сообществам' : accounts ? 'Выбранным людям' : app.grants ? 'Личный доступ' : 'Ваше приложение',
    // The existing external-link glyph avoids a private-lock icon on public aliases.
    icon: publicNamed ? 'external' : accounts || communities ? 'people' : app.grants ? 'lock' : 'app',
    publicNamed,
    details: [publication.activeNamedAddressCount === 0 ? 'Именные ссылки: выключены.'
      : publicNamed ? 'Именные ссылки: доступны всем.' : 'Именные ссылки: по личным допускам.', directDetail],
  };
}

/** The owner inspection has already resolved the active set and domain ownership. */
export function publicationFromInspection(snapshot) {
  const policy = snapshot.publication, active = new Set(policy.activeDomainIds);
  return { launchPolicy: policy.launchPolicy, activeNamedAddressCount: snapshot.app.state === 'enabled'
    ? snapshot.addresses.aliases.filter(alias => alias.state === 'bound' && alias.active && active.has(alias.id)).length : 0 };
}
