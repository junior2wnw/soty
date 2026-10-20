const audienceModes = ['private', 'friends', 'link', 'public'];
function publicationView(value) {
  if (!value || !['restricted', 'anyone'].includes(value.launchPolicy)
    || !Number.isSafeInteger(value.activeNamedAddressCount) || value.activeNamedAddressCount < 0) return null;
  if (value.audience !== undefined && value.audience !== null) {
    const named = ['link', 'public'].includes(value.audience);
    if (!audienceModes.includes(value.audience) || value.launchPolicy !== (named ? 'anyone' : 'restricted')
      || value.listed !== (value.audience === 'public') || named && value.activeNamedAddressCount < 1) return null;
  }
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
  if (publication.audience !== undefined && publication.audience !== null) {
    const mode = publication.audience;
    return { label: ({ private: 'Только вы', friends: 'Друзьям', link: 'По ссылке', public: 'Всем' })[mode],
      icon: mode === 'private' ? 'lock' : mode === 'friends' ? 'people' : 'external',
      publicNamed: mode === 'link' || mode === 'public',
      details: mode === 'private' ? ['Доступ только у вас.'] : mode === 'friends' ? ['Доступ есть у ваших текущих друзей.']
        : mode === 'link' ? ['Открывается по именной ссылке.', 'В общем каталоге не показывается.']
          : ['Доступно по именной ссылке и в общем каталоге.'] };
  }
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
  return { launchPolicy: policy.launchPolicy,
    ...(Object.hasOwn(policy, 'audience') ? { audience: policy.audience, listed: policy.listed } : {}), activeNamedAddressCount: snapshot.app.state === 'enabled'
    ? snapshot.addresses.aliases.filter(alias => alias.state === 'bound' && alias.active && active.has(alias.id)).length : 0 };
}
