import { accountClient, observeAccount } from '../core/connect-client';
import { mountOAuthConsent, type OAuthConsentContext } from '../world/oauth-consent';

function contextValue(value: unknown, interactionId: string): OAuthConsentContext {
  if (!value || typeof value !== 'object') throw new TypeError('Consent unavailable');
  const item = value as Record<string, unknown>;
  const valid = item.interactionId === interactionId
    && typeof item.contextDigest === 'string' && /^[a-f0-9]{64}$/u.test(item.contextDigest)
    && typeof item.browserNonce === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(item.browserNonce)
    && ['soty-codex-cli', 'soty-opencode-cli'].includes(String(item.clientProfile))
    && typeof item.clientLabel === 'string' && item.clientLabel.length > 0 && item.clientLabel.length <= 80
    && typeof item.resource === 'string' && [location.origin, location.origin + '/mcp'].includes(item.resource)
    && item.scope === 'notes.createDraft'
    && Number.isSafeInteger(item.durationMs) && Number(item.durationMs) > 0 && Number(item.durationMs) <= 86400000
    && Number.isSafeInteger(item.budgetLimit) && Number(item.budgetLimit) >= 1 && Number(item.budgetLimit) <= 20
    && Number.isSafeInteger(item.expiresAt) && Number(item.expiresAt) > 0
    && Number.isSafeInteger(item.checkedAt) && Number(item.checkedAt) >= 0
    && Number(item.expiresAt) - Number(item.checkedAt) <= 600000
    && ['pending', 'approved', 'denied'].includes(String(item.decision))
    && (item.decision === 'pending' ? item.decidedAccountId === null
      : typeof item.decidedAccountId === 'string' && item.decidedAccountId.length > 0 && item.decidedAccountId.length <= 160);
  if (!valid) throw new TypeError('Consent unavailable');
  return { interactionId, contextDigest: item.contextDigest as string, browserNonce: item.browserNonce as string,
    clientProfile: item.clientProfile as string, clientLabel: item.clientLabel as string, resource: item.resource as string,
    scope: 'notes.createDraft', durationMs: item.durationMs as number, budgetLimit: item.budgetLimit as number,
    expiresAt: item.expiresAt as number, checkedAt: item.checkedAt as number,
    decision: item.decision as OAuthConsentContext['decision'], decidedAccountId: item.decidedAccountId as string | null };
}

export async function startOAuthConsent(root: HTMLElement, interactionId: string): Promise<void> {
  if (!/^[A-Za-z0-9_-]{16,128}$/u.test(interactionId)) throw new TypeError('Consent unavailable');
  const initial = await accountClient.getLocalState();
  let observedId = initial.accountId;
  const route = `/oauth/interaction/${interactionId}`;
  const handle = mountOAuthConsent(root, {
    account: async () => {
      const state = await accountClient.getLocalState();
      return { accountId: state.accountId, label: state.label || 'Ваш аккаунт' };
    },
    observeAccount: listener => observeAccount(state => {
      if (state.accountId !== observedId || state.current?.revoked === true) { observedId = state.accountId; listener(); }
    }),
    context: async () => {
      const response = await fetch(route + '/context', { cache: 'no-store', credentials: 'same-origin',
        headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(8000) });
      if (!response.ok || !response.headers.get('content-type')?.startsWith('application/json')) throw new TypeError('Consent unavailable');
      return contextValue(await response.json(), interactionId);
    },
    decide: async (kind, args) => {
      const result = await accountClient.extension<{ approved?: boolean; denied?: boolean }>(`oauth.connections.${kind}`, args,
        { expectedAccountId: args.expectedAccountId });
      if (kind === 'approve' ? result.approved !== true : result.denied !== true) throw new TypeError('Consent not confirmed');
    },
    complete: expectedAccountId => {
      // A same-origin top-level POST lets the maintained provider perform its
      // registered callback redirect. Fetch never follows or exposes its code.
      const form = document.createElement('form'); form.method = 'POST'; form.action = route + '/complete';
      form.enctype = 'application/x-www-form-urlencoded'; form.hidden = true;
      const expected = document.createElement('input'); expected.type = 'hidden'; expected.name = 'expectedAccountId'; expected.value = expectedAccountId;
      form.append(expected); document.body.append(form);
      try { form.submit(); } finally { form.remove(); }
    },
    openAccount: async () => {
      const [{ openConnectPanel }, { default: QRCode }] = await Promise.all([
        import('../../modules/connect/ui/index.mjs'), import('qrcode'), import('../../modules/connect/ui/style.css'),
      ]);
      const state = await accountClient.getLocalState();
      const panel = openConnectPanel({ client: accountClient, label: state.label || 'Мой профиль', productName: 'Соты',
        qr: url => QRCode.toDataURL(url, { width: 256, margin: 2 }), initialTab: 'profile' });
      const dialog = document.querySelector<HTMLDialogElement>('dialog.connect-panel');
      if (dialog) await new Promise<void>(resolve => dialog.addEventListener('close', () => resolve(), { once: true }));
      else panel.close();
    },
  });
  window.addEventListener('pagehide', event => { if (!event.persisted) handle.dispose(); }, { once: true });
  window.addEventListener('pageshow', event => { if (event.persisted) location.reload(); });
}
