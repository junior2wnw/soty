import { accountClient, observeAccount } from '../core/connect-client';
import { parseHumanLoginContext } from './human-context.mjs';
import { mountHumanLogin } from '../world/human-login';
import { submitHumanCompletion } from './oauth-navigation';
import { registerUpdateGuard } from './pwa';

export async function startHumanLogin(root: HTMLElement, interactionId: string): Promise<void> {
  if (!/^[A-Za-z0-9_-]{16,128}$/u.test(interactionId)) throw new TypeError('human_login_unavailable');
  let observedId = (await accountClient.getLocalState()).accountId;
  const route = `/human-identity/interaction/${interactionId}`;
  const handle = mountHumanLogin(root, {
    account: async () => { const local = await accountClient.getLocalState(); return {
      accountId: local.accountId, label: local.label || 'Ваш профиль', revoked: local.current?.revoked === true }; },
    observeAccount: listener => observeAccount(local => {
      if (local.accountId !== observedId || local.current?.revoked) { observedId = local.accountId; listener(); }
    }),
    context: async () => {
      const response = await fetch(route + '/context', { cache: 'no-store', credentials: 'same-origin',
        headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(8000), redirect: 'error' });
      if (!response.ok || !response.headers.get('content-type')?.startsWith('application/json')) throw new TypeError('human_login_unavailable');
      const checked = Date.parse(response.headers.get('date') ?? '');
      return parseHumanLoginContext(await response.json(), { interactionId, checkedAt: Number.isFinite(checked) ? checked : Date.now() });
    },
    decide: async args => {
      const result = await accountClient.extension<{ schema: string; interactionId: string; requestId: string; decision: string }>('identity.human.approve', args, { expectedAccountId: args.expectedAccountId });
      if (result.schema !== 'soty.human-login-decision.v1' || result.interactionId !== args.interactionId
        || result.requestId !== args.requestId || result.decision !== (args.decision === 'approve' ? 'approved' : 'denied')) throw new TypeError('human_login_unconfirmed');
    },
    complete: csrf => submitHumanCompletion(interactionId, csrf),
    createProfile: async () => {
      if ((await accountClient.getLocalState()).accountId) throw new TypeError('profile_changed');
      const created = await accountClient.bootstrap('Мои соты'); return { accountId: created.accountId };
    },
    openAccount: async () => {
      const [{ openConnectPanel }, { default: QRCode }] = await Promise.all([
        import('../../modules/connect/ui/index.mjs'), import('qrcode'), import('../../modules/connect/ui/style.css'),
      ]);
      const local = await accountClient.getLocalState();
      const panel = openConnectPanel({ client: accountClient, label: local.label || 'Мой профиль', productName: 'Соты',
        bootstrapOnOpen: false, initialTab: local.accountId ? 'profile' : 'devices',
        qr: url => QRCode.toDataURL(url, { width: 256, margin: 2 }) });
      const dialog = document.querySelector<HTMLDialogElement>('dialog.connect-panel');
      if (dialog) await new Promise<void>(resolve => dialog.addEventListener('close', () => resolve(), { once: true })); else panel.close();
    },
  });
  const unguard = registerUpdateGuard(() => handle.canReload());
  window.addEventListener('pagehide', event => { if (!event.persisted) { unguard(); handle.dispose(); } }, { once: true });
  window.addEventListener('pageshow', event => { if (event.persisted) location.reload(); });
}
