/** Keep the form connected while the browser's planned navigation is pending.
 * pagehide confirms only departure, never successful OAuth or a server ACK. */
export function submitOAuthCompletion(interactionId: string, expectedAccountId: string): Promise<void> {
  if (!/^[A-Za-z0-9_-]{16,128}$/u.test(interactionId) || !expectedAccountId || expectedAccountId.length > 160) {
    return Promise.reject(new TypeError('Completion unavailable'));
  }
  return submitCompletion(`/oauth/interaction/${interactionId}/complete`, 'expectedAccountId', expectedAccountId);
}

export function submitHumanCompletion(interactionId: string, csrf: string): Promise<void> {
  if (!/^[A-Za-z0-9_-]{16,128}$/u.test(interactionId) || !/^[A-Za-z0-9_-]{16,128}$/u.test(csrf)) {
    return Promise.reject(new TypeError('Completion unavailable'));
  }
  return submitCompletion(`/human-identity/interaction/${interactionId}/complete`, 'csrf', csrf);
}

function submitCompletion(route: string, field: string, value: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const form = document.createElement('form');
    form.method = 'POST'; form.action = route;
    form.enctype = 'application/x-www-form-urlencoded'; form.hidden = true;
    const expected = document.createElement('input');
    expected.type = 'hidden'; expected.name = field; expected.value = value;
    form.append(expected); document.body.append(form);
    let finished = false;
    const finish = (navigated: boolean, blocked = false): void => {
      if (finished) return;
      finished = true; clearTimeout(timer); window.removeEventListener('pagehide', left);
      window.removeEventListener('securitypolicyviolation', policy);
      form.remove();
      if (navigated) resolve(); else reject(Object.assign(new TypeError('Completion not confirmed'), {
        code: blocked ? 'navigation_policy_blocked' : 'navigation_unconfirmed',
      }));
    };
    const left = (): void => { finish(true); };
    const policy = (event: SecurityPolicyViolationEvent): void => {
      if (event.effectiveDirective === 'form-action') finish(false, true);
    };
    const timer = setTimeout(() => { finish(false); }, 8000);
    window.addEventListener('pagehide', left, { once: true });
    window.addEventListener('securitypolicyviolation', policy);
    try { form.submit(); } catch { finish(false); }
  });
}
