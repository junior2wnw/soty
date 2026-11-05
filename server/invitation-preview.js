import express from 'express';

/** A link preview exposes its published form, never members, account IDs or management state. */
export function attachInvitationPreview(app, { invitations }) {
  const rates = new Map();
  app.post('/api/invitations/preview', (req, res, next) => {
    res.set('Cache-Control', 'no-store').set('Referrer-Policy', 'no-referrer');
    const key = req.ip || req.socket.remoteAddress, now = Date.now();
    let rate = rates.get(key);
    if (!rate || now - rate.start >= 60000) {
      if (rates.size >= 4096) rates.delete(rates.keys().next().value);
      rate = { start: now, count: 0 }; rates.set(key, rate);
    }
    if (++rate.count > 60) { res.status(429).json({ ok: false, code: 'invitation_rate_limited' }); return; }
    next();
  }, express.json({ limit: '1kb', strict: true }), (req, res) => {
    try {
      if (!req.body || Array.isArray(req.body) || Object.keys(req.body).join(',') !== 'token') {
        res.status(400).json({ ok: false, code: 'invitation_unavailable' }); return;
      }
      res.json(invitations.preview(req.body.token));
    } catch (error) { res.status(error?.status === 400 ? 400 : 404).json({ ok: false, code: 'invitation_unavailable' }); }
  }, (_error, _req, res, _next) => { res.status(400).json({ ok: false, code: 'invitation_unavailable' }); });
}
