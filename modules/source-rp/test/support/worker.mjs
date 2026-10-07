import { createSourceRpSessionService } from '../../server/index.mjs';
import { sqliteSource } from './sqlite-source.mjs';
process.on('message', async input => {
  let native;
  try {
    native = sqliteSource(input.path, { key: Buffer.from(input.key, 'base64url'), clock: () => input.time });
    const protocol = { async ready() {},
      async renew(proof) {
        const result = await fetch(input.origin + '/renew', { method: 'POST', headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ refreshToken: proof.refreshToken }), signal: AbortSignal.timeout(5000), redirect: 'error' });
        if (!result.ok) throw Object.assign(new Error('fixture_refresh_failed'), { status: 503 });
        return { ...await result.json(), nonce: proof.nonce, expiresAt: input.time + 300000 };
      },
      async currentSubject(_token, sub) { return sub; },
    };
    const service = createSourceRpSessionService({ storagePort: native.storagePort, protocol, profileDigest: input.marker.profileDigest,
      keyId: native.keyId, encrypt: native.encrypt, decrypt: native.decrypt, clock: () => input.time });
    const proof = await service.currentProof(input.marker); process.send({ ok: true, generation: proof.sessionGeneration });
  } catch (error) { process.send({ ok: false, code: /^[a-z_]{1,80}$/u.test(error?.code || '') ? error.code : 'fixture_failed' }); }
  finally { native?.close(); process.disconnect(); }
});
