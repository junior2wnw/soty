import test from 'node:test';
import assert from 'node:assert/strict';
import { createNativeVoiceDescriptor, validateNativeVoiceDescriptor } from './native-voice-descriptor.mjs';

const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const mount = () => ({ scope: 'kvartalufa.voice', appId: 'app-' + 'a'.repeat(32), parentOrigin: 'https://shell.example',
  frameOrigin: 'https://voice.example', nativeOrigin: 'https://voice.example', source: { id: 'voice-source', version: 1, digest: 'b'.repeat(64) } });
const response = nativeVoice => ({ ok: true, async json() { return { nativeVoice }; } });

test('pending metadata is shared and never represented as disabled', async () => {
  const gate = deferred(); let calls = 0, settled = false;
  const resolver = createNativeVoiceDescriptor({ fetch: async (url, options) => {
    calls++; assert.equal(url, '/api/apps/capabilities'); assert.equal(options.cache, 'no-store'); return gate.promise;
  } });
  const a = resolver.read(), b = resolver.read(); void a.then(() => { settled = true; });
  await Promise.resolve(); assert.equal(resolver.snapshot().state, 'pending'); assert.equal(settled, false); assert.equal(calls, 1);
  gate.resolve(response(mount())); assert.equal(await a, await b); assert.equal(resolver.snapshot().state, 'ready');
  assert.equal(Object.isFrozen(await a), true); assert.equal(Object.isFrozen((await a).source), true); resolver.close();
});

test('disabled is an explicit successful null descriptor and needs no retry', async () => {
  let calls = 0; const resolver = createNativeVoiceDescriptor({ fetch: async () => { calls++; return response(null); } });
  assert.equal(await resolver.read(), null); assert.equal(await resolver.read({ retry: true }), null); assert.equal(calls, 1); resolver.close();
});

test('a failed lookup remains unavailable until an explicit shared retry', async () => {
  let calls = 0; const gate = deferred();
  const resolver = createNativeVoiceDescriptor({ fetch: async () => { calls++; if (calls === 1) throw Error('network'); return gate.promise; } });
  await assert.rejects(resolver.read(), { code: 'native_voice_descriptor_unavailable' });
  await assert.rejects(resolver.read(), { code: 'native_voice_descriptor_unavailable' }); assert.equal(calls, 1);
  const a = resolver.read({ retry: true }), b = resolver.read({ retry: true }); assert.equal(calls, 2);
  gate.resolve(response(null)); assert.equal(await a, null); assert.equal(await b, null); resolver.close();
});

test('page cancellation aborts the request and cannot publish a late descriptor or retry', async () => {
  const gate = deferred(), page = new AbortController(); let calls = 0, requestSignal;
  const resolver = createNativeVoiceDescriptor({ signal: page.signal, fetch: async (_url, options) => { calls++; requestSignal = options.signal; return gate.promise; } });
  const reading = resolver.read(); page.abort(); assert.equal(requestSignal.aborted, true);
  gate.resolve(response(mount())); await assert.rejects(reading, { code: 'native_voice_descriptor_unavailable' });
  await assert.rejects(resolver.read({ retry: true }), { code: 'native_voice_descriptor_unavailable' });
  assert.equal(resolver.snapshot().state, 'closed'); assert.equal(calls, 1);
});

test('an already closed page starts no metadata request', async () => {
  const page = new AbortController(); page.abort(); let calls = 0;
  const resolver = createNativeVoiceDescriptor({ signal: page.signal, fetch: async () => { calls++; return response(null); } });
  await assert.rejects(resolver.read(), { code: 'native_voice_descriptor_unavailable' }); assert.equal(calls, 0);
});

test('malformed metadata cannot disable the origin gate or inject a Source descriptor', async () => {
  const variants = [undefined, {}, { ...mount(), frameOrigin: 'http://voice.example' }, { ...mount(), parentOrigin: 'https://shell.example/' },
    { ...mount(), nativeOrigin: 'https://other.example' }, { ...mount(), extra: true }, { ...mount(), source: { ...mount().source, version: 0 } },
    { ...mount(), source: { ...mount().source, digest: 'secret-unsupported' } }];
  for (const value of variants) {
    assert.throws(() => validateNativeVoiceDescriptor(value), { code: 'native_voice_descriptor_unavailable' });
    const resolver = createNativeVoiceDescriptor({ fetch: async () => response(value) });
    await assert.rejects(resolver.read(), { code: 'native_voice_descriptor_unavailable' }); assert.equal(resolver.snapshot().state, 'unavailable'); resolver.close();
  }
  assert.throws(() => validateNativeVoiceDescriptor(Object.defineProperty(mount(), 'scope', { value: 'kvartalufa.voice', enumerable: false })),
    { code: 'native_voice_descriptor_unavailable' });
});
