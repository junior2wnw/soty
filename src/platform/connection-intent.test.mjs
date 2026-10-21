import test from 'node:test';
import assert from 'node:assert/strict';
import { createCompactJoinCode, parseCompactJoinCode } from 'trustlink-kernel';
import { MAX_CONNECTION_INPUT_BYTES, parseConnectionIntent } from './connection-intent.mjs';

const ORIGIN = 'https://soty.example';
const CARD = 'card_' + 'a'.repeat(32), REQUEST = 'enroll_' + 'b'.repeat(32);
const APP = 'app-' + 'c'.repeat(32), DOMAIN = 'dom_' + 'd'.repeat(32);
const CONVERSATION = 'conv_' + 'e'.repeat(32), INVITATION = 'f'.repeat(64);
const contact = `/?connect=contact&card=${CARD}`;
const parse = value => parseConnectionIntent(value, ORIGIN);

test('contact and device classification preserve the existing opaque identifiers and freeze the result', () => {
  for (const value of [contact, '?' + contact.split('?')[1], ORIGIN + contact, ' \n' + ORIGIN + contact + '\t ']) {
    const intent = parse(value);
    assert.deepEqual(intent, { kind: 'contact', id: CARD });
    assert.equal(Object.isFrozen(intent), true);
  }
  assert.deepEqual(parse(`/?request=${REQUEST}&connect=device`), { kind: 'device', id: REQUEST });
  assert.equal(Object.hasOwn(parse(contact), 'accountId'), false);
});

test('boundary: origin refuses a foreign host, port, protocol and opaque-origin URL', () => {
  for (const value of [`https://other.example${contact}`, `https://soty.example:444${contact}`,
    `http://soty.example${contact}`, `blob:${ORIGIN}/fixture`, `data:text/plain,${contact}`, `//soty.example${contact}`]) {
    assert.equal(parse(value), null);
  }
  assert.deepEqual(parseConnectionIntent(`http://127.0.0.1:5188${contact}`, 'http://127.0.0.1:5188'), { kind: 'contact', id: CARD });
  assert.deepEqual(parseConnectionIntent(`https://соты.example${contact}`, 'https://xn--n1afe0b.example'), { kind: 'contact', id: CARD });
});

test('expected origin must be one exact HTTP origin, not a path or caller-selected redirect', () => {
  for (const origin of [undefined, null, '', 'null', 'file://', ORIGIN + '/', ORIGIN + '/admin',
    ORIGIN + '?x=1', ORIGIN + '#mine', 'https://user@soty.example', 'https://soty.example:443']) {
    assert.equal(parseConnectionIntent(contact, origin), null);
  }
});

test('boundary: credentials do not turn a same-origin URL into a trusted QR', () => {
  for (const authority of ['user@soty.example', 'user:password@soty.example', 'user%3Apassword@soty.example']) {
    assert.equal(parse(`https://${authority}${contact}`), null);
  }
});

test('root path is literal and no old reset, install or control route is opened', () => {
  for (const prefix of ['/admin', '/install/', '/x/../', '/%2e/', '/%2F', '/human-identity/']) {
    assert.equal(parse(ORIGIN + prefix + contact), null);
  }
  for (const value of ['/?reset-local=1', '/?soty-reset=1', '/?repair=1', '/?action=connect-device', '/install/']) {
    assert.equal(parse(value), null);
  }
});

test('boundary: duplicate fields are rejected even when both values look valid', () => {
  for (const value of [contact + '&card=' + CARD, contact + '&connect=contact',
    contact + '&%63ard=' + CARD, `/?connect=device&request=${REQUEST}&request=${REQUEST}`,
    '/?j=room_fixture.QWxpY2U&j=room_other.Qm9i']) {
    assert.equal(parse(value), null);
  }
});

test('closed query shapes reject unknown fields, mixed purposes and empty segments', () => {
  for (const value of [contact + '&request=' + REQUEST, contact + '&j=room_fixture.QWxpY2U',
    contact + '&trafficProfile=fixture', contact + '&redirect=https%3A%2F%2Fother.example',
    contact + '&', contact + '&&', '/?&' + contact.slice(2), contact + '&unknown=',
    `/?connect=unknown&card=${CARD}`, `/?connect=Contact&card=${CARD}`]) {
    assert.equal(parse(value), null);
  }
});

test('boundary: query and fragment cannot select competing purposes', () => {
  for (const suffix of ['#', '#mine', '#profile/acct_fixture', '#soty-invite=' + 'g'.repeat(43), '#app/' + APP]) {
    assert.equal(parse(contact + suffix), null);
    assert.equal(parse('/?j=room_fixture.QWxpY2U' + suffix), null);
  }
  assert.equal(parse('/?#profile/acct_fixture'), null);
});

test('opaque Connect IDs keep the existing minimum/maximum and reject encoded controls', () => {
  for (const id of ['', 'x'.repeat(15), 'x'.repeat(161), '../profile', '%00' + CARD, CARD + '%0A', 'a+b'.repeat(8)]) {
    assert.equal(parse('/?connect=contact&card=' + id), null);
  }
  assert.deepEqual(parse('/?connect=contact&card=' + 'x'.repeat(16)), { kind: 'contact', id: 'x'.repeat(16) });
  assert.deepEqual(parse('/?connect=device&request=' + 'x'.repeat(160)), { kind: 'device', id: 'x'.repeat(160) });
});

test('legacy room requests keep their carrier and never become a device or contact grant', () => {
  const intent = parse('/?j=room_fixture.QWxpY2U');
  assert.deepEqual(intent, { kind: 'legacy-room', joinCode: 'room_fixture.QWxpY2U', route: '/?j=room_fixture.QWxpY2U' });
  assert.equal(Object.isFrozen(intent), true);
  for (const value of ['/?j=missing_label', '/?j=.QWxpY2U', '/?j=room.', '/?j=room.label.extra',
    '/?j=room.<script>', '/?j=room.' + 'a'.repeat(2048), '/?t=room.secret.label', '/?i=secret-export',
    '/?connector=fixture', '/?trafficProfile=fixture', 'vless://fixture', '{"schema":"soty.device-link.v1"}']) {
    assert.equal(parse(value), null);
  }
});

test('legacy request roundtrips the actual maintained producer and decoder, including Unicode labels', () => {
  for (const label of ['Alice', '  Друзья   и   коллеги  ', 'Соты 🐝']) {
    const joinCode = createCompactJoinCode('room_fixture', label);
    const decoded = parseCompactJoinCode(joinCode);
    const intent = parse('/?j=' + encodeURIComponent(joinCode));
    assert.equal(intent.kind, 'legacy-room');
    assert.equal(intent.joinCode, joinCode);
    assert.deepEqual(parseCompactJoinCode(intent.joinCode), decoded);
    assert.equal(decoded.roomId, 'room_fixture');
    assert.equal(new URL(intent.route, ORIGIN).searchParams.get('j'), joinCode);
    assert.equal(Object.hasOwn(intent, 'deviceId'), false);
  }
});

test('boundary: legacy label decode rejects malformed payloads before routing', () => {
  for (const label of ['A', 'AAAAA', 'AAAAAAAAA']) {
    const code = 'room_fixture.' + label;
    assert.throws(() => parseCompactJoinCode(code), /base64url/u);
    assert.equal(parse('/?j=' + code), null);
  }
  assert.equal(parse('/?j=room_fixture.QWxpY2U').kind, 'legacy-room');
});

test('profile and community links describe an existing screen with no automatic membership', () => {
  assert.deepEqual(parse('#profile/acct_fixture'), { kind: 'profile', id: 'acct_fixture', route: '#profile/acct_fixture' });
  assert.deepEqual(parse('/#community/group_fixture'), { kind: 'community', id: 'group_fixture', tab: 'about', route: '#community/group_fixture/about' });
  for (const tab of ['about', 'chat', 'apps']) {
    assert.deepEqual(parse(`${ORIGIN}/#community/group_fixture/${tab}`), { kind: 'community', id: 'group_fixture', tab, route: `#community/group_fixture/${tab}` });
  }
  for (const value of ['#profile/ab', '#profile/acct_fixture/extra', '#profile/%61cct_fixture',
    '#profile/acct_fixture?q=1', '#community/group_fixture/invite', '#community/group_fixture/chat/extra',
    '#community/group_fixture?join=1', '#messages/acct_fixture', '#mine', '#world']) {
    assert.equal(parse(value), null);
  }
});

test('application routes reuse actual target, domain, path and discussion semantics', () => {
  const app = parse(`#app/${APP}/group_fixture?path=%2Feditor%3Ftab%3Done&panel=discussion&conversation=${CONVERSATION}`);
  assert.equal(app.kind, 'application');
  assert.deepEqual(app.launch.target, { appId: APP, path: '/editor?tab=one' });
  assert.equal(app.launch.communityId, 'group_fixture');
  assert.deepEqual(app.launch.presentation, { panel: 'discussion', conversationId: CONVERSATION });
  const named = parse(`#launch/${APP}/${DOMAIN}?path=%2F_soty%2Fguide`);
  assert.deepEqual(named.launch.target, { appId: APP, domainId: DOMAIN, path: '/_soty/guide' });
  assert.equal(named.launch.kind, 'launch');
  assert.equal(Object.isFrozen(named), true);
  assert.equal(Object.isFrozen(named.launch), true);
});

test('application invitation retains the existing one-use carrier and never serializes its token', () => {
  const intent = parse(`#app/${APP}?invitation=${INVITATION}`);
  assert.equal(intent.kind, 'application');
  assert.equal(intent.launch.route, `app/${APP}`);
  assert.equal(JSON.stringify(intent).includes(INVITATION), false);
  assert.equal(intent.launch.takeInvitation(), INVITATION);
  assert.equal(intent.launch.takeInvitation(), null);
});

test('discussion administrative presentation remains existing metadata with a closed grammar', () => {
  const intent = parse(`#app/${APP}?panel=discussion&admin=1`);
  assert.equal(intent.kind, 'application');
  assert.deepEqual(intent.launch.presentation, { panel: 'discussion', administrative: true });
  for (const tail of ['?panel=discussion&admin=0', '?panel=discussion&admin=1&admin=1',
    '?panel=discussion&admin=1&invitation=' + INVITATION]) {
    assert.equal(parse('#app/' + APP + tail), null);
  }
});

test('encoded duplicate application invitation keys are rejected without consuming a valid carrier', () => {
  assert.equal(parse(`#app/${APP}?invitation=${INVITATION}&%69nvitation=${INVITATION}`), null);
  const intent = parse(`#app/${APP}?invitation=${INVITATION}`);
  assert.equal(intent.launch.route.includes(INVITATION), false);
  assert.equal(JSON.stringify(intent).includes(INVITATION), false);
  assert.equal(intent.launch.takeInvitation(), INVITATION);
  assert.equal(intent.launch.takeInvitation(), null);
});

test('old PWA query-bearing share is rejected and the canonical producer form remains routable', () => {
  assert.equal(parse(ORIGIN + '/?pwa=1#community/group_fixture/about'), null);
  assert.equal(parse(ORIGIN + '/#community/group_fixture/about').kind, 'community');
});

test('boundary: application targets and invitation combinations cannot bypass the maintained parser', () => {
  for (const fragment of ['#app/not-an-app', `#launch/${APP}/not-a-domain`,
    `#app/${APP}?path=%2F_soty%2Fadmin`, `#app/${APP}?path=%2F%2Fother.example`,
    `#app/${APP}?path=%2Fone&path=%2Ftwo`, `#app/${APP}?unknown=1`,
    `#app/${APP}?admin=1`, `#app/${APP}?panel=wrong`,
    `#app/${APP}/group_fixture?invitation=${INVITATION}`, `#app/${APP}?path=%2F&invitation=${INVITATION}`,
    `#launch/${APP}/${DOMAIN}?invitation=${INVITATION}`, `#app/${APP}?invitation=invalid`]) {
    assert.equal(parse(fragment), null);
  }
});

test('platform invitation is only an admission marker; its fragment remains owned by the existing capturer', () => {
  const token = 'h'.repeat(43), intent = parse('#soty-invite=' + token);
  assert.deepEqual(intent, { kind: 'platform-invitation' });
  assert.equal(Object.isFrozen(intent), true);
  assert.equal(JSON.stringify(intent).includes(token), false);
  assert.equal(Object.hasOwn(intent, 'id'), false);
  for (const value of ['#soty-invite=' + token.slice(1), '#soty-invite=' + token + 'x',
    '#soty-invite=' + token + '&connect=device', '#soty-invite=' + token + '#profile/acct_fixture',
    '/?connect=device&request=' + REQUEST + '#soty-invite=' + token]) {
    assert.equal(parse(value), null);
  }
});

test('boundary: raw input is bounded before trimming, including multibyte pasted padding', () => {
  assert.equal(MAX_CONNECTION_INPUT_BYTES, 32_768);
  assert.equal(parse(' '.repeat(MAX_CONNECTION_INPUT_BYTES) + contact), null);
  assert.equal(parse('\u3000'.repeat(11_000) + contact), null);
  assert.deepEqual(parse('\u3000' + contact + '\u3000'), { kind: 'contact', id: CARD });
});

test('malformed, control-bearing and unsupported input is inert', () => {
  for (const value of [null, undefined, 1, {}, [], new URL(ORIGIN + contact), '', ' ',
    '\ud800', contact + '\udfff', 'https://soty.\nexample' + contact,
    ORIGIN + '/\t' + contact, ORIGIN + '\\' + contact, 'javascript:alert(1)',
    `https://${ORIGIN.slice(8)}.attacker.example${contact}`, 'not a URL', '/?']) {
    assert.equal(parse(value), null);
  }
});
