import test from 'node:test';
import assert from 'node:assert/strict';
import * as chessFeature from '../features/chess.ts';

test('personal opponent gets a complete bounded local legal set instead of the truncated coach subset', () => {
  assert.equal(typeof chessFeature.buildChessOpponentPosition, 'function');
  const snapshot = chessFeature.createChessSnapshot({ mode: 'peer', localNick: 'PRIVATE_NAME', opponentNick: 'PRIVATE_NAME' });
  const wide = { ...snapshot, fen: '7k/8/8/8/2Q2Q2/1Q2Q3/Q2Q4/K7 w - - 0 1' };
  const coach = chessFeature.buildChessCoachPosition(wide), opponent = chessFeature.buildChessOpponentPosition(wide);
  assert.equal(coach.legalMovesTruncated, true); assert.equal(opponent.legalMovesTruncated, false);
  assert.ok(opponent.legalUci.length > 64 && opponent.legalUci.length <= 256);
  assert.equal(opponent.legalUci.every(move => /^[a-h][1-8][a-h][1-8][qrbn]?$/u.test(move)), true);
  assert.equal(JSON.stringify(opponent).includes('PRIVATE_NAME'), false);
});

import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';
const text = await readFile(new URL('./chess-personal.ts', import.meta.url), 'utf8');
const compiled = ts.transpileModule(text, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const module = { exports: {} };
vm.runInNewContext(compiled, { module, exports: module.exports, require: name => { assert.equal(name, '../features/chess'); return chessFeature; } });
export const createPersonalChess = module.exports.createPersonalChess;
const plain = value => JSON.parse(JSON.stringify(value));
function fixture() {
  const preferences = new Map(), data = new Map(), writes = [];
  const port = (map, kind) => ({ getItem: key => map.get(key) ?? null, setItem(key, value) { writes.push({ kind, key }); map.set(key, value); } });
  const make = accountId => createPersonalChess({ accountId, preferences: port(preferences, 'preference'), storage: port(data, 'match') });
  return { make, preferences, data, writes };
}
const capture = (match, requestId) => ({ gameId: match.board.gameId, fen: match.board.fen, side: match.side, generation: match.generation, requestId });

test('private state writes only dot/tab namespace and restores paused without recovering a send permission', () => {
  const f = fixture(), game = f.make('account_A'); game.start('w'); assert.equal(game.view().paused, true); game.resume();
  assert.equal(game.canHuman(), true); assert.equal(game.canRequest(), false);
  assert.equal(f.writes.every(row => row.key.startsWith('soty.personal-chess-') && !row.key.startsWith('soty:')), true);
  const restored = f.make('account_A'); assert.equal(restored.view().paused, true); assert.equal(restored.canRequest(), false);
  assert.equal(restored.view().board.fen, game.view().board.fen); assert.equal(restored.preferred(), true);
  assert.equal(f.make('account_B').view(), null);
});

test('humanSide white and black are enforced independently of legacy peer mode', () => {
  for (const side of ['w', 'b']) {
    const f = fixture(), game = f.make('account_A'); game.start(side); game.resume();
    assert.equal(game.view().board.mode, 'peer'); assert.equal(game.view().board.coach, 'quiet');
    assert.equal(game.canHuman(), side === 'w'); assert.equal(game.canRequest(), side === 'b');
    if (side === 'w') { assert.equal(game.human('e7', 'e5'), false); assert.equal(game.human('e2', 'e4'), true); assert.equal(game.canHuman(), false); }
    else assert.equal(game.human('e2', 'e4'), false);
  }
});

test('model move requires offered and fresh legal UCI plus exact position request and side', () => {
  const f = fixture(), game = f.make('account_A'); game.start('b'); game.resume(); const before = game.view();
  const legal = chessFeature.buildChessOpponentPosition(before.board).legalUci;
  assert.equal(game.begin('request_0001', before.generation), true);
  const frame = capture(before, 'request_0001');
  for (const [answer, offers, modified] of [['e2e5', legal, frame], ['e2e4', ['a2a3'], frame], ['e2e4 then run code', legal, frame], ['e2e4', legal, { ...frame, requestId: 'request_foreign' }], ['e2e4', legal, { ...frame, side: 'b' }]]) {
    assert.equal(game.apply(answer, offers, modified), false); assert.equal(game.view().board.fen, before.board.fen);
  }
  assert.equal(game.apply('e2e4', legal, frame), true); assert.equal(game.canHuman(), true); assert.notEqual(game.view().board.fen, before.board.fen);
});

test('pause and restart invalidate captured model move without an engine fallback', () => {
  for (const restart of [false, true]) {
    const f = fixture(), game = f.make('account_A'); game.start('b'); game.resume(); const before = game.view();
    const legal = chessFeature.buildChessOpponentPosition(before.board).legalUci; game.begin('request_0001', before.generation);
    if (restart) { game.start('b'); game.resume(); } else { game.pause(); game.resume(); }
    assert.equal(game.apply('e2e4', legal, capture(before, 'request_0001')), false);
    assert.equal(game.view().board.fen, before.board.fen);
  }
});

test('confirmed invalid move permits a new explicit ID but unknown and cancel retain pause', () => {
  const f = fixture(), game = f.make('account_A'); game.start('b'); game.resume(); const before = game.view();
  game.begin('request_0001', before.generation); game.settle('request_0001', 'invalid');
  assert.equal(game.canRequest(), true); assert.equal(game.view().board.fen, before.board.fen);
  assert.equal(game.begin('request_0002', before.generation), true); game.settle('request_0002', 'unknown'); game.resume();
  assert.equal(game.canRequest(), false); assert.equal(game.view().requestId, 'request_0002');
  const restore = f.make('account_A'); assert.equal(restore.view().paused, true); assert.equal(restore.view().status, 'unknown');
  restore.settle('request_0002', 'cancel_requested'); restore.resume(); assert.equal(restore.canRequest(), false);
});

test('missing promotion is not silently changed to queen and malformed board never uses legacy fallback', () => {
  const f = fixture(), key = 'soty.personal-chess-match.v1:account_A', game = f.make('account_A'); game.start('b');
  const record = plain(game.view()); delete record.side; record.board.fen = '7k/P7/8/8/8/8/8/7K w - - 0 1'; f.data.set(key, JSON.stringify(record));
  const promo = f.make('account_A'); promo.resume(); const before = promo.view(), offered = chessFeature.buildChessOpponentPosition(before.board).legalUci;
  promo.begin('request_0001', before.generation); assert.equal(promo.apply('a7a8', offered, capture(before, 'request_0001')), false);
  assert.equal(promo.apply('a7a8n', offered, capture(before, 'request_0001')), true);
  record.board.fen = 'bad'; f.data.set(key, JSON.stringify(record)); assert.throws(() => f.make('account_A'), { code: 'chess_private_storage_unavailable' });
  record.board.fen = game.view().board.fen; record.board.mode = 'agent'; f.data.set(key, JSON.stringify(record)); assert.throws(() => f.make('account_A'), { code: 'chess_private_storage_unavailable' });
});

test('disposed private state has no display or writes and callers cannot mutate its view', () => {
  const f = fixture(), game = f.make('account_A'); game.start('w'); const snapshot = game.view(); snapshot.board.fen = 'bad';
  assert.notEqual(game.view().board.fen, 'bad'); const writes = f.writes.length; game.dispose();
  assert.equal(game.view(), null); game.resume(); assert.equal(f.writes.length, writes); assert.equal(game.apply('e2e4', [], capture(snapshot, 'request_0001')), false);
});

test('private storage refusal keeps RAM pause but refuses board mutation start or resume and preserves known settlement', () => {
  const data = new Map(); let refuse = false;
  const storage = { getItem: key => data.get(key) ?? null, setItem(key, value) { if (refuse) throw new Error('synthetic_private_write_refusal'); data.set(key, value); } };
  const game = createPersonalChess({ accountId: 'account_A', preferences: storage, storage }); game.start('b'); game.resume();
  const before = game.view(), offers = chessFeature.buildChessOpponentPosition(before.board).legalUci;
  game.begin('request_0001', before.generation); refuse = true;
  assert.throws(() => game.apply('e2e4', offers, capture(before, 'request_0001')), { code: 'chess_private_storage_unavailable' });
  assert.equal(game.view().board.fen, before.board.fen);
  assert.throws(() => game.pause(), { code: 'chess_private_storage_unavailable' });
  assert.equal(game.view().paused, true); assert.ok(game.view().generation > before.generation);
  const paused = game.view(); assert.throws(() => game.resume(), { code: 'chess_private_storage_unavailable' });
  assert.equal(game.view().paused, true); assert.equal(game.view().generation, paused.generation);
  assert.throws(() => game.settle('request_0001', 'complete'), { code: 'chess_private_storage_unavailable' });
  assert.equal(game.view().status, 'idle'); assert.equal(game.view().paused, true);
  assert.throws(() => game.start('w')); assert.equal(game.view().board.gameId, before.board.gameId);
  refuse = false; game.resume(); assert.equal(game.canRequest(), true);
});
