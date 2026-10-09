import { applyChessMove, buildChessOpponentPosition, createChessSnapshot, normalizeChessSnapshot, parseChessOpponentMove, chessSchema } from '../features/chess';
import type { ChessSnapshot } from '../features/chess';
import type { Color, PieceSymbol, Square } from 'chess.js';

type Status = 'idle' | 'running' | 'unknown' | 'cancel_requested' | 'invalid';
type Record = { schema: 'soty.personal-chess.v1'; accountId: string; board: ChessSnapshot; humanSide: Color; generation: number;
  paused: boolean; status: Status; requestId?: string };
export type PersonalChessCapture = { gameId: string; fen: string; side: Color; generation: number; requestId: string };
type StoragePort = Pick<Storage, 'getItem' | 'setItem'>;
const fail = () => Object.assign(new Error('chess_private_storage_unavailable'), { code: 'chess_private_storage_unavailable' });
const copy = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;

/** Tab-local match data. No identity, conversation, transport or legacy room state. */
export function createPersonalChess({ accountId, preferences, storage }: { accountId: string; preferences: StoragePort; storage: StoragePort }) {
  const preferenceKey = `soty.personal-chess-choice.v1:${accountId}`, key = `soty.personal-chess-match.v1:${accountId}`;
  let record: Record | null = null, closed = false;
  const raw = storage.getItem(key);
  if (raw !== null) {
    if (raw.length > 64000) throw fail();
    let value: Record; try { value = JSON.parse(raw) as Record; } catch { throw fail(); }
    if (!value || value.schema !== 'soty.personal-chess.v1' || value.accountId !== accountId
      || !['w', 'b'].includes(value.humanSide) || !Number.isSafeInteger(value.generation) || value.generation < 0
      || typeof value.paused !== 'boolean' || !['idle', 'running', 'unknown', 'cancel_requested', 'invalid'].includes(value.status)
      || value.requestId !== undefined && !/^[A-Za-z0-9_.:-]{8,100}$/u.test(value.requestId)
      || value.board?.schema !== chessSchema || value.board.mode !== 'peer' || value.board.coach !== 'quiet'
      || !/^[A-Za-z0-9_.:-]{1,128}$/u.test(value.board.gameId)) throw fail();
    try { buildChessOpponentPosition(value.board); } catch { throw fail(); }
    const board = normalizeChessSnapshot(value.board, { mode: 'peer', localNick: 'Вы', opponentNick: 'Личный помощник' });
    record = { ...value, board, paused: true, status: value.status === 'running' ? 'unknown' : value.status };
  }
  const save = (next: Record) => {
    if (closed) throw fail(); const text = JSON.stringify(next); if (text.length > 64000) throw fail();
    try { storage.setItem(key, text); } catch { throw fail(); }
    if (closed) throw fail(); record = next;
  };
  let checkedFen = '', checked: ReturnType<typeof buildChessOpponentPosition> | null = null;
  const fresh = () => {
    if (!record) return null;
    if (checkedFen !== record.board.fen) { checked = buildChessOpponentPosition(record.board); checkedFen = record.board.fen; }
    return checked;
  };
  const view = () => closed || !record ? null : { ...copy(record), side: record.board.fen.split(' ')[1] as Color };
  const blocked = () => !record || closed || record.paused || ['running', 'unknown', 'cancel_requested'].includes(record.status) || Boolean(record.board.result);
  const matches = (capture: PersonalChessCapture) => Boolean(record && record.board.gameId === capture.gameId && record.board.fen === capture.fen
    && record.generation === capture.generation && fresh()?.side === capture.side && record.requestId === capture.requestId);
  return {
    view,
    preferred() { return preferences.getItem(preferenceKey) === 'personal'; },
    start(humanSide: Color = 'w') {
      if (closed) throw fail(); preferences.setItem(preferenceKey, 'personal');
      const board = createChessSnapshot({ mode: 'peer', coach: 'quiet', localNick: humanSide === 'w' ? 'Вы' : 'Личный помощник', opponentNick: humanSide === 'w' ? 'Личный помощник' : 'Вы' });
      save({ schema: 'soty.personal-chess.v1', accountId, board, humanSide, generation: (record?.generation ?? 0) + 1, paused: true, status: 'idle' });
    },
    resume() { if (record && !closed) save({ ...record, paused: false, generation: record.generation + 1 }); },
    pause() { if (record && !closed && !record.paused) {
      const next = { ...record, paused: true, generation: record.generation + 1 };
      record = next; save(next); // A failed durable write cannot undo the local safety fence.
    } },
    canHuman() { return !blocked() && fresh()?.side === record!.humanSide; },
    canRequest() { return !blocked() && fresh()?.side !== record!.humanSide && fresh()!.legalUci.length > 0; },
    human(from: Square, to: Square, promotion: PieceSymbol = 'q') {
      if (blocked() || fresh()?.side !== record!.humanSide) return false;
      const move = parseChessOpponentMove(`${from}${to}${promotion && fresh()!.legalUci.includes(`${from}${to}${promotion}`) ? promotion : ''}`, fresh()!.legalUci);
      if (!move) return false;
      const moved = applyChessMove(record!.board, move.from, move.to, move.promotion);
      if (!moved) return false;
      const { requestId: _previous, ...next } = record!;
      save({ ...next, board: moved.snapshot, generation: record!.generation + 1, status: 'idle' }); return true;
    },
    begin(requestId: string, generation: number) {
      if (blocked() || fresh()?.side === record!.humanSide || record!.generation !== generation || !/^[A-Za-z0-9_.:-]{8,100}$/u.test(requestId)) return false;
      save({ ...record!, status: 'running', requestId }); return true;
    },
    settle(requestId: string, status: 'complete' | 'invalid' | 'unknown' | 'cancel_requested') {
      if (!record || closed || record.requestId !== requestId) return;
      const next: Record = { ...record, status: status === 'complete' ? 'idle' : status,
        paused: status === 'unknown' || status === 'cancel_requested' ? true : record.paused };
      record = next;
      try { save(next); } catch (cause) {
        if (!closed) record = { ...next, paused: true, generation: next.generation + 1 };
        throw cause;
      }
    },
    apply(answer: string, offered: readonly string[], capture: PersonalChessCapture) {
      if (!record || closed || record.paused || record.status !== 'running' || !matches(capture) || capture.side === record.humanSide) return false;
      const move = parseChessOpponentMove(answer, offered), now = fresh()!;
      if (!move || !parseChessOpponentMove(answer, now.legalUci)) return false;
      const moved = applyChessMove(record.board, move.from, move.to, move.promotion);
      if (!moved) return false;
      save({ ...record, board: moved.snapshot, generation: record.generation + 1, status: 'idle' }); return true;
    },
    dispose() { closed = true; record = null; },
  };
}
