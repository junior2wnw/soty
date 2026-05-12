import { Chess } from "chess.js";
import type { Color, Move, Piece, PieceSymbol, Square } from "chess.js";

export const chessSchema = "soty-chess-v1";
export const geniusCoach = "genius";

export type ChessMode = "peer" | "agent";
export type ChessCoach = "quiet" | typeof geniusCoach;
export type ChessResult = "" | "white" | "black" | "draw";

export interface ChessPlayer {
  readonly id: string;
  readonly nick: string;
  readonly side: Color;
  readonly agent?: boolean;
}

export interface ChessStats {
  readonly games: number;
  readonly whiteWins: number;
  readonly blackWins: number;
  readonly draws: number;
  readonly humanWins: number;
  readonly agentWins: number;
  readonly longestPly: number;
}

export interface ChessLastMove {
  readonly from: Square;
  readonly to: Square;
  readonly san: string;
  readonly color: Color;
  readonly piece: PieceSymbol;
  readonly captured?: PieceSymbol;
  readonly promotion?: PieceSymbol;
}

export interface ChessSnapshot {
  readonly schema: typeof chessSchema;
  readonly gameId: string;
  readonly mode: ChessMode;
  readonly coach: ChessCoach;
  readonly fen: string;
  readonly history: readonly string[];
  readonly players: Record<Color, ChessPlayer>;
  readonly stats: ChessStats;
  readonly result: ChessResult;
  readonly resultReason: string;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly lastMove?: ChessLastMove;
}

const files = ["a", "b", "c", "d", "e", "f", "g", "h"] as const;
const whiteRanks = ["8", "7", "6", "5", "4", "3", "2", "1"] as const;
const blackRanks = ["1", "2", "3", "4", "5", "6", "7", "8"] as const;
const promotionOrder: readonly PieceSymbol[] = ["q", "r", "b", "n"];
const pieceValues: Record<PieceSymbol, number> = {
  p: 100,
  n: 320,
  b: 330,
  r: 500,
  q: 900,
  k: 20_000
};

export function createChessSnapshot(options: {
  readonly mode: ChessMode;
  readonly localNick: string;
  readonly opponentNick: string;
  readonly stats?: ChessStats | undefined;
  readonly coach?: ChessCoach | undefined;
}): ChessSnapshot {
  const now = new Date().toISOString();
  const mode = options.mode;
  return {
    schema: chessSchema,
    gameId: `chess_${crypto.randomUUID()}`,
    mode,
    coach: options.coach ?? (mode === "agent" ? geniusCoach : "quiet"),
    fen: new Chess().fen(),
    history: [],
    players: defaultPlayers(mode, options.localNick, options.opponentNick),
    stats: sanitizeStats(options.stats),
    result: "",
    resultReason: "",
    createdAt: now,
    updatedAt: now
  };
}

export function normalizeChessSnapshot(
  raw: unknown,
  fallback: {
    readonly mode: ChessMode;
    readonly localNick: string;
    readonly opponentNick: string;
  }
): ChessSnapshot {
  if (!isRecord(raw)) {
    return createChessSnapshot(fallback);
  }

  const mode: ChessMode = raw.mode === "agent" || raw.mode === "peer" ? raw.mode : fallback.mode;
  const defaults = defaultPlayers(mode, fallback.localNick, fallback.opponentNick);
  const rawPlayers = isRecord(raw.players) ? raw.players : {};
  const now = new Date().toISOString();
  const fen = typeof raw.fen === "string" && isUsableFen(raw.fen) ? raw.fen : new Chess().fen();
  const history = Array.isArray(raw.history)
    ? raw.history.filter((item): item is string => typeof item === "string").slice(-400)
    : [];
  const result = raw.result === "white" || raw.result === "black" || raw.result === "draw" ? raw.result : "";
  const coach: ChessCoach = raw.coach === geniusCoach || raw.coach === "quiet"
    ? raw.coach
    : mode === "agent" ? geniusCoach : "quiet";

  return {
    schema: chessSchema,
    gameId: typeof raw.gameId === "string" && raw.gameId ? raw.gameId : `chess_${crypto.randomUUID()}`,
    mode,
    coach,
    fen,
    history,
    players: {
      w: sanitizePlayer(rawPlayers.w, defaults.w, "w"),
      b: sanitizePlayer(rawPlayers.b, defaults.b, "b")
    },
    stats: sanitizeStats(raw.stats),
    result,
    resultReason: typeof raw.resultReason === "string" ? raw.resultReason.slice(0, 80) : "",
    createdAt: typeof raw.createdAt === "string" ? raw.createdAt : now,
    updatedAt: typeof raw.updatedAt === "string" ? raw.updatedAt : now,
    ...sanitizeLastMove(raw.lastMove)
  };
}

export function chessFromSnapshot(snapshot: ChessSnapshot): Chess {
  try {
    return new Chess(snapshot.fen);
  } catch {
    return new Chess();
  }
}

export function boardSquares(orientation: Color): Square[] {
  const ranks = orientation === "b" ? blackRanks : whiteRanks;
  const rankFiles = orientation === "b" ? [...files].reverse() : files;
  const squares: Square[] = [];
  for (const rank of ranks) {
    for (const file of rankFiles) {
      squares.push(`${file}${rank}` as Square);
    }
  }
  return squares;
}

export function isSquare(value: string): value is Square {
  return /^[a-h][1-8]$/u.test(value);
}

export function pieceGlyph(piece: Piece | undefined): string {
  if (!piece) {
    return "";
  }
  const glyphs: Record<Color, Record<PieceSymbol, string>> = {
    w: {
      k: "&#9812;",
      q: "&#9813;",
      r: "&#9814;",
      b: "&#9815;",
      n: "&#9816;",
      p: "&#9817;"
    },
    b: {
      k: "&#9818;",
      q: "&#9819;",
      r: "&#9820;",
      b: "&#9821;",
      n: "&#9822;",
      p: "&#9823;"
    }
  };
  return glyphs[piece.color][piece.type];
}

export function sideName(side: Color): string {
  return side === "w" ? "Белые" : "Черные";
}

export function turnName(snapshot: ChessSnapshot): string {
  const game = chessFromSnapshot(snapshot);
  const player = snapshot.players[game.turn()];
  return player?.nick || sideName(game.turn());
}

export function statusText(snapshot: ChessSnapshot): string {
  const game = chessFromSnapshot(snapshot);
  if (snapshot.result === "white") {
    return `Мат. ${snapshot.players.w.nick} победили`;
  }
  if (snapshot.result === "black") {
    return `Мат. ${snapshot.players.b.nick} победили`;
  }
  if (snapshot.result === "draw") {
    return snapshot.resultReason || "Ничья";
  }
  if (game.isCheck()) {
    return `Шах. Ход: ${turnName(snapshot)}`;
  }
  return `Ход: ${turnName(snapshot)}`;
}

export function legalMovesForSquare(snapshot: ChessSnapshot, square: Square): Move[] {
  const game = chessFromSnapshot(snapshot);
  return game.moves({ verbose: true, square });
}

export function promotionChoices(snapshot: ChessSnapshot, from: Square, to: Square): PieceSymbol[] {
  const moves = legalMovesForSquare(snapshot, from)
    .filter((move) => move.to === to && Boolean(move.promotion))
    .map((move) => move.promotion)
    .filter((piece): piece is PieceSymbol => Boolean(piece));
  return promotionOrder.filter((piece) => moves.includes(piece));
}

export function applyChessMove(
  snapshot: ChessSnapshot,
  from: Square,
  to: Square,
  promotion: PieceSymbol = "q"
): { readonly snapshot: ChessSnapshot; readonly move: Move } | null {
  if (snapshot.result) {
    return null;
  }
  const game = chessFromSnapshot(snapshot);
  let move: Move;
  try {
    move = game.move({ from, to, promotion });
  } catch {
    return null;
  }
  return {
    move,
    snapshot: snapshotAfterMove(snapshot, game, move)
  };
}

export function chooseAgentMove(snapshot: ChessSnapshot): Move | null {
  const game = chessFromSnapshot(snapshot);
  const moves = game.moves({ verbose: true });
  if (moves.length === 0) {
    return null;
  }
  const ranked = moves
    .map((move) => ({ move, score: scoreMove(move) + Math.random() * 18 }))
    .sort((a, b) => b.score - a.score);
  const width = Math.min(3, ranked.length);
  return ranked[Math.floor(Math.random() * width)]?.move ?? moves[0] ?? null;
}

export function agentSide(snapshot: ChessSnapshot): Color {
  return snapshot.players.w.agent ? "w" : "b";
}

export function isAgentTurn(snapshot: ChessSnapshot): boolean {
  return snapshot.mode === "agent" && !snapshot.result && chessFromSnapshot(snapshot).turn() === agentSide(snapshot);
}

export function withCoach(snapshot: ChessSnapshot, coach: ChessCoach): ChessSnapshot {
  return {
    ...snapshot,
    coach,
    updatedAt: new Date().toISOString()
  };
}

export function buildGeniusLine(
  snapshot: ChessSnapshot,
  humanMove: Move | null,
  agentMove: Move | null
): string {
  const ply = snapshot.history.length;
  if (snapshot.result) {
    if (snapshot.result === "draw") {
      return `ГЕНИЙ: ничья. Записываю как философский компромисс, но в следующий раз будем требовать большего.`;
    }
    const winner = snapshot.result === "white" ? "w" : "b";
    if (winner === agentSide(snapshot)) {
      return `ГЕНИЙ: мат. Нравоучение короткое: короля надо беречь до того, как это стало новостью.`;
    }
    return `ГЕНИЙ: мат мне. Ладно, это было красиво. Забирай очко, но не зазнавайся.`;
  }

  const openers = [
    "центр не проси, центр бери",
    "фигура без дела - это налог на позицию",
    "шахматы любят людей, которые считают до двух",
    "каждый темп должен иметь работу",
    "если план не помещается в один ход, начни с лучшего хода"
  ];
  const roasts = [
    "ход принят, следствие разберется",
    "не преступление, но протокол я открыл",
    "смело. Я бы даже сказал: с доказательствами",
    "позиция стала честнее, а это уже опасно",
    "так рождаются партии, о которых потом говорят тише"
  ];
  const advice = openers[ply % openers.length] || openers[0];
  const roast = roasts[(ply + (agentMove?.san.length ?? 0)) % roasts.length] || roasts[0];
  const human = humanMove ? `Твой ${humanMove.san}: ${roast}.` : "";
  const agent = agentMove ? `Я отвечаю ${agentMove.san}.` : "";
  return `ГЕНИЙ: ${human} ${agent} ${advice}.`.replace(/\s+/gu, " ").trim();
}

function defaultPlayers(mode: ChessMode, localNick: string, opponentNick: string): Record<Color, ChessPlayer> {
  const local = localNick.trim() || "Я";
  if (mode === "agent") {
    return {
      w: { id: "local", nick: local, side: "w" },
      b: { id: "agent", nick: "Гений", side: "b", agent: true }
    };
  }
  return {
    w: { id: "white", nick: local || "Белые", side: "w" },
    b: { id: "black", nick: opponentNick.trim() || "Черные", side: "b" }
  };
}

function snapshotAfterMove(snapshot: ChessSnapshot, game: Chess, move: Move): ChessSnapshot {
  const history = [...snapshot.history, move.san].slice(-400);
  const base: ChessSnapshot = {
    ...snapshot,
    fen: game.fen(),
    history,
    updatedAt: new Date().toISOString(),
    lastMove: {
      from: move.from,
      to: move.to,
      san: move.san,
      color: move.color,
      piece: move.piece,
      ...(move.captured ? { captured: move.captured } : {}),
      ...(move.promotion ? { promotion: move.promotion } : {})
    }
  };
  return withResult(base, game);
}

function withResult(snapshot: ChessSnapshot, game: Chess): ChessSnapshot {
  if (!game.isGameOver()) {
    return snapshot;
  }
  const result = game.isCheckmate()
    ? game.turn() === "w" ? "black" : "white"
    : "draw";
  const resultReason = result === "draw" ? drawReason(game) : "Мат";
  if (snapshot.result) {
    return { ...snapshot, result, resultReason };
  }
  return {
    ...snapshot,
    result,
    resultReason,
    stats: addResult(snapshot.stats, snapshot.mode, snapshot.players, result, snapshot.history.length)
  };
}

function addResult(
  stats: ChessStats,
  mode: ChessMode,
  players: Record<Color, ChessPlayer>,
  result: ChessResult,
  ply: number
): ChessStats {
  const agent = players.w.agent ? "w" : players.b.agent ? "b" : "";
  const resultSide: Color | "" = result === "white" ? "w" : result === "black" ? "b" : "";
  return {
    games: stats.games + 1,
    whiteWins: stats.whiteWins + (result === "white" ? 1 : 0),
    blackWins: stats.blackWins + (result === "black" ? 1 : 0),
    draws: stats.draws + (result === "draw" ? 1 : 0),
    humanWins: stats.humanWins + (mode === "agent" && resultSide && resultSide !== agent ? 1 : 0),
    agentWins: stats.agentWins + (mode === "agent" && resultSide && resultSide === agent ? 1 : 0),
    longestPly: Math.max(stats.longestPly, ply)
  };
}

function drawReason(game: Chess): string {
  if (game.isStalemate()) {
    return "Пат";
  }
  if (game.isThreefoldRepetition()) {
    return "Троекратное повторение";
  }
  if (game.isInsufficientMaterial()) {
    return "Недостаточно материала";
  }
  if (game.isDrawByFiftyMoves()) {
    return "Правило 50 ходов";
  }
  return "Ничья";
}

function scoreMove(move: Move): number {
  let score = 0;
  if (move.captured) {
    score += pieceValues[move.captured] - pieceValues[move.piece] * 0.08;
  }
  if (move.promotion) {
    score += pieceValues[move.promotion] + 240;
  }
  if (move.isKingsideCastle() || move.isQueensideCastle()) {
    score += 80;
  }
  score += centerBonus(move.to);
  try {
    const next = new Chess(move.after);
    if (next.isCheckmate()) {
      score += 100_000;
    } else if (next.isCheck()) {
      score += 220;
    }
  } catch {
    // Keep the simple evaluator moving even if a future chess.js shape changes.
  }
  return score;
}

function centerBonus(square: Square): number {
  const file = square.charCodeAt(0) - 97;
  const rank = Number(square[1]) - 1;
  const fileDistance = Math.abs(file - 3.5);
  const rankDistance = Math.abs(rank - 3.5);
  return Math.max(0, 38 - (fileDistance + rankDistance) * 9);
}

function sanitizeStats(raw: unknown): ChessStats {
  const input = isRecord(raw) ? raw : {};
  return {
    games: safeCount(input.games),
    whiteWins: safeCount(input.whiteWins),
    blackWins: safeCount(input.blackWins),
    draws: safeCount(input.draws),
    humanWins: safeCount(input.humanWins),
    agentWins: safeCount(input.agentWins),
    longestPly: safeCount(input.longestPly)
  };
}

function sanitizePlayer(raw: unknown, fallback: ChessPlayer, side: Color): ChessPlayer {
  const input = isRecord(raw) ? raw : {};
  const nick = typeof input.nick === "string" && input.nick.trim() ? input.nick.trim().slice(0, 32) : fallback.nick;
  return {
    id: typeof input.id === "string" && input.id ? input.id.slice(0, 80) : fallback.id,
    nick,
    side,
    ...(input.agent === true || fallback.agent ? { agent: true } : {})
  };
}

function sanitizeLastMove(raw: unknown): { readonly lastMove?: ChessLastMove } {
  if (!isRecord(raw)) {
    return {};
  }
  const from = typeof raw.from === "string" && isSquare(raw.from) ? raw.from : "";
  const to = typeof raw.to === "string" && isSquare(raw.to) ? raw.to : "";
  const color = raw.color === "w" || raw.color === "b" ? raw.color : "";
  const piece = isPiece(raw.piece) ? raw.piece : "";
  if (!from || !to || !color || !piece) {
    return {};
  }
  return {
    lastMove: {
      from,
      to,
      color,
      piece,
      san: typeof raw.san === "string" ? raw.san.slice(0, 20) : "",
      ...(isPiece(raw.captured) ? { captured: raw.captured } : {}),
      ...(isPiece(raw.promotion) ? { promotion: raw.promotion } : {})
    }
  };
}

function isPiece(value: unknown): value is PieceSymbol {
  return value === "p" || value === "n" || value === "b" || value === "r" || value === "q" || value === "k";
}

function isUsableFen(value: string): boolean {
  try {
    new Chess(value);
    return true;
  } catch {
    return false;
  }
}

function safeCount(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) && value > 0
    ? Math.min(9999, Math.trunc(value))
    : 0;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}
