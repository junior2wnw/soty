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
  const book = chooseOpeningMove(snapshot, game);
  if (book) {
    return book;
  }

  const agent = agentSide(snapshot);
  const ordered = orderMoves(moves);
  let bestMove = ordered[0] ?? moves[0] ?? null;
  if (!bestMove) {
    return null;
  }

  const context: SearchContext = {
    agent,
    deadline: Date.now() + searchBudgetMs(game),
    nodes: 0
  };

  const maxDepth = searchDepth(game, snapshot.history.length);
  for (let depth = 1; depth <= maxDepth; depth += 1) {
    try {
      const result = searchRoot(game, depth, context, bestMove);
      bestMove = result.move;
    } catch (error) {
      if (error !== searchTimeout) {
        throw error;
      }
      break;
    }
  }

  return bestMove;
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

const searchTimeout = new Error("search timeout");
const searchInfinity = 9_000_000;
const checkmateScore = 1_000_000;
const maxQuiescencePly = 4;
const openingBook: Record<string, readonly string[]> = {
  "": ["e4", "d4"],
  e4: ["c5", "e5", "c6"],
  "e4 c5 Nf3": ["d6", "Nc6", "e6"],
  "e4 c5 Nf3 d6 d4": ["cxd4"],
  "e4 c5 Nf3 Nc6 d4": ["cxd4"],
  "e4 e5 Nf3": ["Nc6", "Nf6"],
  "e4 e5 Nf3 Nc6 Bb5": ["a6"],
  d4: ["Nf6", "d5"],
  "d4 Nf6 c4": ["e6", "g6"],
  "d4 Nf6 c4 e6 Nc3": ["Bb4"],
  "d4 d5 c4": ["e6", "c6"],
  c4: ["e5", "Nf6"],
  Nf3: ["d5", "Nf6"],
  g3: ["d5"],
  b3: ["e5"]
};

interface SearchContext {
  readonly agent: Color;
  readonly deadline: number;
  nodes: number;
}

interface SearchRootResult {
  readonly move: Move;
  readonly score: number;
}

interface BoardPiece {
  readonly square: Square;
  readonly type: PieceSymbol;
  readonly color: Color;
}

function chooseOpeningMove(snapshot: ChessSnapshot, game: Chess): Move | null {
  if (snapshot.history.length > 8) {
    return null;
  }
  const line = snapshot.history.join(" ");
  const choices = openingBook[line] ?? [];
  for (const san of choices) {
    const move = trySanMove(game, san);
    if (move) {
      return move;
    }
  }
  if (snapshot.history.length === 1) {
    for (const san of ["e5", "d5", "Nf6", "c5", "e6", "c6"]) {
      const move = trySanMove(game, san);
      if (move) {
        return move;
      }
    }
  }
  return null;
}

function trySanMove(game: Chess, san: string): Move | null {
  try {
    const move = game.move(san);
    game.undo();
    return move;
  } catch {
    return null;
  }
}

function searchRoot(game: Chess, depth: number, context: SearchContext, preferred: Move): SearchRootResult {
  let alpha = -searchInfinity;
  let bestMove = preferred;
  let bestScore = -searchInfinity;
  const moves = orderMoves(game.moves({ verbose: true }), preferred);
  for (const move of moves) {
    assertSearchTime(context);
    makeSearchMove(game, move);
    const score = minimax(game, depth - 1, alpha, searchInfinity, context, 1);
    game.undo();
    if (score > bestScore) {
      bestScore = score;
      bestMove = move;
      alpha = Math.max(alpha, score);
    }
  }
  return { move: bestMove, score: bestScore };
}

function minimax(
  game: Chess,
  depth: number,
  alpha: number,
  beta: number,
  context: SearchContext,
  ply: number
): number {
  assertSearchTime(context);
  const terminal = terminalScore(game, context.agent, ply);
  if (terminal !== null) {
    return terminal;
  }
  if (depth <= 0) {
    return quiescence(game, alpha, beta, context, ply, 0);
  }

  const maximizing = game.turn() === context.agent;
  const moves = orderMoves(game.moves({ verbose: true }));
  if (maximizing) {
    let value = -searchInfinity;
    for (const move of moves) {
      makeSearchMove(game, move);
      value = Math.max(value, minimax(game, depth - 1, alpha, beta, context, ply + 1));
      game.undo();
      alpha = Math.max(alpha, value);
      if (alpha >= beta) {
        break;
      }
    }
    return value;
  }

  let value = searchInfinity;
  for (const move of moves) {
    makeSearchMove(game, move);
    value = Math.min(value, minimax(game, depth - 1, alpha, beta, context, ply + 1));
    game.undo();
    beta = Math.min(beta, value);
    if (alpha >= beta) {
      break;
    }
  }
  return value;
}

function quiescence(
  game: Chess,
  alpha: number,
  beta: number,
  context: SearchContext,
  ply: number,
  quietPly: number
): number {
  assertSearchTime(context);
  const terminal = terminalScore(game, context.agent, ply);
  if (terminal !== null) {
    return terminal;
  }

  const standPat = evaluatePosition(game, context.agent);
  if (quietPly >= maxQuiescencePly) {
    return standPat;
  }

  const maximizing = game.turn() === context.agent;
  const tacticalMoves = orderMoves(game.moves({ verbose: true }).filter(isTacticalMove));
  if (tacticalMoves.length === 0) {
    return standPat;
  }

  if (maximizing) {
    if (standPat >= beta) {
      return beta;
    }
    alpha = Math.max(alpha, standPat);
    let value = standPat;
    for (const move of tacticalMoves) {
      makeSearchMove(game, move);
      value = Math.max(value, quiescence(game, alpha, beta, context, ply + 1, quietPly + 1));
      game.undo();
      alpha = Math.max(alpha, value);
      if (alpha >= beta) {
        break;
      }
    }
    return value;
  }

  if (standPat <= alpha) {
    return alpha;
  }
  beta = Math.min(beta, standPat);
  let value = standPat;
  for (const move of tacticalMoves) {
    makeSearchMove(game, move);
    value = Math.min(value, quiescence(game, alpha, beta, context, ply + 1, quietPly + 1));
    game.undo();
    beta = Math.min(beta, value);
    if (alpha >= beta) {
      break;
    }
  }
  return value;
}

function terminalScore(game: Chess, agent: Color, ply: number): number | null {
  if (game.isCheckmate()) {
    return game.turn() === agent ? -checkmateScore + ply : checkmateScore - ply;
  }
  if (game.isDraw()) {
    return 0;
  }
  return null;
}

function evaluatePosition(game: Chess, agent: Color): number {
  const pieces = piecesOnBoard(game);
  const endgame = totalNonKingMaterial(pieces) <= 2_700;
  const pawnsByColor = pawnFiles(pieces);
  const bishops: Record<Color, number> = { w: 0, b: 0 };
  let score = 0;

  for (const piece of pieces) {
    if (piece.type === "b") {
      bishops[piece.color] += 1;
    }
    const signed = piece.color === agent ? 1 : -1;
    score += signed * (
      pieceValues[piece.type]
      + piecePlacementScore(piece, endgame)
      + pawnStructureScore(piece, pawnsByColor)
    );
  }

  if (bishops[agent] >= 2) {
    score += 35;
  }
  const rival = opposite(agent);
  if (bishops[rival] >= 2) {
    score -= 35;
  }

  score += tempoScore(game, agent);
  return score;
}

function piecesOnBoard(game: Chess): BoardPiece[] {
  return game.board().flatMap((rank) => rank.filter(Boolean) as BoardPiece[]);
}

function totalNonKingMaterial(pieces: readonly BoardPiece[]): number {
  return pieces.reduce((sum, piece) => piece.type === "k" ? sum : sum + pieceValues[piece.type], 0);
}

function pawnFiles(pieces: readonly BoardPiece[]): Record<Color, number[]> {
  const result: Record<Color, number[]> = {
    w: Array.from({ length: 8 }, () => 0),
    b: Array.from({ length: 8 }, () => 0)
  };
  for (const piece of pieces) {
    if (piece.type === "p") {
      const file = fileIndex(piece.square);
      result[piece.color][file] = (result[piece.color][file] ?? 0) + 1;
    }
  }
  return result;
}

function piecePlacementScore(piece: BoardPiece, endgame: boolean): number {
  const file = fileIndex(piece.square);
  const forward = forwardRank(piece.square, piece.color);
  const center = centerBonus(piece.square);
  const edge = Math.max(Math.abs(file - 3.5), Math.abs(Number(piece.square[1]) - 4.5));

  if (piece.type === "p") {
    return forward * 8 + center * 0.35;
  }
  if (piece.type === "n") {
    return center * 1.4 - edge * 16 + (forward >= 2 ? 12 : 0);
  }
  if (piece.type === "b") {
    return center * 0.75 + (forward >= 2 ? 10 : 0);
  }
  if (piece.type === "r") {
    return forward * 2 + (file === 0 || file === 7 ? 0 : 6);
  }
  if (piece.type === "q") {
    return center * 0.35 - (forward >= 5 ? 6 : 0);
  }

  return endgame ? center * 1.2 : kingSafetyScore(piece.square, piece.color);
}

function pawnStructureScore(piece: BoardPiece, pawnsByColor: Record<Color, number[]>): number {
  if (piece.type !== "p") {
    return 0;
  }
  const file = fileIndex(piece.square);
  const friendly = pawnsByColor[piece.color];
  const enemy = pawnsByColor[opposite(piece.color)];
  let score = 0;

  if ((friendly[file] ?? 0) > 1) {
    score -= 12;
  }
  if ((friendly[file - 1] ?? 0) === 0 && (friendly[file + 1] ?? 0) === 0) {
    score -= 10;
  }
  if (isPassedPawn(piece, enemy)) {
    score += 22 + forwardRank(piece.square, piece.color) * 12;
  }
  return score;
}

function isPassedPawn(piece: BoardPiece, enemyFiles: readonly number[]): boolean {
  const file = fileIndex(piece.square);
  const rank = Number(piece.square[1]);
  for (let currentFile = file - 1; currentFile <= file + 1; currentFile += 1) {
    if ((enemyFiles[currentFile] ?? 0) === 0) {
      continue;
    }
    return false;
  }
  return piece.color === "w" ? rank >= 4 : rank <= 5;
}

function kingSafetyScore(square: Square, color: Color): number {
  const file = fileIndex(square);
  const rank = Number(square[1]);
  const homeRank = color === "w" ? 1 : 8;
  if (rank === homeRank && (file <= 2 || file >= 5)) {
    return 42;
  }
  if (file >= 3 && file <= 4) {
    return -38;
  }
  return -8;
}

function tempoScore(game: Chess, agent: Color): number {
  return game.turn() === agent ? 8 : -8;
}

function orderMoves(moves: readonly Move[], preferred?: Move): Move[] {
  return [...moves].sort((a, b) => {
    const preferredDelta = sameMove(b, preferred) ? 1 : sameMove(a, preferred) ? -1 : 0;
    if (preferredDelta !== 0) {
      return preferredDelta;
    }
    const scoreDelta = moveOrderingScore(b) - moveOrderingScore(a);
    return scoreDelta !== 0 ? scoreDelta : moveKey(a).localeCompare(moveKey(b));
  });
}

function moveOrderingScore(move: Move): number {
  let score = 0;
  if (move.isCapture()) {
    score += 10_000 + (pieceValues[move.captured ?? "p"] * 10) - pieceValues[move.piece];
  }
  if (move.isPromotion()) {
    score += 15_000 + pieceValues[move.promotion ?? "q"];
  }
  if (move.isKingsideCastle() || move.isQueensideCastle()) {
    score += 600;
  }
  if (move.san.includes("#")) {
    score += 1_000_000;
  } else if (move.san.includes("+")) {
    score += 900;
  }
  score += centerBonus(move.to);
  return score;
}

function isTacticalMove(move: Move): boolean {
  return move.isCapture() || move.isPromotion() || move.san.includes("+") || move.san.includes("#");
}

function makeSearchMove(game: Chess, move: Move): void {
  game.move({ from: move.from, to: move.to, promotion: move.promotion ?? "q" });
}

function searchDepth(game: Chess, ply: number): number {
  const pieces = piecesOnBoard(game).length;
  if (pieces <= 10) {
    return 5;
  }
  if (pieces <= 20 || ply >= 14) {
    return 4;
  }
  return 3;
}

function searchBudgetMs(game: Chess): number {
  const pieces = piecesOnBoard(game).length;
  if (pieces <= 12) {
    return 1_400;
  }
  if (pieces <= 22) {
    return 1_150;
  }
  return 900;
}

function assertSearchTime(context: SearchContext): void {
  context.nodes += 1;
  if ((context.nodes & 2047) === 0 && Date.now() > context.deadline) {
    throw searchTimeout;
  }
}

function centerBonus(square: Square): number {
  const file = fileIndex(square);
  const rank = Number(square[1]) - 1;
  const fileDistance = Math.abs(file - 3.5);
  const rankDistance = Math.abs(rank - 3.5);
  return Math.max(0, 42 - (fileDistance + rankDistance) * 10);
}

function forwardRank(square: Square, color: Color): number {
  const rank = Number(square[1]) - 1;
  return color === "w" ? rank : 7 - rank;
}

function fileIndex(square: Square): number {
  return square.charCodeAt(0) - 97;
}

function opposite(color: Color): Color {
  return color === "w" ? "b" : "w";
}

function sameMove(move: Move, other: Move | undefined): boolean {
  return Boolean(other && move.from === other.from && move.to === other.to && move.promotion === other.promotion);
}

function moveKey(move: Move): string {
  return `${move.from}${move.to}${move.promotion ?? ""}`;
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
