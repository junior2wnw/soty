import {
  isEncryptedFile,
  isEncryptedUpdate,
  isJoinAccept,
  isJoinRequest,
  isLiveDraft,
  isNoticeKnock,
  isP2pCandidate,
  isP2pDescription,
  isRemoteCommand,
  isRemoteCancel,
  isRemoteGrant,
  isRemoteOutput,
  isRemoteRequest,
  isRemoteScript,
  isShortText
} from "./validators.js";
import { pruneWaiting } from './room-waiting.mjs';

const rateWindowMs = 10_000;
const maxMessagesPerWindow = 600;
const maxBytesPerWindow = 70_000_000;
const maxReplaySocketBytes = 1_000_000;
const replayAckTimeoutMs = 90_000;
const maxPendingJoinRequests = 16;
const maxQueuedHandshakeBytes = 128_000;
const roomOperations = new WeakMap();

// Serialize the state transition, persistence and acknowledgement, not merely
// the filesystem writes. Admission is bounded before retaining waiting closures.
function serializeRoom(room, bytes, operation) {
  let gate = roomOperations.get(room);
  if (!gate) { gate = { tail: Promise.resolve(), count: 0, bytes: 0 }; roomOperations.set(room, gate); }
  const capacity = gate.count === 0 ? Math.max(8_000_000, Math.min(bytes, 34_000_000)) : 8_000_000;
  if (gate.count >= 64 || gate.bytes + bytes > capacity) return Promise.reject(new Error("room_busy"));
  gate.count++; gate.bytes += bytes;
  const result = gate.tail.then(operation);
  gate.tail = result.then(() => undefined, () => undefined);
  return result.finally(() => { gate.count--; gate.bytes -= bytes; });
}

export function attachRealtime(wss, store) {
  wss.on("connection", (ws, _request, roomId) => {
    let room = null;
    let retainedRoom = false;
    const queuedMessages = [];
    let queuedBytes = 0;
    const peer = {
      id: "",
      nick: "",
      joinRequestId: "",
      joinRequest: null,
      joinCreatedAt: 0,
      disconnectedAt: 0,
      accept: null,
      acceptedAt: 0,
      deniedAt: 0,
      rateStartedAt: Date.now(),
      messageCount: 0,
      byteCount: 0,
      replayCursor: 0,
      replayInFlight: 0,
      replaySentAt: 0,
      replayTimer: null,
      replayAcknowledged: false,
      replayInitialEnd: 0,
      replayFileId: '',
      skippedFiles: new Set(),
      ws
    };
    ws.isAlive = true;
    ws.on("pong", () => {
      ws.isAlive = true;
    });
    const receiveMessage = (raw) => {
      if (!room) {
        const bytes = Buffer.byteLength(raw);
        if (queuedMessages.length >= 32 || queuedBytes + bytes > maxQueuedHandshakeBytes) {
          ws.close(1013, "room loading");
          return;
        }
        queuedBytes += bytes;
        queuedMessages.push(raw);
        return;
      }
      void handleMessage(room, peer, ws, store, raw).catch(() => {
        ws.close(1011, "message error");
      });
    };
    ws.on("message", receiveMessage);
    ws.on("close", () => {
      if (peer.replayTimer !== null) clearTimeout(peer.replayTimer);
      peer.replayTimer = null;
      if (!room) {
        return;
      }
      if (peer.joinRequestId) {
        const waiting = room.waiting.get(peer.joinRequestId);
        if (waiting === peer) {
          peer.ws = null;
          peer.disconnectedAt = Date.now();
        }
      }
      if (peer.id && room.peers.get(peer.id) === peer) {
        room.peers.delete(peer.id);
        broadcast(room, peer.id, {
          type: "presence",
          peers: [...room.peers.values()].map(publicPeer)
        });
      }
      if (retainedRoom) { retainedRoom = false; store.release(room); }
    });
    void store.load(roomId, { retain: true }).then((loaded) => {
      room = loaded;
      retainedRoom = true;
      if (ws.readyState >= 2) {
        retainedRoom = false; store.release(room);
        return;
      }
      for (const raw of queuedMessages.splice(0)) {
        receiveMessage(raw);
      }
    }).catch(() => {
      ws.close(1011, "room error");
    });
  });

  const heartbeat = setInterval(() => {
    for (const client of wss.clients) {
      if (client.isAlive === false) {
        client.terminate();
        continue;
      }
      client.isAlive = false;
      client.ping();
    }
  }, 30000);

  wss.on("close", () => clearInterval(heartbeat));
}

async function handleMessage(room, peer, ws, store, raw) {
  const rawBytes = Buffer.byteLength(raw);
  if (rawBytes > 512 && !allowMessage(peer, raw)) { ws.close(1008, 'rate limit'); return; }
  let message;
  try {
    message = JSON.parse(raw.toString());
  } catch {
    return;
  }
  if (!message || typeof message !== 'object' || Array.isArray(message)) return;
  const replayControl = rawBytes <= 512 && peer.id && room.peers.get(peer.id) === peer && peer.replayInFlight > 0
    && Number.isSafeInteger(message.sequence) && message.sequence === peer.replayInFlight
    && ((message.type === 'replay.ack' && Object.keys(message).every(key => key === 'type' || key === 'sequence'))
      || (message.type === 'replay.skip' && message.fileId === peer.replayFileId
        && Object.keys(message).every(key => key === 'type' || key === 'sequence' || key === 'fileId')));
  // Only the exact, tiny, authenticated in-flight ACK is free. Padded,
  // unsolicited or repeated control frames use the normal abuse budget.
  if (rawBytes <= 512 && !replayControl && !allowMessage(peer, raw)) {
    ws.close(1008, "rate limit");
    return;
  }
  pruneWaiting(room);

  if (message.type === "ping") {
    ws.send(JSON.stringify({ type: "pong" }));
    return;
  }

  if (message.type === "hello") {
    await serializeRoom(room, Buffer.byteLength(raw), () => handleHello(room, peer, ws, store, message));
    return;
  }

  if (!peer.id) {
    return;
  }
  const joinedPeer = room.peers.get(peer.id) === peer;
  if (!joinedPeer) {
    return;
  }
  if (message.type === 'replay.ack' || message.type === 'replay.skip') {
    if (!replayControl) return;
    if (peer.replayAcknowledged && Number.isSafeInteger(message.sequence) && message.sequence === peer.replayInFlight) {
      if (message.type === 'replay.skip') {
        if (!peer.replayFileId || message.fileId !== peer.replayFileId || peer.skippedFiles.size >= 4096) return;
        peer.skippedFiles.add(message.fileId);
      }
      peer.replayCursor = message.sequence; peer.replayInFlight = 0;
      if (peer.replayTimer !== null) clearTimeout(peer.replayTimer);
      peer.replayTimer = null;
      scheduleReplay(room, peer, store, 0);
    }
    return;
  }

  if (message.type === "update" && isEncryptedUpdate(message.update)) {
    await serializeRoom(room, Buffer.byteLength(raw), () => storeUpdate(room, peer, ws, store, message.update));
    return;
  }
  if (message.type === "file" && isEncryptedFile(message.file)) {
    try { await serializeRoom(room, Buffer.byteLength(raw), () => storeFile(room, peer, ws, store, message.file)); }
    catch (error) {
      if (!error.code) throw error;
      if (ws.readyState === 1) ws.send(JSON.stringify({ type: 'file.error', id: message.file.id, code: error.code }));
    }
    return;
  }
  if (message.type === "notice.knock" && isNoticeKnock(message.knock)) {
    broadcast(room, peer.id, {
      type: "notice.knock",
      knock: withPeer(peer, message.knock)
    });
    return;
  }
  if (message.type === "live.draft" && isLiveDraft(message.draft)) {
    broadcast(room, peer.id, {
      type: "live.draft",
      draft: withPeer(peer, message.draft)
    });
    return;
  }
  if (message.type === "remote.grant" && isRemoteGrant(message.grant)) {
    broadcast(room, peer.id, {
      type: "remote.grant",
      grant: withPeer(peer, message.grant)
    });
    return;
  }
  if (message.type === "remote.request" && isRemoteRequest(message.request)) {
    broadcast(room, peer.id, {
      type: "remote.request",
      request: withPeer(peer, message.request)
    });
    return;
  }
  if (message.type === "remote.command" && isRemoteCommand(message.command)) {
    broadcast(room, peer.id, {
      type: "remote.command",
      command: withPeer(peer, message.command)
    });
    return;
  }
  if (message.type === "remote.script" && isRemoteScript(message.script)) {
    broadcast(room, peer.id, {
      type: "remote.script",
      script: withPeer(peer, message.script)
    });
    return;
  }
  if (message.type === "remote.cancel" && isRemoteCancel(message.cancel)) {
    broadcast(room, peer.id, {
      type: "remote.cancel",
      cancel: withPeer(peer, message.cancel)
    });
    return;
  }
  if (message.type === "remote.output" && isRemoteOutput(message.output)) {
    broadcast(room, peer.id, {
      type: "remote.output",
      output: withPeer(peer, message.output)
    });
    return;
  }
  if (message.type === "p2p.offer" && isP2pDescription(message.signal, "offer")) {
    sendTo(room, message.signal.targetDeviceId, {
      type: "p2p.offer",
      signal: withPeer(peer, message.signal)
    });
    return;
  }
  if (message.type === "p2p.answer" && isP2pDescription(message.signal, "answer")) {
    sendTo(room, message.signal.targetDeviceId, {
      type: "p2p.answer",
      signal: withPeer(peer, message.signal)
    });
    return;
  }
  if (message.type === "p2p.candidate" && isP2pCandidate(message.signal)) {
    sendTo(room, message.signal.targetDeviceId, {
      type: "p2p.candidate",
      signal: withPeer(peer, message.signal)
    });
    return;
  }
  if (message.type === "join.accept" && isShortText(message.requestId, 120) && isJoinAccept(message.accept)) {
    const waiting = room.waiting.get(message.requestId);
    if (waiting?.ws?.readyState === 1) {
      waiting.ws.send(JSON.stringify({
        type: "join.accepted",
        requestId: message.requestId,
        accept: message.accept
      }));
      room.waiting.delete(message.requestId);
      return;
    }
    if (waiting) {
      waiting.accept = message.accept;
      waiting.acceptedAt = Date.now();
      waiting.deniedAt = 0;
      return;
    }
    return;
  }
  if (message.type === "join.deny" && isShortText(message.requestId, 120)) {
    const waiting = room.waiting.get(message.requestId);
    if (waiting?.ws?.readyState === 1) {
      waiting.ws.send(JSON.stringify({ type: "join.denied", requestId: message.requestId }));
      waiting.ws.close(1000, "denied");
      room.waiting.delete(message.requestId);
      return;
    }
    if (waiting) {
      waiting.accept = null;
      waiting.deniedAt = Date.now();
      return;
    }
    return;
  }
  if (message.type === "close") {
    await serializeRoom(room, Buffer.byteLength(raw), async () => {
      await store.closeRoom(room, { deviceId: peer.id, at: new Date().toISOString() });
      room.waiting.clear();
      broadcast(room, "", { type: "closed", closed: room.state.closed });
    });
  }
}

async function handleHello(room, peer, ws, store, message) {
  if (!isShortText(message.deviceId, 120) || !isShortText(message.nick, 80)) {
    ws.close(1008, "bad hello");
    return;
  }
  peer.id = message.deviceId;
  peer.nick = message.nick;
  if (room.state.closed) {
    ws.send(JSON.stringify({ type: "closed", closed: room.state.closed }));
    ws.close(1000, "closed");
    return;
  }
  if (isJoinRequest(message.joinRequest)) {
    peer.joinRequestId = message.joinRequest.requestId;
    const existing = room.waiting.get(peer.joinRequestId);
    if (!existing && pendingJoinRequests(room, "").length >= maxPendingJoinRequests) {
      ws.close(1013, "too many join requests");
      return;
    }
    peer.joinRequest = {
      requestId: peer.joinRequestId,
      deviceId: peer.id,
      nick: peer.nick,
      publicJwk: message.joinRequest.publicJwk
    };
    peer.joinCreatedAt = existing?.joinCreatedAt || Date.now();
    if (existing?.accept) {
      ws.send(JSON.stringify({
        type: "join.accepted",
        requestId: peer.joinRequestId,
        accept: existing.accept
      }));
      room.waiting.delete(peer.joinRequestId);
      return;
    }
    if (existing?.deniedAt) {
      ws.send(JSON.stringify({ type: "join.denied", requestId: peer.joinRequestId }));
      ws.close(1000, "denied");
      room.waiting.delete(peer.joinRequestId);
      return;
    }
    room.waiting.set(peer.joinRequestId, peer);
    ws.send(JSON.stringify({ type: "join.waiting", requestId: peer.joinRequestId }));
    broadcast(room, peer.id, {
      type: "join.request",
      request: peer.joinRequest
    });
    return;
  }
  if (!isShortText(message.roomAuth, 128)) {
    ws.close(1008, "missing auth");
    return;
  }
  try { await store.claimAuth(room, message.roomAuth); }
  catch (error) {
    if (error.code !== 'room_auth_mismatch') throw error;
    ws.close(1008, "bad auth");
    return;
  }
  if (ws.readyState !== 1) return;
  const previous = room.peers.get(peer.id);
  if (previous && previous !== peer) previous.ws?.close(1000, 'connection replaced');
  room.peers.set(peer.id, peer);
  peer.replayAcknowledged = message.replay === 'ack-v1';
  peer.replayInitialEnd = room.state.sequence;
  const peers = [...room.peers.values()].map(publicPeer);
  ws.send(JSON.stringify({
    type: "hello",
    roomId: room.id,
    snapshot: null,
    updates: [],
    files: [],
    pendingFiles: store.pendingFiles(room),
    storageFormat: store.format,
    replay: peer.replayAcknowledged ? 'ack-v1' : 'paced-v1',
    peers,
    joinRequests: pendingJoinRequests(room, peer.id)
  }));
  broadcast(room, peer.id, {
    type: "presence",
    peers
  });
  scheduleReplay(room, peer, store, 0);
}

async function storeUpdate(room, peer, ws, store, update) {
  if (room.state.closed) throw new Error("room_closed");
  const stored = withPeer(peer, update, true);
  await store.appendUpdate(room, stored);
  ws.send(JSON.stringify({ type: "ack", id: stored.id }));
  for (const recipient of room.peers.values()) scheduleReplay(room, recipient, store, 0);
}

async function storeFile(room, peer, ws, store, file) {
  if (room.state.closed) throw new Error("room_closed");
  const stored = withPeer(peer, file, true);
  const result = await store.appendFile(room, stored);
  ws.send(JSON.stringify({ type: "ack", id: stored.id }));
  if (result.pendingChanged) broadcast(room, '', { type: 'files.pending', files: store.pendingFiles(room) });
  else if (result.progress) broadcast(room, '', { type: 'file.progress', ...result.progress });
  for (const recipient of room.peers.values()) scheduleReplay(room, recipient, store, 0);
}

// One cursor and one in-flight sequence per peer. No history array, serialized
// backlog or read transaction survives a socket wait. New clients acknowledge
// after decrypting/storing each frame; legacy clients receive paced old frames.
function scheduleReplay(room, peer, store, delay = 25) {
  if (peer.replayTimer !== null || peer.ws?.readyState !== 1 || room.peers.get(peer.id) !== peer || room.state.closed) return;
  peer.replayTimer = setTimeout(() => {
    peer.replayTimer = null;
    try { pumpReplay(room, peer, store); }
    catch { peer.ws?.close(1011, 'history unavailable'); }
  }, delay);
}

function pumpReplay(room, peer, store) {
  if (peer.ws?.readyState !== 1 || room.peers.get(peer.id) !== peer || room.state.closed) return;
  if (peer.replayInFlight) {
    if (Date.now() - peer.replaySentAt > replayAckTimeoutMs) { peer.ws.close(1013, 'history stalled'); return; }
    scheduleReplay(room, peer, store, 100); return;
  }
  if (peer.ws.bufferedAmount > 0) { scheduleReplay(room, peer, store); return; }
  const event = store.nextEvent(room, peer.replayCursor, [...peer.skippedFiles]);
  if (!event) return;
  const message = JSON.stringify({ type: event.type, [event.type]: event.payload,
    sequence: event.sequence, replay: event.sequence <= peer.replayInitialEnd });
  // Chunk frames fit this bound; a legacy encrypted complete/snapshot is one
  // explicitly documented larger record and cannot be split without its key.
  if (peer.ws.bufferedAmount + Buffer.byteLength(message) > maxReplaySocketBytes && peer.ws.bufferedAmount > 0) {
    scheduleReplay(room, peer, store); return;
  }
  peer.replayInFlight = event.sequence; peer.replaySentAt = Date.now();
  peer.replayFileId = event.type === 'file' ? (event.payload.fileId ?? event.payload.id) : '';
  peer.ws.send(message, error => {
    if (error) { peer.ws.close(1011, 'history send failed'); return; }
    if (!peer.replayAcknowledged) {
      peer.replayCursor = event.sequence; peer.replayInFlight = 0;
      scheduleReplay(room, peer, store, Math.max(100, Math.ceil(event.wireBytes / 2_000)));
    }
  });
  if (peer.replayAcknowledged) scheduleReplay(room, peer, store, 100);
}

function withPeer(peer, payload, includeNick = true) {
  const next = {
    ...payload,
    deviceId: peer.id,
    createdAt: new Date().toISOString()
  };
  if (includeNick) {
    next.deviceNick = peer.nick;
    next.nick = peer.nick;
  }
  return next;
}

function broadcast(room, exceptDeviceId, message) {
  const json = JSON.stringify(message);
  for (const peer of room.peers.values()) {
    if (peer.id !== exceptDeviceId && peer.ws.readyState === 1) {
      if ((message.type === 'files.pending' || message.type === 'file.progress') && peer.ws.bufferedAmount + Buffer.byteLength(json) > maxReplaySocketBytes) continue;
      peer.ws.send(json);
    }
  }
}

function sendTo(room, deviceId, message) {
  const peer = room.peers.get(deviceId);
  if (peer?.ws?.readyState === 1) {
    peer.ws.send(JSON.stringify(message));
  }
}

function pendingJoinRequests(room, ownerDeviceId) {
  const requests = [];
  for (const waiting of room.waiting.values()) {
    if (waiting.joinRequest && !waiting.accept && !waiting.deniedAt && waiting.joinRequest.deviceId !== ownerDeviceId) {
      requests.push(waiting.joinRequest);
    }
  }
  return requests.slice(0, maxPendingJoinRequests);
}

function publicPeer(peer) {
  return { id: peer.id, nick: peer.nick };
}

function allowMessage(peer, raw) {
  const now = Date.now();
  if (now - peer.rateStartedAt > rateWindowMs) {
    peer.rateStartedAt = now;
    peer.messageCount = 0;
    peer.byteCount = 0;
  }
  peer.messageCount += 1;
  peer.byteCount += Buffer.byteLength(raw);
  return peer.messageCount <= maxMessagesPerWindow && peer.byteCount <= maxBytesPerWindow;
}
