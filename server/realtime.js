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

const rateWindowMs = 10_000;
const maxMessagesPerWindow = 600;
const maxBytesPerWindow = 70_000_000;
const maxStoredFiles = 3000;
const maxStoredFileBytes = 512_000_000;
const joinRequestTtlMs = 10 * 60_000;
const joinDecisionTtlMs = 60_000;
const maxPendingJoinRequests = 16;
const maxQueuedHandshakeBytes = 128_000;

export function attachRealtime(wss, store) {
  wss.on("connection", (ws, _request, roomId) => {
    let room = null;
    const queuedMessages = [];
    let queuedBytes = 0;
    const peer = {
      connectionId: `conn_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`,
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
      if (peer.id && room.peers.get(peer.connectionId) === peer) {
        room.peers.delete(peer.connectionId);
        broadcast(room, peer, {
          type: "presence",
          peers: [...room.peers.values()].map(publicPeer)
        });
      }
    });
    void store.load(roomId).then((loaded) => {
      room = loaded;
      if (ws.readyState >= 2) {
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
  if (!allowMessage(peer, raw)) {
    ws.close(1008, "rate limit");
    return;
  }
  let message;
  try {
    message = JSON.parse(raw.toString());
  } catch {
    return;
  }
  pruneWaiting(room);

  if (message.type === "ping") {
    ws.send(JSON.stringify({ type: "pong" }));
    return;
  }

  if (message.type === "hello") {
    await handleHello(room, peer, ws, store, message);
    return;
  }

  if (!peer.id) {
    return;
  }
  const joinedPeer = room.peers.get(peer.connectionId) === peer;
  if (!joinedPeer) {
    return;
  }

  if (message.type === "update" && isEncryptedUpdate(message.update)) {
    await storeUpdate(room, peer, ws, store, message.update);
    return;
  }
  if (message.type === "file" && isEncryptedFile(message.file)) {
    await storeFile(room, peer, ws, store, message.file);
    return;
  }
  if (message.type === "notice.knock" && isNoticeKnock(message.knock)) {
    broadcast(room, peer, {
      type: "notice.knock",
      knock: withPeer(peer, message.knock)
    });
    return;
  }
  if (message.type === "live.draft" && isLiveDraft(message.draft)) {
    broadcast(room, peer, {
      type: "live.draft",
      draft: withPeer(peer, message.draft)
    });
    return;
  }
  if (message.type === "remote.grant" && isRemoteGrant(message.grant)) {
    broadcast(room, peer, {
      type: "remote.grant",
      grant: withPeer(peer, message.grant)
    });
    return;
  }
  if (message.type === "remote.request" && isRemoteRequest(message.request)) {
    broadcast(room, peer, {
      type: "remote.request",
      request: withPeer(peer, message.request)
    });
    return;
  }
  if (message.type === "remote.command" && isRemoteCommand(message.command)) {
    broadcast(room, peer, {
      type: "remote.command",
      command: withPeer(peer, message.command)
    });
    return;
  }
  if (message.type === "remote.script" && isRemoteScript(message.script)) {
    broadcast(room, peer, {
      type: "remote.script",
      script: withPeer(peer, message.script)
    });
    return;
  }
  if (message.type === "remote.cancel" && isRemoteCancel(message.cancel)) {
    broadcast(room, peer, {
      type: "remote.cancel",
      cancel: withPeer(peer, message.cancel)
    });
    return;
  }
  if (message.type === "remote.output" && isRemoteOutput(message.output)) {
    broadcast(room, peer, {
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
    room.state.closed = { deviceId: peer.id, at: new Date().toISOString() };
    room.state.snapshot = null;
    room.state.updates = [];
    room.state.files = [];
    room.waiting.clear();
    await store.save(room);
    broadcast(room, null, { type: "closed", closed: room.state.closed });
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
    broadcast(room, peer, {
      type: "join.request",
      request: peer.joinRequest
    });
    return;
  }
  if (!isShortText(message.roomAuth, 128)) {
    ws.close(1008, "missing auth");
    return;
  }
  if (!room.state.auth) {
    room.state.auth = message.roomAuth;
    await store.save(room);
  }
  if (room.state.auth !== message.roomAuth) {
    ws.close(1008, "bad auth");
    return;
  }
  room.peers.set(peer.connectionId, peer);
  const peers = [...room.peers.values()].map(publicPeer);
  ws.send(JSON.stringify({
    type: "hello",
    roomId: room.id,
    snapshot: room.state.snapshot,
    updates: room.state.updates,
    files: room.state.files,
    peers,
    joinRequests: pendingJoinRequests(room, peer.id)
  }));
  broadcast(room, peer, {
    type: "presence",
    peers
  });
}

async function storeUpdate(room, peer, ws, store, update) {
  const stored = withPeer(peer, update, false);
  if (room.seen.has(stored.id)) {
    ws.send(JSON.stringify({ type: "ack", id: stored.id }));
    return;
  }
  if (stored.kind === "snapshot") {
    room.state.snapshot = stored;
    room.state.updates = [];
  } else {
    room.state.updates.push(stored);
    if (room.state.updates.length > 5000) {
      room.state.updates.splice(0, room.state.updates.length - 5000);
    }
  }
  room.seen.add(stored.id);
  await store.save(room);
  ws.send(JSON.stringify({ type: "ack", id: stored.id }));
  broadcast(room, peer, { type: "update", update: withPeer(peer, stored, true) });
}

async function storeFile(room, peer, ws, store, file) {
  const stored = withPeer(peer, file, false);
  if (room.seen.has(stored.id)) {
    ws.send(JSON.stringify({ type: "ack", id: stored.id }));
    return;
  }
  room.seen.add(stored.id);
  if (stored.kind === "delete") {
    room.state.files = room.state.files.filter((item) => fileIdentity(item) !== stored.fileId);
    await store.save(room);
    ws.send(JSON.stringify({ type: "ack", id: stored.id }));
    broadcast(room, peer, { type: "file", file: withPeer(peer, stored, true) });
    return;
  }
  room.state.files.push(stored);
  trimStoredFiles(room.state.files);
  await store.save(room);
  ws.send(JSON.stringify({ type: "ack", id: stored.id }));
  broadcast(room, peer, { type: "file", file: withPeer(peer, stored, true) });
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

function broadcast(room, exceptPeer, message) {
  const json = JSON.stringify(message);
  for (const peer of room.peers.values()) {
    if (peer !== exceptPeer && peer.ws.readyState === 1) {
      peer.ws.send(json);
    }
  }
}

function sendTo(room, deviceId, message) {
  const json = JSON.stringify(message);
  for (const peer of room.peers.values()) {
    if (peer.id === deviceId && peer.ws?.readyState === 1) {
      peer.ws.send(json);
    }
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

function pruneWaiting(room) {
  const now = Date.now();
  for (const [requestId, waiting] of room.waiting) {
    const decisionAt = waiting.acceptedAt || waiting.deniedAt || 0;
    const startedAt = waiting.joinCreatedAt || waiting.disconnectedAt || waiting.rateStartedAt || now;
    const ttl = decisionAt ? joinDecisionTtlMs : joinRequestTtlMs;
    const since = decisionAt || startedAt;
    if (now - since <= ttl) {
      continue;
    }
    if (!decisionAt && waiting.ws?.readyState === 1) {
      waiting.ws.close(1000, "join expired");
    }
    room.waiting.delete(requestId);
  }
}

function publicPeer(peer) {
  return { id: peer.id, nick: peer.nick };
}

function fileIdentity(file) {
  return typeof file?.fileId === "string" ? file.fileId : file?.id;
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

function trimStoredFiles(files) {
  while (files.length > maxStoredFiles) {
    files.shift();
  }
  let total = files.reduce((sum, file) => sum + (Number.isSafeInteger(file.bytes) ? file.bytes : 0), 0);
  while (total > maxStoredFileBytes && files.length > 0) {
    const removed = files.shift();
    total -= Number.isSafeInteger(removed?.bytes) ? removed.bytes : 0;
  }
}
