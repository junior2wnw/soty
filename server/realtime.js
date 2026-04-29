import {
  isEncryptedFile,
  isEncryptedUpdate,
  isJoinAccept,
  isJoinRequest,
  isP2pCandidate,
  isP2pDescription,
  isRemoteCommand,
  isRemoteGrant,
  isRemoteOutput,
  isShortText
} from "./validators.js";

const rateWindowMs = 10_000;
const maxMessagesPerWindow = 600;
const maxBytesPerWindow = 70_000_000;
const maxStoredFiles = 3000;
const maxStoredFileBytes = 512_000_000;

export function attachRealtime(wss, store) {
  wss.on("connection", async (ws, _request, roomId) => {
    const room = await store.load(roomId);
    const peer = {
      id: "",
      nick: "",
      joinRequestId: "",
      rateStartedAt: Date.now(),
      messageCount: 0,
      byteCount: 0,
      ws
    };
    ws.isAlive = true;
    ws.on("pong", () => {
      ws.isAlive = true;
    });
    ws.on("message", (raw) => {
      void handleMessage(room, peer, ws, store, raw).catch(() => {
        ws.close(1011, "message error");
      });
    });
    ws.on("close", () => {
      if (peer.joinRequestId) {
        room.waiting.delete(peer.joinRequestId);
      }
      if (peer.id && room.peers.get(peer.id) === peer) {
        room.peers.delete(peer.id);
        broadcast(room, peer.id, {
          type: "presence",
          peers: [...room.peers.values()].map(publicPeer)
        });
      }
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
  const joinedPeer = room.peers.get(peer.id) === peer;
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
  if (message.type === "remote.grant" && isRemoteGrant(message.grant)) {
    broadcast(room, peer.id, {
      type: "remote.grant",
      grant: withPeer(peer, message.grant)
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
    if (waiting?.ws.readyState === 1) {
      waiting.ws.send(JSON.stringify({
        type: "join.accepted",
        requestId: message.requestId,
        accept: message.accept
      }));
    }
    room.waiting.delete(message.requestId);
    return;
  }
  if (message.type === "join.deny" && isShortText(message.requestId, 120)) {
    const waiting = room.waiting.get(message.requestId);
    if (waiting?.ws.readyState === 1) {
      waiting.ws.send(JSON.stringify({ type: "join.denied", requestId: message.requestId }));
      waiting.ws.close(1000, "denied");
    }
    room.waiting.delete(message.requestId);
    return;
  }
  if (message.type === "close") {
    room.state.closed = { deviceId: peer.id, at: new Date().toISOString() };
    room.state.snapshot = null;
    room.state.updates = [];
    room.state.files = [];
    await store.save(room);
    broadcast(room, "", { type: "closed", closed: room.state.closed });
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
    room.waiting.set(peer.joinRequestId, peer);
    ws.send(JSON.stringify({ type: "join.waiting", requestId: peer.joinRequestId }));
    broadcast(room, peer.id, {
      type: "join.request",
      request: {
        requestId: peer.joinRequestId,
        deviceId: peer.id,
        nick: peer.nick,
        publicJwk: message.joinRequest.publicJwk
      }
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
  room.peers.set(peer.id, peer);
  ws.send(JSON.stringify({
    type: "hello",
    roomId: room.id,
    snapshot: room.state.snapshot,
    updates: room.state.updates,
    files: room.state.files,
    peers: [...room.peers.values()].map(publicPeer)
  }));
  broadcast(room, peer.id, {
    type: "presence",
    peers: [...room.peers.values()].map(publicPeer)
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
  broadcast(room, peer.id, { type: "update", update: withPeer(peer, stored, true) });
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
    broadcast(room, peer.id, { type: "file", file: withPeer(peer, stored, true) });
    return;
  }
  room.state.files.push(stored);
  trimStoredFiles(room.state.files);
  await store.save(room);
  ws.send(JSON.stringify({ type: "ack", id: stored.id }));
  broadcast(room, peer.id, { type: "file", file: withPeer(peer, stored, true) });
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
      peer.ws.send(json);
    }
  }
}

function sendTo(room, deviceId, message) {
  const peer = room.peers.get(deviceId);
  if (peer?.ws.readyState === 1) {
    peer.ws.send(JSON.stringify(message));
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
