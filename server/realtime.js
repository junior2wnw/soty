import {
  isEncryptedFile,
  isEncryptedUpdate,
  isJoinAccept,
  isJoinRequest,
  isShortText
} from "./validators.js";

export function attachRealtime(wss, store) {
  wss.on("connection", async (ws, _request, roomId) => {
    const room = await store.load(roomId);
    const peer = { id: "", nick: "", joinRequestId: "", ws };
    ws.isAlive = true;
    ws.on("pong", () => {
      ws.isAlive = true;
    });
    ws.on("message", (raw) => {
      void handleMessage(room, peer, ws, store, raw);
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
    handleHello(room, peer, ws, message);
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
  if (message.type === "remote.request" && isShortText(message.request?.id, 120)) {
    broadcast(room, peer.id, {
      type: "remote.request",
      request: withPeer(peer, { id: message.request.id })
    });
    return;
  }
  if (message.type === "remote.response" && isShortText(message.response?.id, 120) && typeof message.response.accepted === "boolean") {
    broadcast(room, peer.id, {
      type: "remote.response",
      response: withPeer(peer, {
        id: message.response.id,
        accepted: message.response.accepted
      })
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

function handleHello(room, peer, ws, message) {
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
  const stored = withPeer(peer, update);
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
  broadcast(room, peer.id, { type: "update", update: stored });
}

async function storeFile(room, peer, ws, store, file) {
  const stored = withPeer(peer, file);
  if (room.seen.has(stored.id)) {
    ws.send(JSON.stringify({ type: "ack", id: stored.id }));
    return;
  }
  room.seen.add(stored.id);
  room.state.files.push(stored);
  if (room.state.files.length > 300) {
    room.state.files.splice(0, room.state.files.length - 300);
  }
  await store.save(room);
  ws.send(JSON.stringify({ type: "ack", id: stored.id }));
  broadcast(room, peer.id, { type: "file", file: stored });
}

function withPeer(peer, payload) {
  return {
    ...payload,
    deviceId: peer.id,
    deviceNick: peer.nick,
    nick: peer.nick,
    createdAt: new Date().toISOString()
  };
}

function broadcast(room, exceptDeviceId, message) {
  const json = JSON.stringify(message);
  for (const peer of room.peers.values()) {
    if (peer.id !== exceptDeviceId && peer.ws.readyState === 1) {
      peer.ws.send(json);
    }
  }
}

function publicPeer(peer) {
  return { id: peer.id, nick: peer.nick };
}
