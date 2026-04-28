import { existsSync } from "node:fs";
import { chmod, mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";

export function createRoomStore(dataDir) {
  const rooms = new Map();

  return {
    async load(roomId) {
      const existing = rooms.get(roomId);
      if (existing) {
        return existing;
      }
      await mkdir(dataDir, { recursive: true, mode: 0o700 });
      const file = path.join(dataDir, `${roomId}.json`);
      const state = existsSync(file)
        ? await readState(file)
        : { auth: null, snapshot: null, updates: [], files: [], closed: null };
      state.auth ??= null;
      state.updates ??= [];
      state.files ??= [];
      const seen = new Set([
        state.snapshot?.id,
        ...state.updates.map((update) => update.id),
        ...state.files.map((item) => item.id)
      ].filter(Boolean));
      const room = {
        id: roomId,
        file,
        state,
        peers: new Map(),
        waiting: new Map(),
        seen,
        save: Promise.resolve()
      };
      rooms.set(roomId, room);
      return room;
    },

    save(room) {
      room.save = room.save
        .catch(() => undefined)
        .then(() => writeRoom(room));
      return room.save;
    }
  };
}

async function readState(file) {
  try {
    return JSON.parse(await readFile(file, "utf8"));
  } catch {
    return { auth: null, snapshot: null, updates: [], files: [], closed: null };
  }
}

async function writeRoom(room) {
  const tmp = `${room.file}.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2)}.tmp`;
  await writeFile(tmp, JSON.stringify(room.state), { encoding: "utf8", mode: 0o600 });
  await rename(tmp, room.file);
  await chmod(room.file, 0o600).catch(() => undefined);
}
