import { randomUUID } from "node:crypto";

export function createArtifactStore({ ttlMs }) {
  const artifacts = new Map();

  return {
    put({ bytes, name, mimeType, sha256, relayId, deviceId }) {
      cleanup();
      const now = Date.now();
      const id = `${now.toString(36)}_${randomUUID().replace(/-/gu, "")}`;
      const artifact = {
        bytes,
        name,
        mimeType,
        sha256,
        relayId,
        deviceId,
        createdAt: now,
        expiresAt: now + ttlMs
      };
      artifacts.set(id, artifact);
      return { id, artifact };
    },

    get(id) {
      cleanup();
      const artifact = artifacts.get(id) || null;
      if (artifact) {
        artifact.lastReadAt = Date.now();
      }
      return artifact;
    },

    cleanup
  };

  function cleanup() {
    const now = Date.now();
    for (const [id, artifact] of artifacts) {
      if (!artifact || now > artifact.expiresAt) {
        artifacts.delete(id);
      }
    }
  }
}
