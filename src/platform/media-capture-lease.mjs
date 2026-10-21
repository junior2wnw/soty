/** Local resource exclusion only. A lease carries no account or media authority. */
export function createMediaCaptureCoordinator() {
  const owners = new Map();
  const valid = resource => { if (!['camera', 'microphone'].includes(resource)) throw new TypeError('media_capture_resource_invalid'); };
  return Object.freeze({
    busy(resource) { valid(resource); return owners.has(resource); },
    acquire(resource, owner) {
      valid(resource);
      if (!owner || typeof owner !== 'object') throw new TypeError('media_capture_owner_invalid');
      if (owners.has(resource)) return null;
      const token = Object.freeze({ owner }); owners.set(resource, token);
      return Object.freeze({
        isCurrent: () => owners.get(resource) === token,
        release() { if (owners.get(resource) === token) owners.delete(resource); },
      });
    },
  });
}
export const sharedMediaCaptureCoordinator = createMediaCaptureCoordinator();
