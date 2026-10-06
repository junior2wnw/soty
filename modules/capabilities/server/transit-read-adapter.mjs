import { createHash } from 'node:crypto';
import { open, realpath } from 'node:fs/promises';
import { resolve, relative, isAbsolute } from 'node:path';
import { snapshot } from '../../app-contract/json.mjs';

export const TRANSIT_READ_PROFILE = 'soty.transit-source-documents.v1';
const DOCUMENTS = Object.freeze({
  guide: 'README.md',
  policy: 'AGENTS.md',
  readiness: 'docs/READINESS.md',
});
export class TransitReadError extends Error {
  constructor(code, status = 400) {
    super(code);
    this.name = 'TransitReadError';
    this.code = code;
    this.status = status;
  }
}
const requireThat = (ok, code = 'transit_read_invalid', status) => {
  if (!ok) throw new TransitReadError(code, status);
};
const inside = (root, target) => {
  const part = relative(root, target);
  return (
    part !== '' &&
    !isAbsolute(part) &&
    part !== '..' &&
    !part.startsWith('../') &&
    !part.startsWith('..\\')
  );
};

/** Local-device host binding. It reads only three exact approved source files.
 * No CLI, manifest, wallet, config, .transit directory, network, account API,
 * paid flow, node installation or process control is reachable through it. */
export function createTransitReadAdapter({
  sourceRoot,
  resource,
  pins,
  authorize,
  deviceOnline,
} = {}) {
  requireThat(
    typeof sourceRoot === 'string' && isAbsolute(sourceRoot),
    'transit_source_root_required',
  );
  requireThat(
    typeof authorize === 'function' && typeof deviceOnline === 'function',
    'transit_host_required',
  );
  const scope = snapshot(resource),
    approved = snapshot(pins);
  requireThat(
    scope &&
      ['registryId', 'tenantId', 'appId', 'environmentId', 'resourceId', 'deviceId'].every(
        (key) => typeof scope[key] === 'string' && scope[key].length > 0,
      ) &&
      Object.keys(scope).length === 6,
    'transit_scope_invalid',
  );
  Object.freeze(scope);
  requireThat(
    approved &&
      Object.keys(approved).length === 3 &&
      Object.keys(DOCUMENTS).every((key) => /^[a-f0-9]{64}$/u.test(approved[key])),
    'transit_source_pin_required',
  );
  Object.freeze(approved);
  let stopped = false;
  async function check(context) {
    requireThat(!stopped, 'transit_adapter_closed', 503);
    requireThat((await authorize(context, scope)) === true, 'transit_access_denied', 403);
    requireThat((await deviceOnline(context, scope)) === true, 'transit_device_offline', 503);
  }
  async function read(context, documentId) {
    await check(context);
    const root = await realpath(sourceRoot),
      path = await realpath(resolve(root, DOCUMENTS[documentId]));
    requireThat(inside(root, path), 'transit_source_escape', 403);
    const file = await open(path, 'r');
    let bytes;
    try {
      const stat = await file.stat();
      requireThat(stat.isFile() && stat.size <= 2097152, 'transit_source_limit');
      const parts = [];
      let size = 0;
      while (true) {
        const buffer = Buffer.alloc(65536),
          { bytesRead } = await file.read(buffer, 0, buffer.length, null);
        if (!bytesRead) break;
        size += bytesRead;
        requireThat(size <= 2097152, 'transit_source_limit');
        parts.push(buffer.subarray(0, bytesRead));
      }
      bytes = Buffer.concat(parts, size);
    } finally {
      await file.close();
    }
    // A changed file or a path race yields no text, not a newly approved pin.
    requireThat(
      createHash('sha256').update(bytes).digest('hex') === approved[documentId],
      'transit_source_changed',
      409,
    );
    let text;
    try {
      text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
    } catch {
      throw new TransitReadError('transit_source_encoding');
    }
    await check(context);
    return text;
  }
  return Object.freeze({
    profile: TRANSIT_READ_PROFILE,
    async discover(context) {
      await check(context);
      return Object.freeze({
        profile: TRANSIT_READ_PROFILE,
        resourceId: scope.resourceId,
        documents: Object.keys(DOCUMENTS).map((id) => ({
          id,
          path: DOCUMENTS[id],
          digest: approved[id],
        })),
        mode: 'development-source-read',
        networkExecution: false,
        financialEffects: false,
        requiresHostAdmission: true,
      });
    },
    async readDocument(context, input) {
      const args = snapshot(input);
      requireThat(
        args &&
          Object.hasOwn(DOCUMENTS, args.documentId) &&
          Object.keys(args).every((key) => ['documentId', 'fromLine', 'limit'].includes(key)),
      );
      const fromLine = args.fromLine ?? 1,
        limit = args.limit ?? 40;
      requireThat(
        Number.isSafeInteger(fromLine) &&
          fromLine >= 1 &&
          Number.isSafeInteger(limit) &&
          limit >= 1 &&
          limit <= 80,
      );
      const text = await read(context, args.documentId),
        lines = text.split(/\r?\n/u);
      requireThat(fromLine <= lines.length + 1, 'transit_source_range');
      const selected = [];
      let characters = 0;
      for (const line of lines.slice(fromLine - 1, fromLine - 1 + limit)) {
        if (characters + line.length + 1 > 12000) break;
        selected.push(line);
        characters += line.length + 1;
      }
      requireThat(
        selected.length > 0 || fromLine === lines.length + 1,
        'transit_source_line_limit',
      );
      const next = fromLine - 1 + selected.length;
      return Object.freeze({
        resourceId: scope.resourceId,
        documentId: args.documentId,
        relativePath: DOCUMENTS[args.documentId],
        digest: approved[args.documentId],
        fromLine,
        text: selected.join('\n'),
        nextLine: next < lines.length ? next + 1 : null,
        sourceContent: true,
        instructionsToExecutor: false,
        networkExecution: false,
        financialEffects: false,
      });
    },
    close() {
      stopped = true;
    },
  });
}
