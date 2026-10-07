import { createHash } from 'node:crypto';
import { z } from 'zod';
import { ApiError, parse } from './validation.ts';
import type { PlannerStore } from './store.ts';
import type { User } from '../shared/types.ts';
import type { AgentScope } from './agent-auth.ts';

export const WORK_ITEM_PROOF_PROTOCOL = 'planner.work-item-proof.v1';
const sha = z.string().regex(/^[a-f0-9]{64}$/);
const requestSchema = z
  .object({
    protocol: z.literal(WORK_ITEM_PROOF_PROTOCOL),
    requestId: z.string().min(1).max(200),
    workspaceId: z.string().min(1).max(500),
    inputDigest: sha,
    providerInputDigest: sha,
    scopeDigest: sha,
  })
  .strict();
const canonical = (value: unknown): string => {
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (value && typeof value === 'object')
    return (
      '{' +
      Object.keys(value)
        .sort()
        .map(
          (key) => JSON.stringify(key) + ':' + canonical((value as Record<string, unknown>)[key]),
        )
        .join(',') +
      '}'
    );
  return JSON.stringify(value) ?? 'null';
};
export const proofHash = (value: unknown) =>
  createHash('sha256').update(canonical(value)).digest('hex');
const profileBody = Object.freeze({
  protocol: WORK_ITEM_PROOF_PROTOCOL,
  inputSchema: z.toJSONSchema(requestSchema),
  effect: 'one-undated-object',
  sourceAuthority: 'current-bearer-selected-workspace',
  receiptAuthority: 'atomic-agent-request',
});
export const workItemProofProfile = Object.freeze({
  ...profileBody,
  digest: proofHash(profileBody),
});

export function migrateWorkItemProofs(store: PlannerStore) {
  const columns = store.db.prepare('PRAGMA table_info(agent_requests)').all() as { name: string }[];
  for (const name of ['proof_profile', 'proof_json'])
    if (!columns.some((column) => column.name === name))
      store.db.exec(`ALTER TABLE agent_requests ADD COLUMN ${name} TEXT`);
}

/** Only the approved title-only operation earns this proof. Existing general
 * planner_apply schemas, request IDs, replay checks and results stay intact. */
export function workItemProof(
  args: Record<string, any>,
  scope: AgentScope,
  user: User,
  response: Record<string, any>,
  workspaceIds: string[],
) {
  const operations = args.operations;
  if (
    Object.keys(args).sort().join(',') !== 'operations,requestId' ||
    !scope.keyId ||
    scope.readOnly ||
    scope.workspaceIds?.length !== 1 ||
    workspaceIds.length !== 1 ||
    workspaceIds[0] !== scope.workspaceIds[0] ||
    !Array.isArray(operations) ||
    operations.length !== 1
  )
    return null;
  const op = operations[0];
  if (
    Object.keys(op).sort().join(',') !== 'collection,data,key,op,workspace' ||
    op.op !== 'create' ||
    op.collection !== 'objects' ||
    op.key !== 'workItem' ||
    op.workspace !== workspaceIds[0] ||
    !op.data ||
    Object.keys(op.data).join(',') !== 'title' ||
    typeof op.data.title !== 'string' ||
    !op.data.title.trim() ||
    op.data.title.length > 500
  )
    return null;
  const object = response.results?.[0]?.object;
  if (
    response.results?.length !== 1 ||
    object?.workspaceId !== workspaceIds[0] ||
    object?.id !== response.refs?.workItem ||
    object?.kind !== 'note' ||
    object?.plan?.start !== null ||
    object?.plan?.end !== null ||
    object.version !== 1
  )
    throw new ApiError(500, 'Нельзя подтвердить выбранный результат', 'proof_invalid');
  return {
    protocol: WORK_ITEM_PROOF_PROTOCOL,
    requestId: args.requestId,
    workspaceId: workspaceIds[0],
    sourceActorId: user.id,
    inputDigest: proofHash({ title: op.data.title }),
    providerInputDigest: proofHash(args),
    scopeDigest: proofHash(scope),
    outcome: 'committed',
    objectId: object.id,
    objectRevision: object.version,
    revision: response.revision,
    receiptDigest: proofHash(response),
  };
}

export function readWorkItemProof(
  store: PlannerStore,
  user: User,
  scope: AgentScope,
  input: unknown,
) {
  const args = parse(requestSchema, input);
  if (
    !scope.keyId ||
    scope.workspaceIds?.length !== 1 ||
    scope.workspaceIds[0] !== args.workspaceId
  )
    throw new ApiError(
      403,
      'Ключ должен разрешать ровно выбранное пространство',
      'scope_violation',
    );
  store.requireRole(user, args.workspaceId, ['owner', 'editor', 'approver', 'viewer']);
  const row = store.db
    .prepare(
      'SELECT workspace_ids,proof_profile,proof_json FROM agent_requests WHERE user_id=? AND request_id=?',
    )
    .get(user.id, args.requestId) as
    { workspace_ids: string; proof_profile: string | null; proof_json: string | null } | undefined;
  const result = { ...args, sourceActorId: user.id };
  if (!row) return { ...result, outcome: 'not_applied' };
  const workspaces = JSON.parse(row.workspace_ids) as string[];
  if (workspaces.length !== 1 || workspaces[0] !== args.workspaceId)
    throw new ApiError(403, 'Результат вне выбранного пространства', 'scope_violation');
  if (row.proof_profile !== WORK_ITEM_PROOF_PROTOCOL || !row.proof_json)
    return { ...result, outcome: 'unknown' };
  const proof = JSON.parse(row.proof_json);
  if (
    proof.inputDigest !== args.inputDigest ||
    proof.providerInputDigest !== args.providerInputDigest ||
    proof.scopeDigest !== args.scopeDigest ||
    proof.sourceActorId !== user.id ||
    proof.workspaceId !== args.workspaceId
  )
    throw new ApiError(
      409,
      'ID запроса использован с другими аргументами или доступом',
      'idempotency_mismatch',
    );
  return proof;
}
