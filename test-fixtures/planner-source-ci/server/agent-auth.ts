import { createHash, randomBytes } from 'node:crypto';
import { z } from 'zod';
import { PlannerStore, uid } from './store.ts';
import { ApiError, parse, id } from './validation.ts';
import type { User } from '../shared/types.ts';

export interface AgentScope {
  workspaceIds?: string[];
  readOnly?: boolean;
  keyId?: string;
}
export interface AgentIdentity {
  user: User;
  scope: AgentScope;
}
type KeyRow = {
  id: string;
  user_id: string;
  name: string;
  workspace_ids: string;
  read_only: number;
  expires_at: string;
  created_at: string;
  last_used_at: string | null;
};
const digest = (value: string) => createHash('sha256').update(value).digest('hex');
const createSchema = z
  .object({
    name: z.string().trim().min(1).max(100),
    workspaceIds: z.array(id).min(1).max(30),
    readOnly: z.boolean().default(true),
    expiresInDays: z.number().int().min(1).max(90).default(30),
  })
  .strict();

export class AgentKeys {
  constructor(private store: PlannerStore) {
    store.db.exec(`CREATE TABLE IF NOT EXISTS agent_keys (
      id TEXT PRIMARY KEY, token_hash TEXT NOT NULL UNIQUE, user_id TEXT NOT NULL,
      name TEXT NOT NULL, workspace_ids TEXT NOT NULL, read_only INTEGER NOT NULL,
      expires_at TEXT NOT NULL, created_at TEXT NOT NULL, last_used_at TEXT)`);
  }
  private safe(row: KeyRow) {
    return {
      id: row.id,
      name: row.name,
      workspaceIds: JSON.parse(row.workspace_ids) as string[],
      readOnly: !!row.read_only,
      expiresAt: row.expires_at,
      createdAt: row.created_at,
      lastUsedAt: row.last_used_at,
    };
  }
  list(user: User) {
    return (
      this.store.db
        .prepare(
          'SELECT id,user_id,name,workspace_ids,read_only,expires_at,created_at,last_used_at FROM agent_keys WHERE user_id=? ORDER BY created_at DESC',
        )
        .all(user.id) as KeyRow[]
    ).map((row) => this.safe(row));
  }
  create(user: User, input: unknown) {
    const raw = parse(createSchema, input);
    const workspaceIds = [...new Set(raw.workspaceIds)];
    for (const workspace of workspaceIds) this.store.requireRole(user, workspace, ['owner']);
    const token = `plnr_${randomBytes(32).toString('base64url')}`;
    const keyId = uid('agent-key');
    const createdAt = new Date().toISOString();
    const expiresAt = new Date(Date.now() + raw.expiresInDays * 86400000).toISOString();
    this.store.db
      .prepare(
        'INSERT INTO agent_keys(id,token_hash,user_id,name,workspace_ids,read_only,expires_at,created_at) VALUES(?,?,?,?,?,?,?,?)',
      )
      .run(
        keyId,
        digest(token),
        user.id,
        raw.name,
        JSON.stringify(workspaceIds),
        raw.readOnly ? 1 : 0,
        expiresAt,
        createdAt,
      );
    return {
      id: keyId,
      name: raw.name,
      workspaceIds,
      readOnly: raw.readOnly,
      createdAt,
      expiresAt,
      token,
    };
  }
  revoke(user: User, keyId: string) {
    const row = this.store.db.prepare('SELECT user_id FROM agent_keys WHERE id=?').get(keyId) as
      { user_id: string } | undefined;
    if (!row || row.user_id !== user.id) throw new ApiError(404, 'Ключ не найден', 'not_found');
    this.store.db.prepare('DELETE FROM agent_keys WHERE id=?').run(keyId);
    return { revoked: true, id: keyId };
  }
  authenticate(token: string): AgentIdentity {
    if (token.length > 256)
      throw new ApiError(401, 'Ключ MCP недействителен или истёк', 'unauthenticated');
    const row = this.store.db
      .prepare(
        'SELECT id,user_id,name,workspace_ids,read_only,expires_at,created_at,last_used_at FROM agent_keys WHERE token_hash=? AND expires_at > ?',
      )
      .get(digest(token), new Date().toISOString()) as KeyRow | undefined;
    const user = row && this.store.user(row.user_id);
    if (!row || !user)
      throw new ApiError(401, 'Ключ MCP недействителен или истёк', 'unauthenticated');
    const workspaceIds = (JSON.parse(row.workspace_ids) as string[]).filter((id) =>
      this.store.role(user.id, id),
    );
    if (!workspaceIds.length)
      throw new ApiError(403, 'Доступ ключа к пространствам отозван', 'forbidden');
    this.store.db
      .prepare('UPDATE agent_keys SET last_used_at=? WHERE id=?')
      .run(new Date().toISOString(), row.id);
    return { user, scope: { workspaceIds, readOnly: !!row.read_only, keyId: row.id } };
  }
}
