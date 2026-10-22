import { assert, identifier } from './validation.mjs';

export function requireDirectThread(m, args, actor, authority) {
  identifier(args.threadId); identifier(args.expectedRelationshipId);
  assert(authority && typeof authority.relationshipId === 'function', 'direct_authority_unavailable');
  const row = m.get('SELECT * FROM world_direct_threads WHERE id=?', args.threadId);
  const peer = row?.account_low === actor.accountId ? row.account_high : row?.account_high === actor.accountId ? row.account_low : null;
  assert(peer && row.relationship_id === args.expectedRelationshipId && authority.relationshipId(peer) === row.relationship_id, 'direct_not_found');
  return row;
}
