import { assert, choice, exact, limit, searchText, text, WorldError } from './validation.mjs';

const COUNT_LIMIT = 1000;

// Search cursors follow immutable projection sequence numbers, never offsets into a changing list.
function readCursor(value, scope) {
  if (value == null) return 0;
  assert(typeof value === 'string' && value.length < 1024, 'invalid_cursor');
  try {
    const cursor = JSON.parse(Buffer.from(value, 'base64url').toString('utf8'));
    assert(cursor.version === 1 && cursor.scope === scope && Number.isSafeInteger(cursor.after) && cursor.after >= 0, 'invalid_cursor');
    return cursor.after;
  } catch { throw new WorldError('invalid_cursor'); }
}

export function discoveryOperation(m, args, actor) {
  exact(args, [], ['query', 'kind', 'limit', 'cursor']);
  const rawQuery = text(args.query ?? '', 100), query = searchText(rawQuery);
  const kind = choice(args.kind ?? 'all', ['all', 'people', 'communities']);
  const entityKind = kind === 'people' ? 'person' : kind === 'communities' ? 'community' : null;
  const size = limit(args.limit), scope = JSON.stringify([rawQuery, kind]), after = readCursor(args.cursor, scope);
  const terms = query.match(/[\p{L}\p{N}\p{M}]+/gu) ?? [];
  // Bind each term as a literal prefix. User text is never interpreted as FTS operators or SQL.
  const match = terms.length ? `search_text : (${terms.map(term => `"${term}"*`).join(' AND ')})` : '';
  const scopedMatch = type => type ? `kind : ${type} AND ${match}` : match;
  let rows, totals;

  if (!query) {
    rows = entityKind
      ? m.all('SELECT * FROM world_directory WHERE kind=? AND seq>? ORDER BY seq LIMIT ?', entityKind, after, size + 1)
      : m.all('SELECT * FROM world_directory WHERE seq>? ORDER BY seq LIMIT ?', after, size + 1);
    const counts = Object.fromEntries(m.all('SELECT kind,n FROM world_directory_counts').map(row => [row.kind, row.n]));
    totals = { people: kind === 'communities' ? 0 : counts.person ?? 0, communities: kind === 'people' ? 0 : counts.community ?? 0 };
  } else {
    const direct = entityKind
      ? m.get('SELECT * FROM world_directory WHERE kind=? AND entity_id=?', entityKind, rawQuery)
      : m.get('SELECT * FROM world_directory WHERE entity_id=?', rawQuery);
    if (direct) {
      rows = direct.seq > after ? [direct] : [];
      totals = { people: Number(direct.kind === 'person'), communities: Number(direct.kind === 'community') };
    } else if (!match) {
      rows = []; totals = { people: 0, communities: 0 };
    } else {
      rows = m.all(`SELECT d.* FROM world_directory_fts JOIN world_directory d ON d.seq=world_directory_fts.rowid
        WHERE world_directory_fts MATCH ? AND world_directory_fts.rowid>?
        ORDER BY world_directory_fts.rowid LIMIT ?`, scopedMatch(entityKind), after, size + 1);
      // Exact counts for a popular query must not scan millions of matches on every keystroke.
      // The wire explicitly distinguishes a lower bound; the UI renders 1000+ instead of a false total.
      const boundedCount = type => m.get(`SELECT COUNT(*) AS n FROM (
        SELECT 1 FROM world_directory_fts
        WHERE world_directory_fts MATCH ? LIMIT ?)`, scopedMatch(type), COUNT_LIMIT + 1).n;
      const people = kind === 'communities' ? 0 : boundedCount('person');
      const communities = kind === 'people' ? 0 : boundedCount('community');
      totals = { people: Math.min(COUNT_LIMIT, people), communities: Math.min(COUNT_LIMIT, communities),
        ...(people > COUNT_LIMIT ? { peopleExact: false } : {}), ...(communities > COUNT_LIMIT ? { communitiesExact: false } : {}) };
    }
  }
  const page = rows.slice(0, size);
  return {
    people: page.filter(row => row.kind === 'person').map(row => m.profile(m.person(row.entity_id))),
    communities: page.filter(row => row.kind === 'community').map(row => m.community(m.group(row.entity_id), actor.accountId)),
    nextCursor: rows.length > size ? Buffer.from(JSON.stringify({ version: 1, scope, after: page.at(-1).seq })).toString('base64url') : null,
    totals,
  };
}

export const DISCOVERY_MIGRATION = `
CREATE TABLE world_directory (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL CHECK(kind IN('person','community')),
  entity_id TEXT NOT NULL,
  name TEXT NOT NULL,
  search_text TEXT NOT NULL,
  UNIQUE(kind,entity_id)
);
CREATE INDEX world_directory_entity ON world_directory(entity_id);
CREATE INDEX world_directory_kind_seq ON world_directory(kind,seq);
CREATE TABLE world_directory_counts (kind TEXT PRIMARY KEY, n INTEGER NOT NULL CHECK(n>=0));
INSERT INTO world_directory_counts VALUES ('person',0),('community',0);
CREATE VIRTUAL TABLE world_directory_fts USING fts5(kind,search_text, content='world_directory', content_rowid='seq', tokenize='unicode61', prefix='2 3 4');
CREATE TRIGGER world_directory_ai AFTER INSERT ON world_directory BEGIN
  INSERT INTO world_directory_fts(rowid,kind,search_text) VALUES(new.seq,new.kind,new.search_text);
  UPDATE world_directory_counts SET n=n+1 WHERE kind=new.kind;
END;
CREATE TRIGGER world_directory_ad AFTER DELETE ON world_directory BEGIN
  INSERT INTO world_directory_fts(world_directory_fts,rowid,kind,search_text) VALUES('delete',old.seq,old.kind,old.search_text);
  UPDATE world_directory_counts SET n=n-1 WHERE kind=old.kind;
END;
CREATE TRIGGER world_directory_au AFTER UPDATE OF search_text ON world_directory BEGIN
  INSERT INTO world_directory_fts(world_directory_fts,rowid,kind,search_text) VALUES('delete',old.seq,old.kind,old.search_text);
  INSERT INTO world_directory_fts(rowid,kind,search_text) VALUES(new.seq,new.kind,new.search_text);
END;
CREATE TRIGGER world_profile_directory_ai AFTER INSERT ON profiles WHEN new.discoverable=1 BEGIN
  INSERT INTO world_directory(kind,entity_id,name,search_text) VALUES('person',new.account_id,new.display_name,new.search_text);
END;
CREATE TRIGGER world_profile_directory_au AFTER UPDATE OF discoverable,display_name,search_text ON profiles BEGIN
  DELETE FROM world_directory WHERE kind='person' AND entity_id=old.account_id AND new.discoverable=0;
  INSERT INTO world_directory(kind,entity_id,name,search_text)
    SELECT 'person',new.account_id,new.display_name,new.search_text WHERE new.discoverable=1
    ON CONFLICT(kind,entity_id) DO UPDATE SET name=excluded.name,search_text=excluded.search_text;
END;
CREATE TRIGGER world_profile_directory_ad AFTER DELETE ON profiles BEGIN
  DELETE FROM world_directory WHERE kind='person' AND entity_id=old.account_id;
END;
CREATE TRIGGER world_community_directory_ai AFTER INSERT ON communities WHEN new.state='active' AND new.join_policy<>'invite' BEGIN
  INSERT INTO world_directory(kind,entity_id,name,search_text) VALUES('community',new.id,new.name,new.search_text);
END;
CREATE TRIGGER world_community_directory_au AFTER UPDATE OF state,join_policy,name,search_text ON communities BEGIN
  DELETE FROM world_directory WHERE kind='community' AND entity_id=old.id AND (new.state<>'active' OR new.join_policy='invite');
  INSERT INTO world_directory(kind,entity_id,name,search_text)
    SELECT 'community',new.id,new.name,new.search_text WHERE new.state='active' AND new.join_policy<>'invite'
    ON CONFLICT(kind,entity_id) DO UPDATE SET name=excluded.name,search_text=excluded.search_text;
END;
CREATE TRIGGER world_community_directory_ad AFTER DELETE ON communities BEGIN
  DELETE FROM world_directory WHERE kind='community' AND entity_id=old.id;
END;
INSERT INTO world_directory(kind,entity_id,name,search_text) SELECT 'person',account_id,display_name,search_text FROM profiles WHERE discoverable=1;
INSERT INTO world_directory(kind,entity_id,name,search_text) SELECT 'community',id,name,search_text FROM communities WHERE state='active' AND join_policy<>'invite';
`;
