import { DatabaseSync, backup } from 'node:sqlite';
import { chmodSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { createHash, randomBytes } from 'node:crypto';

const ddl = [
  'CREATE TABLE push_meta(project_id TEXT PRIMARY KEY,schema_version INTEGER NOT NULL CHECK(schema_version=1))',
  `CREATE TABLE push_subscriptions(id TEXT PRIMARY KEY,account_id TEXT NOT NULL,device_id TEXT NOT NULL UNIQUE,
    subscription TEXT NOT NULL,key_id TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>=1),expires_at INTEGER,updated_at INTEGER NOT NULL)`,
  'CREATE INDEX push_subscriptions_account ON push_subscriptions(account_id,id)',
  'CREATE INDEX push_subscriptions_expiry ON push_subscriptions(expires_at) WHERE expires_at IS NOT NULL',
  `CREATE TABLE push_jobs(id TEXT PRIMARY KEY,subscription_id TEXT NOT NULL REFERENCES push_subscriptions(id) ON DELETE CASCADE,
    subscription_revision INTEGER NOT NULL,event_key TEXT NOT NULL,event TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('pending','sent','failed')),
    attempts INTEGER NOT NULL DEFAULT 0,next_attempt INTEGER NOT NULL,expires_at INTEGER NOT NULL,UNIQUE(subscription_id,event_key))`,
  'CREATE INDEX push_jobs_due ON push_jobs(state,next_attempt)',
  'CREATE INDEX push_jobs_expiry ON push_jobs(expires_at)',
  `CREATE TABLE push_fanouts(id TEXT PRIMARY KEY,event TEXT NOT NULL,account_cursor TEXT NOT NULL DEFAULT '',
    accounts TEXT,subscription_cursor TEXT NOT NULL DEFAULT '',state TEXT NOT NULL CHECK(state IN('pending','done')),expires_at INTEGER NOT NULL)`,
  'CREATE INDEX push_fanouts_state ON push_fanouts(state)',
  'CREATE INDEX push_fanouts_expiry ON push_fanouts(expires_at)',
];
const normalized = value => value.replace(/\s+/gu,'').replace(/;$/u,'');
const hash = value => createHash('sha256').update(value).digest('hex');
const need = value => { if (!value) throw Object.assign(new Error('push_store_invalid'), { code:'push_store_invalid' }); };

/** Private endpoint/key storage. No row is a public response or a permission grant. */
export function createPushStore({ databasePath, projectId }) {
  need(typeof databasePath === 'string' && databasePath.length && typeof projectId === 'string' && /^[A-Za-z0-9_.:-]{1,128}$/u.test(projectId));
  const file = databasePath === ':memory:' ? databasePath : resolve(databasePath);
  if (file !== ':memory:') mkdirSync(dirname(file), { recursive:true, mode:0o700 });
  const db = new DatabaseSync(file); let closed = false;
  const get = (sql,...args) => db.prepare(sql).get(...args), all = (sql,...args) => db.prepare(sql).all(...args);
  const run = (sql,...args) => db.prepare(sql).run(...args);
  try {
    db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=1000');
    const version = get('PRAGMA user_version').user_version;
    if (version === 0) {
      need(!get("SELECT 1 FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' LIMIT 1"));
      db.exec('BEGIN IMMEDIATE');
      try { ddl.forEach(sql => db.exec(sql)); run('INSERT INTO push_meta VALUES (?,1)',projectId); db.exec('PRAGMA user_version=1; COMMIT'); }
      catch (error) { db.exec('ROLLBACK'); throw error; }
    }
    need(get('PRAGMA user_version').user_version === 1);
    const rows = all("SELECT sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name");
    need(rows.length === ddl.length && rows.map(row => normalized(row.sql)).sort().join('\n') === ddl.map(normalized).sort().join('\n'));
    const meta = all('SELECT * FROM push_meta'); need(meta.length === 1 && meta[0].project_id === projectId && meta[0].schema_version === 1);
    db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL');
    if (file !== ':memory:') chmodSync(file,0o600);
  } catch (error) { db.close(); throw error; }
  const transaction = work => { db.exec('BEGIN IMMEDIATE'); try { const result=work(); db.exec('COMMIT'); return result; } catch(error) { db.exec('ROLLBACK'); throw error; } };
  return Object.freeze({
    own(actor,keyId,now) { return get('SELECT id FROM push_subscriptions WHERE account_id=? AND device_id=? AND key_id=? AND (expires_at IS NULL OR expires_at>?)',actor.accountId,actor.deviceId,keyId,now); },
    put(actor,subscription,keyId,now) {
      const id=hash(subscription.endpoint), json=JSON.stringify(subscription), old=get('SELECT * FROM push_subscriptions WHERE id=?',id);
      if (old && (old.account_id !== actor.accountId || old.device_id !== actor.deviceId)) throw Object.assign(new Error('push_subscription_bound'),{code:'push_subscription_bound'});
      const device=get('SELECT account_id FROM push_subscriptions WHERE device_id=?',actor.deviceId);
      need(!device || device.account_id === actor.accountId);
      if (old?.subscription === json && old.key_id === keyId) return;
      transaction(() => {
        run('DELETE FROM push_subscriptions WHERE device_id=?',actor.deviceId);
        run('INSERT INTO push_subscriptions VALUES (?,?,?,?,?,?,?,?)',id,actor.accountId,actor.deviceId,json,keyId,(old?.revision??0)+1,subscription.expirationTime,now);
      });
    },
    remove(actor) { run('DELETE FROM push_subscriptions WHERE account_id=? AND device_id=?',actor.accountId,actor.deviceId); },
    revoke(deviceId) { run('DELETE FROM push_subscriptions WHERE device_id=?',deviceId); },
    expire(job) { run('DELETE FROM push_subscriptions WHERE id=? AND revision=?',job.subscription_id,job.subscription_revision); },
    stageEvents(events,now) {
      return transaction(()=>{
        let available=128-get("SELECT count(*) AS n FROM push_fanouts WHERE state='pending'").n;const accepted=[];
        for(const event of events){const json=JSON.stringify(event),id=hash(json);
          if(get('SELECT 1 FROM push_fanouts WHERE id=?',id)){accepted.push(id);continue;}
          if(available<=0)continue;
          run("INSERT INTO push_fanouts(id,event,state,expires_at) VALUES(?,?,'pending',?)",id,json,now+3600_000);available--;accepted.push(id);
        }return accepted;
      });
    },
    fanouts(now) { return all("SELECT * FROM push_fanouts WHERE state='pending' AND expires_at>? ORDER BY rowid LIMIT 4",now); },
    fanoutPage(id,accounts) {
      need(Array.isArray(accounts)&&accounts.length<=256);
      run(accounts.length?"UPDATE push_fanouts SET accounts=? WHERE id=? AND accounts IS NULL AND state='pending'"
        :"UPDATE push_fanouts SET accounts=?,state='done' WHERE id=? AND accounts IS NULL AND state='pending'",JSON.stringify(accounts),id);
    },
    enqueueFanout(id,keyId,now) {
      return transaction(()=>{
        const fanout=get("SELECT * FROM push_fanouts WHERE id=? AND state='pending' AND expires_at>?",id,now);if(!fanout?.accounts)return;
        const accounts=JSON.parse(fanout.accounts);need(Array.isArray(accounts)&&accounts.length>0&&accounts.length<=256);
        const subscriptions=all(`SELECT id,revision FROM push_subscriptions WHERE account_id IN(SELECT value FROM json_each(?)) AND key_id=?
          AND (expires_at IS NULL OR expires_at>?) AND id>? ORDER BY id LIMIT 256`,fanout.accounts,keyId,now,fanout.subscription_cursor);
        let available=4096-get("SELECT count(*) AS n FROM push_jobs WHERE state='pending'").n,cursor=fanout.subscription_cursor,processed=0;
        const insert=db.prepare("INSERT OR IGNORE INTO push_jobs VALUES (?,?,?,?,?,'pending',0,?,?)");
        for(const subscription of subscriptions){if(available<=0)break;
          const inserted=insert.run(randomBytes(24).toString('base64url'),subscription.id,subscription.revision,fanout.id,fanout.event,now,fanout.expires_at);
          available-=Number(inserted.changes);cursor=subscription.id;processed++;
        }
        if(processed===subscriptions.length&&subscriptions.length<256)
          run("UPDATE push_fanouts SET account_cursor=?,accounts=NULL,subscription_cursor='',state=? WHERE id=?",accounts.at(-1),accounts.length<256?'done':'pending',id);
        else run('UPDATE push_fanouts SET subscription_cursor=? WHERE id=?',cursor,id);
      });
    },
    due(now) { return all(`SELECT j.*,s.account_id,s.device_id,s.subscription,s.key_id,s.revision FROM push_jobs j
      JOIN push_subscriptions s ON s.id=j.subscription_id WHERE j.state='pending' AND j.next_attempt<=? AND j.expires_at>? ORDER BY j.next_attempt,j.id LIMIT 8`,now,now); },
    isCurrent(job,keyId,now) { return Boolean(get(`SELECT 1 FROM push_jobs j JOIN push_subscriptions s ON s.id=j.subscription_id
      WHERE j.id=? AND j.state='pending' AND s.revision=? AND s.key_id=? AND (s.expires_at IS NULL OR s.expires_at>?)`,job.id,job.subscription_revision,keyId,now)); },
    settle(id,state,attempts,next) { run('UPDATE push_jobs SET state=?,attempts=?,next_attempt=? WHERE id=?',state,attempts,next,id); },
    open(id,actor,now) { return get(`SELECT j.event FROM push_jobs j JOIN push_subscriptions s ON s.id=j.subscription_id
      WHERE j.id=? AND j.expires_at>? AND s.account_id=? AND s.device_id=?`,id,now,actor.accountId,actor.deviceId); },
    prune(now) { run('DELETE FROM push_jobs WHERE expires_at<=?',now); run('DELETE FROM push_fanouts WHERE expires_at<=?',now); run('DELETE FROM push_subscriptions WHERE expires_at IS NOT NULL AND expires_at<=?',now); },
    async backupTo(destination) { need(!closed && typeof destination === 'string' && destination !== ':memory:'); const target=resolve(destination);
      need(target !== file); mkdirSync(dirname(target),{recursive:true,mode:0o700}); await backup(db,target); chmodSync(target,0o600); },
    close() { if (!closed) { closed=true; db.close(); } },
  });
}
