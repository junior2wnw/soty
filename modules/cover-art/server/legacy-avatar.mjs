import { createHash, randomBytes } from 'node:crypto';
import { validateAvatar, AVATAR_LIMITS } from '../../world/server/avatars.mjs';
import { CoverError } from './index.mjs';

const check=(value,code)=>{if(!value)throw new CoverError(code);};
const newId=prefix=>`${prefix}_${randomBytes(16).toString('hex')}`;
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
const canonicalPair=(image,thumbnail)=>({recipe:'legacy-avatar-1',crop:{x:.5,y:.5,zoom:1},renditions:[
  {role:'image',width:image.width,height:image.height,mime:image.mime,sha256:hash(image.bytes),bytes:image.bytes},
  {role:'thumbnail',width:thumbnail.width,height:thumbnail.height,mime:thumbnail.mime,sha256:hash(thumbnail.bytes),bytes:thumbnail.bytes},
]});
function validatePair(image,thumbnail) {
  return canonicalPair(validateAvatar(image,{maximumBytes:AVATAR_LIMITS.imageBytes,maximumPixels:AVATAR_LIMITS.imagePixels}),validateAvatar(thumbnail,{maximumBytes:AVATAR_LIMITS.thumbnailBytes,maximumPixels:AVATAR_LIMITS.thumbnailPixels}));
}
export function profileAvatarViewSql() {
  return `CREATE VIEW profile_avatars AS SELECT h.target_id AS account_id,i.mime AS mime,i.bytes AS image,t.mime AS thumbnail_mime,t.bytes AS thumbnail FROM profile_avatar_heads h JOIN profile_avatar_versions v ON v.id=h.active_version_id AND v.target_id=h.target_id AND v.owner_id=h.owner_id JOIN profile_avatar_renditions i ON i.asset_id=v.asset_id AND i.role='image' JOIN profile_avatar_renditions t ON t.asset_id=v.asset_id AND t.role='thumbnail' WHERE v.mode='custom' AND v.purged_at IS NULL`;
}
/** Root-only atomic migration helper, invoked after its new fragments are
 * created inside the single admitted World5 -> World6+DM migration transaction.
 * It never starts/commits a transaction or changes user_version itself. */
export function migrateLegacyAvatarTable(db,{now,receiptMs=30*86400_000}={}) {
  check(db.isTransaction&&Number.isSafeInteger(now)&&now>0,'cover_migration_transaction_required');
  check(Number.isSafeInteger(receiptMs)&&receiptMs>=86400_000,'cover_migration_retention_invalid');
  const version=Number(db.prepare('PRAGMA user_version').get().user_version);
  check([5,6].includes(version)&&db.prepare("SELECT value FROM world_meta WHERE key='schema_version'").get()?.value===String(version)&&db.prepare("SELECT value FROM world_meta WHERE key='lineage'").get()?.value==='soty.world.sqlite.v1','cover_legacy_world_format_invalid');
  check(db.prepare("SELECT type FROM sqlite_schema WHERE name='profile_avatars'").get()?.type==='table','cover_legacy_table_required');
  check(JSON.stringify(db.prepare('PRAGMA table_info(profile_avatars)').all().map(r=>r.name))===JSON.stringify(['account_id','mime','image','thumbnail_mime','thumbnail']),'cover_legacy_table_invalid');
  check(db.prepare('SELECT count(*) AS n FROM profile_avatar_heads').get().n===0&&db.prepare('SELECT count(*) AS n FROM profile_avatar_assets').get().n===0,'cover_legacy_destination_not_empty');
  let count=0,total=0;
  for(const row of db.prepare('SELECT * FROM profile_avatars ORDER BY account_id').iterate()) {
    const profile=db.prepare('SELECT account_id,revision,avatar_revision FROM profiles WHERE account_id=?').get(row.account_id);
    check(profile&&Number.isSafeInteger(profile.revision)&&profile.revision>=1&&Number.isSafeInteger(profile.avatar_revision)&&profile.avatar_revision>=1&&profile.avatar_revision<=profile.revision,'cover_legacy_profile_invalid');
    const pair=validatePair(`data:${row.mime};base64,${Buffer.from(row.image).toString('base64')}`,`data:${row.thumbnail_mime};base64,${Buffer.from(row.thumbnail).toString('base64')}`);
    const assetId=newId('coverasset'),versionId=newId('coverversion'),bytes=pair.renditions.reduce((sum,r)=>sum+r.bytes.length,0);
    db.prepare('INSERT INTO profile_avatar_assets VALUES(?,?,?,\'legacy\',?,?,?, ?,?,NULL)').run(assetId,row.account_id,row.account_id,profile.revision,profile.revision,pair.recipe,bytes,now);
    for(const r of pair.renditions)db.prepare('INSERT INTO profile_avatar_renditions VALUES(?,?,?,?,?,?,?,?)').run(assetId,r.role,r.width,r.height,r.mime,r.sha256,r.bytes.length,r.bytes);
    db.prepare("INSERT INTO profile_avatar_versions VALUES(?,?,?,?, 'custom',?,?,?,NULL,?)").run(versionId,row.account_id,row.account_id,1,assetId,JSON.stringify(pair.crop),now,now+receiptMs);
    db.prepare('INSERT INTO profile_avatar_heads VALUES(?,?,1,2,?,NULL,NULL,?)').run(row.account_id,row.account_id,versionId,now);
    db.prepare('INSERT INTO profile_avatar_owner_usage VALUES(?,?,0,0,0,0,0)').run(row.account_id,bytes);count++;total+=bytes;
  }
  db.prepare('UPDATE profile_avatar_usage SET encoded_bytes=encoded_bytes+?,logical_now=max(logical_now,?) WHERE id=1').run(total,now);
  db.exec('DROP TABLE profile_avatars');db.exec(profileAvatarViewSql());
  return {migratedProfiles:count,encodedBytes:total,exactBytesPreserved:true};
}
/** Route world.profile.avatar.set to this adapter BEFORE World.execute opens
 * its legacy BEGIN. Existing read/batch continue querying the compatibility
 * view under unchanged World audience/room authorization. */
export function createLegacyAvatarAdapter({store}) {
  check(store&&typeof store.legacyCommit==='function','cover_profile_adapter_invalid');
  return Object.freeze({set(actor,args) {
    check(args&&Object.keys(args).sort().join(',')==='avatarUrl,expectedRevision,thumbnailUrl','invalid_world_arguments');
    let canonical=null;
    if(args.avatarUrl===null||args.thumbnailUrl===null)check(args.avatarUrl===null&&args.thumbnailUrl===null,'invalid_avatar_data');
    else canonical=validatePair(args.avatarUrl,args.thumbnailUrl);
    const result=store.legacyCommit(actor,{targetId:actor.accountId,expectedRevision:args.expectedRevision,canonical});
    return {profile:result.ownerProjection};
  }});
}
