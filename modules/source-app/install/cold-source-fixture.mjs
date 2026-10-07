// PUBLIC synthetic persistence fixture only. It creates no Root/Human/OIDC
// proof and never treats restored bytes as current Source authorization.
import {mkdir,writeFile,readFile,lstat,readdir} from 'node:fs/promises';
import {join} from 'node:path';
import {randomBytes,createHash} from 'node:crypto';
import {createOrdinaryAppStore} from '../examples/ordinary-app/store.mjs';
import {readOrdinaryFormat3} from '../examples/ordinary-app/reader3.mjs';
import {readOrdinaryFormat2} from '../examples/ordinary-app/reader2.mjs';
import {STANDARD_SELECTED_SOURCE_V2} from '../server/standard-profile.mjs';

const ROOT='/data',REALM='cold-synthetic',sha=bytes=>createHash('sha256').update(bytes).digest('hex');
let phase='preflight';
const DB='/data/native/native.sqlite',KEY='/data/install/secrets/cipher.key';
const png=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAYAAABytg0kAAAAEklEQVR4AWIqn+36H4SZGKAAAAAA///TQrpwAAAABklEQVQDAD4YBLFsqVAqAAAAAElFTkSuQmCC','base64');
async function privateDirectory(path){const stat=await lstat(path);if(!stat.isDirectory()||stat.isSymbolicLink()||stat.uid!==1000||(stat.mode&0o777)!==0o700)throw Error('source_cold_custody_denied');}
function configuredProfile(){const appId='app-'+'a'.repeat(32);return{schema:'soty.selected-human-embed.v2',appId,
  connector:{linkId:'synthetic-cold-link',hostDeviceId:'synthetic-cold-host',connectorId:'synthetic-cold-connector'},target:{revision:1,digest:'a'.repeat(64)},sourceProfile:STANDARD_SELECTED_SOURCE_V2,
  resource:{registryId:'soty',environmentId:'fixture',tenantId:'synthetic-cold-root',appId,resourceId:'cold-resource',selection:{kind:'soty.resource.v1',nativeId:'cold-resource',incarnationId:'cold-incarnation'}},
  issuer:'https://root.fixture.invalid/human-identity',clientId:'synthetic-cold-client',embedOrigin:'https://'+appId+'.root.fixture.invalid',nativeOrigin:'https://native.fixture.invalid',parentOrigin:'https://root.fixture.invalid'};}
async function seed(){
  phase='seed_directories';
  await privateDirectory(ROOT);if((await readdir(ROOT)).length)throw Error('source_cold_requires_empty_volume');
  for(const path of ['/data/native','/data/install','/data/install/secrets'])await mkdir(path,{mode:0o700});
  phase='seed_keys';for(const name of ['cipher.key','transport.key','client.key'])await writeFile('/data/install/secrets/'+name,randomBytes(32).toString('base64url')+'\n',{flag:'wx',mode:0o600});
  const cfg={schema:'soty.ordinary-source.operator.v1',profile:configuredProfile(),connectorPort:49424,listener:{host:'127.0.0.1',port:5317},
    storage:{directory:'/data/native',keyId:'cold-key',cipherKeyFile:'cipher.key'},rp:{clientSecretFile:'client.key',transportKeyFile:'transport.key'},
    native:{realmId:REALM,resourceTitle:'Synthetic cold resource'},policy:{newEmptyGuest:false,linkedLogin:false}};
  phase='seed_config';await writeFile('/data/install/operator.json',JSON.stringify(cfg)+'\n',{flag:'wx',mode:0o600});
  const key=Buffer.from((await readFile(KEY,'utf8')).trim(),'base64url'),store=createOrdinaryAppStore({databasePath:DB,realmId:REALM,key,keyId:'cold-key',initialize:true,format:3});
  phase='seed_rows';try{store.createResource({id:'cold-resource',incarnationId:'cold-incarnation',title:'Synthetic preserved resource',guestEmpty:false});
    for(const [id,role] of [['cold-owner','owner'],['cold-reporter','participant']]){store.createPrincipal(id);store.grant('cold-resource',id,role);store.createNativeSession(id);}
    store.tx(()=>{
      store.db.prepare("INSERT INTO native_tickets VALUES('cold-ticket','cold-resource','cold-reporter','Synthetic private PNG','received',1,?,?)").run(Date.now(),Date.now());
      store.db.prepare("INSERT INTO native_ticket_media VALUES('cold-ticket',0,?,?)").run(JSON.stringify({kind:'image',name:'synthetic.png',mimeType:'image/png',size:png.length}),png);
      store.db.prepare("INSERT INTO native_receipts VALUES('cold-resource','cold-reporter','cold-original-request',?,?)").run(sha('synthetic original intent'),JSON.stringify({synthetic:true,ticketId:'cold-ticket',revision:1}));
    });
    // Real AES-GCM/AAD ciphertext, explicitly synthetic persistence data; no
    // forged RP session/token/issuer rows or executable permission are seeded.
    phase='seed_cipher';const cipher=store.encrypt('ColdWitness','checkpoint',0,{synthetic:true,nativeIds:['cold-owner','cold-reporter'],version:3});
    await writeFile('/data/native/cipher-witness.json',JSON.stringify({cipher,keyId:'cold-key'})+'\n',{flag:'wx',mode:0o600});
    store.db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
  }finally{store.close();key.fill(0);}
}
async function snapshot(){
  phase='reader';
  const reader=readOrdinaryFormat3(DB,REALM);let oldReaderRefused=false;try{readOrdinaryFormat2(DB,REALM);}catch{oldReaderRefused=true;}
  if(!oldReaderRefused)throw Error('source_cold_old_reader_must_refuse');
  const key=Buffer.from((await readFile(KEY,'utf8')).trim(),'base64url'),store=createOrdinaryAppStore({databasePath:DB,realmId:REALM,key,keyId:'cold-key'});
  phase='cipher_readback';let data;try{const cipher=JSON.parse(await readFile('/data/native/cipher-witness.json','utf8'));
    if(store.decrypt('ColdWitness','checkpoint',0,cipher.cipher,cipher.keyId).synthetic!==true)throw Error('source_cold_cipher_denied');
    data={reader,oldReaderRefused,foreignKeys:store.db.prepare('PRAGMA foreign_key_check').all().length,
      principals:store.db.prepare('SELECT count(*) n FROM native_principals').get().n,memberships:store.db.prepare('SELECT count(*) n FROM native_memberships').get().n,
      tickets:store.db.prepare('SELECT count(*) n FROM native_tickets').get().n,receipts:store.db.prepare('SELECT count(*) n FROM native_receipts').get().n,
      mediaDigest:sha(store.db.prepare("SELECT bytes FROM native_ticket_media WHERE ticket_id='cold-ticket'").get().bytes),
      nativeIdentityDigest:sha(JSON.stringify(store.db.prepare('SELECT * FROM native_resources ORDER BY id').all())),
      rolesDigest:sha(JSON.stringify(store.db.prepare('SELECT * FROM native_memberships ORDER BY resource_id,principal_id').all())),cipherDigest:sha(cipher.cipher)};
    store.db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
  }finally{store.close();key.fill(0);}
  // Inventory is made only after closing/checkpointing every own SQL handle.
  phase='inventory';const files=[];async function scan(path,name){const stat=await lstat(path);if(stat.isSymbolicLink()||(!stat.isDirectory()&&!stat.isFile()))throw Error('source_cold_inventory_denied');
    const mode=stat.mode&0o777;if(stat.uid!==1000||stat.gid!==1000||mode!==(stat.isDirectory()?0o700:0o600))throw Error('source_cold_custody_denied');
    files.push({path:name,type:stat.isDirectory()?'directory':'file',size:stat.isDirectory()?0:stat.size,sha256:stat.isDirectory()?null:sha(await readFile(path)),uid:1000,gid:1000,mode});
    if(stat.isDirectory())for(const child of (await readdir(path)).sort())await scan(join(path,child),name?name+'/'+child:child);}
  await scan(ROOT,'');return{schema:'soty.source-cold-witness.v1',synthetic:true,authenticationProved:false,models:false,productionReady:false,data,files};
}
try{if(process.platform!=='linux'||process.getuid()!==1000||process.argv.length!==3||!['seed','witness'].includes(process.argv[2]))throw Error('source_cold_usage');
  process.umask(0o077);if(process.argv[2]==='seed')await seed();console.log(JSON.stringify(await snapshot()));
}catch{console.log(JSON.stringify({passed:false,phase,code:'source_cold_fixture_failed',productionReady:false}));process.exitCode=1;}
