// Only application data. The shared SQLite host supplies the current personal
// principal and owns Root authentication, sessions, ACL fences and receipts.
const invalid=()=>{throw Object.assign(new Error('reading_input_invalid'),{code:'reading_input_invalid',status:400});};
export function createDomain(){return{
  initialize(database){database.exec('CREATE TABLE reading_items(person TEXT NOT NULL,id TEXT NOT NULL,title TEXT NOT NULL,PRIMARY KEY(person,id))');},
  inspect(database){if(database.prepare('PRAGMA table_info(reading_items)').all().map(row=>row.name).join(',')!=='person,id,title')invalid();},
  read({database,principalId,resourceId},{input}){
    if(!input||Object.keys(input).join(',')!=='operation'||input.operation!=='reading.list')invalid();
    // A UI-storage namespace only, obtained AFTER authorization. It is never
    // accepted back as a principal, resource selector or permission.
    return{storageScope:resourceId,items:database.prepare('SELECT id,title FROM reading_items WHERE person=? ORDER BY rowid DESC LIMIT 100').all(principalId)};
  },
  execute({database,principalId},{requestId,input}){
    if(!input||Object.keys(input).sort().join(',')!=='operation,title'||input.operation!=='reading.add'
      ||typeof input.title!=='string'||!input.title.trim()||input.title.length>240)invalid();
    database.prepare('INSERT INTO reading_items VALUES(?,?,?)').run(principalId,requestId,input.title.trim());
    return{result:{id:requestId},workspaceReceipt:{schema:'soty.workspace-receipt.v1',
      effects:[{kind:'created',resourceType:'reading-item',resourceId:requestId,revision:1}],
      receipt:{verificationMethod:'domain_read',artifacts:[{type:'reading-item',id:requestId,revision:1}]}}};
  },
};}
