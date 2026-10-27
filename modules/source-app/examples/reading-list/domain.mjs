// Only application data. The shared host supplies storage for the current
// person and owns Root authentication, sessions, ACL fences and receipts.
const invalid=()=>{throw Object.assign(new Error('reading_input_invalid'),{code:'reading_input_invalid',status:400});};
export function createDomain(){return{
  personalDocuments:true,
  read({personal,resourceId},{input}){
    if(!input||Object.keys(input).join(',')!=='operation'||input.operation!=='reading.list')invalid();
    // A UI-storage namespace only, obtained AFTER authorization. It is never
    // accepted back as a principal, resource selector or permission.
    return{storageScope:resourceId,items:personal.list('reading-items').map(({id,value})=>({id,title:value.title}))};
  },
  execute({personal},{requestId,input}){
    if(!input||Object.keys(input).sort().join(',')!=='operation,title'||input.operation!=='reading.add'
      ||typeof input.title!=='string'||!input.title.trim()||input.title.length>240)invalid();
    personal.create('reading-items',requestId,{title:input.title.trim()});
    return{result:{id:requestId},workspaceReceipt:{schema:'soty.workspace-receipt.v1',
      effects:[{kind:'created',resourceType:'reading-item',resourceId:requestId,revision:1}],
      receipt:{verificationMethod:'domain_read',artifacts:[{type:'reading-item',id:requestId,revision:1}]}}};
  },
};}
