// A second application uses the same host's personal store without knowing the
// current subject, writing SQL, implementing an issuer, or handling login.
const invalid=()=>{throw Object.assign(new Error('notes_input_invalid'),{code:'notes_input_invalid',status:400});};
export function createDomain(){return{
  personalDocuments:true,
  read(context,{input}){
    if(Object.keys(context).sort().join(',')!=='personal,resourceId')invalid();
    const {personal,resourceId}=context;
    if(!input||!['operation','cursor,operation'].includes(Object.keys(input).sort().join(','))||input.operation!=='notes.list')invalid();
    const page=personal.list('notes',input.cursor??null);
    return{storageScope:resourceId,items:page.items.map(({id,value})=>({id,text:value.text})),nextCursor:page.nextCursor};
  },
  execute(context,{requestId,input}){
    if(Object.keys(context).sort().join(',')!=='personal,resourceId')invalid();
    const {personal}=context;
    if(!input||Object.keys(input).sort().join(',')!=='operation,text'||input.operation!=='notes.add'
      ||typeof input.text!=='string'||!input.text.trim()||input.text.length>240)invalid();
    personal.create('notes',requestId,{text:input.text.trim()});
    return{result:{id:requestId},workspaceReceipt:{schema:'soty.workspace-receipt.v1',
      effects:[{kind:'created',resourceType:'note',resourceId:requestId,revision:1}],
      receipt:{verificationMethod:'domain_read',artifacts:[{type:'note',id:requestId,revision:1}]}}};
  },
};}
