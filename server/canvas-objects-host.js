import path from 'node:path';
import {createCanvasObjectsService} from '../modules/canvas-objects/server.mjs';
import {CanvasObjectError,exact,identity} from '../modules/canvas-objects/contract.mjs';
import {createCanvasWidgetProvider} from '../modules/canvas-widgets/server.mjs';

const denied=()=>{throw new CanvasObjectError('canvas_provider_access_denied',403);};
const missing=new Set(['notes_note_not_found','materials_material_not_found','notes_canvas_authority_required','materials_owner_required']);
const sourceScope=request=>({actor:request.actor,ownerId:request.objectOwnerId??request.actor?.accountId,mode:request.mode,...(request.purpose?{purpose:request.purpose}:{})});

/** Only a live Object authority context can reach these native source ports. */
export function readCanvasSourceContent({notes,materials,vault},context,args){
  if(context.providerId==='soty.notes'&&args.part==='note'&&args.index===undefined)return notes().canvas.readNote({context});
  if(context.providerId==='soty.materials'){
    if(args.part==='metadata'&&args.index===undefined)return materials().native.readMetadata({context});
    if(args.part==='chunk'&&Number.isSafeInteger(args.index)&&args.index>=0)return materials().native.readChunk({context,index:args.index});
  }
  if(['soty.vault','soty.notes','soty.materials'].includes(context.providerId)&&args.part==='vault'&&vault&&(args.index===undefined||Number.isSafeInteger(args.index)&&args.index>=0)){
    const {bytes,...value}=vault().readContent({context,index:args.index??0});return {...value,bytesBase64:Buffer.from(bytes).toString('base64url')};
  }
  throw new CanvasObjectError('canvas_content_part_invalid');
}

/** Every provider stays in its own storage. Canvas grants delegate one exact
 * source to the current viewer; they never manufacture an owner login. */
export function createCanvasObjectsHost({dataDir,connect,world,notes,materials,widgets,vault,onChange,allowFormat2Migration=false}){
  let service;
  const providers=[
    {providerId:'soty.notes',ownerRepresents:[],
      withBindingAuthority(request,callback){try{return notes().canvas.withSourceAuthority({...sourceScope(request),noteId:request.ref},callback);}catch(error){if(missing.has(error?.code))return denied();throw error;}},
      project(context){const {note}=notes().canvas.readMetadata({context});return {schema:'soty.canvas.note.v1',kind:'note',providerId:'soty.notes',title:note.title||'Записка',note,
        canEdit:context.viewer.kind==='account'&&context.viewer.accountId===context.ownerId};}},
    {providerId:'soty.materials',ownerRepresents:[],
      withBindingAuthority(request,callback){try{return materials().native.withSourceAuthority({...sourceScope(request),materialId:request.ref},callback);}catch(error){if(missing.has(error?.code))return denied();throw error;}},
      project(context){const {material}=materials().native.readMetadata({context});return {schema:'soty.canvas.material.v1',kind:material.kind,providerId:'soty.materials',title:material.name,material,
        canEdit:context.viewer.kind==='account'&&context.viewer.accountId===context.ownerId};}},
  ];
  if(widgets)providers.push(createCanvasWidgetProvider({widgets:widgets(),withObjectAuthority:(context,callback)=>service.withObjectAuthority(context,callback)}));
  service=createCanvasObjectsService({databasePath:path.join(dataDir,'canvas','objects.sqlite'),projectId:'soty',providers,allowFormat2Migration,
    ...(vault?{withProtectionAuthority:(request,callback)=>vault().withProtectionAuthority(request,callback)}:{}),
    actorActive:actor=>connect()?.isActorActive(actor)===true,withActorAuthorityFence:(actor,callback)=>connect().withActorAuthorityFence(actor,callback),
    canAccessCommunity:(actor,id)=>world().canAccessCommunity(actor.accountId,id),
    canShareCommunity:(actor,id)=>world().isGroupAdmin(actor.accountId,id),onChange});
  const content=({actor,args})=>{
    exact(args,['expectedAccountId','objectId','part','index'],['expectedAccountId','objectId','part']);
    if(args.expectedAccountId!==actor?.accountId)throw new CanvasObjectError('canvas_account_changed',409);
    identity(args.objectId);
    return service.withObjectRead({actor,objectId:args.objectId},context=>readCanvasSourceContent({notes,materials,vault},context,args));
  };
  const extension=Object.freeze({operations:new Set([...service.operations,'canvas.objects.content']),maxResultBytes:2*1024*1024,
    execute(request){return request.op==='canvas.objects.content'?content(request):service.execute(request);}});
  return Object.freeze({...service,extension});
}
