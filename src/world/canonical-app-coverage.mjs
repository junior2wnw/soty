import {validateAnyFieldDocument} from '../../modules/field/contract.mjs';
import {ownerRepresentation} from '../../modules/canvas-objects/contract.mjs';

/** A read model of the accepted batch, never another Source/permission owner. */
export function deriveCanonicalAppCoverage({document,representations=new Map()}={}){
 const checked=validateAnyFieldDocument(document),appIds=new Set(),objects=new Set(),unknownObjectIds=[];
 for(const shortcut of checked.shortcuts){if(shortcut.entity.kind==='app')appIds.add(shortcut.entity.id);else if(shortcut.entity.kind==='object')objects.add(shortcut.entity.id);}
 for(const objectId of objects){
  const value=representations.get(objectId);
  if(value===undefined){unknownObjectIds.push(objectId);continue;}
  for(const ref of ownerRepresentation(value))if(ref.kind==='app')appIds.add(ref.id);
 }
 return {appIds,status:unknownObjectIds.length?'unknown':'ready',unknownObjectIds};
}
