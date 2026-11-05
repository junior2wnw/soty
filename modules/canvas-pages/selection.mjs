import {check,exact,identity,binding} from '../canvas-objects/contract.mjs';
export const PAGE_SELECTION_SCHEMA='soty.canvas-page-selection.v1';
export const PAGE_SELECTION_RECORD_SCHEMA='soty.canvas-page-selection-record.v1';
export const PAGE_SELECTION_LIMIT=256;
const revision=value=>check(Number.isSafeInteger(value)&&value>=1,'canvas_page_selection_invalid');
export function normalizePageSelection(value){
 exact(value,['schema','fieldRevision','items']);check(value.schema===PAGE_SELECTION_SCHEMA&&Number.isSafeInteger(value.fieldRevision)&&value.fieldRevision>=0&&Array.isArray(value.items)&&value.items.length<=PAGE_SELECTION_LIMIT,'canvas_page_selection_invalid');const seen=new Set();
 return {schema:PAGE_SELECTION_SCHEMA,fieldRevision:value.fieldRevision,items:value.items.map(item=>{exact(item,['shortcutId','entity','expectedAccessRevision'],['shortcutId','entity']);identity(item.shortcutId);check(!seen.has(item.shortcutId),'canvas_page_selection_duplicate');seen.add(item.shortcutId);exact(item.entity,['kind','id']);check(['object','app'].includes(item.entity.kind),'canvas_page_selection_invalid');identity(item.entity.id);if(item.expectedAccessRevision!==undefined)revision(item.expectedAccessRevision);return {shortcutId:item.shortcutId,entity:{kind:item.entity.kind,id:item.entity.id},...(item.expectedAccessRevision===undefined?{}:{expectedAccessRevision:item.expectedAccessRevision})};})};
}
export function readPageSelection(row){
 if(row.selection_json===undefined||row.selection_json===null)return null;let value;try{value=JSON.parse(row.selection_json);}catch{check(false,'canvas_page_storage_invalid',503);}
 exact(value,['schema','fieldRevision','items']);check(value.schema===PAGE_SELECTION_RECORD_SCHEMA&&Number.isSafeInteger(value.fieldRevision)&&value.fieldRevision>=0&&Array.isArray(value.items)&&value.items.length<=PAGE_SELECTION_LIMIT,'canvas_page_storage_invalid',503);const seen=new Set();
 for(const item of value.items){exact(item,['shortcutId','entity','accessRevision','binding'],['shortcutId','entity']);identity(item.shortcutId);check(!seen.has(item.shortcutId),'canvas_page_storage_invalid',503);seen.add(item.shortcutId);exact(item.entity,['kind','id']);check(['object','app'].includes(item.entity.kind),'canvas_page_storage_invalid',503);identity(item.entity.id);
  if(item.entity.kind==='object'){revision(item.accessRevision);binding(item.binding);}else check(item.accessRevision===undefined&&item.binding===undefined,'canvas_page_storage_invalid',503);
 }return value;
}
export function pageSelectionDto(row){const selected=readPageSelection(row);return selected?{schema:PAGE_SELECTION_SCHEMA,fieldRevision:selected.fieldRevision,items:selected.items.map(item=>({shortcutId:item.shortcutId,entity:{...item.entity},...(item.entity.kind==='object'?{expectedAccessRevision:item.accessRevision}:{})}))}:undefined;}
