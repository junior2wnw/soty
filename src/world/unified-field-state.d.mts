import type { FieldDocument, FieldEntityRef, FieldEntityKind } from '../../modules/field/contract.mjs';
export type FieldCommand = {expected?:FieldDocument} & (
  {type:'add-context';contextId:string;title:string;x:number;y:number} |
  {type:'rename-context';contextId:string;title:string} |
  {type:'move-context';contextId:string;x:number;y:number} |
  {type:'remove-context';contextId:string;removeShortcuts?:boolean} |
  {type:'add-shortcut';shortcutId:string;entity:FieldEntityRef;contextId:string;slot?:[number,number]} |
  {type:'remove-shortcut';shortcutId:string} |
  {type:'move-shortcut';shortcutId:string;contextId:string;slot:[number,number];swap?:boolean} |
  {type:'restore';document:FieldDocument});
export interface FieldChange {before:FieldDocument;document:FieldDocument;changed:boolean;affected:string[];type:FieldCommand['type']}
export interface FieldHistory {limit:number;entries:{before:FieldDocument;after:FieldDocument}[]}
export function nextFieldSlot(document:FieldDocument,contextId:string,kind?:FieldEntityKind):[number,number];
export function applyFieldCommand(document:FieldDocument,command:FieldCommand):FieldChange;
export function createFieldHistory(limit?:number):FieldHistory;
export function recordFieldHistory(history:FieldHistory,change:FieldChange):void;
export function undoFieldHistory(history:FieldHistory,current:FieldDocument):FieldChange|null;
export {createFieldDocument} from '../../modules/field/contract.mjs';
