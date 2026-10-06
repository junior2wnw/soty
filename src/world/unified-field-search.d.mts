import type { FieldDocument } from '../../modules/field/contract.mjs';
import type { FieldDirectoryEntity } from './unified-field-layout.mjs';
export function createFieldSearchPlacement(options?: {id?:()=>string}): {
 clear():void;
 contextEntity(contextId:string):FieldDirectoryEntity|null;
 update(scope:string,items:readonly FieldDirectoryEntity[],metadata:readonly FieldDirectoryEntity[]):FieldDocument;
};
