import type {AnyFieldDocument as FieldDocument} from '../../modules/field/contract.mjs';
import type {FieldDirectoryEntity,FieldScene} from './unified-field-layout.mjs';
export function createLiveAppScene():{update(document:FieldDocument,items:readonly FieldDirectoryEntity[],options?:{representedAppIds?:ReadonlySet<string>}):FieldScene;clear():void};
