import type {DirectoryApi} from './field-directory';
import type {CanvasVaultClient} from './canvas-vault-client.mjs';
import type {NativeCacheTransition} from './native-cache-transition.mjs';
import type {CanvasObjectSecurityActions} from './canvas-object-security';
export function createCanvasObjectSecurityActions(options:{api:DirectoryApi;accountId:string;objectId:string;isCurrent():boolean;vault:CanvasVaultClient;nativeTransition?:NativeCacheTransition}):CanvasObjectSecurityActions;
