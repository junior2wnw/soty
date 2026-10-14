import { createSelectedNativeSourceApplication } from './selected-native-source.js';
import { createCurrentContextSourceApplication } from './current-context-source.js';
import { STANDARD_ROOT_WORKSPACE_SOURCE } from '../modules/source-app/server/standard-profile.mjs';
import { canonicalHash, AccessError } from '../modules/capabilities/server/validation.mjs';

/** Same owned app/profile/Root reference and Native READ transport as the other
 * installed apps. It grants no device, app or Source permission by itself. */
export function createSelectedTransitSourceApplications(scope) {
  if (scope.resource?.selection?.kind !== 'soty.workspace.v1'
    || canonicalHash(scope.sourceProfile) !== canonicalHash(STANDARD_ROOT_WORKSPACE_SOURCE)) throw new AccessError('transit_source_denied');
  const resource = `workspace:${scope.appId}:${scope.resource.resourceId}`;
  const row = createSelectedNativeSourceApplication({ scope, catalog: {
    capabilityId:`app.${scope.appId}:transit.native.status`,version:1,appId:scope.appId,
    title:'Состояние Транзита на моём устройстве',description:'Only the authenticated own-device Native status reader.',visibility:'private',executionEnabled:true,
    inputSchema:{type:'object',additionalProperties:false,required:['operation'],properties:{operation:{type:'string',maxLength:40,enum:['transit.native.status']}}},
    outputSchema:{type:'object',additionalProperties:false,required:['name','version','running','nativeState','binaryPinned','tunReady'],properties:{
      name:{type:'string',maxLength:16,enum:['TRANSIT']},version:{type:'string',maxLength:40},running:{type:'boolean'},
      nativeState:{type:'string',maxLength:32,enum:['active','active-unverified','off']},binaryPinned:{type:'boolean'},tunReady:{type:'boolean'}}},
    effects:[],resources:[resource],recipients:[resource]
  },guidance:[{kind:'skill',language:'ru',title:'Читать состояние собственного Транзита',summary:'Only Native own-device status; no VPN or device changes.',
    content:'Read transit.native.status through this exact granted Source reference. It reports the Native reader only. It never starts Transit, changes TUN, checks public IP, claims a device or runs a command. Unavailable is an error, never a fabricated OFF.'}] });
  return Object.freeze([createCurrentContextSourceApplication(scope),row]);
}
