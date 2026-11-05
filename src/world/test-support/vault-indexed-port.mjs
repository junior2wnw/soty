import {indexedNativePort} from '../../../modules/notes/test/indexed-native-port.mjs';
// Observe the real Vault cipher store, not the unrelated creation journal DB.
export function vaultIndexedPort({gateFirstWrite=false}={}){
  const port=indexedNativePort();if(gateFirstWrite)port.hold('soty-canvas-vault-local-v1','cipher');
  return{indexedDB:port.indexedDB,get rows(){return new Map(port.rows('soty-canvas-vault-local-v1','cipher').map(row=>[row.key,row]));},get gated(){return Boolean(port.held);},release(){port.release();}};
}
