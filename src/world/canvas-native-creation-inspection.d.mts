import type {CanvasCreateRecord} from './canvas-create-flow.mjs';
import type {NativeSourceIdentity,NativeCreationInspection} from './native-cache-transition.mjs';
export function nativeCreationSource(record:CanvasCreateRecord):Pick<NativeSourceIdentity,'providerId'|'ref'>|null;
export function inspectNativeCreationRecords(records:CanvasCreateRecord[],source:Pick<NativeSourceIdentity,'providerId'|'ref'>):NativeCreationInspection[];
