import type {MaterialKind} from '../../modules/materials/browser/client.mjs';
export function canvasWebmKind(bytes:Uint8Array):'audio'|'video';
export function normalizeCanvasWebmFile(blob:Blob,kind?:MaterialKind):Promise<{blob:Blob;kind:MaterialKind|undefined}>;
