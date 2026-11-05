import type {AnyFieldDocument,AnyFieldEntityRef} from '../../modules/field/contract.mjs';
export interface CanonicalAppCoverage {appIds:Set<string>;status:'ready'|'unknown';unknownObjectIds:string[]}
export function deriveCanonicalAppCoverage(input:{document:AnyFieldDocument;representations?:ReadonlyMap<string,readonly AnyFieldEntityRef[]|undefined>}):CanonicalAppCoverage;
