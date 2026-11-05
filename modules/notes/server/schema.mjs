export const SCHEMA_VERSION=3;
export const SUPPORTED_SCHEMA_VERSIONS=Object.freeze([1,2,3]);
export {inspectNotesSchema,migrateNotes} from './schema-vault.mjs';
