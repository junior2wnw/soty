export type FieldEntityKind = 'app' | 'person' | 'community' | 'device' | 'builtin';
export interface FieldEntityRef { kind: FieldEntityKind; id: string }
export interface FieldContext { contextId: string; title: string; x: number; y: number }
export interface FieldShortcut { shortcutId: string; entity: FieldEntityRef; contextId: string; slot: [number, number] }
export interface FieldDocument { schema: 'soty.field.v1'; contexts: FieldContext[]; shortcuts: FieldShortcut[] }
export const FIELD_SCHEMA: 'soty.field.v1';
export const FIELD_ENTITY_KINDS: readonly FieldEntityKind[];
export const FIELD_LIMITS: Readonly<{contexts:24;shortcuts:256;bytes:131072;idLength:128;titleLength:64;coordinate:1000000;slotCoordinate:10000}>;
export class FieldContractError extends TypeError { readonly code: string; constructor(code?: string) }
export function createFieldDocument(): FieldDocument;
export function validateFieldEntity(value: unknown): FieldEntityRef;
export function fieldEntityKey(entity: FieldEntityRef): string;
export function validateFieldDocument(value: unknown): FieldDocument;
