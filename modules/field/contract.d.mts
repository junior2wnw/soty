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
export function fieldEntityKey(entity: AnyFieldEntityRef): string;
export function validateFieldDocument(value: unknown): FieldDocument;
export type FieldPlacement = {kind:'hex';slot:[number,number]} | {kind:'free';x:number;y:number;width:number;height:number};
export type AnyFieldEntityKind = FieldEntityKind | 'object';
export interface AnyFieldEntityRef {kind:AnyFieldEntityKind;id:string}
export interface FieldShortcutV2 { shortcutId:string; entity:AnyFieldEntityRef; contextId:string; placement:FieldPlacement }
export interface FieldDocumentV2 { schema:'soty.field.v2'; contexts:FieldContext[]; shortcuts:FieldShortcutV2[] }
export type AnyFieldShortcut = FieldShortcut | FieldShortcutV2;
export type AnyFieldDocument = FieldDocument | FieldDocumentV2;
export const FIELD_SCHEMA_V2:'soty.field.v2';
export const FIELD_ENTITY_KINDS_V2:readonly AnyFieldEntityKind[];
export function validateAnyFieldEntity(value:unknown):AnyFieldEntityRef;
export const FIELD_V2_LIMITS:Readonly<{minExtent:16;maxExtent:8192}>;
export function createFieldDocumentV2():FieldDocumentV2;
export function validateFieldPlacement(value:unknown):FieldPlacement;
export function validateFieldDocumentV2(value:unknown):FieldDocumentV2;
export function validateAnyFieldDocument(value:unknown):AnyFieldDocument;
export function convertFieldDocumentToV2(value:unknown):FieldDocumentV2;
export function fieldShortcutPlacement(shortcut:AnyFieldShortcut):FieldPlacement;
