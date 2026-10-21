export type CoverKind = 'app' | 'profile';
export interface CoverIdentity { kind: CoverKind; targetId: string; accountId: string }
export interface CoverHead { revision: number; mode: 'inherit' | 'none' | 'custom'; version: string | null; artVersion?: number }
export interface CoverScope { storeInstanceId: string; kind: CoverKind; targetId: string; ownerAccountId: string; targetRevision: number; policyEpoch: number; coverRevision: number }
export interface CoverCrop { x: number; y: number; zoom: number }
export interface CoverVersion { id: string; version: number; mode: 'inherit' | 'none' | 'custom'; createdAt: number; restorable: boolean }
export interface CoverImage { mime: string; width: number; height: number; bytes: Uint8Array }
export interface CoverRead { current: CoverHead; versionId: string | null; image: CoverImage | null }
export interface CoverStatus { current: CoverHead; scope: CoverScope; serverTime: number; receipt: Record<string, unknown> | null; draft: { id: string; state: string; expiresAt: number } | null }
export interface CoverMutation { current: CoverHead; receipt: Record<string, unknown>; replayed: boolean; draft?: { id: string; state: string; expiresAt: number } }
export interface CoverApi {
  readonly identity: Readonly<CoverIdentity>;
  call(method: 'read', args?: Record<string, unknown>): Promise<CoverRead>;
  call(method: 'status', args?: Record<string, unknown>): Promise<CoverStatus>;
  call(method: 'versions', args?: Record<string, unknown>): Promise<{ current: CoverHead; versions: CoverVersion[] }>;
  call(method: 'prepare' | 'prepareFromVersion' | 'commit' | 'restore' | 'remove' | 'cancel', args: Record<string, unknown>): Promise<CoverMutation>;
}
export function createCoverApi(options: { api: { request<T>(op: string, args?: Record<string, unknown>): Promise<T> }; kind: CoverKind; targetId: string; accountId: string; isCurrent(): boolean; digest?: (bytes: Uint8Array) => Promise<ArrayBuffer> }): CoverApi;
export const COVER_UPLOAD_LIMIT: number;
export function coverError(code: string): Error & { code: string };
export function coverId(value: unknown): value is string;
export function coverRequestId(value: unknown): value is string;
export function closedData(value: unknown, required?: string[], optional?: string[]): boolean;
export function coverHead(value: unknown): CoverHead;
export function coverScope(value: unknown, identity: CoverIdentity): CoverScope;
export function sameCoverScope(a: CoverScope | null, b: CoverScope | null): boolean;
export function expectedCover(scope: CoverScope): Record<string, number>;
export function bytesToBase64(bytes: Uint8Array): string;
export function base64Bytes(value: string, maximum?: number): Uint8Array;
