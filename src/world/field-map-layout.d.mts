import type { WorldEntity, WorldAppRecord } from './types';
import type { Axial, Point } from '../geometry/hex.mjs';
export const FIELD_SCALE_MIN: number;
export const FIELD_SCALE_MAX: number;
export interface FieldShape { width: number; height: number; radius: number; tileWidth: number; tileHeight: number; titleHeight: number; contour: string; cells: { axial: Axial; x: number; y: number; left: number; top: number }[] }
interface FieldBounds { id: string; x: number; y: number; width: number; height: number }
export type FieldItem = (FieldBounds & { kind: 'community'; entity: Extract<WorldEntity, { type: 'community' }>; shape: FieldShape; hasApps: boolean; appCount: number })
  | (FieldBounds & { kind: 'person'; entity: Extract<WorldEntity, { type: 'person' }> })
  | (FieldBounds & { kind: 'app'; app: WorldAppRecord; radius: number; communityId?: string; axial?: Axial })
  | (FieldBounds & { kind: 'overflow'; entity: Extract<WorldEntity, { type: 'community' }>; count: number; radius: number });
export interface FieldConnection { kind: 'app' | 'person'; communityId: string; from: Point; to: Point }
export interface FieldLayout { width: number; height: number; items: FieldItem[]; connections: FieldConnection[]; compact: boolean }
export function clampFieldScale(value: number): number;
export function fieldEntityId(entity: WorldEntity): string;
export function softContourPath(points: Point[], padding?: number): string;
export function communityFieldGeometry(tileCount?: number, radius?: number): FieldShape;
export function layoutField(entities: WorldEntity[], allowedApps?: WorldAppRecord[], viewportWidth?: number, viewportHeight?: number): FieldLayout;
export function fieldItemVisible(item: FieldItem, bounds: { left: number; right: number; top: number; bottom: number }, overscan?: number): boolean;
export function fieldNeighbour(items: FieldItem[], currentId: string, direction: readonly [number, number]): FieldItem | undefined;
