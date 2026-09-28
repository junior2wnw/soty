export type Axial = readonly [number, number];
export interface Point { x: number; y: number }
export interface HexMetrics { radius: number; gap: number; width: number; height: number; apothem: number; layoutRadius: number; stepX: number; stepY: number }
export interface HexCluster extends HexMetrics { cells: { axial: Axial; x: number; y: number; left: number; top: number }[]; loops: Point[][] }
export const SQRT3: number;
export const HEX_HEIGHT_FACTOR: number;
export const HEX_ASPECT_RATIO: number;
export const HEX_SAFE_WIDTH: number;
export const HEX_SAFE_HEIGHT: number;
export const HEX_CORNER_RATIO: number;
export const HEX_POLYGON: string;
export const HEX_DIRECTIONS: readonly Axial[];
export const HEX_FLOWER: readonly Axial[];
export function hexMetrics(radius: number, gap?: number): HexMetrics;
export function hexVertices(radius: number, centre?: Point): Point[];
export function roundedHexPath(radius: number, cornerRadius?: number, centre?: Point): string;
export function axialToPixel(axial: Axial, radius: number, gap?: number): Point;
export function pixelToAxial(point: Point, radius: number, gap?: number): [number, number];
export function axialDistance(axial: Axial, other?: Axial): number;
export function hexRing(distance: number): [number, number][];
export function hexSpiral(distance: number): [number, number][];
export function hexGrid(count: number, columns: number): [number, number][];
export function insetHex(radius: number, inset: number): HexMetrics & { block: number; inline: number };
export function hexSafeRect(radius: number, inset?: number): { width: number; height: number; left: number; top: number };
export function pointInHex(point: Point, radius: number, epsilon?: number): boolean;
export function hexBoundary(coordinates: readonly Axial[], radius: number): Point[][];
export function hexCluster(coordinates: readonly Axial[], radius: number, gap?: number, padding?: number): HexCluster;
export function hexPolygonPoints(radius: number, centre?: Point): string;
export function polygonPoints(points: readonly Point[]): string;
