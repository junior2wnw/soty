export interface FieldPosition { x: number; y: number; width: number; height: number }
export interface FieldLayoutState {
  key: string; placements: Map<string, FieldPosition>;
  batch: { type: 'community' | 'person'; top: number; count: number } | null; bottom: number;
}
export interface FieldLayoutMetrics {
  padding: number; gap: number;
  community: { left: number; columns: number; pitch: number; width: number; height: number };
  person: { left: number; columns: number; pitch: number; width: number; height: number };
}
export function createFieldLayoutState(): FieldLayoutState;
export function stableFieldLayout(state: FieldLayoutState, entities: { id: string; type: 'community' | 'person' }[], metrics: FieldLayoutMetrics): (FieldPosition & { id: string })[];
