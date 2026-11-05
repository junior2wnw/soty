import type { AnyFieldDocument as FieldDocument } from '../../modules/field/contract.mjs';
import type { FieldScene, UnifiedFieldLayout } from './unified-field-layout.mjs';
export function validateLiveFieldScene(value:FieldScene,personal:FieldDocument):FieldScene;
export function combineFieldScene(personal:FieldScene,live:FieldScene):FieldScene;
export function combineFieldLayout(personal:UnifiedFieldLayout,live:UnifiedFieldLayout):UnifiedFieldLayout;
