export interface InvitationField { key: string; label: string; type: 'text' | 'multiline' | 'number' | 'email' | 'phone' | 'date' | 'select' | 'checkbox'; required: boolean; options?: string[]; min?: number; max?: number }
export const INVITATION_SCHEMA: 'soty.invitations.v1';
export const FIELD_TYPES: readonly InvitationField['type'][];
export const FIELD_PRESETS: readonly Omit<InvitationField, 'key'>[];
export function fields(value: unknown): InvitationField[];
export function answers(form: InvitationField[], value: unknown): Record<string, string | number | boolean>;
export function invitationUrl(origin: string, token: string): string;
export function parseInvitationRoute(hash: string): { token: string; route: string } | null;
