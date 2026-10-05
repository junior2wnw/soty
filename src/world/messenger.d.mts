import type { WorldMessage } from './types';
export function chatDayKey(value: number | string | Date): string;
export function chatDayLabel(value: number | string | Date, now?: number | string | Date): string;
export function chatListTime(value: number | string | Date, now?: number | string | Date): string;
export function canGroupMessages(previous: WorldMessage | undefined, current: WorldMessage): boolean;
export function shouldSendOnEnter(event: Pick<KeyboardEvent, 'key' | 'shiftKey' | 'altKey' | 'ctrlKey' | 'metaKey' | 'isComposing' | 'keyCode'>): boolean;
export function chatPreview(message: WorldMessage | undefined): string;
