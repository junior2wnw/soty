import type { NativeVoiceMount } from '../world/native-voice-carrier.mjs';
export function validateNativeVoiceDescriptor(value: unknown): NativeVoiceMount | null;
export function createNativeVoiceDescriptor(options?: {
  fetch?: typeof globalThis.fetch;
  signal?: AbortSignal;
}): {
  snapshot(): Readonly<{ state: 'pending' | 'ready' | 'unavailable' | 'closed' }>;
  read(options?: { retry?: boolean }): Promise<NativeVoiceMount | null>;
  close(): void;
};
