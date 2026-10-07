export const GLM_MODEL: 'zai-org/GLM-5.3-Flash';
export type Json = null | boolean | number | string | readonly Json[] | { readonly [key: string]: Json };
export type Usage = Readonly<{ status: 'unknown' }> | Readonly<{
  status: 'reported'; promptTokens: number; completionTokens: number; totalTokens: number;
}>;
export type ModelMessage =
  | Readonly<{ role: 'system' | 'developer' | 'user'; content: string }>
  | Readonly<{ role: 'assistant'; content: string | null; tool_calls?: readonly {
    id: string; type: 'function'; function: { name: string; arguments: string };
  }[] }>
  | Readonly<{ role: 'tool'; content: string; tool_call_id: string }>;
export type ProviderPolicy = {
  requestBytes: number; streamBytes: number; eventBytes: number; textBytes: number; reasoningBytes: number;
  toolArgumentBytes: number; toolCalls: number; messages: number; messageBytes: number; jsonDepth: number;
  jsonNodes: number; events: number; maxOutputTokens: number; timeoutMs: number; concurrency: number; returnReasoning: boolean;
};
export type Tool = Readonly<{
  name: string; description: string; parameters: { readonly [key: string]: Json };
  validateArguments(argumentsValue: { readonly [key: string]: Json }): boolean;
}>;
export type ProviderRequest = Readonly<{
  requestId: string; requestDigest: string; body: Readonly<{
    model: typeof GLM_MODEL; messages: readonly ModelMessage[]; max_tokens: number;
    stream: true; stream_options: Readonly<{ include_usage: true }>;
    tool_choice?: 'auto'; tools?: readonly Readonly<{ type: 'function'; function: Readonly<{
      name: string; description: string; parameters: { readonly [key: string]: Json };
    }> }>[];
  }>; signal: AbortSignal;
}>;
export type ProviderResult = Readonly<{
  requestId: string; requestDigest: string; providerRequestId: string | null;
  model: typeof GLM_MODEL; completionId: string | null; finishReason: 'stop' | 'tool_calls';
  message: Readonly<{ role: 'assistant'; content: string; toolCalls: readonly Readonly<{
    id: string; name: string; arguments: { readonly [key: string]: Json };
  }>[] }>;
  diagnostics: Readonly<{ reasoningBytes: number; reasoning?: string }>; usage: Usage;
}>;
export type Accounting = Readonly<{
  requestId: string; requestDigest: string; dispatchAttempted: boolean; usage: Usage;
  completionId: string | null; providerRequestId: string | null; httpStatus: number | null;
}>;
export class ProviderError extends Error {
  readonly code: string; readonly accounting?: Accounting; constructor(code: string, accounting?: Accounting);
}
export function createGlmProvider(options: {
  transport(request: ProviderRequest): Response | Promise<Response>;
  policy?: Partial<ProviderPolicy>; tools?: readonly Tool[];
}): Readonly<{
  complete(input: { requestId: string; messages: readonly ModelMessage[]; signal?: AbortSignal;
    onText?: (text: string) => void }): Promise<ProviderResult>;
  close(): void;
}>;
export const HARD_LIMITS: Readonly<Omit<ProviderPolicy, 'returnReasoning'>>;
