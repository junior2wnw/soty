export type NativeVoiceMount = Readonly<{
  scope: "kvartalufa.voice";
  appId: string;
  frameOrigin: string;
  nativeOrigin: string;
  parentOrigin: string;
  source: Readonly<{ id: string; version: number; digest: string }>;
}>;
export type NativeVoiceRootClient = {
  getLocalState(): Promise<{
    accountId?: string | null;
    deviceId?: string | null;
    current?: { revoked?: boolean };
  }>;
  extension(operation: "voice.agent.turn" | "voice.agent.cancel", args: Record<string, string>,
    context: { expectedAccountId: string }): Promise<unknown>;
};
export function mountRootVoiceCarrier(options: {
  view: Window;
  getFrame(): HTMLIFrameElement | null;
  isCurrent(): boolean;
  client: NativeVoiceRootClient;
  mount: NativeVoiceMount;
  appId: string;
  accountId: string;
}): () => void;
