export interface VoiceOrbView {
  phase: 'checking' | 'disabled' | 'idle' | 'connecting' | 'listening' | 'working' | 'ending' | 'unknown';
  configured: boolean;
  missingBinding: string | null;
  microphone: boolean;
  muted: boolean;
  outputMuted: boolean;
  playbackBlocked: boolean;
  input: string;
  output: string;
  answer: string;
  code: string | null;
  active: boolean;
  hangupAccepted: boolean;
  remoteTerminationVerified: boolean;
  accountingFinalized: false;
  playbackVerified: false;
}
export interface VoiceOrbHandle {
  view(): VoiceOrbView;
  subscribe(listener: (state: VoiceOrbView) => void): () => void;
  refresh(): Promise<VoiceOrbView>;
  start(): Promise<void>;
  mute(): void;
  interrupt(): Promise<void>;
  resumePlayback(): Promise<void>;
  check(): Promise<void>;
  end(): Promise<unknown>;
  dispose(): void;
}
export function createVoiceOrbClient(options: {
  client: {
    voiceSessionCleanup?(reply: unknown): (() => Promise<unknown>) | null;
    getLocalState(): Promise<{ accountId?: string | null; deviceId?: string | null; localRevision?: number | null }>;
    extension<T = Record<string, unknown>>(op: string, args: Record<string, unknown>, context: { expectedAccountId: string }): Promise<T>;
  };
  accountId: string;
  audio: HTMLAudioElement;
  acquireCaptureLease(): (() => void) | null;
  readSelection?(): { appId: string; handle: string } | null;
  takeNotePermission?(): boolean;
  isCurrent?(): boolean;
  onChange?(state: VoiceOrbView): void;
  getUserMedia?(constraints: MediaStreamConstraints): Promise<MediaStream>;
  makePeer?(): RTCPeerConnection;
  makeStream?(tracks: MediaStreamTrack[]): MediaStream;
  idFactory?(): string;
  events?: Pick<Window, 'addEventListener' | 'removeEventListener'>;
}): VoiceOrbHandle;
