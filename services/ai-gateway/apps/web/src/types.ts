export type ConnectionTab = 'chat' | 'api' | 'opencode';
export type Panel = 'connect' | 'key' | 'new-key' | 'topup' | 'models' | 'usage' | 'workspace' | 'install' | null;

export interface Model {
  id: string;
  name: string;
  version?: string;
  status: string;
  checkedAt?: string | null;
  contextWindow?: number | null;
  inputPriceMicrorublesPerMillion?: string | null;
  outputPriceMicrorublesPerMillion?: string | null;
  capabilities?: string[];
  description?: string;
}

export interface KeyLimits {
  dailyMicrorubles?: string | null;
  monthlyMicrorubles?: string | null;
  totalMicrorubles?: string | null;
  rpm?: number | null;
  tpm?: number | null;
  concurrency?: number | null;
}

export interface ApiKey {
  id: string;
  name: string;
  prefix: string;
  status: 'active' | 'paused' | 'revoked' | 'expired' | 'pending_activation';
  spentMicrorubles: string;
  createdAt?: string;
  lastUsedAt?: string | null;
  expiresAt?: string | null;
  limits: KeyLimits;
  allowedModels?: string[] | null;
  autoFallback?: boolean;
}

export interface DailyUsage {
  date: string;
  spentMicrorubles: string;
  tokens?: number | string;
  requests?: number;
  successfulRequests?: number;
}

export interface Usage {
  spentMicrorubles: string;
  tokens: number | string;
  requests: number;
  successfulRequests: number;
  successRate?: number | null;
  daily: DailyUsage[];
  p95Ms?: number | null;
  medianMs?: number | null;
}

export interface Wallet {
  availableMicrorubles: string;
  reservedMicrorubles: string;
  balanceMicrorubles: string;
  isDemo?: boolean;
}

export interface Dashboard {
  models: Model[];
  usage: Usage;
  keys: ApiKey[];
  wallet: Wallet | null;
  settings: { routingMode?: string; autoFallback?: boolean; baseUrl?: string };
  capabilities: {
    managementEnabled: boolean;
    chatEnabled: boolean;
    demoTopupEnabled: boolean;
    installerEnabled: boolean;
  };
  updatedAt?: string;
  temporaryWorkspace?: boolean;
  isDemo?: boolean;
}

export interface ConnectProfile {
  baseUrl: string;
  model: string;
  config?: unknown;
}

export interface ChatMessage {
  role: 'user' | 'assistant';
  content: string;
  actualModel?: string;
  status?: 'sending' | 'failed';
}
