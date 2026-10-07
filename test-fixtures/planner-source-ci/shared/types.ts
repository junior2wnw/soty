export type Role = 'owner' | 'editor' | 'approver' | 'viewer';
export type EntityStatus = 'draft' | 'planned' | 'active' | 'done' | 'cancelled';
export type EntityKind = 'point' | 'period' | 'process' | 'note' | 'metric';
export type FieldValue = string | number | boolean | null;
/** Exact coordinates are JSON-safe integer nanoseconds from the Unix epoch. */
export interface PreciseTimeRange {
  scale: 'unix-nanoseconds';
  start: string | null;
  end: string | null;
  earliest?: string | null;
  latest?: string | null;
  /** Measurement resolution; zooming never implies greater source precision. */
  resolutionNs?: string;
}
export interface TimeRange {
  start: string | null;
  end: string | null;
  timezone: string;
  precision: 'exact' | 'day' | 'month' | 'approximate' | 'unknown';
  earliest?: string | null;
  latest?: string | null;
  /** Authoritative coordinates; ISO fields remain compatible calendar mirrors. */
  precise?: PreciseTimeRange;
}
export interface User {
  id: string;
  displayName: string;
  email?: string;
  local?: boolean;
  pending?: boolean;
}
export interface Membership {
  workspaceId: string;
  userId: string;
  role: Role;
}
export interface Workspace {
  id: string;
  name: string;
  mode: 'personal' | 'team';
  timezone: string;
  createdAt: string;
  description?: string;
}
export interface FieldDefinition {
  id: string;
  label: string;
  type: 'text' | 'number' | 'boolean' | 'date' | 'url' | 'select';
  required?: boolean;
  options?: string[];
}
export interface TypeDefinition {
  id: string;
  workspaceId?: string;
  label: string;
  icon: string;
  color: string;
  kind: EntityKind;
  fields: FieldDefinition[];
  builtin?: boolean;
}
export interface ContentLink {
  id: string;
  label: string;
  url: string;
  kind: 'url' | 'file';
  fileId?: string;
}
export interface Allocation {
  resourceId: string;
  amount: number;
}
export interface Recurrence {
  frequency: 'day' | 'week' | 'month';
  interval: number;
  weekdays?: number[];
  count?: number;
  until?: string;
  exceptions: string[];
  calendarPolicy?: 'adjust' | 'skip-invalid';
  durationPolicy?: 'calendar' | 'elapsed';
}
export interface Source {
  kind: 'manual' | 'sample' | 'import' | 'webhook';
  label: string;
  externalId?: string;
  observedAt: string;
  receivedAt: string;
  staleAfterMinutes?: number;
}
export interface Entity {
  id: string;
  workspaceId: string;
  typeId: string;
  kind: EntityKind;
  title: string;
  description: string;
  parentId: string | null;
  ownerId: string | null;
  participantIds: string[];
  status: EntityStatus;
  plan: TimeRange;
  baseline: TimeRange;
  actual: TimeRange | null;
  forecast: TimeRange | null;
  forecastProvenance?: 'manual' | 'derived';
  dueAt: string | null;
  tags: string[];
  fields: Record<string, FieldValue>;
  links: ContentLink[];
  allocations: Allocation[];
  recurrence: Recurrence | null;
  source: Source;
  createdAt: string;
  updatedAt: string;
  version: number;
}
export type EntityDraft = Omit<
  Entity,
  'id' | 'baseline' | 'createdAt' | 'updatedAt' | 'version' | 'source'
> & { source?: Source; baseline?: TimeRange };
export interface Dependency {
  id: string;
  workspaceId: string;
  fromId: string;
  toId: string;
  kind: 'finish-start' | 'start-start' | 'finish-finish' | 'related';
  lagMinutes: number;
}
export interface Resource {
  id: string;
  workspaceId: string;
  name: string;
  kind: 'person' | 'equipment' | 'place' | 'budget' | 'other';
  capacity: number;
  unit: string;
  timezone: string;
  workingWeekdays: number[];
}
export interface TemplateItem {
  key: string;
  parentKey?: string;
  title: string;
  typeId: string;
  kind: EntityKind;
  offsetDays: number;
  durationDays: number;
  fields?: Record<string, FieldValue>;
  description?: string;
  schedule?: TimeRange;
  allocations?: Entity['allocations'];
  recurrence?: Recurrence | null;
  dueAt?: string | null;
  tags?: string[];
}
export interface Template {
  id: string;
  workspaceId?: string;
  name: string;
  description: string;
  anchorDate?: string;
  anchorNs?: string;
  timezone?: string;
  items: TemplateItem[];
  dependencies: { fromKey: string; toKey: string; kind: Dependency['kind']; lagMinutes: number }[];
}
export interface Rule {
  id: string;
  workspaceId: string;
  name: string;
  enabled: boolean;
  trigger: 'before-start' | 'before-due' | 'after-done' | 'overdue' | 'resource-conflict';
  typeId?: string;
  leadMinutes: number;
  action: 'signal' | 'create-followup';
  followupTitle?: string;
  ownerId?: string;
}
export type SignalKind =
  | 'overdue'
  | 'forecast-risk'
  | 'resource-conflict'
  | 'approval'
  | 'missing-date'
  | 'stale-source'
  | 'reminder'
  | 'dependency';
export interface Signal {
  id: string;
  workspaceId: string;
  entityId: string;
  kind: SignalKind;
  severity: 'info' | 'warning' | 'critical';
  title: string;
  description: string;
  affectedIds: string[];
  responsibleUserId: string | null;
  dueAt: string | null;
  state: 'open' | 'acknowledged' | 'resolved' | 'accepted-risk';
  createdAt: string;
  updatedAt: string;
  acknowledgedBy?: string;
  resolvedAt?: string;
  dedupeKey: string;
}
export interface Notification {
  id: string;
  workspaceId: string;
  signalId: string;
  userId: string;
  channel: 'in-app' | 'browser' | 'webhook';
  state: 'pending' | 'delivered' | 'failed' | 'snoozed';
  scheduledAt: string;
  deliveredAt?: string;
  attempts: number;
  summary: string;
  snoozedUntil?: string;
}
export interface Comment {
  id: string;
  workspaceId: string;
  entityId: string;
  userId: string;
  text: string;
  createdAt: string;
}
export interface AuditEntry {
  id: string;
  workspaceId: string;
  entityId: string | null;
  userId: string;
  action: string;
  reason: string;
  createdAt: string;
  before: unknown;
  after: unknown;
}
export interface PlanChange {
  entityId: string;
  plan: TimeRange;
}
export interface Conflict {
  kind: 'cycle' | 'resource' | 'deadline' | 'dependency' | 'invalid-time';
  entityIds: string[];
  message: string;
  resourceId?: string;
}
export interface ScenarioPreview {
  changes: PlanChange[];
  conflicts: Conflict[];
  affectedIds: string[];
  explanations: string[];
}
export interface Scenario {
  id: string;
  workspaceId: string;
  name: string;
  reason: string;
  requestedBy: string;
  createdAt: string;
  state: 'draft' | 'pending' | 'approved' | 'rejected' | 'stale';
  baseVersions: Record<string, number>;
  changes: PlanChange[];
  preview: ScenarioPreview;
  decidedBy?: string;
  decidedAt?: string;
}
export interface NotificationPreferences {
  quietStart: string;
  quietEnd: string;
  timezone: string;
  browserEnabled: boolean;
  repeatMinutes: number;
  escalationMinutes: number;
}
export interface PlannerSettings {
  notifications: NotificationPreferences;
  lastSeenAt: string | null;
}
export interface PlannerSnapshot {
  user: User;
  users: User[];
  memberships: Membership[];
  workspaces: Workspace[];
  types: TypeDefinition[];
  entities: Entity[];
  dependencies: Dependency[];
  resources: Resource[];
  templates: Template[];
  rules: Rule[];
  signals: Signal[];
  notifications: Notification[];
  comments: Comment[];
  scenarios: Scenario[];
  audit: AuditEntry[];
  settings: PlannerSettings;
  serverTime: string;
  revision: number;
}
export interface Occurrence {
  id: string;
  entityId: string;
  start: string | null;
  end: string | null;
  index: number;
  precise?: PreciseTimeRange;
}
export interface AssistantSuggestion {
  summary: string;
  drafts: EntityDraft[];
  evidenceIds: string[];
  assumptions: string[];
  provider: 'local';
  preview?: ScenarioPreview;
}
