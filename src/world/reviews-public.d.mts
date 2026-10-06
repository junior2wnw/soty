export interface ReviewReference { id: string; version: number; digest: string; }
export interface ReviewBinding { subjectKind: 'app' | 'project' | 'person'; providerRef: ReviewReference; subjectRef: ReviewReference; providerSubjectId: string; providerEntityType: 'product' | 'service' | 'project' | 'profile'; api: { subject: string; rating: string; reviews: string }; publicPageUrl: string; origin: string; }
export interface PublicReviewSubject { id: string; entityType: string; title: string; description?: string; counts: { publishedItems: number; publishedReviews: number; discussionItems: number }; rating: { average: number | null; count: number; distributionCount: number; distributionComplete: boolean; updatedAt: string | null; provenance: { kind: 'native' | 'imported'; source: string } }; }
export interface PublicReviewItem { id: string; type: 'review'; title?: string; body: string; pros?: string; cons?: string; rating?: number; author: { name: string; verified: boolean }; createdAt: string; imported: boolean; }
export interface PublicReviewPage { items: readonly PublicReviewItem[]; total: number; nextCursor: string | null; }
export function reviewSubjectKey(binding: ReviewBinding): string;
export function parseReviewsContext(input: unknown, options?: { allowFixtureOrigins?: boolean }): { mode: 'disabled'; subjects: readonly [] } | { mode: 'public-read'; availability: 'unprobed'; subjects: readonly ReviewBinding[] };
export function parsePublicReviewSubject(value: unknown, binding: ReviewBinding): PublicReviewSubject;
export function parsePublicReviewPage(value: unknown, binding: ReviewBinding): PublicReviewPage;
export function fetchPublicReviewJson(binding: ReviewBinding, endpoint: 'subject', options?: { signal?: AbortSignal; fetch?: typeof globalThis.fetch }): Promise<PublicReviewSubject>;
export function fetchPublicReviewJson(binding: ReviewBinding, endpoint: 'reviews', options?: { cursor?: string; signal?: AbortSignal; fetch?: typeof globalThis.fetch }): Promise<PublicReviewPage>;
