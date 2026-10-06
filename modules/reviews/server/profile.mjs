// The provider wire is Povédai public API 1.1. These are declaration bounds,
// not a guarantee that a provider or a subject is available at request time.
export const REVIEWS_OPERATIONS = Object.freeze(['apps.reviews.context']);
export const REVIEWS_LIMITS = Object.freeze({ providerPins: 128, subjectBindings: 128, subjectsPerApp: 3 });
export const PUBLIC_REVIEW_ENTITY_TYPES = Object.freeze([
  'organization', 'project', 'building', 'complex', 'builder', 'product', 'service',
  'place', 'event', 'course', 'article', 'profile', 'page',
]);
export const LOCAL_REVIEW_SUBJECT_TYPES = Object.freeze({
  app: Object.freeze(['product', 'service']),
  project: Object.freeze(['project']),
  person: Object.freeze(['profile']),
});
export const EMPTY_REVIEWS_CONFIGURATION = Object.freeze({ providers: Object.freeze([]), bindings: Object.freeze([]) });
