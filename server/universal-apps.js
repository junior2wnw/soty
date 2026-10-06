import path from 'node:path';
import { createUniversalRegistrationService } from '../modules/app-contract/server/registration.mjs';
import { createFeedbackService } from '../modules/feedback/server/index.mjs';
import { DEFAULT_FEEDBACK_PROFILE } from '../modules/feedback/server/profile.mjs';
import { createReviewsService } from '../modules/reviews/server/index.mjs';

/** Host composition only: HTTP bodies never instantiate reviewed authority. */
export function createUniversalApps({ dataDir, apps, actorActive, reviews: reviewedReviews, registryId = 'soty', environmentId = 'production' }) {
  const base = dataDir || path.resolve('data');
  const reviews = reviewedReviews ?? createReviewsService({ registryId, environmentId, actorActive, withAppAuthority: apps.withAppAuthority });
  const feedback = createFeedbackService({ databasePath: path.join(base, 'feedback', 'feedback.sqlite'),
    registryId, environmentId, actorActive, withAppAuthority: apps.withAppAuthority });
  let registration;
  try {
    registration = createUniversalRegistrationService({ databasePath: path.join(base, 'app-registration', 'registry.sqlite'),
      registryId, environmentId, actorActive, withReviewedAppAuthority: apps.withReviewedAppAuthority,
      reviewedProfile: DEFAULT_FEEDBACK_PROFILE, approvedReferences: reviews.approvedReferences(),
      selectApprovedReferences: ({ scope }) => reviews.approvedReferencesFor(scope), feedback: feedback.provider });
  } catch (error) { feedback.close(); reviews.close(); throw error; }
  function confirmFeedback(request, result) {
      // Commits share the original verified Connect action, but are not a
      // distributed atomic transaction. The accepted pending intent survives.
      try {
        const confirmed = registration.reconcileFeedback({ actor: request.actor, appId: request.args.appId,
          expectedRevision: result.registration.revision });
        return { ...result, registration: confirmed.registration };
      } catch (error) {
        if (error?.status >= 500 || ['feedback_busy', 'registration_feedback_pending'].includes(error?.code)) return result;
        throw error;
      }
  }
  const universalExtension = Object.freeze({ operations: registration.operations,
    execute(request) {
      const result = registration.execute(request);
      return request.op === 'apps.universal.admit' ? confirmFeedback(request, result) : result;
    }
  });
  const appsExtension = Object.freeze({ operations: apps.operations,
    execute(request) {
      const result = apps.execute(request);
      if (request.op !== 'apps.register') return result;
      // Legacy registration already has stable per-owner/connector/port replay.
      // The deterministic universal request key joins that same app without
      // creating another card, namespace or feedback installation.
      const appId = result.app.id;
      const found = registration.execute({ op: 'apps.universal.get', actor: request.actor,
        args: { appId, expectedAccountId: request.actor.accountId } }).registration;
      if (found) {
        const current = found.state === 'pending-feedback'
          ? confirmFeedback({ actor: request.actor, args: { appId } }, { registration: found }).registration : found;
        return { ...result, universalRegistration: current };
      }
      const accepted = universalExtension.execute({ op: 'apps.universal.admit', actor: request.actor,
        args: { expectedAccountId: request.actor.accountId, appId, requestId: `initial.${appId}`,
          expectedRevision: 0, proposal: { kind: 'author-draft', draft: { title: result.app.name } } } });
      return { ...result, universalRegistration: accepted.registration };
    }
  });
  return { registration, feedback, reviews, universalExtension, appsExtension, close() { registration.close(); feedback.close(); reviews.close(); } };
}
