import { createDescriptor } from '../app-contract/sdk.mjs';

/** Metadata only. authority is obtained from the currently inspected app, not a guessed IP or URL. */
export function createAppAgentDescriptor({ authority, namespace, capabilities, feedback, skills = [], docs = [], reviews = { mode: 'disabled' } }) {
  return createDescriptor({ app: { id: authority.appId, namespace, title: authority.title, visibility: authority.visibility,
    source: { id: `apps:${authority.appId}/target`, revision: authority.target.revision, digest: authority.target.digest }, auth: { mode: 'public' } },
    capabilities, feedback, skills, docs, reviews });
}
