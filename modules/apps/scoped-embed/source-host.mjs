// Portable Source-host entry, bundled without any outside-project runtime path.
export { createSourceProofVerifier } from "./source-proof.mjs";
export { createSourceCurrentSubjectPort } from "./local-broker.mjs";
export {
  scopedEmbedProfile,
  sourceConsentDigest,
  ScopedEmbedError,
  SCOPED_EMBED_PROFILE,
  SCOPED_EMBED_LIMITS,
} from "./profile.mjs";
