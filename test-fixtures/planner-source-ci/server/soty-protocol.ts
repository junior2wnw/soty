// Portable shared SDK pin725f618; native account/link/resource ownership stays here.
import { createSourceRpProtocol, SourceRpError, type SourceRpProfile } from './source-rp/index.mjs';
export type SotyBffProfile = SourceRpProfile;
export { SourceRpError as SotyBffError };
export function createSotyBffProtocol(profile: SotyBffProfile) {
  const protocol = createSourceRpProtocol(profile);
  return Object.freeze({
    ...protocol,
    async currentSubject(accessToken: string, subject: string) {
      try {
        return await protocol.currentSubject(accessToken, subject);
      } catch (error) {
        if ((error as { status?: number }).status === 503)
          throw new SourceRpError('account_provider_unavailable', 503);
        throw error;
      }
    },
  });
}
