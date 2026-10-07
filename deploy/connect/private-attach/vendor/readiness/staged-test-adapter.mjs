// Test-only compatibility seam for accepted donor tests; never imported by runtime.
import { createStagedAuthenticatedBackupSender } from '../../../restore-backup.mjs';
export const sendAuthenticatedBackup = ({beforeBody,...options}) => createStagedAuthenticatedBackupSender({beforeBody}).send(options);
