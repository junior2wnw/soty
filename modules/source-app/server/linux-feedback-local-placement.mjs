// Trusted host placement for the task-local Linux lab. It is neither publisher
// input nor a general path/daemon selector. Old Dev placement stays separate.
import {deepFreeze} from './wire.mjs';
export const LOCAL_LINUX_FEEDBACK_PLACEMENT=deepFreeze({
  id:'soty.feedback.local-linux-wsl',version:1,
  lab:'/home/junio/codex-soty-universal-local-20261007-4c8284c7',
  directory:'/home/junio/codex-soty-universal-local-20261007-4c8284c7/source-feedback-jobs',
  socketHost:'/home/junio/codex-soty-universal-local-20261007-4c8284c7/source-docker.sock',
  socketContainer:'/run/soty-docker.sock',socketGid:1001,
  dockerBinary:'/usr/bin/docker',
  dockerHostBinary:'/home/junio/codex-soty-universal-local-20261007-4c8284c7/tools/docker/docker',
  lifecycleCommit:'208f6af852199de82201d5e650b276a8db82e729',
});
