import {check} from '../server/wire.mjs';
import {withSourceOperatorConfiguration} from './config.mjs';

/** A fixed Source-owned public TLS entry, separate from Root embed/broker.
 * It forwards no identity claim or caller-selected path/destination. */
export function renderSourceNativePortal(handle){return withSourceOperatorConfiguration(handle,options=>{
  const native=new URL(options.profile.nativeOrigin);check(native.protocol==='https:','source_install_public_https_required',503);
  const hostname=native.host;check(/^[A-Za-z0-9.-]+(?::[0-9]+)?$/u.test(hostname),'source_install_public_https_required',503);
  return `${hostname} {
  @nativeGet {
    method GET
    path /soty/connect
  }
  @nativePost {
    method POST
    path /soty/authorize
  }
  route {
    reverse_proxy @nativeGet 127.0.0.1:${options.listen.port} {
      header_up Host ${hostname}
    }
    reverse_proxy @nativePost 127.0.0.1:${options.listen.port} {
      header_up Host ${hostname}
    }
    respond 404
  }
}
`;
});}
