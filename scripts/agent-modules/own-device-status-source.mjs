// Native OS/device authority for the existing Source App BFF. Constructor CODE
// receives an approved owner/profile and the current local identity reader;
// neither HTTP input nor a descriptor can create an owner or an OS permission.
export function createOwnDeviceStatusNative({ sdk, profileDigest, ownerAccountId, expectedIdentity,
  currentIdentity, readStatus, digest, clock = Date.now }) {
  const deny = () => { throw Object.assign(new Error('native_device_status_denied'), {code:'native_device_status_denied',status:403}); };
  if (!sdk || typeof currentIdentity !== 'function' || typeof readStatus !== 'function' || typeof digest !== 'function'
    || !ownerAccountId || !expectedIdentity?.principalId || !expectedIdentity.deviceId || !expectedIdentity.installId) deny();
  const expected = digest(expectedIdentity), proofs = new WeakMap();
  function inspect(proof, binding) {
    const held = proofs.get(proof);
    if (!held || digest(currentIdentity()) !== expected || digest(binding) !== held.binding
      || binding.rootPrincipal.accountId !== ownerAccountId || binding.resource.selection.nativeId !== expectedIdentity.deviceId
      || binding.rootExpiresAt <= clock()) deny();
    sdk.readVerifiedRootContext(held.root, profileDigest);
    return held;
  }
  return sdk.createSourceNativeAuthorityPort({
    async capture() { deny(); },
    async captureRoot(binding, root) {
      sdk.readVerifiedRootContext(root, profileDigest);
      const proof = {}; proofs.set(proof,{root,binding:digest(binding)}); inspect(proof,binding); return proof;
    },
    withCurrent(proof,binding,action) { inspect(proof,binding); return action(); },
    commitRootSession(proof,binding) {
      inspect(proof,binding);
      return {nativeSessionHash:digest({identity:expectedIdentity,reference:binding.rootReferenceDigest,instance:binding.instanceId??null}),
        principalId:expectedIdentity.principalId,resourceId:binding.resource.resourceId};
    },
    currentContext(proof,binding) {
      inspect(proof,binding); return {key:expectedIdentity.deviceId,generation:1,title:'Транзит · моё устройство',
        role:'reader',revision:1,expiresAt:binding.rootExpiresAt};
    },
    async read(proof,binding,input) {
      inspect(proof,binding);
      if (!input?.input || Object.keys(input.input).join(',') !== 'operation' || input.input.operation !== 'transit.native.status') deny();
      const result = await readStatus(); inspect(proof,binding);
      if (result?.running === null || result?.tunReady === null) throw Object.assign(new Error('native_device_status_unavailable'),{code:'native_device_status_unavailable',status:503});
      return result;
    }
    // No execute, write, guest, role, consent or receipt mutation callback.
  });
}

// Mount this returned maintained BFF in the Native app's existing HTTP server.
// The app still owns storage/encryption and the approved profile/transport key.
// This helper opens no listener, allocates no key and registers no Source.
export function createOwnDeviceStatusBff({sdk,profile,profileDigest,ownerAccountId,expectedIdentity,
  currentIdentity,readStatus,digest,storage,transportKey,connectorPort,clock=Date.now}) {
  if (profile.connector.hostDeviceId !== expectedIdentity.deviceId || digest(profile) !== profileDigest) {
    throw Object.assign(new Error('native_device_status_denied'),{code:'native_device_status_denied',status:403});
  }
  const native=createOwnDeviceStatusNative({sdk,profileDigest,ownerAccountId,expectedIdentity,currentIdentity,readStatus,digest,clock});
  return sdk.createSourceAppBff({profile,transportKey,connectorPort,storage,native,clock});
}
