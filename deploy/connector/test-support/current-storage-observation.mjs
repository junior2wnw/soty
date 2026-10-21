// Test-only current-probe DTO for examined synthetic fixtures. Missing stores
// are actually absent/empty in those fixtures. Pass observed nonempty versions
// explicitly (for example Connect3); never use this to validate a live store or
// to rewrite historical parser/start-receipt/adversarial-old-probe vectors.
export const currentObservation = stores => ({
  ok: true, rooms: 'empty', apps: 'empty', notes: 'empty', capabilities: 'empty',
  appRegistration: 'empty', feedback: 'empty', humanIdentity: 'empty', connect: 'empty', world: 'empty',
  ...stores, schema: 'soty.storage-format.v7',
});

// Exact pre-World current image declaration from the captured v6 Source. Tests
// constructing historical5/6 labels must not inherit new7 keys/capabilities.
export const frozenStorageReaders6 = '{"version":6,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6,7,8,9,10,11],"notes":[1,2],"capabilities":[1,2,3],"appRegistration":[1],"feedback":[1],"humanIdentity":[1,2,3],"connect":[3,4]}}';
