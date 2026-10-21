import { initializeFeedbackSchema } from '../../modules/feedback/server/schema.mjs';
import { initializeRegistrationSchema } from '../../modules/app-contract/server/schema.mjs';
import { initializeHumanIdentitySchema } from '../../modules/human-identity/schema.mjs';

// Fixed synthetic Root identity, matching createUniversalApps defaults and the
// http-app human profile fence. These are not operator-selectable profiles.
export function initializeSyntheticRootFeedback(db){return initializeFeedbackSchema(db,'soty','production');}
export function initializeSyntheticRootRegistration(db){return initializeRegistrationSchema(db,{registryId:'soty',environmentId:'production'});}
export function initializeSyntheticRootHumanIdentity(db){
  db.function('human_identity_gc_epoch',()=>0);
  return initializeHumanIdentitySchema(db,{enabled:true,registryId:'soty',environmentId:'production',issuer:'https://human.fixture.invalid/human-identity'},
    {allowRenewalMigration:true,allowDeviceConsentMigration:true});
}
