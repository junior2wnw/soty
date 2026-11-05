import type {WorldApi,WorldCommunity,WorldDirectThread} from './types';
export interface SocialInboxEpoch{direct:number;community:number;communityRead:number}
export interface SocialInbox{refresh():Promise<void>;dispose():void;readonly pending:boolean}
export function createSocialInbox(options:{api:WorldApi;accountId:string;isCurrent():boolean;directEnabled?():boolean;epochs?():SocialInboxEpoch;
 onDirect?(threads:WorldDirectThread[],epoch:SocialInboxEpoch):boolean|void;onCommunities?(communities:WorldCommunity[],epoch:SocialInboxEpoch):boolean|void;
 onContacts?(incoming:number):void;onFailure?(kind:'direct'|'communities'|'contacts',reason:unknown):void}):SocialInbox;
