import type {WorldApi,WorldAppRecord} from './types';
import type {AppResolvedEntry} from './app-launch.mjs';
export function refreshAppStageLive(options:{api:WorldApi;accountId:string;appId:string;entry:AppResolvedEntry|null;isCurrent():boolean;
  readEntry():AppResolvedEntry|null;onMetadata(value:Pick<WorldAppRecord,'name'|'status'|'ownerAccountId'>):void;onDenied(error:unknown):Promise<void>|void}):Promise<void>;
