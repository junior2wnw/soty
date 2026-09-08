import {DatabaseSync} from 'node:sqlite';
import {isMainThread} from 'node:worker_threads';
if(!isMainThread){
  const exec=DatabaseSync.prototype.exec;
  DatabaseSync.prototype.exec=function(sql){
    if(sql==='ROLLBACK')throw Object.assign(new Error('synthetic rollback unavailable'),{code:'SQLITE_IOERR'});
    return exec.call(this,sql);
  };
}
