// Mechanical IndexedDB contract port: multiple databases/stores, serialized
// transactions, commit/abort boundaries, upgrade blocking and versionchange.
// Production storage code is not copied here; real browser IDB is a separate gate.
export function indexedNativePort(){
  const databases=new Map(),commitHolds=[],holdWaiters=[],aborts=[];let holdNext=null,abortNext=null;
  function open(name,version){
    const request={};queueMicrotask(()=>{
      let record=databases.get(name);if(!record){record={version:0,stores:new Map(),connections:new Set(),queue:[],running:false};databases.set(name,record);}
      const target=version??(record.version||1);
      if(target<record.version){request.error=new Error('VersionError');request.onerror?.();return;}
      const connect=()=>{
        const connection={version:target,closed:false,objectStoreNames:{contains:key=>record.stores.has(key)},close(){this.closed=true;record.connections.delete(this);},
          createObjectStore(key,{keyPath}={}){if(record.stores.has(key))throw new Error('ConstraintError');const value={rows:new Map(),keyPath,indexes:new Map()};record.stores.set(key,value);return{createIndex(name,path){value.indexes.set(name,path);}};},
          transaction(names,mode='readonly'){if(this.closed)throw new Error('InvalidStateError');return createTransaction(record,name,Array.isArray(names)?names:[names],mode);}};
        request.result=connection;
        if(target>record.version){record.version=target;request.onupgradeneeded?.({oldVersion:0,newVersion:target});}
        record.connections.add(connection);request.onsuccess?.();
      };
      if(target>record.version&&record.connections.size){for(const connection of [...record.connections])connection.onversionchange?.({newVersion:target});if(record.connections.size){request.onblocked?.();return;}}
      connect();
    });return request;
  }
  function createTransaction(record,dbName,names,mode){
    const tx={};let started=false,ended=false,pending=0,finishScheduled=false,views;
    const operations=[];
    const finish=()=>{if(!started||ended||pending||finishScheduled)return;finishScheduled=true;queueMicrotask(()=>{
      finishScheduled=false;if(ended||pending)return;
      const commit=()=>{if(ended)return;if(mode==='readwrite'&&abortNext?.dbName===dbName&&(!abortNext.store||names.includes(abortNext.store))){abortNext=null;tx.abort();return;}
        ended=true;if(mode==='readwrite')for(const[name,value]of views)record.stores.get(name).rows=value;tx.oncomplete?.();record.running=false;runNext(record);};
      if(mode==='readwrite'&&holdNext?.dbName===dbName&&(!holdNext.store||names.includes(holdNext.store))){holdNext=null;commitHolds.push(commit);for(const resolve of holdWaiters.splice(0))resolve();}else commit();
    });};
    const request=read=>{const value={};pending++;const execute=()=>queueMicrotask(()=>{if(ended)return;try{value.result=structuredClone(read());value.onsuccess?.();}catch(error){tx.error=error;tx.abort();}pending--;finish();});if(started)execute();else operations.push(execute);return value;};
    tx.abort=()=>{if(ended)return;ended=true;aborts.push(dbName);queueMicrotask(()=>{tx.onabort?.();record.running=false;runNext(record);});};
    const store=name=>{if(!names.includes(name))throw new Error('NotFoundError');const descriptor=record.stores.get(name);if(!descriptor)throw new Error('NotFoundError');
      const rows=()=>views.get(name),keyOf=(value,key)=>descriptor.keyPath?value[descriptor.keyPath]:key;
      const write=action=>{if(mode!=='readwrite')throw new Error('ReadOnlyError');return request(()=>{action();return undefined;});};
      return{get:key=>request(()=>rows().get(key)),getAll:(_query,count=Infinity)=>request(()=>[...rows().values()].slice(0,count)),
        put:(value,key)=>write(()=>rows().set(keyOf(value,key),structuredClone(value))),delete:key=>write(()=>rows().delete(key)),
        index:index=>({getAll:(query,count=Infinity)=>request(()=>[...rows().values()].filter(value=>value[descriptor.indexes.get(index)]===query).slice(0,count)),
          openKeyCursor:query=>{const req={};let values,position=0;const deliver=()=>{pending++;queueMicrotask(()=>{if(ended)return;if(!values)values=[...rows()].filter(([_key,value])=>value[descriptor.indexes.get(index)]===query);const entry=values[position++];req.result=entry?{primaryKey:entry[0],continue:deliver}:null;req.onsuccess?.();pending--;finish();});};if(started)deliver();else operations.push(deliver);return req;}})};
    };
    tx.objectStore=store;
    tx._start=()=>{started=true;views=new Map(names.map(name=>[name,structuredClone(record.stores.get(name).rows)]));for(const operation of operations)operation();finish();};
    record.queue.push(tx);queueMicrotask(()=>runNext(record));return tx;
  }
  function runNext(record){if(record.running)return;const tx=record.queue.shift();if(tx){record.running=true;tx._start();}}
  return{indexedDB:{open},databases,aborts,
    hold(dbName,store){holdNext={dbName,store};},abort(dbName,store){abortNext={dbName,store};},get held(){return commitHolds.length;},release(){commitHolds.shift()?.();},
    whenHeld(){return commitHolds.length?Promise.resolve():new Promise(resolve=>holdWaiters.push(resolve));},
    rows(dbName,store){return[...(databases.get(dbName)?.stores.get(store)?.rows.values()??[])].map(value=>structuredClone(value));},
  };
}
