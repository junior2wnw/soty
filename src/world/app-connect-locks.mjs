// App settings uses synchronous local transitions. IndexedDB also serializes
// these transitions across tabs on LAN HTTP, where Web Locks is unavailable.
export function createAppConnectLocks({ locks = globalThis.navigator?.locks, indexedDB = globalThis.indexedDB } = {}) {
  if (locks?.request) return locks;
  let connection;
  return {
    request(name, action) {
      if (!indexedDB) return Promise.reject(Object.assign(new Error('app_connect_storage_unavailable'), { code: 'app_connect_storage_unavailable' }));
      connection ||= new Promise((resolve, reject) => {
        let refused=false;
        const request = indexedDB.open('soty-app-connect-locks-v1', 1);
        request.onupgradeneeded = () => request.result.createObjectStore('local-transitions');
        request.onsuccess = () => { const db = request.result;if(refused){db.close();return;} db.onversionchange = () => { db.close(); connection = null; }; resolve(db); };
        request.onerror = () => { connection = null; reject(request.error); };
        request.onblocked = () => { refused=true;connection = null; reject(Object.assign(new Error('app_connect_storage_blocked'), { code: 'app_connect_storage_blocked' })); };
      });
      return connection.then(db => new Promise((resolve, reject) => {
        const transaction = db.transaction('local-transitions', 'readwrite'); let result, failure;
        transaction.oncomplete = () => resolve(result);
        transaction.onabort = () => reject(failure || transaction.error || new Error('app_connect_storage_unavailable'));
        transaction.onerror = () => {};
        const request = transaction.objectStore('local-transitions').get(name);
        request.onsuccess = () => {
          try {
            result = action();
            if (result && typeof result.then === 'function'){Promise.resolve(result).catch(()=>{});throw Object.assign(new Error('app_connect_async_local_transition'), { code: 'app_connect_async_local_transition' });}
          } catch (error) { failure = error; transaction.abort(); }
        };
      }));
    },
  };
}
