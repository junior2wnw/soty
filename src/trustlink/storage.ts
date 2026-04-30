export const deviceKey = "device";
export const tunnelsKey = "soty:tunnels:v1";
export const selectedKey = "soty:selected:v1";
export const appModeKey = "soty:pwa-enabled:v1";
export const pendingInviteKey = "soty:pending-invite:v1";
export const pendingJoinKey = "soty:pending-join:v1";

export const dbName = "soty-online";

export function loadJson<T>(key: string): T | null {
  const raw = localStorage.getItem(key);
  if (!raw) {
    return null;
  }
  try {
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

export function saveJson<T>(key: string, value: T): void {
  localStorage.setItem(key, JSON.stringify(value));
}

export function removeStored(key: string): void {
  localStorage.removeItem(key);
}

export function loadStored(key: string): string | null {
  return localStorage.getItem(key);
}

export function saveStored(key: string, value: string): void {
  localStorage.setItem(key, value);
}

export function openDb(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(dbName, 1);
    request.onupgradeneeded = () => {
      request.result.createObjectStore("kv");
    };
    request.onerror = () => reject(request.error);
    request.onsuccess = () => resolve(request.result);
  });
}

export async function idbGet<T>(key: string): Promise<T | undefined> {
  const db = await openDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction("kv", "readonly");
    const request = tx.objectStore("kv").get(key);
    request.onerror = () => reject(request.error);
    request.onsuccess = () => resolve(request.result as T | undefined);
    tx.oncomplete = () => db.close();
  });
}

export async function idbSet<T>(key: string, value: T): Promise<void> {
  const db = await openDb();
  await new Promise<void>((resolve, reject) => {
    const tx = db.transaction("kv", "readwrite");
    tx.objectStore("kv").put(value, key);
    tx.onerror = () => reject(tx.error);
    tx.oncomplete = () => {
      db.close();
      resolve();
    };
  });
}

export async function resetLocalSotyState(): Promise<void> {
  removeSotyKeys(localStorage);
  removeSotyKeys(sessionStorage);
  await Promise.all([
    deleteLocalDatabase(dbName),
    deleteSotyCaches(),
    unregisterSotyWorkers()
  ]);
}

function removeSotyKeys(storage: Storage): void {
  try {
    const keys: string[] = [];
    for (let index = 0; index < storage.length; index += 1) {
      const key = storage.key(index) || "";
      if (key === deviceKey || key.startsWith("soty:")) {
        keys.push(key);
      }
    }
    for (const key of keys) {
      storage.removeItem(key);
    }
  } catch {
    // Storage may be blocked in private or damaged browser profiles.
  }
}

function deleteLocalDatabase(name: string): Promise<void> {
  if (!("indexedDB" in window)) {
    return Promise.resolve();
  }

  return new Promise((resolve) => {
    let finished = false;
    let timer = 0;
    const done = () => {
      if (!finished) {
        finished = true;
        window.clearTimeout(timer);
        resolve();
      }
    };
    timer = window.setTimeout(done, 1800);
    const request = indexedDB.deleteDatabase(name);
    request.onsuccess = done;
    request.onerror = done;
    request.onblocked = done;
  });
}

async function deleteSotyCaches(): Promise<void> {
  if (!("caches" in window)) {
    return;
  }

  try {
    const keys = await caches.keys();
    await Promise.all(keys
      .filter((key) => key.startsWith("soty-online-"))
      .map((key) => caches.delete(key)));
  } catch {
    // Cache cleanup is best effort; a reload will still fetch fresh HTML.
  }
}

async function unregisterSotyWorkers(): Promise<void> {
  if (!("serviceWorker" in navigator)) {
    return;
  }

  try {
    const registrations = await navigator.serviceWorker.getRegistrations();
    await Promise.all(registrations
      .filter((registration) => registration.scope.startsWith(window.location.origin))
      .map((registration) => registration.unregister()));
  } catch {
    // The next registration pass will repair stale workers if unregister fails.
  }
}
