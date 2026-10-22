// SPDX-License-Identifier: Apache-2.0
// The local database (plan §9.2): each user's chat state in IndexedDB, so the app opens
// instantly with what it had, and history isn't capped by localStorage's ~5 MB. States
// saved in localStorage by earlier versions move over on first load. Where IndexedDB
// isn't available (some private modes), localStorage keeps working as before.

import { restore, type ChatState } from "@xmsg/client-core";
import { safe } from "./session.ts";

const DB = "xmsg";
const STORE = "state";
const legacyKey = (me: string) => `xmsg.state.v3.${me}`;

function open(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB, 1);
    request.onupgradeneeded = () => request.result.createObjectStore(STORE);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

let db: Promise<IDBDatabase> | null = null;
const database = () => (db ??= open());

function run<T>(mode: IDBTransactionMode, f: (store: IDBObjectStore) => IDBRequest<T>): Promise<T> {
  return database().then(
    (d) =>
      new Promise<T>((resolve, reject) => {
        const request = f(d.transaction(STORE, mode).objectStore(STORE));
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
      }),
  );
}

export async function loadState(me: string): Promise<ChatState> {
  try {
    const stored = await run("readonly", (s) => s.get(me));
    let state: ChatState;
    if (stored) {
      state = restore(stored, me);
    } else {
      // First load with IndexedDB: take over what localStorage had.
      const legacy = safe(() => localStorage.getItem(legacyKey(me)));
      state = restore(legacy ? safe(() => JSON.parse(legacy)) : null, me);
      if (legacy) await run("readwrite", (s) => s.put(state, me));
    }
    // IndexedDB works: drop this and older versions' localStorage copies.
    for (const key of [legacyKey(me), `xmsg.state.v2.${me}`, `xmsg.state.${me}`]) {
      safe(() => localStorage.removeItem(key));
    }
    return state;
  } catch {
    const legacy = safe(() => localStorage.getItem(legacyKey(me)));
    return restore(legacy ? safe(() => JSON.parse(legacy)) : null, me);
  }
}

export async function saveState(me: string, state: ChatState): Promise<void> {
  try {
    // Structured clone: no JSON round trip on every save.
    await run("readwrite", (s) => s.put(state, me));
  } catch {
    safe(() => localStorage.setItem(legacyKey(me), JSON.stringify(state)));
  }
}
