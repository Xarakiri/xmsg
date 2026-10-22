// SPDX-License-Identifier: Apache-2.0
// The browser's API client and login session; the refresh token lives in localStorage.

import { Api, Session, type TokenStore } from "@xmsg/client-core";

const REFRESH_KEY = "xmsg.refresh";

const tokenStore: TokenStore = {
  load: () => safe(() => localStorage.getItem(REFRESH_KEY)) ?? null,
  save: (token) => safe(() => (token ? localStorage.setItem(REFRESH_KEY, token) : localStorage.removeItem(REFRESH_KEY))),
};

export const api = new Api({ auth: "/auth", channels: "/channels", messages: "/messages", users: "/users", readstate: "/readstate" });
export const session = new Session(api, tokenStore);

export const gatewayUrl = () => `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`;

/** Storage can be unavailable (private mode, blocked site data): degrade instead of crashing. */
export function safe<T>(f: () => T): T | undefined {
  try {
    return f();
  } catch {
    return undefined;
  }
}
