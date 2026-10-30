// SPDX-License-Identifier: Apache-2.0
// The browser's API client and login session; the refresh token lives in localStorage.
// Desktop builds (VITE_XMSG_DESKTOP=1) talk to a server URL saved in localStorage and
// send REST through Tauri's native HTTP plugin (no CORS); the web build is same-origin.

import { Api, Session, type TokenStore } from "@xmsg/client-core";

const REFRESH_KEY = "xmsg.refresh";
const SERVER_KEY = "xmsg.server";

/** Set by the desktop build; the web build compiles this branch away. */
export const isDesktop = import.meta.env.VITE_XMSG_DESKTOP === "1";

/** The server the desktop app connects to, prefilled on first run. */
export const DEFAULT_SERVER = "https://3-13-72-4.sslip.io";

/** A server URL the desktop app may use: https anywhere, http only on localhost. */
export function validServerUrl(raw: string): boolean {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    return false;
  }
  if (url.protocol === "https:") return true;
  if (url.protocol !== "http:") return false;
  return url.hostname === "localhost" || url.hostname === "127.0.0.1";
}

/** The saved server URL ("" in the web build and before the desktop connect screen). */
export function serverUrl(): string {
  if (!isDesktop) return "";
  try {
    return localStorage.getItem(SERVER_KEY) ?? "";
  } catch {
    return "";
  }
}

export function saveServerUrl(url: string): void {
  localStorage.setItem(SERVER_KEY, url.replace(/\/+$/, ""));
}

const tokenStore: TokenStore = {
  load: () => safe(() => localStorage.getItem(REFRESH_KEY)) ?? null,
  save: (token) => safe(() => (token ? localStorage.setItem(REFRESH_KEY, token) : localStorage.removeItem(REFRESH_KEY))),
};

const base = serverUrl();
const at = (path: string) => (base ? `${base}${path}` : path);

// Tauri's fetch is native (no CORS), loaded lazily so the web build never imports it.
const nativeFetch = isDesktop
  ? (input: string, init?: RequestInit) => import("@tauri-apps/plugin-http").then((m) => m.fetch(input, init))
  : undefined;

export const api = new Api(
  {
    auth: at("/auth"),
    channels: at("/channels"),
    messages: at("/messages"),
    users: at("/users"),
    readstate: at("/readstate"),
    push: at("/push"),
    media: at("/media"),
    calls: at("/calls"),
  },
  nativeFetch ? { fetch: nativeFetch } : {},
);
export const session = new Session(api, tokenStore);

export function gatewayUrl(): string {
  if (base) {
    const url = new URL(base);
    return `${url.protocol === "https:" ? "wss" : "ws"}://${url.host}/ws`;
  }
  return `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`;
}

/** Storage can be unavailable (private mode, blocked site data): degrade instead of crashing. */
export function safe<T>(f: () => T): T | undefined {
  try {
    return f();
  } catch {
    return undefined;
  }
}
