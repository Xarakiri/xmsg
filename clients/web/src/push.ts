// SPDX-License-Identifier: Apache-2.0
// Web Push opt-in (plan §5.11): service worker, browser permission, subscription with the
// server's VAPID key, stored by the push service. Nothing happens until the user asks.

import { api, session } from "./session.ts";

export type PushState = "unsupported" | "off" | "on" | "blocked";

export async function pushState(): Promise<PushState> {
  if (!("serviceWorker" in navigator) || !("PushManager" in window) || !("Notification" in window)) return "unsupported";
  if (Notification.permission === "denied") return "blocked";
  const registration = await navigator.serviceWorker.getRegistration("/sw.js");
  const subscription = await registration?.pushManager.getSubscription();
  return subscription && Notification.permission === "granted" ? "on" : "off";
}

/** Asks for permission and subscribes this browser. Returns the resulting state. */
export async function enablePush(): Promise<PushState> {
  if ((await pushState()) === "unsupported") return "unsupported";
  const permission = await Notification.requestPermission();
  if (permission !== "granted") return permission === "denied" ? "blocked" : "off";
  const registration = await navigator.serviceWorker.register("/sw.js");
  await navigator.serviceWorker.ready;
  const key = await api.pushKey();
  const subscription =
    (await registration.pushManager.getSubscription()) ??
    (await registration.pushManager.subscribe({ userVisibleOnly: true, applicationServerKey: fromBase64Url(key) }));
  const json = subscription.toJSON() as { endpoint: string; keys: { p256dh: string; auth: string } };
  await api.subscribePush(await session.accessToken(), json);
  return "on";
}

/** Stops notifications for this browser. */
export async function disablePush(): Promise<PushState> {
  const registration = await navigator.serviceWorker.getRegistration("/sw.js");
  const subscription = await registration?.pushManager.getSubscription();
  if (subscription) {
    await api.unsubscribePush(await session.accessToken(), subscription.endpoint).catch(() => undefined);
    await subscription.unsubscribe();
  }
  return "off";
}

function fromBase64Url(s: string): Uint8Array<ArrayBuffer> {
  const base64 = s.replace(/-/g, "+").replace(/_/g, "/").padEnd(Math.ceil(s.length / 4) * 4, "=");
  const raw = atob(base64);
  const out = new Uint8Array(new ArrayBuffer(raw.length));
  for (let i = 0; i < raw.length; i++) out[i] = raw.charCodeAt(i);
  return out;
}
