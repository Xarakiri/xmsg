// SPDX-License-Identifier: Apache-2.0
// Native notifications (desktop) and the dock badge. Imported lazily so the web build
// never loads the Tauri plugins; every call degrades quietly off-desktop.

import { isDesktop } from "./session.ts";

/** Ask once, then remember; false means never pester (or that the platform lacks it). */
export async function ensureNotifyPermission(): Promise<boolean> {
  if (!isDesktop) return false;
  try {
    const { isPermissionGranted, requestPermission } = await import("@tauri-apps/plugin-notification");
    if (await isPermissionGranted()) return true;
    return (await requestPermission()) === "granted";
  } catch {
    return false;
  }
}

/** One native notification; fires only on desktop, only with permission. */
export async function notify(title: string, body: string): Promise<void> {
  if (!isDesktop || !(await ensureNotifyPermission())) return;
  try {
    const { sendNotification } = await import("@tauri-apps/plugin-notification");
    sendNotification({ title, body });
  } catch {
    // a denied or broken notification channel never blocks chat
  }
}

/** Dock badge with the unread total; 0 clears it. */
export async function setBadge(count: number): Promise<void> {
  if (!isDesktop) return;
  try {
    const { getCurrentWindow } = await import("@tauri-apps/api/window");
    await getCurrentWindow().setBadgeCount(count || undefined);
  } catch {
    // badge unsupported: the title already carries the count
  }
}
