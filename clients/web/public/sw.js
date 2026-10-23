// SPDX-License-Identifier: Apache-2.0
// Service worker: shows Web Push notifications (plan §5.11) and opens the channel on click.
// The server sends {title, body, channel_id, msg_seq, tag}; `tag` collapses repeats.

self.addEventListener("push", (event) => {
  let data = {};
  try {
    data = event.data ? event.data.json() : {};
  } catch {
    data = { body: event.data ? event.data.text() : "" };
  }
  event.waitUntil(
    self.registration.showNotification(data.title || "xmsg", {
      body: data.body || "",
      tag: data.tag,
      data: { channelId: data.channel_id },
    }),
  );
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const channelId = event.notification.data && event.notification.data.channelId;
  const url = channelId ? `/?channel=${encodeURIComponent(channelId)}` : "/";
  event.waitUntil(
    self.clients.matchAll({ type: "window", includeUncontrolled: true }).then((windows) => {
      for (const w of windows) {
        if ("focus" in w) {
          w.postMessage({ type: "open-channel", channelId });
          return w.focus();
        }
      }
      return self.clients.openWindow(url);
    }),
  );
});
