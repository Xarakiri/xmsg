// SPDX-License-Identifier: Apache-2.0
// Chat state for the logged-in user: the reducer from client-core, fed by the gateway
// connection, saved to localStorage so a reload keeps channels and messages.

import { useEffect, useReducer, useRef, useState } from "react";
import {
  BACKGROUND_SYNC_MS,
  Connection,
  SyncScheduler,
  inPartition,
  reduce,
  sync,
  unsent,
  type Action,
  type ChatState,
  type ConnectionStatus,
} from "@xmsg/client-core";
import { api, gatewayUrl, session } from "./session.ts";
import { saveState } from "./storage.ts";

/** `initial`: the state loaded from the local database (see storage.ts). */
export function useChat(me: string, initial: ChatState) {
  const [state, dispatch] = useReducer((s: ChatState, a: Action) => reduce(s, a), initial);
  const [status, setStatus] = useState<ConnectionStatus>("connecting");
  const [syncError, setSyncError] = useState(false);
  const conn = useRef<Connection | null>(null);
  const latest = useRef(state);
  latest.current = state;

  useEffect(() => {
    // Catching up (plan §6.6): on reconnect, on a gateway Resync (one partition or all),
    // when the tab comes back, and every 30 minutes. One sync at a time.
    const scheduler = new SyncScheduler((only) =>
      sync({ api, token: () => session.accessToken(), state: () => latest.current, dispatch, only })
        .then(() => setSyncError(false))
        .catch((e) => {
          setSyncError(true);
          throw e;
        }),
    );
    const connection = new Connection({
      url: gatewayUrl(),
      token: () => session.accessToken(),
      onEvent: (event) => dispatch({ type: "event", event, at: Date.now() }),
      onAck: (clientMsgId) => dispatch({ type: "ack", clientMsgId }),
      onSendRejected: (clientMsgId, reason, detail) => dispatch({ type: "sendRejected", clientMsgId, reason, detail }),
      onStatus: (s) => {
        setStatus(s);
        if (s !== "online") return;
        // Back online: resend the outbox (the sequencer drops repeats), then catch up on
        // what happened meanwhile. Live events keep flowing during the sync.
        for (const m of unsent(latest.current)) connection.send(m.channelId, m.key, m.body);
        scheduler.request();
      },
      onResync: (partition) =>
        scheduler.request(partition === undefined ? undefined : inPartition(partition, connection.partitionCount)),
    });
    conn.current = connection;
    connection.start();
    const onVisible = () => {
      if (document.visibilityState === "visible" && connection.isOnline) scheduler.request();
    };
    document.addEventListener("visibilitychange", onVisible);
    const timer = setInterval(() => connection.isOnline && scheduler.request(), BACKGROUND_SYNC_MS);
    // The outbox gives up on its own after OUTBOX_RETRY_MS (plan §9.2); checked now (a
    // reload after a long time offline) and every 30 s.
    dispatch({ type: "expire", now: Date.now() });
    const expiry = setInterval(() => dispatch({ type: "expire", now: Date.now() }), 30_000);
    return () => {
      document.removeEventListener("visibilitychange", onVisible);
      clearInterval(timer);
      clearInterval(expiry);
      connection.stop();
    };
  }, [me]);

  useEffect(() => {
    const id = setTimeout(() => void saveState(me, state), 200);
    return () => clearTimeout(id);
  }, [me, state]);

  const send = (channelId: string, body: string) => {
    const clientMsgId = crypto.randomUUID();
    dispatch({ type: "send", channelId, clientMsgId, body, at: Date.now() });
    conn.current?.send(channelId, clientMsgId, body); // if offline it goes out on reconnect
  };

  const nameChannel = (channelId: string, name: string) => dispatch({ type: "channelNamed", channelId, name });

  // Read positions (ADR-0012): shown at once, applied by the user's readstate owner.
  const markRead = (channelId: string) => {
    const channel = latest.current.channels[channelId];
    if (!channel || channel.msgSeq <= channel.readSeq) return;
    const msgSeq = channel.msgSeq;
    dispatch({ type: "markRead", channelId, msgSeq });
    void session
      .accessToken()
      .then((token) => api.markRead(token, channelId, msgSeq))
      .catch(() => undefined); // the next sync brings the server's position back; harmless
  };

  // Personal settings (ADR-0016): shown at once, stored by the settings-writer shortly.
  const setMuted = (channelId: string, muted: boolean) => {
    const channel = latest.current.channels[channelId];
    if (!channel) return;
    const notifyLevel = channel.notifyLevel ?? "all";
    dispatch({ type: "settings", settings: [{ channelId, muted, notifyLevel }] });
    void session
      .accessToken()
      .then((token) => api.putSettings(token, channelId, muted, notifyLevel))
      .catch(() => dispatch({ type: "settings", settings: [{ channelId, muted: !muted, notifyLevel }] }));
  };

  // A failed message goes out again under a new client_msg_id: the old one may still
  // be in the sequencer's dedupe window, which would drop the retry silently.
  const retry = (oldKey: string) => {
    const message = Object.values(latest.current.messages)
      .flat()
      .find((m) => m.key === oldKey && m.status === "failed");
    if (!message) return;
    const newKey = crypto.randomUUID();
    dispatch({ type: "retry", oldKey, newKey, at: Date.now() });
    conn.current?.send(message.channelId, newKey, message.body);
  };

  return { state, status, syncError, send, nameChannel, markRead, setMuted, retry };
}
