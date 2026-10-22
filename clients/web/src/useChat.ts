// SPDX-License-Identifier: Apache-2.0
// Chat state for the logged-in user: the reducer from client-core, fed by the gateway
// connection, saved to localStorage so a reload keeps channels and messages.

import { useEffect, useReducer, useRef, useState } from "react";
import {
  BACKGROUND_SYNC_MS,
  Connection,
  SyncScheduler,
  inPartition,
  initialState,
  reduce,
  sync,
  unsent,
  type Action,
  type ChatState,
  type ConnectionStatus,
} from "@xmsg/client-core";
import { api, gatewayUrl, safe, session } from "./session.ts";

// Versioned: a saved state from an older client shape is dropped and re-synced.
const stateKey = (me: string) => `xmsg.state.v3.${me}`;

function load(me: string): ChatState {
  const saved = safe(() => localStorage.getItem(stateKey(me)));
  const parsed = saved ? safe(() => JSON.parse(saved) as ChatState) : undefined;
  return parsed?.me === me ? parsed : initialState(me);
}

export function useChat(me: string) {
  const [state, dispatch] = useReducer((s: ChatState, a: Action) => reduce(s, a), me, load);
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
    return () => {
      document.removeEventListener("visibilitychange", onVisible);
      clearInterval(timer);
      connection.stop();
    };
  }, [me]);

  useEffect(() => {
    const id = setTimeout(() => safe(() => localStorage.setItem(stateKey(me), JSON.stringify(state))), 200);
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

  return { state, status, syncError, send, nameChannel, markRead };
}
