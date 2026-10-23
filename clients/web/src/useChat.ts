// SPDX-License-Identifier: Apache-2.0
// Chat state for the logged-in user: the reducer from client-core, fed by the gateway
// connection, saved to localStorage so a reload keeps channels and messages.

import { useEffect, useReducer, useRef, useState } from "react";
import {
  BACKGROUND_SYNC_MS,
  Connection,
  SyncScheduler,
  TypingTracker,
  inPartition,
  reduce,
  sync,
  unsent,
  type Action,
  type ChatState,
  type ConnectionStatus,
  type ThreadRootRef,
} from "@xmsg/client-core";
import { api, gatewayUrl, session } from "./session.ts";
import { saveState } from "./storage.ts";

/** `initial`: the state loaded from the local database (see storage.ts). */
export function useChat(me: string, initial: ChatState) {
  const [state, dispatch] = useReducer((s: ChatState, a: Action) => reduce(s, a), initial);
  const [status, setStatus] = useState<ConnectionStatus>("connecting");
  const [syncError, setSyncError] = useState(false);
  const conn = useRef<Connection | null>(null);
  // Typing indicators live only in memory: never saved, never part of the chat state.
  const typing = useRef(new TypingTracker());
  const [typingTick, setTypingTick] = useState(0);
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
      onEvent: (event) => {
        // Their message ends their typing indicator.
        if (event.payload.case === "messageCreated") {
          typing.current.stop(event.channelId.toString(), event.actorId.toString());
          setTypingTick((n) => n + 1);
        }
        dispatch({ type: "event", event, at: Date.now() });
      },
      onTyping: (channelId, userId) => {
        typing.current.note(channelId, userId);
        setTypingTick((n) => n + 1);
      },
      onAck: (clientMsgId) => dispatch({ type: "ack", clientMsgId }),
      onSendRejected: (clientMsgId, reason, detail) => dispatch({ type: "sendRejected", clientMsgId, reason, detail }),
      onStatus: (s) => {
        setStatus(s);
        if (s !== "online") return;
        // Back online: resend the outbox (the sequencer drops repeats), then catch up on
        // what happened meanwhile. Live events keep flowing during the sync.
        for (const m of unsent(latest.current)) connection.send(m.channelId, m.key, m.body, m.threadRoot);
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

  // Re-render once a second while anyone is typing, so indicators run out on time.
  useEffect(() => {
    const id = setInterval(() => {
      const anyone = Object.keys(latest.current.channels).some((c) => typing.current.who(c).length > 0);
      if (anyone || typingTick > 0) setTypingTick((n) => (anyone ? n + 1 : 0));
    }, 1_000);
    return () => clearInterval(id);
  }, [typingTick]);

  /** Who else is typing in the channel now. */
  const typingIn = (channelId: string) => typing.current.who(channelId);
  /** Call on every keystroke in the composer; the connection throttles. */
  const notifyTyping = (channelId: string) => conn.current?.typing(channelId);

  /** A message, or a thread reply when `threadRoot` is given (ADR-0005). */
  const send = (channelId: string, body: string, threadRoot?: ThreadRootRef) => {
    const clientMsgId = crypto.randomUUID();
    dispatch({ type: "send", channelId, clientMsgId, body, at: Date.now(), threadRoot });
    conn.current?.send(channelId, clientMsgId, body, threadRoot); // if offline it goes out on reconnect
  };

  /** Loads a thread's replies (live ones keep arriving as events). */
  const openThread = (channelId: string, root: ThreadRootRef) =>
    void session
      .accessToken()
      .then((token) => api.thread(token, channelId, root.msgSeq))
      .then(({ replies }) => dispatch({ type: "threadLoaded", channelId, root, replies }))
      .catch(() => undefined); // shown from what we have; the next open tries again

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

  // Reactions (ADR-0015): shown at once, the server's count follows within ~1 s; a
  // refused request is rolled back.
  const react = (channelId: string, msgSeq: number, emoji: string, add: boolean) => {
    dispatch({ type: "react", channelId, msgSeq, emoji, add });
    void session
      .accessToken()
      .then((token) => api.react(token, channelId, msgSeq, emoji, add))
      .catch(() => dispatch({ type: "react", channelId, msgSeq, emoji, add: !add }));
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
    conn.current?.send(message.channelId, newKey, message.body, message.threadRoot);
  };

  return { state, status, syncError, send, nameChannel, markRead, setMuted, retry, typingIn, notifyTyping, react, openThread };
}
