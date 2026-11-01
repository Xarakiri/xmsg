// SPDX-License-Identifier: Apache-2.0
// Chat state for the logged-in user: the reducer from client-core, fed by the gateway
// connection, saved to localStorage so a reload keeps channels and messages.

import { useEffect, useReducer, useRef, useState } from "react";
import {
  BACKGROUND_SYNC_MS,
  Connection,
  PAGE_SIZE,
  SyncScheduler,
  TypingTracker,
  inPartition,
  needsCatchUp,
  reduce,
  sync,
  unsent,
  type Action,
  type Attachment,
  type Channel,
  type ChatState,
  type ConnectionStatus,
  type Message,
  type Presence,
  type ThreadRootRef,
} from "@xmsg/client-core";
import { api, gatewayUrl, session } from "./session.ts";
import { saveState } from "./storage.ts";

/** No input for this long counts as away. */
const IDLE_AFTER_MS = 5 * 60_000;

/** `initial`: the state loaded from the local database (see storage.ts). */
export function useChat(me: string, initial: ChatState, hooks: { onIncoming?: (channel: import("@xmsg/client-core").Channel, msgSeq: number, mentioned: boolean) => void } = {}) {
  const [state, dispatch] = useReducer((s: ChatState, a: Action) => reduce(s, a), initial);
  const [status, setStatus] = useState<ConnectionStatus>("connecting");
  const [syncError, setSyncError] = useState(false);
  const conn = useRef<Connection | null>(null);
  const syncs = useRef<SyncScheduler | null>(null);
  // Typing indicators live only in memory: never saved, never part of the chat state.
  const typing = useRef(new TypingTracker());
  const [typingTick, setTypingTick] = useState(0);
  // Presence of watched users (plan §5.9): in memory only, refreshed on every reconnect.
  const [presence, setPresence] = useState<Record<string, Presence>>({});
  const latest = useRef(state);
  latest.current = state;
  const hooksRef = useRef(hooks);
  hooksRef.current = hooks;

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
    syncs.current = scheduler;
    const connection = new Connection({
      url: gatewayUrl(),
      token: () => session.accessToken(),
      onEvent: (event) => {
        // Their message ends their typing indicator.
        if (event.payload.case === "messageCreated") {
          typing.current.stop(event.channelId.toString(), event.actorId.toString());
          setTypingTick((n) => n + 1);
          const value = event.payload.value;
          const mentioned = value.mentionsChannel || value.mentionedUserIds.some((u) => u.toString() === me);
          hooksRef.current.onIncoming?.(latest.current.channels[event.channelId.toString()] ?? { id: event.channelId.toString() }, Number(event.msgSeq), mentioned);
        }
        dispatch({ type: "event", event, at: Date.now() });
      },
      onReceipt: (channelId, userId, msgSeq) => dispatch({ type: "receipt", channelId, userId, msgSeq }),
      onBump: (channelId, eventSeq, msgSeq) => dispatch({ type: "bump", channelId, eventSeq, msgSeq }),
      onPresence: (updates) =>
        setPresence((p) => {
          const next = { ...p };
          for (const u of updates) next[u.userId] = u.status;
          return next;
        }),
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
        for (const m of unsent(latest.current)) connection.send(m.channelId, m.key, m.body, m.threadRoot, fileIds(m));
        scheduler.request();
      },
      onResync: (partition) =>
        scheduler.request(partition === undefined ? undefined : inPartition(partition, connection.partitionCount)),
    });
    conn.current = connection;
    connection.start();
    // Away when the tab is hidden or there's been no input for 5 minutes (plan §5.9).
    let idleTimer: ReturnType<typeof setTimeout> | undefined;
    const activity = () => {
      connection.setActive(document.visibilityState === "visible");
      clearTimeout(idleTimer);
      idleTimer = setTimeout(() => connection.setActive(false), IDLE_AFTER_MS);
    };
    const inputs = ["pointerdown", "keydown", "wheel", "touchstart"] as const;
    for (const e of inputs) window.addEventListener(e, activity, { passive: true });
    activity();
    const onVisible = () => {
      activity();
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
      for (const e of inputs) window.removeEventListener(e, activity);
      clearTimeout(idleTimer);
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

  /** Watch these users' presence (people on screen); the connection keeps the set. */
  const watchPresence = (userIds: string[]) => conn.current?.watchPresence(userIds);
  const presenceOf = (userId: string): Presence => presence[userId] ?? "offline";

  /**
   * The channel on screen (null: none, or the tab is hidden). Large channels send full
   * events only for it; opening one that moved on meanwhile fetches what it missed.
   */
  const view = (channelId: string | null) => {
    conn.current?.view(channelId);
    const channel = channelId ? latest.current.channels[channelId] : undefined;
    if (channel && needsCatchUp(channel)) syncs.current?.request((id) => id === channelId);
  };

  /** Who else is typing in the channel now. */
  const typingIn = (channelId: string) => typing.current.who(channelId);
  /** Call on every keystroke in the composer; the connection throttles. */
  const notifyTyping = (channelId: string) => conn.current?.typing(channelId);

  /** A message, or a thread reply when `threadRoot` is given (ADR-0005), with files already uploaded. */
  const send = (channelId: string, body: string, threadRoot?: ThreadRootRef, attachments: Attachment[] = []) => {
    const clientMsgId = crypto.randomUUID();
    dispatch({ type: "send", channelId, clientMsgId, body, at: Date.now(), threadRoot, attachments });
    // If offline it goes out on reconnect.
    conn.current?.send(channelId, clientMsgId, body, threadRoot, attachments.map((a) => a.fileId));
  };

  /** Uploads a file to the channel; attach the result to the next send (ADR-0024). */
  const upload = (channelId: string, file: File) =>
    session.accessToken().then((token) => api.uploadFile(token, channelId, file, file.name));

  /** Loads a thread's replies (live ones keep arriving as events). */
  const openThread = (channelId: string, root: ThreadRootRef) =>
    void session
      .accessToken()
      .then((token) => api.thread(token, channelId, root.msgSeq))
      .then(({ replies }) => dispatch({ type: "threadLoaded", channelId, root, replies }))
      .catch(() => undefined); // shown from what we have; the next open tries again

  // Scrolling up: one earlier page at a time, anchored at the oldest event we have.
  const loadingEarlier = useRef(false);
  const [earlierTick, setEarlierTick] = useState(0);
  const loadEarlier = (channelId: string) => {
    if (loadingEarlier.current) return; // at most one request in flight
    const channel = latest.current.channels[channelId];
    if (!channel || channel.hasEarlier === false || !channel.earliestSeq) return;
    const earliestSeq = channel.earliestSeq;
    loadingEarlier.current = true;
    setEarlierTick((n) => n + 1);
    void session
      .accessToken()
      .then((token) => api.eventsBefore(token, channelId, earliestSeq, PAGE_SIZE))
      .then((page) => {
        const last = page.events.at(-1);
        dispatch({
          type: "history",
          channelId,
          events: page.events,
          through: last ? Number(last.eventSeq) : 0,
          at: Date.now(),
          hasEarlier: page.hasMore,
          ...(page.truncated && { earlierGap: true }),
        });
      })
      .catch(() => undefined) // the scroll stays put; the next try loads the page
      .finally(() => {
        loadingEarlier.current = false;
        setEarlierTick((n) => n + 1);
      });
  };

  const nameChannel = (channelId: string, name: string) => dispatch({ type: "channelNamed", channelId, name });

  // Read positions (ADR-0012): shown at once, applied by the user's readstate owner.
  const markRead = (channelId: string) => {
    const channel = latest.current.channels[channelId];
    if (!channel || channel.msgSeq <= channel.readSeq) return;
    const msgSeq = channel.msgSeq;
    dispatch({ type: "markRead", channelId, msgSeq });
    // DMs: the other members see "Seen" (ADR-0012; the fan-out ignores other channels).
    if (channel.kind === "dm" || channel.kind === "group_dm") conn.current?.receipt(channelId, msgSeq);
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
    conn.current?.send(message.channelId, newKey, message.body, message.threadRoot, fileIds(message));
  };

  const presenceKnown = (userId: string) => userId in presence;

  // Edits (shown at once, rolled back with one quiet line on failure).
  const editMessage = (channelId: string, msgSeq: number, body: string) => {
    const list = latest.current.messages[channelId] ?? [];
    const original = list.find((m) => m.msgSeq === msgSeq)?.body ?? "";
    dispatch({ type: "editLocal", channelId, msgSeq, body, at: Date.now() });
    void session
      .accessToken()
      .then((token) => api.editMessage(token, channelId, msgSeq, body, crypto.randomUUID()))
      .catch(() => dispatch({ type: "editFailed", channelId, msgSeq, body: original }));
  };

  // Preload the most recent channels after sign-in: fetch what they missed while away.
  const preloaded = useRef(false);
  const preloadRecent = (ids: string[]) => {
    if (preloaded.current || ids.length === 0) return;
    preloaded.current = true;
    syncs.current?.request((id) => ids.includes(id));
  };

  return { state, status, syncError, send, upload, nameChannel, markRead, setMuted, retry, typingIn, notifyTyping, react, openThread, watchPresence, presenceOf, presenceKnown, view, editMessage, preloadRecent, loadEarlier, loadingEarlier: earlierTick > 0 && loadingEarlier.current };
}

const fileIds = (m: Message) => (m.attachments ?? []).map((a) => a.fileId);
