// SPDX-License-Identifier: Apache-2.0
// Client chat state: a pure reducer over server events and local actions. Plain JSON
// (IDs as decimal strings, sequence numbers as numbers), so it can be saved and restored.

import type { SequencedEvent } from "./gen/xmsg/v1/events_pb.js";
import { ChannelType, RejectReason } from "./gen/xmsg/v1/enums_pb.js";

export type NotifyLevel = "all" | "mentions" | "nothing";

export type ChannelKind = "public" | "private" | "dm" | "group_dm" | "broadcast" | "unknown";

export interface Channel {
  id: string;
  kind: ChannelKind;
  name: string;
  members: string[];
  /** Highest event_seq seen (live or from history). */
  eventSeq: number;
  /** Every event up to here has been applied: sync fetches what comes after it. */
  syncedSeq: number;
  /** event_seq of the last membership change applied; older ones are ignored. */
  membershipSeq: number;
  /** The channel's latest message (msg_seq) known to this client. */
  msgSeq: number;
  /** Read up to here (ADR-0012); unread = msgSeq - readSeq. */
  readSeq: number;
  /** Personal settings (ADR-0016). Muted channels show no count. */
  muted: boolean;
  notifyLevel: NotifyLevel;
  /** The current user was removed or left. */
  left: boolean;
  /** msg_seqs of messages mentioning me (or `@channel`); those after readSeq are unread. */
  mentions: number[];
}

export type MessageStatus = "pending" | "sent" | "confirmed" | "rejected" | "failed";

/**
 * How long the outbox resends a message on its own (plan §9.2): inside the sequencer's
 * 15-minute dedupe window, so a resend can never be sequenced twice. After that it is
 * `failed` and the user retries it under a new client_msg_id.
 */
export const OUTBOX_RETRY_MS = 10 * 60_000;

export interface Message {
  /** client_msg_id for own messages, otherwise `seq:<msg_seq>`. */
  key: string;
  channelId: string;
  authorId: string;
  body: string;
  /** 0 until the sequencer has ordered it. */
  msgSeq: number;
  /** The sequencer's message_id, once known (a thread reply names its root by it). */
  messageId?: string;
  status: MessageStatus;
  error?: string;
  at: number;
  /** Set once edited (ms). */
  editedAt?: number;
  /** event_seq of the edit the body comes from, so a redelivered older edit can't win. */
  editSeq?: number;
  /** Deleted messages keep their place with no body. */
  deleted?: boolean;
  /** Reactions by emoji (ADR-0015); absent until the first one. */
  reactions?: Record<string, Reaction>;
  /** Replies in its thread (root messages only; absent until the first one). */
  replyCount?: number;
  /**
   * Set on thread replies: the root they belong to. A reply lives in the messages list
   * under `threadKey(channelId, root.msgSeq)`, and its `msgSeq` holds its reply_seq.
   */
  threadRoot?: ThreadRootRef;
}

export interface ThreadRootRef {
  msgSeq: number;
  messageId: string;
}

/** Where a thread's replies are kept in `ChatState.messages`. */
export function threadKey(channelId: string, rootMsgSeq: number): string {
  return `${channelId}#${rootMsgSeq}`;
}

export interface Reaction {
  /** Users with this reaction, from the server (or adjusted locally until it answers). */
  count: number;
  /** Whether I reacted. */
  mine: boolean;
  /** event_seq of the server update this comes from (0: only a local change so far). */
  seq: number;
}

export interface ChatState {
  me: string;
  channels: Record<string, Channel>;
  messages: Record<string, Message[]>;
}

export type Action =
  | { type: "event"; event: SequencedEvent; at: number }
  | { type: "send"; channelId: string; clientMsgId: string; body: string; at: number; threadRoot?: ThreadRootRef }
  /** A thread's replies from the server (opening the thread). */
  | { type: "threadLoaded"; channelId: string; root: ThreadRootRef; replies: LoadedReply[] }
  | { type: "ack"; clientMsgId: string }
  | { type: "sendRejected"; clientMsgId: string; reason: RejectReason; detail: string }
  | { type: "channelNamed"; channelId: string; name: string }
  /** The server's list of my channels (sync, cheap step). */
  | { type: "channelList"; channels: ListedChannel[] }
  /** Read positions from the server (sync); merged with max, like heads (plan §6.6). */
  | { type: "readPositions"; positions: { channelId: string; msgSeq: number }[] }
  /** Own messages unsent for longer than OUTBOX_RETRY_MS stop being resent. */
  | { type: "expire"; now: number }
  /** The user retries a failed message: it goes out again under a new client_msg_id. */
  | { type: "retry"; oldKey: string; newKey: string; at: number }
  /** Personal settings from the server, or one just changed (shown at once). */
  | { type: "settings"; settings: { channelId: string; muted: boolean; notifyLevel: NotifyLevel }[] }
  /** The user read the channel up to here (shown at once; the server applies it shortly). */
  | { type: "markRead"; channelId: string; msgSeq: number }
  /** A contiguous page of a channel's events, ending at `through` (sync, lazy step). */
  | { type: "history"; channelId: string; events: SequencedEvent[]; through: number; at: number }
  /** The server says we're no longer a member (history refused). */
  | { type: "lostAccess"; channelId: string }
  /** I added or removed a reaction: shown at once, the server's count follows (~1 s). */
  | { type: "react"; channelId: string; msgSeq: number; emoji: string; add: boolean }
  /** Unread mentions from the server (sync); merged as a set, so repeats change nothing. */
  | { type: "mentions"; mentions: { channelId: string; msgSeqs: number[] }[] };

export interface LoadedReply {
  replySeq: number;
  messageId: string;
  authorId: string;
  body: string;
}

export interface ListedChannel {
  id: string;
  kind: ChannelKind;
  name: string;
  eventSeq: number;
  msgSeq: number;
}

export function initialState(me: string): ChatState {
  return { me, channels: {}, messages: {} };
}

/**
 * A state saved by an older client, brought up to date: channel fields added since get
 * their defaults (so a saved channel never lacks, say, `notifyLevel`). Anything that
 * isn't this user's state starts fresh.
 */
export function restore(saved: unknown, me: string): ChatState {
  const s = saved as Partial<ChatState> | null;
  if (!s || s.me !== me || typeof s.channels !== "object" || s.channels === null) return initialState(me);
  const channels: Record<string, Channel> = {};
  for (const [id, c] of Object.entries(s.channels)) {
    channels[id] = { ...emptyChannel(id, c.kind ?? "unknown"), ...c };
  }
  return { me, channels, messages: s.messages ?? {} };
}

const KINDS: Record<number, ChannelKind> = {
  [ChannelType.PUBLIC]: "public",
  [ChannelType.PRIVATE]: "private",
  [ChannelType.DM]: "dm",
  [ChannelType.GROUP_DM]: "group_dm",
  [ChannelType.BROADCAST]: "broadcast",
};

export function rejectText(reason: RejectReason, detail = ""): string {
  const text: Record<number, string> = {
    [RejectReason.NOT_A_MEMBER]: "You are not a member of this channel",
    [RejectReason.UNKNOWN_CHANNEL]: "Unknown channel",
    [RejectReason.FORBIDDEN]: "Not allowed here",
    [RejectReason.INVALID]: "Invalid message",
    [RejectReason.UNAVAILABLE]: "Service unavailable",
  };
  return text[reason] ?? (detail || "Rejected");
}

export function reduce(state: ChatState, action: Action): ChatState {
  switch (action.type) {
    case "send": {
      const message: Message = {
        key: action.clientMsgId,
        channelId: action.channelId,
        authorId: state.me,
        body: action.body,
        msgSeq: 0,
        status: "pending",
        at: action.at,
        ...(action.threadRoot && { threadRoot: action.threadRoot }),
      };
      const listKey = action.threadRoot ? threadKey(action.channelId, action.threadRoot.msgSeq) : action.channelId;
      return withMessages(state, listKey, (list) => [...list, message]);
    }
    case "threadLoaded": {
      const key = threadKey(action.channelId, action.root.msgSeq);
      return withMessages(state, key, (list) => {
        const known = new Set(list.filter((m) => m.msgSeq > 0).map((m) => m.msgSeq));
        const loaded: Message[] = action.replies
          .filter((r) => !known.has(r.replySeq))
          .map((r) => ({
            key: `reply:${r.replySeq}`,
            channelId: action.channelId,
            authorId: r.authorId,
            body: r.body,
            msgSeq: r.replySeq,
            messageId: r.messageId,
            status: "confirmed",
            at: 0,
            threadRoot: action.root,
          }));
        return loaded.length ? sortMessages([...list, ...loaded]) : list;
      });
    }
    case "ack":
      return updateOwn(state, action.clientMsgId, (m) => (m.status === "pending" ? { ...m, status: "sent" } : m));
    case "sendRejected":
      return updateOwn(state, action.clientMsgId, (m) => ({
        ...m,
        status: "rejected",
        error: rejectText(action.reason, action.detail),
      }));
    case "channelNamed": {
      const channel = state.channels[action.channelId];
      if (!channel || channel.name) return state;
      return withChannel(state, { ...channel, name: action.name });
    }
    case "event":
      return applyEvent(state, action.event, action.at);
    case "channelList": {
      let next = state;
      for (const listed of action.channels) {
        const known = next.channels[listed.id];
        next = withChannel(next, {
          ...(known ?? emptyChannel(listed.id, listed.kind)),
          kind: known && known.kind !== "unknown" ? known.kind : listed.kind,
          name: known?.name || listed.name,
          msgSeq: Math.max(known?.msgSeq ?? 0, listed.msgSeq),
          left: false,
        });
      }
      return next;
    }
    case "history": {
      let next = action.events.reduce((s, e) => applyEvent(s, e, action.at), state);
      const channel = next.channels[action.channelId];
      if (channel && action.through > channel.syncedSeq) {
        next = withChannel(next, { ...channel, syncedSeq: action.through, eventSeq: Math.max(channel.eventSeq, action.through) });
      }
      return next;
    }
    case "react": {
      return withMessages(state, action.channelId, (list) =>
        list.map((m) => {
          if (m.msgSeq !== action.msgSeq || m.deleted) return m;
          const current = m.reactions?.[action.emoji] ?? { count: 0, mine: false, seq: 0 };
          if (current.mine === action.add) return m;
          const next = { ...current, mine: action.add, count: Math.max(0, current.count + (action.add ? 1 : -1)) };
          return { ...m, reactions: withReaction(m.reactions, action.emoji, next) };
        }),
      );
    }
    case "mentions": {
      let next = state;
      for (const m of action.mentions) {
        const channel = next.channels[m.channelId];
        if (channel) next = withChannel(next, withMentions(channel, m.msgSeqs));
      }
      return next;
    }
    case "readPositions": {
      let next = state;
      for (const p of action.positions) {
        const channel = next.channels[p.channelId];
        if (channel && p.msgSeq > channel.readSeq) next = withChannel(next, withMentions({ ...channel, readSeq: p.msgSeq }, []));
      }
      return next;
    }
    case "settings": {
      let next = state;
      for (const s of action.settings) {
        const channel = next.channels[s.channelId];
        if (channel) next = withChannel(next, { ...channel, muted: s.muted, notifyLevel: s.notifyLevel });
      }
      return next;
    }
    case "expire": {
      let next = state;
      for (const [channelId, list] of Object.entries(state.messages)) {
        if (!list.some((m) => isStale(m, state.me, action.now))) continue;
        next = withMessages(next, channelId, (l) =>
          l.map((m) => (isStale(m, state.me, action.now) ? { ...m, status: "failed" as const, error: "Not sent" } : m)),
        );
      }
      return next;
    }
    case "retry":
      return updateOwn(state, action.oldKey, (m) =>
        m.status === "failed" ? { ...m, key: action.newKey, status: "pending", error: undefined, at: action.at } : m,
      );
    case "markRead": {
      const channel = state.channels[action.channelId];
      if (!channel || action.msgSeq <= channel.readSeq) return state;
      return withChannel(state, withMentions({ ...channel, readSeq: action.msgSeq }, []));
    }
    case "lostAccess": {
      const channel = state.channels[action.channelId];
      return channel ? withChannel(state, { ...channel, left: true }) : state;
    }
  }
}

function emptyChannel(id: string, kind: ChannelKind): Channel {
  return { id, kind, name: "", members: [], eventSeq: 0, syncedSeq: 0, membershipSeq: 0, msgSeq: 0, readSeq: 0, muted: false, notifyLevel: "all", left: false, mentions: [] };
}

/** Adds mentions, keeping only unread ones (after readSeq), ascending, without repeats. */
function withMentions(channel: Channel, seqs: number[]): Channel {
  const all = [...new Set([...channel.mentions, ...seqs])].filter((s) => s > channel.readSeq).sort((a, b) => a - b);
  if (all.length === channel.mentions.length && all.every((s, i) => s === channel.mentions[i])) return channel;
  return { ...channel, mentions: all };
}

/** Records that `seq` was applied: the highest seen, and contiguous progress. */
function seen(channel: Channel, seq: number): Channel {
  return {
    ...channel,
    eventSeq: Math.max(channel.eventSeq, seq),
    syncedSeq: seq === channel.syncedSeq + 1 ? seq : channel.syncedSeq,
  };
}

function applyEvent(state: ChatState, e: SequencedEvent, at: number): ChatState {
  const channelId = e.channelId.toString();
  const actor = e.actorId.toString();
  const payload = e.payload;

  // Rejections aren't channel events (event_seq 0) and only reach the actor.
  if (payload.case === "rejected") {
    if (e.requestId || actor !== state.me) return state; // command results come over HTTP
    return updateOwn(state, e.dedupeId, (m) => ({
      ...m,
      status: "rejected",
      error: rejectText(payload.value.reason, payload.value.detail),
    }));
  }

  const eventSeq = Number(e.eventSeq);
  const kind = KINDS[e.channelType] ?? "unknown";
  // A channel we hear about first through a later event (e.g. we were invited).
  let channel = state.channels[channelId] ?? emptyChannel(channelId, kind);
  if (channel.kind === "unknown") channel = { ...channel, kind };
  const membershipChange = payload.case === "channelCreated" || payload.case === "memberJoined" || payload.case === "memberLeft";
  if (membershipChange && eventSeq <= channel.membershipSeq) {
    // Older than the membership we already have (history arriving after live events).
    return withChannel(state, seen(channel, eventSeq));
  }
  if (membershipChange) channel = { ...channel, membershipSeq: eventSeq };
  channel = seen(channel, eventSeq);

  switch (payload.case) {
    case "channelCreated": {
      const members = payload.value.members.map((m) => m.userId.toString());
      return withChannel(state, {
        ...channel,
        name: channel.name || payload.value.name,
        members: members.length ? members : payload.value.initialMemberIds.map(String),
      });
    }
    case "memberJoined": {
      const user = payload.value.userId.toString();
      // Joining: what came before isn't unread.
      if (user === state.me) channel = { ...channel, readSeq: Math.max(channel.readSeq, channel.msgSeq) };
      const members = channel.members.includes(user) ? channel.members : [...channel.members, user];
      return withChannel(state, { ...channel, members, left: user === state.me ? false : channel.left });
    }
    case "memberLeft": {
      const user = payload.value.userId.toString();
      return withChannel(state, {
        ...channel,
        members: channel.members.filter((m) => m !== user),
        left: user === state.me ? true : channel.left,
      });
    }
    case "messageCreated": {
      const msgSeq = Number(e.msgSeq);
      const own = actor === state.me && e.dedupeId !== "";
      const incoming: Message = {
        key: own ? e.dedupeId : `seq:${msgSeq}`,
        channelId,
        authorId: actor,
        body: payload.value.body,
        msgSeq,
        messageId: payload.value.messageId.toString(),
        status: "confirmed",
        at,
      };
      // Own messages count as read (the server's feeder does the same, ADR-0012).
      let updated: Channel = {
        ...channel,
        msgSeq: Math.max(channel.msgSeq, msgSeq),
        readSeq: actor === state.me ? Math.max(channel.readSeq, msgSeq) : channel.readSeq,
      };
      const mentionsMe = payload.value.mentionsChannel || payload.value.mentionedUserIds.some((u) => u.toString() === state.me);
      updated = withMentions(updated, mentionsMe && actor !== state.me ? [msgSeq] : []);
      const next = withChannel(state, updated);
      return withMessages(next, channelId, (list) => {
        const rest = list.filter((m) => m.key !== incoming.key && !(m.msgSeq !== 0 && m.msgSeq === msgSeq));
        const known = list.find((m) => m.key === incoming.key || (m.msgSeq !== 0 && m.msgSeq === msgSeq));
        // A replayed creation never undoes a later edit or delete we already have.
        let merged: Message = known ? { ...incoming, at: known.at } : incoming;
        if (known?.deleted || known?.editSeq) {
          const { editedAt, editSeq, deleted, body } = known;
          merged = { ...merged, body, ...(editedAt && { editedAt }), ...(editSeq && { editSeq }), ...(deleted && { deleted }) };
        }
        return sortMessages([...rest, merged]);
      });
    }
    case "messageEdited": {
      const edit = payload.value;
      const next = withChannel(state, channel);
      return withMessages(next, channelId, (list) =>
        list.map((m) =>
          m.msgSeq === Number(edit.msgSeq) && !m.deleted && (m.editSeq ?? 0) < eventSeq
            ? { ...m, body: edit.body, editedAt: Number(edit.editedAtMs), editSeq: eventSeq }
            : m,
        ),
      );
    }
    case "threadReply": {
      const r = payload.value;
      const root: ThreadRootRef = { msgSeq: Number(r.rootMsgSeq), messageId: r.rootMessageId.toString() };
      const replySeq = Number(r.replySeq);
      const own = actor === state.me && e.dedupeId !== "";
      const incoming: Message = {
        key: own ? e.dedupeId : `reply:${replySeq}`,
        channelId,
        authorId: actor,
        body: r.body,
        msgSeq: replySeq,
        messageId: r.messageId.toString(),
        status: "confirmed",
        at,
        threadRoot: root,
      };
      // The root shows how many replies it has (absolute, so repeats change nothing).
      let next = withMessages(withChannel(state, channel), channelId, (list) =>
        list.map((m) =>
          m.msgSeq === root.msgSeq && (m.replyCount ?? 0) < Number(r.replyCount) ? { ...m, replyCount: Number(r.replyCount) } : m,
        ),
      );
      next = withMessages(next, threadKey(channelId, root.msgSeq), (list) => {
        const rest = list.filter((m) => m.key !== incoming.key && !(m.msgSeq !== 0 && m.msgSeq === replySeq));
        const mine = list.find((m) => m.key === incoming.key);
        return sortMessages([...rest, mine ? { ...incoming, at: mine.at } : incoming]);
      });
      return next;
    }
    case "reactionsChanged": {
      const r = payload.value;
      const target = Number(r.msgSeq);
      const next = withChannel(state, channel);
      return withMessages(next, channelId, (list) =>
        list.map((m) => {
          if (m.msgSeq !== target) return m;
          const current = m.reactions?.[r.emoji];
          // A replayed older update (history after live) never undoes a newer one.
          if (current && current.seq >= eventSeq) return m;
          const me = (ids: bigint[]) => ids.some((u) => u.toString() === state.me);
          const mine = me(r.added) ? true : me(r.removed) ? false : (current?.mine ?? false);
          return { ...m, reactions: withReaction(m.reactions, r.emoji, { count: Number(r.count), mine, seq: eventSeq }) };
        }),
      );
    }
    case "messageDeleted": {
      const target = Number(payload.value.msgSeq);
      const next = withChannel(state, channel);
      return withMessages(next, channelId, (list) =>
        list.map((m) => (m.msgSeq === target ? { ...m, body: "", deleted: true } : m)),
      );
    }
    default:
      return withChannel(state, channel);
  }
}

/** Sets one emoji's reaction; an emoji nobody has any more is dropped. */
function withReaction(reactions: Record<string, Reaction> | undefined, emoji: string, r: Reaction): Record<string, Reaction> {
  const next = { ...reactions };
  if (r.count > 0 || r.mine) next[emoji] = r;
  else delete next[emoji];
  return next;
}

/** Ordered by msg_seq; pending messages (0) stay at the end in send order. */
function sortMessages(list: Message[]): Message[] {
  return [...list].sort((a, b) => {
    if (a.msgSeq === 0 && b.msgSeq === 0) return a.at - b.at;
    if (a.msgSeq === 0) return 1;
    if (b.msgSeq === 0) return -1;
    return a.msgSeq - b.msgSeq;
  });
}

function withChannel(state: ChatState, channel: Channel): ChatState {
  return { ...state, channels: { ...state.channels, [channel.id]: channel } };
}

function withMessages(state: ChatState, channelId: string, f: (list: Message[]) => Message[]): ChatState {
  return { ...state, messages: { ...state.messages, [channelId]: f(state.messages[channelId] ?? []) } };
}

function updateOwn(state: ChatState, clientMsgId: string, f: (m: Message) => Message): ChatState {
  for (const [channelId, list] of Object.entries(state.messages)) {
    const index = list.findIndex((m) => m.key === clientMsgId && m.authorId === state.me);
    if (index >= 0) {
      const updated = [...list];
      updated[index] = f(list[index]!);
      return { ...state, messages: { ...state.messages, [channelId]: updated } };
    }
  }
  return state;
}

/** Channels whose server head is past what we have, oldest gap first. */
export function behind(state: ChatState, listed: ListedChannel[]): { id: string; after: number }[] {
  return listed
    .map((l) => ({ id: l.id, head: l.eventSeq, after: state.channels[l.id]?.syncedSeq ?? 0 }))
    .filter((c) => c.head > c.after)
    .map(({ id, after }) => ({ id, after }));
}

function isStale(m: Message, me: string, now: number): boolean {
  return m.authorId === me && m.msgSeq === 0 && (m.status === "pending" || m.status === "sent") && now - m.at > OUTBOX_RETRY_MS;
}

/** Unread messages in a channel (ADR-0012: the head minus the read position). */
export function unread(channel: Channel): number {
  return Math.max(0, channel.msgSeq - channel.readSeq);
}

/** Unread mentions of me (or `@channel`) in the channel. */
export function mentionCount(channel: Channel): number {
  return channel.mentions.filter((s) => s > channel.readSeq).length;
}

/** Messages still waiting for the gateway's ack, oldest first (the outbox to resend on reconnect). */
export function unsent(state: ChatState): Message[] {
  return Object.values(state.messages)
    .flat()
    .filter((m) => m.status === "pending" || m.status === "sent")
    .filter((m) => m.msgSeq === 0)
    .sort((a, b) => a.at - b.at);
}
