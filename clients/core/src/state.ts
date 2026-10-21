// SPDX-License-Identifier: Apache-2.0
// Client chat state: a pure reducer over server events and local actions. Plain JSON
// (IDs as decimal strings, sequence numbers as numbers), so it can be saved and restored.

import type { SequencedEvent } from "./gen/xmsg/v1/events_pb.js";
import { ChannelType, RejectReason } from "./gen/xmsg/v1/enums_pb.js";

export type ChannelKind = "public" | "private" | "dm" | "group_dm" | "broadcast" | "unknown";

export interface Channel {
  id: string;
  kind: ChannelKind;
  name: string;
  members: string[];
  /** Highest event_seq applied; older or repeated events are ignored. */
  eventSeq: number;
  /** The current user was removed or left. */
  left: boolean;
}

export type MessageStatus = "pending" | "sent" | "confirmed" | "rejected";

export interface Message {
  /** client_msg_id for own messages, otherwise `seq:<msg_seq>`. */
  key: string;
  channelId: string;
  authorId: string;
  body: string;
  /** 0 until the sequencer has ordered it. */
  msgSeq: number;
  status: MessageStatus;
  error?: string;
  at: number;
}

export interface ChatState {
  me: string;
  channels: Record<string, Channel>;
  messages: Record<string, Message[]>;
}

export type Action =
  | { type: "event"; event: SequencedEvent; at: number }
  | { type: "send"; channelId: string; clientMsgId: string; body: string; at: number }
  | { type: "ack"; clientMsgId: string }
  | { type: "sendRejected"; clientMsgId: string; reason: RejectReason; detail: string }
  | { type: "channelNamed"; channelId: string; name: string };

export function initialState(me: string): ChatState {
  return { me, channels: {}, messages: {} };
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
      };
      return withMessages(state, action.channelId, (list) => [...list, message]);
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
  }
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
  let channel = state.channels[channelId];
  if (channel && eventSeq <= channel.eventSeq) return state; // duplicate or replay

  if (payload.case === "channelCreated") {
    const members = payload.value.members.map((m) => m.userId.toString());
    return withChannel(state, {
      id: channelId,
      kind: KINDS[e.channelType] ?? "unknown",
      name: payload.value.name,
      members: members.length ? members : payload.value.initialMemberIds.map(String),
      eventSeq,
      left: false,
    });
  }

  // A channel we hear about first through a later event (e.g. we were invited).
  channel ??= { id: channelId, kind: KINDS[e.channelType] ?? "unknown", name: "", members: [], eventSeq: 0, left: false };
  channel = { ...channel, eventSeq };

  switch (payload.case) {
    case "memberJoined": {
      const user = payload.value.userId.toString();
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
        status: "confirmed",
        at,
      };
      const next = withChannel(state, channel);
      return withMessages(next, channelId, (list) => {
        const rest = list.filter((m) => m.key !== incoming.key && !(m.msgSeq !== 0 && m.msgSeq === msgSeq));
        const mine = list.find((m) => m.key === incoming.key);
        return sortMessages([...rest, mine ? { ...incoming, at: mine.at } : incoming]);
      });
    }
    default:
      return withChannel(state, channel);
  }
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

/** Messages still waiting for the gateway's ack, oldest first (the outbox to resend on reconnect). */
export function unsent(state: ChatState): Message[] {
  return Object.values(state.messages)
    .flat()
    .filter((m) => m.status === "pending" || m.status === "sent")
    .filter((m) => m.msgSeq === 0)
    .sort((a, b) => a.at - b.at);
}
