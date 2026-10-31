// SPDX-License-Identifier: Apache-2.0
// Pure chat-log helpers: grouping, date labels, drafts and unread totals. Kept free of
// DOM and React so they run under node --test.

import type { Channel, Message } from "@xmsg/client-core";

const MIN_GROUP_MS = 5 * 60_000;

/** `── today ──`, `── yesterday ──`, then `── 28 oct ──`. */
export function dateLabel(now: number, at: number): string {
  const day = (t: number) => {
    const d = new Date(t);
    return `${d.getFullYear()}-${d.getMonth()}-${d.getDate()}`;
  };
  if (day(at) === day(now)) return "today";
  if (day(at) === day(now - 86_400_000)) return "yesterday";
  const d = new Date(at);
  return `${d.getDate()} ${d.toLocaleDateString([], { month: "short" }).toLowerCase()}`;
}

export interface GroupedMessage {
  message: Message;
  /** First of a run from this author: show avatar, name and time. */
  start: boolean;
  /** Render a `── label ──` separator above this message. */
  dateSeparator?: string;
}

/** Consecutive messages from one sender within 5 minutes collapse to one header. */
export function groupMessages(messages: Message[], now: number): GroupedMessage[] {
  const sorted = [...messages].sort((a, b) => (a.msgSeq || 0) - (b.msgSeq || 0));
  const out: GroupedMessage[] = [];
  let prev: Message | undefined;
  let prevDay = "";
  for (const message of sorted) {
    const day = new Date(message.at).toDateString();
    const start = !prev || prev.authorId !== message.authorId || message.at - prev.at > MIN_GROUP_MS || day !== prevDay;
    const dateSeparator = day !== prevDay ? dateLabel(now, message.at) : undefined;
    out.push({ message, start, dateSeparator });
    prev = message;
    prevDay = day;
  }
  return out;
}

/** Drafts live under this key; thread replies get their own. */
export function draftKey(channelId: string, threadRootMsgSeq?: number | null): string {
  return threadRootMsgSeq ? `xmsg.draft.${channelId}.t${threadRootMsgSeq}` : `xmsg.draft.${channelId}`;
}

/** Sum of unread across the channels that show a count (muted ones don't). */
export function unreadTotal(channels: Channel[]): number {
  return channels.reduce((sum, c) => sum + (c.muted ? 0 : Math.max(0, c.msgSeq - c.readSeq)), 0);
}

export interface AutocompleteItem {
  /** What replaces the trigger text (a mention token, channel name, command). */
  insert: string;
  label: string;
  hint?: string;
}

/** `@` mentions members, `#` names channels, `/` lists commands. */
export function autocomplete(trigger: "@" | "#" | "/", query: string, context: { members: { id: string; name: string }[]; channels: { id: string; name: string }[]; commands: { name: string; hint: string }[] }): AutocompleteItem[] {
  const q = query.toLowerCase();
  const match = (value: string) => value.toLowerCase().includes(q);
  if (trigger === "@") return context.members.filter((m) => match(m.name)).map((m) => ({ insert: `@${m.name} `, label: m.name, hint: m.id }));
  if (trigger === "#") return context.channels.filter((c) => match(c.name)).map((c) => ({ insert: `#${c.name} `, label: c.name }));
  return context.commands.filter((c) => match(c.name)).map((c) => ({ insert: `/${c.name} `, label: `/${c.name}`, hint: c.hint }));
}
