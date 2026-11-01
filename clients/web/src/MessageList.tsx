// SPDX-License-Identifier: Apache-2.0
// The scrolling message log: per-channel scroll positions, stick-to-bottom, a `↓ n new`
// pill when the reader is scrolled up, the `── new ──` divider at the first unread
// message, and date separators between days. Long logs stay fluid through the browser's
// own rendering skip (content-visibility), which needs no measuring.

import { Fragment, useEffect, useLayoutEffect, useRef, useState } from "react";
import type { Channel, Directory, Message, Presence } from "@xmsg/client-core";
import { MessageRow } from "./components.tsx";
import { groupMessages } from "./chatlog.ts";

const BOTTOM_EPS = 48;
/** This close to the top, scrolling asks for the previous page. */
const TOP_LOAD_EPS = 80;

export interface MessageListProps {
  channel: Channel;
  messages: Message[];
  me: string;
  names: Directory;
  /** Messages past this msg_seq count as unread; the list opens at the first of them. */
  newMarker: number;
  presenceOf?: (userId: string) => Presence;
  presenceKnown?: (userId: string) => boolean;
  onOpenThread: (message: Message) => void;
  onReact: (message: Message, emoji: string, add: boolean) => void;
  onEdit: (message: Message, body: string) => Promise<void>;
  onDelete: (message: Message) => void;
  onRetry: (key: string) => void;
  /** The reader reached (or jumped to) the bottom: mark the channel read. */
  onCaughtUp: () => void;
  /** More retained history may exist above the loaded window: load the previous page. */
  onLoadEarlier?: () => void;
  /** False once the retained log's oldest event is loaded (the start line shows). */
  hasEarlier?: boolean;
  /** An earlier page request is in flight. */
  loadingEarlier?: boolean;
  /** Shown above the log when the channel has no messages (invite actions). */
  emptyExtra?: React.ReactNode;
  /** Request editing a message: nonce bump opens the editor for `seq`. */
  editRequest?: { seq: number; nonce: number };
}

export function MessageList({ channel, messages, me, names, newMarker, presenceOf, presenceKnown, onOpenThread, onReact, onEdit, onDelete, onRetry, onCaughtUp, onLoadEarlier, hasEarlier, loadingEarlier, emptyExtra, editRequest }: MessageListProps) {
  const listRef = useRef<HTMLOListElement | null>(null);
  // scrollTop per channel; a channel without an entry opens at its first unread (or bottom).
  const positions = useRef(new Map<string, number>());
  const atBottom = useRef(true);
  const prev = useRef<{ channelId: string; count: number; msgSeq: number }>({ channelId: channel.id, count: 0, msgSeq: 0 });
  const [pill, setPill] = useState(0);
  // Scroll-up paging: where to anchor after an earlier page prepends rows.
  const anchor = useRef<{ height: number; top: number; firstKey: string | undefined } | null>(null);
  const grouped = groupMessages(messages, Date.now());
  const newIndex = newMarker > 0 ? messages.findIndex((m) => m.msgSeq > newMarker && !m.deleted) : -1;

  const scrollToBottom = () => {
    const el = listRef.current;
    if (el) el.scrollTop = el.scrollHeight;
    atBottom.current = true;
    setPill(0);
  };

  // Channel switch: cached messages render in the same frame; the scroll position (or the
  // first unread) is restored right after paint, without a remount.
  useLayoutEffect(() => {
    const el = listRef.current;
    if (!el) return;
    setPill(0);
    anchor.current = null;
    const saved = positions.current.get(channel.id);
    if (saved !== undefined) {
      el.scrollTop = saved;
      atBottom.current = el.scrollHeight - el.scrollTop - el.clientHeight < BOTTOM_EPS;
      return;
    }
    if (newIndex >= 0) {
      const target = el.querySelector<HTMLElement>("[data-new-marker]");
      if (target) {
        el.scrollTop = Math.max(0, target.offsetTop - el.clientHeight / 3);
        atBottom.current = false;
        return;
      }
    }
    el.scrollTop = el.scrollHeight;
    atBottom.current = true;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [channel.id]);

  // An earlier page merged in: keep the visible row exactly where it was by restoring the
  // scroll offset plus the height the prepended rows added. A page that prepended nothing
  // (empty or fully deduplicated) leaves the scroll untouched and clears the anchor.
  useLayoutEffect(() => {
    const el = listRef.current;
    const pending = anchor.current;
    if (!el || !pending || prev.current.channelId !== channel.id) return;
    anchor.current = null;
    if (messages[0]?.key === pending.firstKey) return;
    el.scrollTop = pending.top + (el.scrollHeight - pending.height);
    positions.current.set(channel.id, el.scrollTop);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [messages.length, channel.id]);

  // New messages: stick when at the bottom, count a pill otherwise.
  useEffect(() => {
    const last = messages.at(-1);
    const was = prev.current;
    if (was.channelId !== channel.id) {
      prev.current = { channelId: channel.id, count: messages.length, msgSeq: last?.msgSeq ?? 0 };
      return;
    }
    if (last && last.msgSeq > was.msgSeq) {
      if (atBottom.current) scrollToBottom();
      else setPill((n) => Math.max(n + messages.length - was.count, last.msgSeq - Math.max(newMarker, was.msgSeq)));
    }
    prev.current = { channelId: channel.id, count: messages.length, msgSeq: last?.msgSeq ?? 0 };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [messages.length, channel.id]);

  const onScroll = () => {
    const el = listRef.current;
    if (!el) return;
    positions.current.set(channel.id, el.scrollTop);
    if (el.scrollTop < TOP_LOAD_EPS && hasEarlier !== false && onLoadEarlier && !loadingEarlier) {
      // Near the top: remember where the viewport is, then prepend the previous page.
      anchor.current = { height: el.scrollHeight, top: el.scrollTop, firstKey: messages[0]?.key };
      onLoadEarlier();
    }
    const bottom = el.scrollHeight - el.scrollTop - el.clientHeight < BOTTOM_EPS;
    if (bottom && !atBottom.current) {
      setPill(0);
      onCaughtUp();
    }
    atBottom.current = bottom;
  };

  return (
    <div className="message-log">
      <ol className="message-list" ref={listRef} onScroll={onScroll}>
        {messages.length === 0 && emptyExtra}
        {hasEarlier === false && messages.length > 0 && (
          <li className="date-divider" aria-hidden="true">── start of #{channel.name || channel.id.slice(-6)} ──</li>
        )}
        {loadingEarlier && <li className="date-divider" aria-hidden="true">…</li>}
        {grouped.map(({ message: m, start, dateSeparator }, i) => (
          <Fragment key={m.key}>
            {dateSeparator && <li className="date-divider" aria-hidden="true">── {dateSeparator} ──</li>}
            {i === newIndex && <li className="date-divider new-marker" data-new-marker="true">── new ──</li>}
            <MessageRow
              message={m}
              me={me}
              names={names}
              start={start}
              editRequest={editRequest?.seq === m.msgSeq ? editRequest.nonce : undefined}
              presence={presenceKnown?.(m.authorId) ? presenceOf?.(m.authorId) : undefined}
              onOpenThread={() => onOpenThread(m)}
              onReact={(emoji, add) => onReact(m, emoji, add)}
              onEdit={m.authorId === me && m.msgSeq > 0 && !m.deleted ? (body) => onEdit(m, body) : undefined}
              onDelete={m.authorId === me && m.msgSeq > 0 && !m.deleted ? () => onDelete(m) : undefined}
              onRetry={() => onRetry(m.key)}
            />
          </Fragment>
        ))}
      </ol>
      {pill > 0 && (
        <button className="new-pill" onClick={() => { scrollToBottom(); onCaughtUp(); }}>
          ↓ {pill} new
        </button>
      )}
    </div>
  );
}
