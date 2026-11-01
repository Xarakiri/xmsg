// SPDX-License-Identifier: Apache-2.0
// Catching up after (re)connecting (plan §6.6): list my channels with their heads,
// then fetch events only for channels whose head moved past what we have.
// Live events keep arriving meanwhile; the reducer applies both in any order.

import { ApiError, type Api } from "./api.ts";
import { partition as partitionOf } from "./partition.ts";
import { behind, type Action, type ChatState } from "./state.ts";

export const MAX_PAGES_PER_CHANNEL = 5;
export const PAGE_SIZE = 200;

export interface SyncDeps {
  api: Api;
  token(): Promise<string>;
  state(): ChatState;
  dispatch(action: Action): void;
  /** Only fetch events for channels this accepts (a partition-scoped resync). */
  only?(channelId: string): boolean;
}

/** `only` for a resync of one partition (ADR-0021: the same partition function as Kafka). */
export function inPartition(partition: number, partitionCount: number): (channelId: string) => boolean {
  return (channelId) => partitionOf(BigInt(channelId), partitionCount) === partition;
}

/** Returns how many events were fetched. Errors other than lost access propagate. */
export async function sync({ api, token, state, dispatch, only }: SyncDeps): Promise<number> {
  const [listed, positions, settings, mentions] = await Promise.all([
    api.myChannels(await token()),
    // Best effort: without read positions the badges are just higher, without settings
    // muted channels just look unmuted until the next sync.
    api.readState(await token()).catch(() => []),
    api.settings(await token()).catch(() => []),
    api.mentions(await token()).catch(() => []),
  ]);
  dispatch({ type: "channelList", channels: listed });
  dispatch({ type: "readPositions", positions });
  dispatch({ type: "settings", settings });
  // After the positions, so mentions already read are dropped.
  dispatch({ type: "mentions", mentions });
  let fetched = 0;
  const targets = behind(state(), listed).filter(({ id }) => !only || only(id));
  await Promise.all(
    targets.map(async ({ id, after, head }) => {
      let position = after;
      if (position === 0 && head > 0) {
        // Opening a channel with no local events: start from the page with the first
        // unread message (or the newest page when caught up), never from sequence 0.
        position = await openNearUnread(api, token, state, dispatch, id, head, (n) => (fetched += n));
        if (state().channels[id]?.left) return; // refused while walking back (404)
      }
      // Catch up forward from where the backward walk (or a previous sync) got us.
      for (let page = 0; position < head && page < MAX_PAGES_PER_CHANNEL; page++) {
        let result;
        try {
          result = await api.events(await token(), id, position, PAGE_SIZE);
        } catch (e) {
          if (e instanceof ApiError && e.status === 404) {
            dispatch({ type: "lostAccess", channelId: id });
            return;
          }
          throw e;
        }
        const last = result.events.at(-1);
        // A truncated page starts after a gap (events older than the 30-day log are gone);
        // what follows is continuous, so it still counts as synced up to its last event.
        const through = last ? Number(last.eventSeq) : position;
        dispatch({ type: "history", channelId: id, events: result.events, through, at: Date.now() });
        fetched += result.events.length;
        if (!result.hasMore || through === position) break;
        position = through;
      }
      // Loaded everything there is from the front: the channel's start is local.
      const channel = state().channels[id];
      if (channel && !channel.left && channel.syncedSeq > 0 && (channel.earliestSeq === undefined || channel.earliestSeq <= 1)) {
        dispatch({ type: "history", channelId: id, events: [], through: 0, at: Date.now(), hasEarlier: false });
      }
    }),
  );
  return fetched;
}

/**
 * Loads the newest backward pages until the page holding the first unread message (or
 * just the newest page when there's nothing unread), so opening a long channel doesn't
 * fetch from sequence 0. Pages dispatch newest-first; the first one's `through` is the
 * head, so `syncedSeq` lands there and forward catch-up has nothing to do. Returns the
 * position to continue forward from.
 */
async function openNearUnread(
  api: Api,
  token: SyncDeps["token"],
  state: () => ChatState,
  dispatch: (action: Action) => void,
  id: string,
  head: number,
  addFetched: (n: number) => void,
): Promise<number> {
  const channel = state().channels[id];
  // msg_seq of the first unread message, if any (read positions and heads are merged by now).
  const firstUnread = channel && channel.msgSeq > channel.readSeq ? channel.readSeq + 1 : null;
  let before = head + 1;
  for (let page = 0; page < MAX_PAGES_PER_CHANNEL; page++) {
    let result;
    try {
      result = await api.eventsBefore(await token(), id, before, PAGE_SIZE);
    } catch (e) {
      if (e instanceof ApiError && e.status === 404) {
        dispatch({ type: "lostAccess", channelId: id });
        return 0;
      }
      throw e;
    }
    const first = result.events[0];
    const last = result.events.at(-1);
    const through = last ? Number(last.eventSeq) : before - 1;
    dispatch({
      type: "history",
      channelId: id,
      events: result.events,
      through,
      at: Date.now(),
      hasEarlier: result.hasMore,
      ...(result.truncated && { earlierGap: true }),
    });
    addFetched(result.events.length);
    if (!first) break;
    if (!result.hasMore) break;
    if (firstUnread === null) break; // nothing unread: the newest page is enough
    const oldestMsgSeq = result.events
      .filter((e) => e.payload.case === "messageCreated" && e.msgSeq > 0n)
      .reduce((min, e) => Math.min(min, Number(e.msgSeq)), Number.POSITIVE_INFINITY);
    if (oldestMsgSeq <= firstUnread) break; // the first unread is on screen now
    before = Number(first.eventSeq);
  }
  return state().channels[id]?.syncedSeq ?? 0;
}

/** Every 30 minutes in the background (plan §6.6 step 5). */
export const BACKGROUND_SYNC_MS = 30 * 60_000;

/**
 * Runs syncs one at a time. Requests made while one runs are merged into a single
 * follow-up: a full request wins, otherwise the scoped filters are combined.
 */
export class SyncScheduler {
  private running = false;
  /** null: nothing pending; "all": a full sync; else the filters to combine. */
  private pending: null | "all" | ((channelId: string) => boolean)[] = null;
  private readonly run: (only?: (channelId: string) => boolean) => Promise<unknown>;

  constructor(run: (only?: (channelId: string) => boolean) => Promise<unknown>) {
    this.run = run;
  }

  /** `only` absent: everything (reconnect, foreground, timer); present: e.g. one partition. */
  request(only?: (channelId: string) => boolean): void {
    if (!only) this.pending = "all";
    else if (this.pending !== "all") this.pending = [...(this.pending ?? []), only];
    if (!this.running) void this.drain();
  }

  private async drain(): Promise<void> {
    this.running = true;
    while (this.pending) {
      const next = this.pending;
      this.pending = null;
      const only = next === "all" ? undefined : (id: string) => next.some((f) => f(id));
      await this.run(only).catch(() => undefined);
    }
    this.running = false;
  }
}
