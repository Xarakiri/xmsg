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
  const [listed, positions] = await Promise.all([
    api.myChannels(await token()),
    // Read positions are best effort: without them the badges are just higher.
    api.readState(await token()).catch(() => []),
  ]);
  dispatch({ type: "channelList", channels: listed });
  dispatch({ type: "readPositions", positions });
  let fetched = 0;
  const targets = behind(state(), listed).filter(({ id }) => !only || only(id));
  await Promise.all(
    targets.map(async ({ id, after }) => {
      let position = after;
      for (let page = 0; page < MAX_PAGES_PER_CHANNEL; page++) {
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
        if (!result.hasMore || through === position) return;
        position = through;
      }
    }),
  );
  return fetched;
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
