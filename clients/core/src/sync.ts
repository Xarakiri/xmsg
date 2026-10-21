// SPDX-License-Identifier: Apache-2.0
// Catching up after (re)connecting (plan §6.6): list my channels with their heads,
// then fetch events only for channels whose head moved past what we have.
// Live events keep arriving meanwhile; the reducer applies both in any order.

import { ApiError, type Api } from "./api.ts";
import { behind, type Action, type ChatState } from "./state.ts";

export const MAX_PAGES_PER_CHANNEL = 5;
export const PAGE_SIZE = 200;

export interface SyncDeps {
  api: Api;
  token(): Promise<string>;
  state(): ChatState;
  dispatch(action: Action): void;
}

/** Returns how many events were fetched. Errors other than lost access propagate. */
export async function sync({ api, token, state, dispatch }: SyncDeps): Promise<number> {
  const listed = await api.myChannels(await token());
  dispatch({ type: "channelList", channels: listed });
  let fetched = 0;
  await Promise.all(
    behind(state(), listed).map(async ({ id, after }) => {
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
