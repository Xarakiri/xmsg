// SPDX-License-Identifier: Apache-2.0
// Display names for the logged-in user's session. One directory per login: what a user
// may see depends on their workspaces, so nothing carries over to the next account.

import { useMemo, useSyncExternalStore } from "react";
import { Directory } from "@xmsg/client-core";
import { api, session } from "./session.ts";

export function useNames(me: string): Directory {
  const directory = useMemo(() => new Directory(async (ids) => api.users(await session.accessToken(), ids)), [me]);
  // Re-render when names arrive; label() asks for unknown IDs as they're rendered.
  useSyncExternalStore(
    (onChange) => directory.subscribe(onChange),
    () => directory.version,
  );
  return directory;
}
