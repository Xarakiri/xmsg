// SPDX-License-Identifier: Apache-2.0
// Chat state for the logged-in user: the reducer from client-core, fed by the gateway
// connection, saved to localStorage so a reload keeps channels and messages.

import { useEffect, useReducer, useRef, useState } from "react";
import {
  Connection,
  initialState,
  reduce,
  sync,
  unsent,
  type Action,
  type ChatState,
  type ConnectionStatus,
} from "@xmsg/client-core";
import { api, gatewayUrl, safe, session } from "./session.ts";

// Versioned: a saved state from an older client shape is dropped and re-synced.
const stateKey = (me: string) => `xmsg.state.v2.${me}`;

function load(me: string): ChatState {
  const saved = safe(() => localStorage.getItem(stateKey(me)));
  const parsed = saved ? safe(() => JSON.parse(saved) as ChatState) : undefined;
  return parsed?.me === me ? parsed : initialState(me);
}

export function useChat(me: string) {
  const [state, dispatch] = useReducer((s: ChatState, a: Action) => reduce(s, a), me, load);
  const [status, setStatus] = useState<ConnectionStatus>("connecting");
  const [syncError, setSyncError] = useState(false);
  const conn = useRef<Connection | null>(null);
  const latest = useRef(state);
  latest.current = state;

  useEffect(() => {
    const connection = new Connection({
      url: gatewayUrl(),
      token: () => session.accessToken(),
      onEvent: (event) => dispatch({ type: "event", event, at: Date.now() }),
      onAck: (clientMsgId) => dispatch({ type: "ack", clientMsgId }),
      onSendRejected: (clientMsgId, reason, detail) => dispatch({ type: "sendRejected", clientMsgId, reason, detail }),
      onStatus: (s) => {
        setStatus(s);
        if (s !== "online") return;
        // Back online: resend the outbox (the sequencer drops repeats), then catch up on
        // what happened meanwhile. Live events keep flowing during the sync.
        for (const m of unsent(latest.current)) connection.send(m.channelId, m.key, m.body);
        sync({ api, token: () => session.accessToken(), state: () => latest.current, dispatch })
          .then(() => setSyncError(false))
          .catch(() => setSyncError(true));
      },
    });
    conn.current = connection;
    connection.start();
    return () => connection.stop();
  }, [me]);

  useEffect(() => {
    const id = setTimeout(() => safe(() => localStorage.setItem(stateKey(me), JSON.stringify(state))), 200);
    return () => clearTimeout(id);
  }, [me, state]);

  const send = (channelId: string, body: string) => {
    const clientMsgId = crypto.randomUUID();
    dispatch({ type: "send", channelId, clientMsgId, body, at: Date.now() });
    conn.current?.send(channelId, clientMsgId, body); // if offline it goes out on reconnect
  };

  const nameChannel = (channelId: string, name: string) => dispatch({ type: "channelNamed", channelId, name });

  return { state, status, syncError, send, nameChannel };
}
