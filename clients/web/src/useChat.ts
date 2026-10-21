// SPDX-License-Identifier: Apache-2.0
// Chat state for the logged-in user: the reducer from client-core, fed by the gateway
// connection, saved to localStorage so a reload keeps channels and messages.

import { useEffect, useReducer, useRef, useState } from "react";
import {
  Connection,
  initialState,
  reduce,
  unsent,
  type Action,
  type ChatState,
  type ConnectionStatus,
} from "@xmsg/client-core";
import { gatewayUrl, safe, session } from "./session.ts";

const stateKey = (me: string) => `xmsg.state.${me}`;

function load(me: string): ChatState {
  const saved = safe(() => localStorage.getItem(stateKey(me)));
  const parsed = saved ? safe(() => JSON.parse(saved) as ChatState) : undefined;
  return parsed?.me === me ? parsed : initialState(me);
}

export function useChat(me: string) {
  const [state, dispatch] = useReducer((s: ChatState, a: Action) => reduce(s, a), me, load);
  const [status, setStatus] = useState<ConnectionStatus>("connecting");
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
        // Back online: resend the outbox (the sequencer drops repeats).
        if (s === "online") for (const m of unsent(latest.current)) connection.send(m.channelId, m.key, m.body);
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

  return { state, status, send, nameChannel };
}
