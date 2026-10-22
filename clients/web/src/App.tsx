// SPDX-License-Identifier: Apache-2.0
import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import { ApiError, unread, type Channel, type ConnectionStatus, type Directory, type Message } from "@xmsg/client-core";
import { api, session } from "./session.ts";
import { useChat } from "./useChat.ts";
import { useNames } from "./useNames.ts";

export function App() {
  const [me, setMe] = useState<string | null>(null);
  const [checking, setChecking] = useState(session.hasRefreshToken);

  // Resume a previous login from the stored refresh token.
  useEffect(() => {
    if (!checking) return;
    session
      .accessToken()
      .then(() => setMe(session.userId))
      .catch(() => undefined)
      .finally(() => setChecking(false));
  }, [checking]);

  if (checking) return <div className="center muted">Loading…</div>;
  if (!me) return <SignIn onSignedIn={setMe} />;
  return (
    <Chat
      key={me}
      me={me}
      onSignOut={async () => {
        await session.logout();
        setMe(null);
      }}
    />
  );
}

function errorText(e: unknown): string {
  if (e instanceof ApiError) return e.status >= 500 ? "Service unavailable, try again" : e.message;
  return "Can't reach the server";
}

function SignIn({ onSignedIn }: { onSignedIn: (userId: string) => void }) {
  const [mode, setMode] = useState<"in" | "up">("in");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [name, setName] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setError("");
    try {
      if (mode === "up") await api.signup(email, password, name);
      session.adopt(await api.login(email, password));
      onSignedIn(session.userId);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="center">
      <form className="card auth" onSubmit={submit}>
        <h1>xmsg</h1>
        <div className="tabs" role="tablist">
          <button type="button" role="tab" aria-selected={mode === "in"} onClick={() => setMode("in")}>
            Sign in
          </button>
          <button type="button" role="tab" aria-selected={mode === "up"} onClick={() => setMode("up")}>
            Create account
          </button>
        </div>
        {mode === "up" && (
          <label>
            Name
            <input value={name} onChange={(e) => setName(e.target.value)} required maxLength={80} autoComplete="name" />
          </label>
        )}
        <label>
          Email
          <input type="email" value={email} onChange={(e) => setEmail(e.target.value)} required autoComplete="email" />
        </label>
        <label>
          Password
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
            minLength={mode === "up" ? 10 : undefined}
            autoComplete={mode === "up" ? "new-password" : "current-password"}
          />
        </label>
        {error && <p className="error">{error}</p>}
        <button className="primary" disabled={busy}>
          {mode === "in" ? "Sign in" : "Create account"}
        </button>
      </form>
    </div>
  );
}

function channelTitle(c: Channel, me: string, names: Directory): string {
  if (c.name) return c.kind === "public" || c.kind === "private" || c.kind === "broadcast" ? `# ${c.name}` : c.name;
  const others = c.members.filter((m) => m !== me);
  if (c.kind === "dm" || c.kind === "group_dm") return others.length ? others.map((m) => names.label(m)).join(", ") : "Direct message";
  return `Channel …${c.id.slice(-6)}`;
}

/** "Ada, Bob, You and 4 more" */
function memberList(members: string[], me: string, names: Directory, shown = 5): string {
  const labels = members.slice(0, shown).map((m) => (m === me ? "You" : names.label(m)));
  const more = members.length - labels.length;
  return more > 0 ? `${labels.join(", ")} and ${more} more` : labels.join(", ");
}

function Chat({ me, onSignOut }: { me: string; onSignOut: () => void }) {
  const { state, status, syncError, send, nameChannel, markRead } = useChat(me);
  const names = useNames(me);
  const [selected, setSelected] = useState<string | null>(null);
  const [panel, setPanel] = useState<"none" | "channel" | "dm">("none");

  const channels = useMemo(() => Object.values(state.channels).filter((c) => !c.left), [state.channels]);
  const rooms = channels.filter((c) => c.kind !== "dm" && c.kind !== "group_dm");
  const dms = channels.filter((c) => c.kind === "dm" || c.kind === "group_dm");
  const current = selected ? state.channels[selected] : undefined;

  // The open channel is read while the tab is visible (debounced, so a burst of
  // messages costs one update; also when the tab comes back).
  const pendingUnread = current ? unread(current) : 0;
  useEffect(() => {
    if (!current || pendingUnread === 0) return;
    const id = setTimeout(() => {
      if (document.visibilityState === "visible") markRead(current.id);
    }, 400);
    const onVisible = () => document.visibilityState === "visible" && markRead(current.id);
    document.addEventListener("visibilitychange", onVisible);
    return () => {
      clearTimeout(id);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, [current?.id, pendingUnread]);

  return (
    <div className="app">
      <aside className="sidebar">
        <header className="me">
          <StatusDot status={status} />
          <div>
            <div className="strong">
              <button className="link strong" title="Change your display name" onClick={() => rename(me, names)}>
                {names.label(me)}
              </button>
              {syncError && <span className="muted small"> · couldn't load history</span>}
            </div>
            <button className="link mono" title="Copy your user ID" onClick={() => navigator.clipboard?.writeText(me)}>
              {me}
            </button>
          </div>
        </header>
        <Section title="Channels" onAdd={() => setPanel("channel")}>
          {rooms.map((c) => (
            <Item key={c.id} active={c.id === selected} onClick={() => setSelected(c.id)} label={channelTitle(c, me, names)} unread={unread(c)} />
          ))}
        </Section>
        <Section title="Direct messages" onAdd={() => setPanel("dm")}>
          {dms.map((c) => (
            <Item key={c.id} active={c.id === selected} onClick={() => setSelected(c.id)} label={channelTitle(c, me, names)} unread={unread(c)} />
          ))}
        </Section>
        <footer>
          <button className="link" onClick={onSignOut}>
            Sign out
          </button>
        </footer>
      </aside>
      <main className="main">
        {panel !== "none" && (
          <CreatePanel
            kind={panel}
            onClose={() => setPanel("none")}
            onCreated={(id, name) => {
              if (name) nameChannel(id, name);
              setSelected(id);
              setPanel("none");
            }}
          />
        )}
        {current ? (
          <Conversation channel={current} me={me} names={names} messages={state.messages[current.id] ?? []} onSend={(b) => send(current.id, b)} />
        ) : (
          <div className="center muted">{channels.length ? "Pick a conversation" : "Create a channel or start a DM"}</div>
        )}
      </main>
    </div>
  );
}

async function rename(me: string, names: Directory) {
  const name = prompt("Your display name", names.profile(me)?.name ?? "")?.trim();
  if (!name) return;
  try {
    names.remember(await api.updateMe(await session.accessToken(), { name }));
  } catch (err) {
    alert(errorText(err));
  }
}

function StatusDot({ status }: { status: ConnectionStatus }) {
  const label = { connecting: "Connecting…", online: "Connected", offline: "Reconnecting…", unauthorized: "Signed out" }[status];
  return <span className={`dot ${status}`} title={label} aria-label={label} />;
}

function Section({ title, onAdd, children }: { title: string; onAdd: () => void; children: React.ReactNode }) {
  return (
    <section>
      <h2>
        {title}
        <button className="icon" onClick={onAdd} aria-label={title === "Channels" ? "New channel" : "New direct message"}>
          +
        </button>
      </h2>
      <ul>{children}</ul>
    </section>
  );
}

function Item({ label, active, unread, onClick }: { label: string; active: boolean; unread: number; onClick: () => void }) {
  const classes = ["item", active && "active", unread > 0 && !active && "unread"].filter(Boolean).join(" ");
  return (
    <li>
      <button className={classes} onClick={onClick} aria-label={unread > 0 ? `${label}, ${unread} unread` : label}>
        <span className="label">{label}</span>
        {unread > 0 && !active && <span className="badge">{unread > 99 ? "99+" : unread}</span>}
      </button>
    </li>
  );
}

const ids = (text: string) => text.split(/[\s,]+/).filter(Boolean);

function CreatePanel({ kind, onClose, onCreated }: { kind: "channel" | "dm"; onClose: () => void; onCreated: (id: string, name?: string) => void }) {
  const [name, setName] = useState("");
  const [type, setType] = useState<"public" | "private" | "broadcast">("public");
  const [members, setMembers] = useState("");
  const [error, setError] = useState("");

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setError("");
    try {
      const token = await session.accessToken();
      const reply = kind === "channel" ? await api.createChannel(token, type, name.trim(), ids(members)) : await api.openDm(token, ids(members));
      onCreated(reply.channel_id, kind === "channel" ? name.trim() : undefined);
    } catch (err) {
      setError(errorText(err));
    }
  };

  return (
    <form className="card panel" onSubmit={submit}>
      <h3>{kind === "channel" ? "New channel" : "New direct message"}</h3>
      {kind === "channel" && (
        <div className="row">
          <label className="grow">
            Name
            <input value={name} onChange={(e) => setName(e.target.value)} required maxLength={80} autoFocus />
          </label>
          <label>
            Type
            <select value={type} onChange={(e) => setType(e.target.value as typeof type)}>
              <option value="public">Public</option>
              <option value="private">Private</option>
              <option value="broadcast">Broadcast</option>
            </select>
          </label>
        </div>
      )}
      <label>
        {kind === "channel" ? "Members (user IDs, optional)" : "User IDs (one for a DM, up to 8 for a group)"}
        <input
          className="mono"
          value={members}
          onChange={(e) => setMembers(e.target.value)}
          required={kind === "dm"}
          autoFocus={kind === "dm"}
          placeholder="106449829163870339, …"
        />
      </label>
      {error && <p className="error">{error}</p>}
      <div className="row end">
        <button type="button" onClick={onClose}>
          Cancel
        </button>
        <button className="primary">{kind === "channel" ? "Create" : "Open"}</button>
      </div>
    </form>
  );
}

function Conversation({
  channel,
  me,
  names,
  messages,
  onSend,
}: {
  channel: Channel;
  me: string;
  names: Directory;
  messages: Message[];
  onSend: (body: string) => void;
}) {
  const [draft, setDraft] = useState("");
  const [note, setNote] = useState("");
  const bottom = useRef<HTMLDivElement>(null);
  // Block bodies: an effect may only return a cleanup function, and scrollIntoView
  // returns a Promise in browsers with scroll promises.
  useEffect(() => {
    void bottom.current?.scrollIntoView({ block: "end" });
  }, [messages.length, channel.id]);
  useEffect(() => {
    setNote("");
  }, [channel.id]);

  const submit = (e?: FormEvent) => {
    e?.preventDefault();
    const body = draft.trim();
    if (!body) return;
    onSend(body);
    setDraft("");
  };

  const membership = async (action: "invite" | "leave") => {
    setNote("");
    try {
      const token = await session.accessToken();
      if (action === "invite") {
        const user = prompt("User ID to invite")?.trim();
        if (!user) return;
        const r = await api.join(token, channel.id, user);
        setNote(r.status === "pending" ? "Invite sent; waiting for confirmation" : "Invited");
      } else {
        await api.remove(token, channel.id, me);
      }
    } catch (err) {
      setNote(errorText(err));
    }
  };

  const canManage = channel.kind !== "dm";

  return (
    <section className="conversation">
      <header className="conv-header">
        <div>
          <div className="strong">{channelTitle(channel, me, names)}</div>
          <div className="muted small" title={channel.members.map((m) => (m === me ? "You" : names.label(m))).join(", ")}>
            {channel.members.length} member{channel.members.length === 1 ? "" : "s"} · {channel.kind.replace("_", " ")} · {memberList(channel.members, me, names)}
          </div>
        </div>
        {canManage && (
          <div className="row">
            <button onClick={() => membership("invite")}>Invite</button>
            <button onClick={() => membership("leave")}>Leave</button>
          </div>
        )}
      </header>
      {note && <p className="note">{note}</p>}
      <ol className="messages">
        {messages.map((m) => (
          <li key={m.key} className={`message ${m.authorId === me ? "own" : ""} ${m.status}`}>
            <div className="meta">
              <span className="strong">{m.authorId === me ? "You" : names.label(m.authorId)}</span>
              <time className="muted small">{new Date(m.at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}</time>
              {m.status === "pending" && <span className="muted small">sending…</span>}
              {m.status === "sent" && <span className="muted small">sent</span>}
            </div>
            <div className="body">{m.body}</div>
            {m.error && <div className="error small">{m.error}</div>}
          </li>
        ))}
        <div ref={bottom} />
      </ol>
      <form className="composer" onSubmit={submit}>
        <textarea
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) submit(e);
          }}
          placeholder={`Message ${channelTitle(channel, me, names)}`}
          rows={2}
          aria-label="Message"
        />
        <button className="primary" disabled={!draft.trim()}>
          Send
        </button>
      </form>
    </section>
  );
}
