// SPDX-License-Identifier: Apache-2.0
import { useEffect, useMemo, useRef, useState, type FormEvent, type ReactNode } from "react";
import { ApiError, mentionCount, unread, type Channel, type ChatState, type ConnectionStatus, type Directory, type Message } from "@xmsg/client-core";
import { api, session } from "./session.ts";
import { loadState } from "./storage.ts";
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

/** Opens the local database first, so the chat renders with what it had (plan §9.2). */
function Chat({ me, onSignOut }: { me: string; onSignOut: () => void }) {
  const [initial, setInitial] = useState<ChatState | null>(null);
  useEffect(() => {
    void loadState(me).then(setInitial);
  }, [me]);
  if (!initial) return <div className="center muted">Loading…</div>;
  return <ChatView me={me} initial={initial} onSignOut={onSignOut} />;
}

function ChatView({ me, initial, onSignOut }: { me: string; initial: ChatState; onSignOut: () => void }) {
  const { state, status, syncError, send, nameChannel, markRead, setMuted, retry, typingIn, notifyTyping, react } = useChat(me, initial);
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
            <Item key={c.id} active={c.id === selected} onClick={() => setSelected(c.id)} label={channelTitle(c, me, names)} unread={unread(c)} mentions={mentionCount(c)} muted={c.muted} />
          ))}
        </Section>
        <Section title="Direct messages" onAdd={() => setPanel("dm")}>
          {dms.map((c) => (
            <Item key={c.id} active={c.id === selected} onClick={() => setSelected(c.id)} label={channelTitle(c, me, names)} unread={unread(c)} mentions={mentionCount(c)} muted={c.muted} />
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
          <Conversation
            channel={current}
            me={me}
            names={names}
            messages={state.messages[current.id] ?? []}
            onSend={(b) => send(current.id, b)}
            onToggleMute={() => setMuted(current.id, !current.muted)}
            onRetry={retry}
            typing={typingIn(current.id)}
            onTyping={() => notifyTyping(current.id)}
            onReact={(msgSeq, emoji, add) => react(current.id, msgSeq, emoji, add)}
          />
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

function Item({
  label,
  mentions,
  active,
  unread,
  muted,
  onClick,
}: {
  label: string;
  active: boolean;
  unread: number;
  mentions: number;
  muted: boolean;
  onClick: () => void;
}) {
  // Muted channels never shout: no bold, no badge (as in Slack). Mentions still show.
  const shown = muted ? 0 : unread;
  const classes = ["item", active && "active", shown > 0 && !active && "unread", muted && "muted"].filter(Boolean).join(" ");
  return (
    <li>
      <button className={classes} onClick={onClick} aria-label={`${label}${muted ? ", muted" : ""}${shown > 0 ? `, ${shown} unread` : ""}`}>
        <span className="label">{label}</span>
        {mentions > 0 && !active && <span className="badge mention">@{mentions > 99 ? "99+" : mentions}</span>}
        {shown > 0 && !mentions && !active && <span className="badge">{shown > 99 ? "99+" : shown}</span>}
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
  onToggleMute,
  onRetry,
  typing,
  onTyping,
  onReact,
}: {
  channel: Channel;
  me: string;
  names: Directory;
  messages: Message[];
  onSend: (body: string) => void;
  onToggleMute: () => void;
  onRetry: (key: string) => void;
  typing: string[];
  onTyping: () => void;
  onReact: (msgSeq: number, emoji: string, add: boolean) => void;
}) {
  const [draft, setDraft] = useState("");
  const [note, setNote] = useState("");
  // The message being edited: its key, the new text, and the edit_id reused on retries.
  const [editing, setEditing] = useState<{ key: string; text: string; editId: string } | null>(null);
  const bottom = useRef<HTMLDivElement>(null);
  // Block bodies: an effect may only return a cleanup function, and scrollIntoView
  // returns a Promise in browsers with scroll promises.
  useEffect(() => {
    void bottom.current?.scrollIntoView({ block: "end" });
  }, [messages.length, channel.id]);
  useEffect(() => {
    setNote("");
    setEditing(null);
  }, [channel.id]);

  // Edits and deletes are sequenced like messages; the result arrives as an event.
  const saveEdit = async (m: Message) => {
    if (!editing) return;
    const body = editing.text.trim();
    if (!body || body === m.body) return setEditing(null);
    try {
      await api.editMessage(await session.accessToken(), channel.id, m.msgSeq, body, editing.editId);
      setEditing(null);
    } catch (err) {
      setNote(errorText(err));
    }
  };

  const remove = async (m: Message) => {
    if (!confirm("Delete this message for everyone?")) return;
    try {
      await api.deleteMessage(await session.accessToken(), channel.id, m.msgSeq);
    } catch (err) {
      setNote(errorText(err));
    }
  };

  const submit = (e?: FormEvent) => {
    e?.preventDefault();
    const body = draft.trim();
    if (!body) return;
    onSend(toTokens(body, channel.members, me, names));
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
        <div className="row">
          <button onClick={onToggleMute} aria-pressed={channel.muted}>
            {channel.muted ? "Unmute" : "Mute"}
          </button>
          {canManage && (
            <>
              <button onClick={() => membership("invite")}>Invite</button>
              <button onClick={() => membership("leave")}>Leave</button>
            </>
          )}
        </div>
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
              {m.editedAt && !m.deleted && <span className="muted small" title={new Date(m.editedAt).toLocaleString()}>(edited)</span>}
              {m.authorId === me && m.msgSeq > 0 && !m.deleted && editing?.key !== m.key && (
                <span className="actions small">
                  <button className="link" onClick={() => setEditing({ key: m.key, text: m.body, editId: crypto.randomUUID() })}>
                    Edit
                  </button>
                  <button className="link" onClick={() => remove(m)}>
                    Delete
                  </button>
                </span>
              )}
            </div>
            {m.deleted ? (
              <div className="body muted">This message was deleted.</div>
            ) : editing?.key === m.key ? (
              <form
                className="edit"
                onSubmit={(e) => {
                  e.preventDefault();
                  void saveEdit(m);
                }}
              >
                <textarea
                  autoFocus
                  value={editing.text}
                  onChange={(e) => setEditing({ ...editing, text: e.target.value })}
                  onKeyDown={(e) => {
                    if (e.key === "Escape") setEditing(null);
                    if (e.key === "Enter" && !e.shiftKey) {
                      e.preventDefault();
                      void saveEdit(m);
                    }
                  }}
                  rows={2}
                />
                <div className="row small">
                  <button type="submit">Save</button>
                  <button type="button" className="link" onClick={() => setEditing(null)}>
                    Cancel
                  </button>
                </div>
              </form>
            ) : (
              <div className="body">{renderBody(m.body, me, names)}</div>
            )}
            {m.msgSeq > 0 && !m.deleted && <Reactions reactions={m.reactions ?? {}} onToggle={(emoji, add) => onReact(m.msgSeq, emoji, add)} />}
            {m.error && (
              <div className="error small">
                {m.error}
                {m.status === "failed" && (
                  <>
                    {" · "}
                    <button className="link" onClick={() => onRetry(m.key)}>
                      Retry
                    </button>
                  </>
                )}
              </div>
            )}
          </li>
        ))}
        <div ref={bottom} />
      </ol>
      <div className="typing muted small" aria-live="polite">
        {typing.length > 0 && typingText(typing.map((u) => names.label(u)))}
      </div>
      <form className="composer" onSubmit={submit}>
        <textarea
          value={draft}
          onChange={(e) => {
            setDraft(e.target.value);
            if (e.target.value.trim()) onTyping();
          }}
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

/**
 * `@Name` of a channel member becomes `<@user_id>`, and `@channel` / `@here` become
 * `<!channel>` / `<!here>`: the tokens the sequencer turns into mentions (ADR-0012).
 * Longer names first, so "@Ann Lee" wins over "@Ann".
 */
function toTokens(body: string, members: string[], me: string, names: Directory): string {
  let out = body.replace(/@(channel|here)\b/g, "<!$1>");
  const people = members
    .filter((id) => id !== me)
    .map((id) => ({ id, name: names.label(id) }))
    .sort((a, b) => b.name.length - a.name.length);
  for (const { id, name } of people) {
    out = out.split(`@${name}`).join(`<@${id}>`);
  }
  return out;
}

/** Shows mention tokens as highlighted names; mentions of me stand out more. */
function renderBody(body: string, me: string, names: Directory): ReactNode[] {
  return body.split(/(<@\d+>|<!(?:channel|here)>)/).map((part, i) => {
    const user = /^<@(\d+)>$/.exec(part)?.[1];
    if (user) {
      return (
        <span key={i} className={`mention ${user === me ? "me" : ""}`}>
          @{user === me ? names.label(me) : names.label(user)}
        </span>
      );
    }
    const everyone = /^<!(channel|here)>$/.exec(part)?.[1];
    if (everyone) {
      return (
        <span key={i} className="mention me">
          @{everyone}
        </span>
      );
    }
    return part;
  });
}

/** "Ann is typing…", "Ann and Bo are typing…", "Several people are typing…". */
function typingText(who: string[]): string {
  if (who.length === 1) return `${who[0]} is typing…`;
  if (who.length === 2) return `${who[0]} and ${who[1]} are typing…`;
  return "Several people are typing…";
}

const QUICK_REACTIONS = ["👍", "❤️", "😂", "🎉", "👀"];

/** A message's reactions as toggles, plus a small picker for the common ones. */
function Reactions({
  reactions,
  onToggle,
}: {
  reactions: Record<string, { count: number; mine: boolean }>;
  onToggle: (emoji: string, add: boolean) => void;
}) {
  const [picking, setPicking] = useState(false);
  const shown = Object.entries(reactions);
  return (
    <div className="reactions">
      {shown.map(([emoji, r]) => (
        <button
          key={emoji}
          className={`reaction ${r.mine ? "mine" : ""}`}
          aria-pressed={r.mine}
          aria-label={`${emoji} ${r.count}${r.mine ? ", including you" : ""}`}
          onClick={() => onToggle(emoji, !r.mine)}
        >
          {emoji} <span className="small">{r.count}</span>
        </button>
      ))}
      {picking ? (
        QUICK_REACTIONS.filter((e) => !reactions[e]?.mine).map((emoji) => (
          <button
            key={emoji}
            className="reaction"
            onClick={() => {
              onToggle(emoji, true);
              setPicking(false);
            }}
          >
            {emoji}
          </button>
        ))
      ) : (
        <button className="reaction add" aria-label="Add a reaction" onClick={() => setPicking(true)}>
          +
        </button>
      )}
    </div>
  );
}
