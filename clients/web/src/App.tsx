// SPDX-License-Identifier: Apache-2.0
import { Suspense, lazy, useCallback, useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import { ApiError, canPost, fromSsoFragment, ringing, threadKey, unread, type ChatState, type SearchHit, type Workspace } from "@xmsg/client-core";
import { api, session } from "./session.ts";
import { loadState } from "./storage.ts";
import { useChat } from "./useChat.ts";
import { useNames } from "./useNames.ts";
import { AuthScreen, ConversationHeader, KeyHints, MessageRow, Modal, ProfilePanel, Prompt, SearchBar, SearchResults, Sidebar, StatusLine, ThreadPanel, WorkspacesPanel, channelTitle, errorText, messageIdOf, type Theme } from "./components.tsx";

const CallPanel = lazy(() => import("./calls.tsx").then((m) => ({ default: m.CallPanel })));
const sso = fromSsoFragment(location.hash);
if (sso.tokens || sso.error) history.replaceState(null, "", location.pathname + location.search);
if (sso.tokens) session.adopt(sso.tokens);

function useTheme(): [Theme, () => void] {
  const [theme, setTheme] = useState<Theme>(() => {
    const saved = localStorage.getItem("xmsg.theme");
    return saved === "light" || saved === "dark" ? saved : matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark";
  });
  useEffect(() => { document.documentElement.dataset.theme = theme; }, [theme]);
  useEffect(() => {
    const media = matchMedia("(prefers-color-scheme: light)");
    const changed = () => { if (!localStorage.getItem("xmsg.theme")) setTheme(media.matches ? "light" : "dark"); };
    media.addEventListener("change", changed);
    return () => media.removeEventListener("change", changed);
  }, []);
  return [theme, () => setTheme((current) => { const next = current === "dark" ? "light" : "dark"; localStorage.setItem("xmsg.theme", next); return next; })];
}

export function App() {
  const [me, setMe] = useState<string | null>(null);
  const [checking, setChecking] = useState(session.hasRefreshToken);
  const [theme, toggleTheme] = useTheme();
  useEffect(() => {
    if (!checking) return;
    session.accessToken().then(() => setMe(session.userId)).catch(() => undefined).finally(() => setChecking(false));
  }, [checking]);
  if (checking) return <div className="center muted">loading session…</div>;
  if (!me) return <><StatusLine workspace="" channel="not connected" status="offline" theme={theme} onToggle={toggleTheme} /><SignIn onSignedIn={setMe} /></>;
  return <Chat key={me} me={me} theme={theme} toggleTheme={toggleTheme} onSignOut={async () => { await session.logout(); setMe(null); }} />;
}

function SignIn({ onSignedIn }: { onSignedIn: (userId: string) => void }) {
  const [mode, setMode] = useState<"in" | "up" | "code">("in");
  const [email, setEmail] = useState(""); const [password, setPassword] = useState(""); const [passwordAgain, setPasswordAgain] = useState("");
  const [code, setCode] = useState(""); const [name, setName] = useState(""); const [error, setError] = useState(sso.error ?? ""); const [busy, setBusy] = useState(false);
  const [providers, setProviders] = useState<{ id: string; name: string; url: string }[]>([]);
  useEffect(() => { api.ssoProviders().then((items) => setProviders(items.map((item) => ({ ...item, url: api.ssoStartUrl(item.id, location.pathname) })))).catch(() => setProviders([])); }, []);
  const submit = async (e: FormEvent) => {
    e.preventDefault(); setBusy(true); setError("");
    try {
      if (mode === "up") {
        if (password !== passwordAgain) throw new Error("Passwords do not match");
        await api.signup(email, password, name);
      }
      session.adopt(mode === "code" ? await api.loginWithRecoveryCode(email, code) : await api.login(email, password));
      onSignedIn(session.userId);
    } catch (err) { setError(err instanceof ApiError ? errorText(err) : err instanceof Error ? err.message : errorText(err)); }
    finally { setBusy(false); }
  };
  return <AuthScreen mode={mode} setMode={setMode} name={name} setName={setName} email={email} setEmail={setEmail} password={password} setPassword={setPassword} passwordAgain={passwordAgain} setPasswordAgain={setPasswordAgain} code={code} setCode={setCode} error={error} busy={busy} providers={providers} onSubmit={(e) => void submit(e)} />;
}

function Chat({ me, theme, toggleTheme, onSignOut }: { me: string; theme: Theme; toggleTheme: () => void; onSignOut: () => void }) {
  const [initial, setInitial] = useState<ChatState | null>(null);
  useEffect(() => { void loadState(me).then(setInitial); }, [me]);
  if (!initial) return <div className="center muted">loading local state…</div>;
  return <ChatView me={me} initial={initial} theme={theme} toggleTheme={toggleTheme} onSignOut={onSignOut} />;
}

function ChatView({ me, initial, theme, toggleTheme, onSignOut }: { me: string; initial: ChatState; theme: Theme; toggleTheme: () => void; onSignOut: () => void }) {
  const { state, status, syncError, send, upload, nameChannel, markRead, setMuted, retry, typingIn, notifyTyping, react, openThread, watchPresence, presenceOf, presenceKnown, view } = useChat(me, initial);
  const names = useNames(me);
  const [query, setQuery] = useState(""); const [hits, setHits] = useState<SearchHit[] | null>(null); const [searchNote, setSearchNote] = useState("");
  const [threadOf, setThreadOf] = useState<number | null>(null); const [inCall, setInCall] = useState<string | null>(null); const [declined, setDeclined] = useState<Set<string>>(new Set());
  const [selected, setSelected] = useState<string | null>(() => new URLSearchParams(location.search).get("channel"));
  const [modal, setModal] = useState<"none" | "channel" | "dm" | "workspaces" | "profile">("none");
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const searchInput = useRef<HTMLInputElement>(null);
  const leaveCall = useCallback(() => setInCall(null), []);
  const channels = useMemo(() => Object.values(state.channels).filter((c) => !c.left), [state.channels]);
  const rooms = channels.filter((c) => c.kind !== "dm" && c.kind !== "group_dm"); const dms = channels.filter((c) => c.kind === "dm" || c.kind === "group_dm");
  const calls = channels.filter((c) => c.call);
  const current = selected ? state.channels[selected] : undefined;
  const currentTitle = current ? channelTitle(current, me, names) : "";
  const watched = useMemo(() => { const ids = new Set<string>(); for (const c of channels) if (c.kind === "dm") for (const m of c.members) if (m !== me) ids.add(m); for (const m of current?.members ?? []) if (m !== me) ids.add(m); return [...ids].sort().slice(0, 200); }, [channels, current, me]);
  useEffect(() => watchPresence(watched), [watched.join(",")]); // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => {
    const update = () => view(document.visibilityState === "visible" ? current?.id ?? null : null); update();
    document.addEventListener("visibilitychange", update); return () => document.removeEventListener("visibilitychange", update);
  }, [current?.id]); // eslint-disable-line react-hooks/exhaustive-deps
  const pendingUnread = current ? unread(current) : 0;
  useEffect(() => {
    if (!current || pendingUnread === 0) return;
    const id = setTimeout(() => { if (document.visibilityState === "visible") markRead(current.id); }, 400);
    const onVisible = () => document.visibilityState === "visible" && markRead(current.id);
    document.addEventListener("visibilitychange", onVisible); return () => { clearTimeout(id); document.removeEventListener("visibilitychange", onVisible); };
  }, [current?.id, pendingUnread]); // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => {
    const onMessage = (e: MessageEvent) => { if (e.data?.type === "open-channel" && typeof e.data.channelId === "string") setSelected(e.data.channelId); };
    navigator.serviceWorker?.addEventListener("message", onMessage); return () => navigator.serviceWorker?.removeEventListener("message", onMessage);
  }, []);
  useEffect(() => { session.accessToken().then((token) => api.myWorkspaces(token)).then(setWorkspaces).catch(() => undefined); }, [modal === "workspaces"]); // eslint-disable-line react-hooks/exhaustive-deps
  const openThreadFor = useCallback((message: import("@xmsg/client-core").Message) => {
    if (!current) return; setThreadOf(message.msgSeq); openThread(current.id, { msgSeq: message.msgSeq, messageId: messageIdOf(message) });
  }, [current, openThread]); // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => {
    const keys = (e: KeyboardEvent) => {
      if (e.ctrlKey && e.key.toLowerCase() === "k") { e.preventDefault(); searchInput.current?.focus(); }
      if (e.ctrlKey && e.key.toLowerCase() === "n") { e.preventDefault(); setModal("channel"); }
      if (e.ctrlKey && e.key.toLowerCase() === "t") { e.preventDefault(); const roots = current ? state.messages[current.id] ?? [] : []; const root = [...roots].reverse().find((m) => m.msgSeq > 0 && messageIdOf(m)); if (root) openThreadFor(root); }
      if (e.key === "Escape") { setModal("none"); setThreadOf(null); setHits(null); setInCall(null); }
    };
    window.addEventListener("keydown", keys); return () => window.removeEventListener("keydown", keys);
  }, [current, state.messages, openThreadFor]);
  const runSearch = async () => { const q = query.trim(); if (!q) return setHits(null); setSearchNote(""); try { setHits(await api.search(await session.accessToken(), q)); } catch (err) { setHits([]); setSearchNote(errorText(err)); } };
  const refreshWorkspaces = () => session.accessToken().then(api.myWorkspaces).then(setWorkspaces);
  const currentRoot = current && threadOf !== null ? (state.messages[current.id] ?? []).find((m) => m.msgSeq === threadOf) : undefined;
  const closePanels = () => { setThreadOf(null); setInCall(null); };

  return <div className="terminal-app"><StatusLine workspace={workspaces[0]?.slug ?? "default"} channel={currentTitle} status={status} theme={theme} onToggle={toggleTheme} /><div className={`workspace-grid ${currentRoot || inCall ? "with-panel" : ""}`}>
    <Sidebar rooms={rooms.map((channel) => ({ channel, title: channelTitle(channel, me, names) }))} dms={dms.map((channel) => { const other = channel.kind === "dm" ? channel.members.find((m) => m !== me) : undefined; return { channel, title: channelTitle(channel, me, names), presence: other && presenceKnown(other) ? presenceOf(other) : undefined }; })} calls={calls.map((channel) => ({ channel, title: channel.call?.huddle ? `huddle-${channel.id.slice(-4)}` : channelTitle(channel, me, names) }))} selected={selected} onSelect={(id) => { setSelected(id); setThreadOf(null); setHits(null); }} onNewChannel={() => setModal("channel")} onNewDm={() => setModal("dm")} onWorkspaces={() => setModal("workspaces")} onProfile={() => setModal("profile")} onSignOut={onSignOut} />
    <main className="main-terminal"><SearchBar query={query} setQuery={setQuery} inputRef={searchInput} onSubmit={() => void runSearch()} /><div id="screen-share-slot" />{syncError && <p className="note-text">couldn't load history</p>}{Object.values(state.channels).filter((c) => c.id !== inCall && c.call && ringing(c, me) && !declined.has(c.call.callId)).map((c) => <div key={c.id} className="call-ring" role="alert"><span>{names.label(c.call!.participants[0]!)} is calling</span><button onClick={() => { setSelected(c.id); setInCall(c.id); }}>join</button><button onClick={() => setDeclined((d) => new Set(d).add(c.call!.callId))}>decline</button></div>)}
      {hits !== null ? <SearchResults hits={hits} note={searchNote} me={me} names={names} channelName={(id) => state.channels[id] ? channelTitle(state.channels[id]!, me, names) : "unknown"} onPick={(hit) => { setHits(null); setSelected(hit.channelId); setThreadOf(hit.reply ? hit.rootMsgSeq : null); const root = hit.reply ? (state.messages[hit.channelId] ?? []).find((m) => m.msgSeq === hit.rootMsgSeq) : undefined; if (root?.messageId) openThread(hit.channelId, { msgSeq: root.msgSeq, messageId: root.messageId }); }} onClose={() => setHits(null)} /> : current ? <section className="conversation"><ConversationHeader channel={current} title={currentTitle} me={me} names={names} onCall={() => { setInCall(current.id); setThreadOf(null); }} onMute={() => setMuted(current.id, !current.muted)} onInvite={current.kind !== "dm" ? () => { const user = prompt("User ID to invite")?.trim(); if (user) void session.accessToken().then((token) => api.join(token, current.id, user)).catch((e) => alert(errorText(e))); } : undefined} onLeave={current.kind !== "dm" ? () => { void session.accessToken().then((token) => api.remove(token, current.id, me)).catch((e) => alert(errorText(e))); } : undefined} /><ol className="message-list">{(state.messages[current.id] ?? []).map((m) => <MessageRow key={m.key} message={m} me={me} names={names} presence={presenceKnown(m.authorId) ? presenceOf(m.authorId) : undefined} onOpenThread={() => openThreadFor(m)} onReact={(emoji, add) => react(current.id, m.msgSeq, emoji, add)} onRetry={() => retry(m.key)} onEdit={m.authorId === me ? async (body) => { await api.editMessage(await session.accessToken(), current.id, m.msgSeq, body, crypto.randomUUID()); } : undefined} onDelete={m.authorId === me ? () => { if (confirm("Delete this message for everyone?")) void session.accessToken().then((token) => api.deleteMessage(token, current.id, m.msgSeq)).catch((e) => alert(errorText(e))); } : undefined} />)}</ol><div className="typing">{typingIn(current.id).length > 0 && `${typingIn(current.id).map((id) => names.label(id)).join(", ")} typing…`}</div><Prompt channel={currentTitle} members={current.members} me={me} names={names} onSend={(body, files) => send(current.id, body, undefined, files)} onUpload={(file) => upload(current.id, file)} onTyping={() => notifyTyping(current.id)} onCommand={(command) => { if (command === "/huddle") { setInCall(current.id); setThreadOf(null); } }} readOnly={!canPost(current, me)} /></section> : <div className="center muted">{channels.length ? "select a conversation" : "create a channel or start a DM"}</div>}
    </main>
    {currentRoot && current && !inCall && <ThreadPanel root={currentRoot} replies={state.messages[threadKey(current.id, currentRoot.msgSeq)] ?? []} me={me} names={names} channel={currentTitle} onSend={(body, files) => send(current.id, body, { msgSeq: currentRoot.msgSeq, messageId: messageIdOf(currentRoot) }, files)} onUpload={(file) => upload(current.id, file)} onRetry={retry} onClose={() => setThreadOf(null)} />}
    {inCall && state.channels[inCall] && <Suspense fallback={<aside className="right-panel"><p className="muted">starting call…</p></aside>}><CallPanel key={inCall} channelId={inCall} title={channelTitle(state.channels[inCall]!, me, names)} label={(id) => names.label(id)} onLeave={leaveCall} onClose={closePanels} /></Suspense>}
  </div><KeyHints inCall={Boolean(inCall)} />
  {modal === "channel" || modal === "dm" ? <CreatePanel kind={modal} onClose={() => setModal("none")} onCreated={(id, name) => { if (name) nameChannel(id, name); setSelected(id); setModal("none"); }} /> : null}
  {modal === "profile" && <ProfilePanel me={me} names={names} onSave={async (name, avatar) => names.remember(await api.updateMe(await session.accessToken(), { name, avatar_url: avatar || null }))} onClose={() => setModal("none")} />}
  {modal === "workspaces" && <WorkspacesPanel workspaces={workspaces} onCreate={async (name, slug) => { await api.createWorkspace(await session.accessToken(), name, slug); await refreshWorkspaces(); }} onInvite={async (id) => (await api.createInvite(await session.accessToken(), id)).code} onAccept={async (code) => { await api.acceptInvite(await session.accessToken(), code); await refreshWorkspaces(); }} onClose={() => setModal("none")} />}
  </div>;
}

function CreatePanel({ kind, onClose, onCreated }: { kind: "channel" | "dm"; onClose: () => void; onCreated: (id: string, name?: string) => void }) {
  const [name, setName] = useState(""); const [type, setType] = useState<"public" | "private" | "broadcast">("public"); const [members, setMembers] = useState(""); const [error, setError] = useState("");
  const submit = async (e: FormEvent) => { e.preventDefault(); try { const token = await session.accessToken(); const ids = members.split(/[\s,]+/).filter(Boolean); const reply = kind === "channel" ? await api.createChannel(token, type, name.trim(), ids) : await api.openDm(token, ids); onCreated(reply.channel_id, kind === "channel" ? name.trim() : undefined); } catch (err) { setError(errorText(err)); } };
  return <Modal title={kind === "channel" ? "NEW CHANNEL · ^N" : "NEW DIRECT MESSAGE"} onClose={onClose}><form className="modal-form" onSubmit={(e) => void submit(e)}>{kind === "channel" && <><label>name<input value={name} onChange={(e) => setName(e.target.value)} required maxLength={80} autoFocus /></label><label>type<select value={type} onChange={(e) => setType(e.target.value as typeof type)}><option value="public">public</option><option value="private">private</option><option value="broadcast">broadcast</option></select></label></>}<label>{kind === "channel" ? "members (user IDs, optional)" : "user IDs (one for DM, up to 8 for group)"}<input value={members} onChange={(e) => setMembers(e.target.value)} required={kind === "dm"} autoFocus={kind === "dm"} /></label>{error && <p className="error">{error}</p>}<button>[ {kind === "channel" ? "create" : "open"} ↵ ]</button></form></Modal>;
}
