// SPDX-License-Identifier: Apache-2.0
import { Suspense, lazy, useCallback, useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import { ApiError, canPost, fromSsoFragment, ringing, threadKey, unread, type ChatState, type SearchHit, type Workspace } from "@xmsg/client-core";
import { api, DEFAULT_SERVER, isDesktop, saveServerUrl, serverUrl, session, validServerUrl } from "./session.ts";
import { loadState } from "./storage.ts";
import { useChat } from "./useChat.ts";
import { useNames } from "./useNames.ts";
import { AdminPanel, AuthScreen, ChannelInfoPanel, ConversationHeader, KeyHints, MessageRow, Modal, Prompt, SearchBar, SearchResults, SettingsPanel, SetupScreen, Sidebar, StatusLine, ThreadPanel, WaitingScreen, channelTitle, errorText, messageIdOf, type AdminMember, type Theme } from "./components.tsx";

const CallPanel = lazy(() => import("./calls.tsx").then((m) => ({ default: m.CallPanel })));
const sso = fromSsoFragment(location.hash);
if (sso.tokens || sso.error) history.replaceState(null, "", location.pathname + location.search);
if (sso.tokens) session.adopt(sso.tokens);

const WORKSPACE_KEY = "xmsg.workspace";

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
  const [server, setServer] = useState(serverUrl());
  useEffect(() => {
    if (!checking) return;
    session.accessToken().then(() => setMe(session.userId)).catch(() => undefined).finally(() => setChecking(false));
  }, [checking]);
  if (isDesktop && !server) return <ConnectScreen onConnect={(url) => { saveServerUrl(url); setServer(url); location.reload(); }} />;
  if (checking) return <div className="center muted">loading session…</div>;
  if (!me) return <><StatusLine workspace="" channel="not connected" status="offline" /><SignIn onSignedIn={setMe} /></>;
  return <Chat key={me} me={me} theme={theme} toggleTheme={toggleTheme} onSignOut={async () => { await session.logout(); setMe(null); }} />;
}

function ConnectScreen({ onConnect }: { onConnect: (url: string) => void }) {
  const [url, setUrl] = useState(DEFAULT_SERVER);
  const [error, setError] = useState("");
  const submit = (e: FormEvent) => {
    e.preventDefault();
    const trimmed = url.trim();
    if (!validServerUrl(trimmed)) {
      setError("Use an https URL (or http for localhost / 127.0.0.1)");
      return;
    }
    onConnect(trimmed);
  };
  return (
    <div className="terminal-app">
      <StatusLine workspace="" channel="not connected" status="offline" />
      <div className="center">
        <form className="modal-form connect-screen" onSubmit={submit}>
          <h1>xmsg · connect</h1>
          <p className="muted">server URL</p>
          <label>
            <input value={url} onChange={(e) => setUrl(e.target.value)} autoFocus spellCheck={false} />
          </label>
          {error && <p className="error">{error}</p>}
          <button type="submit">[ connect ↵ ]</button>
        </form>
      </div>
    </div>
  );
}

function SignIn({ onSignedIn }: { onSignedIn: (userId: string) => void }) {
  const [mode, setMode] = useState<"in" | "up" | "code">("in");
  const [email, setEmail] = useState(""); const [password, setPassword] = useState(""); const [passwordAgain, setPasswordAgain] = useState("");
  const [code, setCode] = useState(""); const [name, setName] = useState(""); const [error, setError] = useState(sso.error ?? ""); const [busy, setBusy] = useState(false);
  const [providers, setProviders] = useState<{ id: string; name: string; url: string }[]>([]);
  useEffect(() => { if (!isDesktop) api.ssoProviders().then((items) => setProviders(items.map((item) => ({ ...item, url: api.ssoStartUrl(item.id, location.pathname) })))).catch(() => setProviders([])); }, []);
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

type Gate =
  | { phase: "loading" }
  | { phase: "setup"; workspace: Workspace }
  | { phase: "waiting" }
  | { phase: "error" }
  | { phase: "ready"; initial: ChatState; initialChannel: string | null; workspace: Workspace | undefined; workspaces: Workspace[] };

async function resolveGate(me: string): Promise<Gate> {
  try {
    const token = await session.accessToken();
    const workspaces = await api.myWorkspaces(token);
    const saved = localStorage.getItem(WORKSPACE_KEY);
    const workspace = workspaces.find((w) => w.workspace_id === saved) ?? workspaces[0];
    if (workspace && workspace.role !== "member" && workspace.setup_completed === false) return { phase: "setup", workspace };
    if (workspace) await api.joinDefaultChannels(token, workspace.workspace_id).catch(() => undefined);
    const initial = await loadState(me);
    const defaults = workspace ? await api.defaultChannels(token, workspace.workspace_id).catch(() => []) : [];
    const hasChannels = Object.values(initial.channels).some((c) => !c.left);
    if (!hasChannels && workspace?.setup_completed === false) return { phase: "waiting" };
    return { phase: "ready", initial, initialChannel: defaults[0]?.channelId ?? null, workspace, workspaces };
  } catch {
    return { phase: "error" };
  }
}

function Chat({ me, theme, toggleTheme, onSignOut }: { me: string; theme: Theme; toggleTheme: () => void; onSignOut: () => void }) {
  const [gate, setGate] = useState<Gate>({ phase: "loading" });
  const resolve = useCallback(() => { setGate({ phase: "loading" }); void resolveGate(me).then(setGate); }, [me]);
  useEffect(resolve, [resolve]);
  if (gate.phase === "loading") return <div className="center muted">loading…</div>;
  if (gate.phase === "setup") return <Setup gate={gate} onDone={resolve} />;
  if (gate.phase === "waiting") return <WaitingScreen />;
  if (gate.phase === "error")
    return <div className="center"><div className="modal-form"><p className="error">couldn't reach the server</p><button onClick={resolve}>[ retry ]</button></div></div>;
  return <ChatView me={me} initial={gate.initial} initialChannel={gate.initialChannel} workspace={gate.workspace} workspaces={gate.workspaces} theme={theme} toggleTheme={toggleTheme} onSignOut={onSignOut} />;
}

function Setup({ gate, onDone }: { gate: Extract<Gate, { phase: "setup" }>; onDone: () => void }) {
  const [workspaceName, setWorkspaceName] = useState(gate.workspace.name);
  const [channelName, setChannelName] = useState("general");
  const [error, setError] = useState(""); const [busy, setBusy] = useState(false);
  const submit = async (e: FormEvent) => {
    e.preventDefault(); setBusy(true); setError("");
    try {
      const token = await session.accessToken();
      const { channel_id } = await api.createChannel(token, "public", channelName.trim());
      await api.addDefaultChannel(token, gate.workspace.workspace_id, channel_id, false);
      await api.updateWorkspace(token, gate.workspace.workspace_id, { name: workspaceName.trim(), setup_completed: true });
      onDone();
    } catch (err) { setError(errorText(err)); setBusy(false); }
  };
  return <SetupScreen workspaceName={workspaceName} setWorkspaceName={setWorkspaceName} channelName={channelName} setChannelName={setChannelName} error={error} busy={busy} onSubmit={(e) => void submit(e)} />;
}

function ChatView({ me, initial, initialChannel, workspace, workspaces, theme, toggleTheme, onSignOut }: { me: string; initial: ChatState; initialChannel: string | null; workspace: Workspace | undefined; workspaces: Workspace[]; theme: Theme; toggleTheme: () => void; onSignOut: () => void }) {
  const { state, status, syncError, send, upload, nameChannel, markRead, setMuted, retry, typingIn, notifyTyping, react, openThread, watchPresence, presenceOf, presenceKnown, view } = useChat(me, initial);
  const names = useNames(me);
  const [query, setQuery] = useState(""); const [hits, setHits] = useState<SearchHit[] | null>(null); const [searchNote, setSearchNote] = useState("");
  const [threadOf, setThreadOf] = useState<number | null>(null); const [inCall, setInCall] = useState<string | null>(null); const [declined, setDeclined] = useState<Set<string>>(new Set());
  const [selected, setSelected] = useState<string | null>(() => (initialChannel && initial.channels[initialChannel] ? initialChannel : new URLSearchParams(location.search).get("channel")));
  const [main, setMain] = useState<"chat" | "admin">("chat");
  const [modal, setModal] = useState<"none" | "channel" | "dm" | "settings" | "info">("none");
  const searchInput = useRef<HTMLInputElement>(null);
  const pendingInitial = useRef<string | null>(initialChannel && !initial.channels[initialChannel] ? initialChannel : null);
  const leaveCall = useCallback(() => setInCall(null), []);
  const channels = useMemo(() => Object.values(state.channels).filter((c) => !c.left), [state.channels]);
  const rooms = channels.filter((c) => c.kind !== "dm" && c.kind !== "group_dm"); const dms = channels.filter((c) => c.kind === "dm" || c.kind === "group_dm");
  const calls = channels.filter((c) => c.call);
  const current = selected ? state.channels[selected] : undefined;
  const currentTitle = current ? channelTitle(current, me, names) : "";
  const myRole = workspace?.role ?? "member";
  const isStaff = myRole === "owner" || myRole === "admin";
  const watched = useMemo(() => { const ids = new Set<string>(); for (const c of channels) if (c.kind === "dm") for (const m of c.members) if (m !== me) ids.add(m); for (const m of current?.members ?? []) if (m !== me) ids.add(m); return [...ids].sort().slice(0, 200); }, [channels, current, me]);
  useEffect(() => watchPresence(watched), [watched.join(",")]); // eslint-disable-line react-hooks/exhaustive-deps
  // Open the first default channel once the sync brings it (a brand-new member has nothing locally).
  useEffect(() => {
    if (selected !== null || !pendingInitial.current) return;
    if (state.channels[pendingInitial.current]) { setSelected(pendingInitial.current); pendingInitial.current = null; }
  }, [selected, state.channels]);
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
  const openThreadFor = useCallback((message: import("@xmsg/client-core").Message) => {
    if (!current) return; setThreadOf(message.msgSeq); openThread(current.id, { msgSeq: message.msgSeq, messageId: messageIdOf(message) });
  }, [current, openThread]); // eslint-disable-line react-hooks/exhaustive-deps
  const inviteTo = useCallback((channel: NonNullable<typeof current>) => {
    const user = prompt("User ID to invite")?.trim();
    if (user) void session.accessToken().then((token) => api.join(token, channel.id, user)).catch((e) => alert(errorText(e)));
  }, []);
  useEffect(() => {
    const keys = (e: KeyboardEvent) => {
      if (e.ctrlKey && e.key.toLowerCase() === "k") { e.preventDefault(); searchInput.current?.focus(); }
      if (e.ctrlKey && e.key.toLowerCase() === "n") { e.preventDefault(); setModal("channel"); }
      if (e.ctrlKey && e.key.toLowerCase() === "t") { e.preventDefault(); const roots = current ? state.messages[current.id] ?? [] : []; const root = [...roots].reverse().find((m) => m.msgSeq > 0 && messageIdOf(m)); if (root) openThreadFor(root); }
      if (e.ctrlKey && e.key.toLowerCase() === "i") { e.preventDefault(); if (current && isStaff && current.kind !== "dm") inviteTo(current); }
      if (e.key === "Escape") { setModal("none"); setThreadOf(null); setHits(null); setInCall(null); setMain("chat"); }
    };
    window.addEventListener("keydown", keys); return () => window.removeEventListener("keydown", keys);
  }, [current, state.messages, openThreadFor, isStaff, inviteTo]);
  const runSearch = async () => { const q = query.trim(); if (!q) return setHits(null); setSearchNote(""); try { setHits(await api.search(await session.accessToken(), q)); } catch (err) { setHits([]); setSearchNote(errorText(err)); } };
  const currentRoot = current && threadOf !== null ? (state.messages[current.id] ?? []).find((m) => m.msgSeq === threadOf) : undefined;
  const closePanels = () => { setThreadOf(null); setInCall(null); };

  return <div className="terminal-app"><StatusLine workspace={workspace?.slug ?? "default"} channel={currentTitle} status={status} /><div className={`workspace-grid ${currentRoot || inCall ? "with-panel" : ""}`}>
    <Sidebar rooms={rooms.map((channel) => ({ channel, title: channelTitle(channel, me, names) }))} dms={dms.map((channel) => { const other = channel.kind === "dm" ? channel.members.find((m) => m !== me) : undefined; return { channel, title: channelTitle(channel, me, names), presence: other && presenceKnown(other) ? presenceOf(other) : undefined }; })} calls={calls.map((channel) => ({ channel, title: channel.call?.huddle ? `huddle-${channel.id.slice(-4)}` : channelTitle(channel, me, names) }))} selected={selected} onSelect={(id) => { setSelected(id); setThreadOf(null); setHits(null); setMain("chat"); }} onNewChannel={() => setModal("channel")} onNewDm={() => setModal("dm")} onAdmin={isStaff ? () => setMain("admin") : undefined} meName={names.label(me)} onOpenSettings={() => setModal("settings")} />
      <main className="main-terminal">{main === "admin" && workspace ? <AdminScreen workspace={workspace} myRole={myRole} me={me} rooms={rooms} onClose={() => setMain("chat")} /> : <><SearchBar query={query} setQuery={setQuery} inputRef={searchInput} onSubmit={() => void runSearch()} /><div id="screen-share-slot" />{syncError && <p className="note-text">couldn't load history</p>}{Object.values(state.channels).filter((c) => c.id !== inCall && c.call && ringing(c, me) && !declined.has(c.call.callId)).map((c) => <div key={c.id} className="call-ring" role="alert"><span>{names.label(c.call!.participants[0]!)} is calling</span><button onClick={() => { setSelected(c.id); setInCall(c.id); }}>join</button><button onClick={() => setDeclined((d) => new Set(d).add(c.call!.callId))}>decline</button></div>)}
      {hits !== null ? <SearchResults hits={hits} note={searchNote} me={me} names={names} channelName={(id) => state.channels[id] ? channelTitle(state.channels[id]!, me, names) : "unknown"} onPick={(hit) => { setHits(null); setSelected(hit.channelId); setThreadOf(hit.reply ? hit.rootMsgSeq : null); const root = hit.reply ? (state.messages[hit.channelId] ?? []).find((m) => m.msgSeq === hit.rootMsgSeq) : undefined; if (root?.messageId) openThread(hit.channelId, { msgSeq: root.msgSeq, messageId: root.messageId }); }} onClose={() => setHits(null)} /> : current ? <section className="conversation"><ConversationHeader title={currentTitle} onCall={() => { setInCall(current.id); setThreadOf(null); }} onOpenInfo={() => setModal("info")} /><ol className="message-list">{(state.messages[current.id] ?? []).length === 0 && <li className="start-of-channel"><span className="muted">this is the start of #{current.name || current.id.slice(-6)}</span>{isStaff && current.kind !== "dm" && <button className="link" onClick={() => inviteTo(current)}>invite people → ^I</button>}</li>}{(state.messages[current.id] ?? []).map((m) => <MessageRow key={m.key} message={m} me={me} names={names} presence={presenceKnown(m.authorId) ? presenceOf(m.authorId) : undefined} onOpenThread={() => openThreadFor(m)} onReact={(emoji, add) => react(current.id, m.msgSeq, emoji, add)} onRetry={() => retry(m.key)} onEdit={m.authorId === me ? async (body) => { await api.editMessage(await session.accessToken(), current.id, m.msgSeq, body, crypto.randomUUID()); } : undefined} onDelete={m.authorId === me ? () => { if (confirm("Delete this message for everyone?")) void session.accessToken().then((token) => api.deleteMessage(token, current.id, m.msgSeq)).catch((e) => alert(errorText(e))); } : undefined} />)}</ol><div className="typing">{typingIn(current.id).length > 0 && `${typingIn(current.id).map((id) => names.label(id)).join(", ")} typing…`}</div><Prompt channel={currentTitle} members={current.members} me={me} names={names} onSend={(body, files) => send(current.id, body, undefined, files)} onUpload={(file) => upload(current.id, file)} onTyping={() => notifyTyping(current.id)} onCommand={(command) => { if (command === "/huddle") { setInCall(current.id); setThreadOf(null); } }} readOnly={!canPost(current, me)} /></section> : <div className="center muted">{channels.length ? "select a conversation" : "create a channel or start a DM"}</div>}</>}</main>
    {currentRoot && current && !inCall && <ThreadPanel root={currentRoot} replies={state.messages[threadKey(current.id, currentRoot.msgSeq)] ?? []} me={me} names={names} channel={currentTitle} onSend={(body, files) => send(current.id, body, { msgSeq: currentRoot.msgSeq, messageId: messageIdOf(currentRoot) }, files)} onUpload={(file) => upload(current.id, file)} onRetry={retry} onClose={() => setThreadOf(null)} />}
    {inCall && state.channels[inCall] && <Suspense fallback={<aside className="right-panel"><p className="muted">starting call…</p></aside>}><CallPanel key={inCall} channelId={inCall} title={channelTitle(state.channels[inCall]!, me, names)} label={(id) => names.label(id)} onLeave={leaveCall} onClose={closePanels} /></Suspense>}
  </div><KeyHints inCall={Boolean(inCall)} />
  {modal === "channel" || modal === "dm" ? <CreatePanel kind={modal} onClose={() => setModal("none")} onCreated={(id, name) => { if (name) nameChannel(id, name); setSelected(id); setModal("none"); }} /> : null}
  {modal === "settings" && <SettingsPanel me={me} names={names} theme={theme} onToggleTheme={toggleTheme} workspaces={workspaces} currentWorkspaceId={workspace?.workspace_id ?? ""} serverInitial={isDesktop ? serverUrl() : undefined} onSaveProfile={async (name, avatar) => names.remember(await api.updateMe(await session.accessToken(), { name, avatar_url: avatar || null }))} onSaveServer={isDesktop ? (url) => { if (!validServerUrl(url)) return alert("Use an https URL (or http for localhost / 127.0.0.1)"); saveServerUrl(url); location.reload(); } : undefined} onAcceptInvite={async (code) => { await api.acceptInvite(await session.accessToken(), code); }} onSignOut={onSignOut} onClose={() => setModal("none")} />}
  {modal === "info" && current && <ChannelInfoPanel channel={current} title={currentTitle} me={me} names={names} onMute={() => setMuted(current.id, !current.muted)} onInvite={current.kind !== "dm" ? () => inviteTo(current) : undefined} onLeave={current.kind !== "dm" ? () => { void session.accessToken().then((token) => api.remove(token, current.id, me)).catch((e) => alert(errorText(e))); } : undefined} onClose={() => setModal("none")} />}
  </div>;
}

/** The admin view's data and actions, behind the [admin] sidebar entry. */
function AdminScreen({ workspace, myRole, me, rooms, onClose }: { workspace: Workspace; myRole: "owner" | "admin" | "member"; me: string; rooms: import("@xmsg/client-core").Channel[]; onClose: () => void }) {
  const [members, setMembers] = useState<AdminMember[]>([]);
  const [defaults, setDefaults] = useState<{ channelId: string; name: string }[]>([]);
  const [denied, setDenied] = useState(false);
  const token = () => session.accessToken();
  const load = useCallback(async () => {
    try {
      const t = await token();
      const all: AdminMember[] = [];
      let after: string | undefined;
      for (;;) {
        const page = await api.workspaceMembers(t, workspace.workspace_id, after);
        all.push(...page.members);
        if (!page.next) break;
        after = page.next;
      }
      setMembers(all);
      setDefaults(await api.defaultChannels(t, workspace.workspace_id));
    } catch (e) {
      if (e instanceof ApiError && e.status === 403) setDenied(true);
    }
  }, [workspace.workspace_id]);
  useEffect(() => { void load(); }, [load]);
  const act = (action: Promise<unknown>) => action.then(load).catch(() => undefined);
  if (denied) return <section className="admin-panel"><header className="panel-title">ADMIN<button className="link" onClick={onClose}>×</button></header><p className="muted admin-note">not allowed</p></section>;
  const publicChannels = rooms.filter((c) => c.kind === "public" && c.name).map((c) => ({ channelId: c.id, name: c.name }));
  const withToken = (f: (t: string) => Promise<unknown>) => act(token().then(f));
  return <AdminPanel workspace={workspace} myRole={myRole} me={me} members={members} defaults={defaults} publicChannels={publicChannels} onRename={(name) => withToken((t) => api.updateWorkspace(t, workspace.workspace_id, { name }))} onAddDefault={(channelId, addExistingMembers) => withToken((t) => api.addDefaultChannel(t, workspace.workspace_id, channelId, addExistingMembers))} onRemoveDefault={(channelId) => withToken((t) => api.removeDefaultChannel(t, workspace.workspace_id, channelId))} onSetRole={(userId, role) => withToken((t) => api.updateWorkspaceMemberRole(t, workspace.workspace_id, userId, role))} onRemoveMember={(userId) => withToken((t) => api.removeWorkspaceMember(t, workspace.workspace_id, userId))} onCreateInvite={async () => (await api.createInvite(await token(), workspace.workspace_id)).code} onClose={onClose} />;
}

function CreatePanel({ kind, onClose, onCreated }: { kind: "channel" | "dm"; onClose: () => void; onCreated: (id: string, name?: string) => void }) {
  const [name, setName] = useState(""); const [type, setType] = useState<"public" | "private" | "broadcast">("public"); const [members, setMembers] = useState(""); const [error, setError] = useState("");
  const submit = async (e: FormEvent) => { e.preventDefault(); try { const token = await session.accessToken(); const ids = members.split(/[\s,]+/).filter(Boolean); const reply = kind === "channel" ? await api.createChannel(token, type, name.trim(), ids) : await api.openDm(token, ids); onCreated(reply.channel_id, kind === "channel" ? name.trim() : undefined); } catch (err) { setError(errorText(err)); } };
  return <Modal title={kind === "channel" ? "NEW CHANNEL · ^N" : "NEW DIRECT MESSAGE"} onClose={onClose}><form className="modal-form" onSubmit={(e) => void submit(e)}>{kind === "channel" && <><label>name<input value={name} onChange={(e) => setName(e.target.value)} required maxLength={80} autoFocus /></label><label>type<select value={type} onChange={(e) => setType(e.target.value as typeof type)}><option value="public">public</option><option value="private">private</option><option value="broadcast">broadcast</option></select></label></>}<label>{kind === "channel" ? "members (user IDs, optional)" : "user IDs (one for DM, up to 8 for group)"}<input value={members} onChange={(e) => setMembers(e.target.value)} required={kind === "dm"} autoFocus={kind === "dm"} /></label>{error && <p className="error">{error}</p>}<button>[ {kind === "channel" ? "create" : "open"} ↵ ]</button></form></Modal>;
}
