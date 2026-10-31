// SPDX-License-Identifier: Apache-2.0
import { Suspense, lazy, useCallback, useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import { ApiError, canPost, fromSsoFragment, ringing, threadKey, unread, type Channel, type ChatState, type Message, type SearchHit, type Workspace } from "@xmsg/client-core";
import { api, DEFAULT_SERVER, isDesktop, saveServerUrl, serverUrl, session, validServerUrl } from "./session.ts";
import { loadState } from "./storage.ts";
import { useChat } from "./useChat.ts";
import { useNames } from "./useNames.ts";
import { notify, setBadge } from "./notify.ts";
import { draftKey, unreadTotal } from "./chatlog.ts";
import { MessageList } from "./MessageList.tsx";
import { ShortcutsPanel, Switcher } from "./Switcher.tsx";
import { AdminPanel, AuthScreen, ChannelInfoPanel, ConversationHeader, InviteLinkAction, KeyHints, Modal, Prompt, SearchResults, SettingsPanel, SetupScreen, Sidebar, StatusLine, ThreadPanel, WaitingScreen, channelTitle, errorText, messageIdOf, type AdminMember, type Theme } from "./components.tsx";

const CallPanel = lazy(() => import("./calls.tsx").then((m) => ({ default: m.CallPanel })));
const sso = fromSsoFragment(location.hash);
if (sso.tokens || sso.error) history.replaceState(null, "", location.pathname + location.search);
if (sso.tokens) session.adopt(sso.tokens);
const joinParams = new URLSearchParams(location.search);
const joinCode = joinParams.get("join");
const joinWorkspace = joinParams.get("ws");

const WORKSPACE_KEY = "xmsg.workspace";
const WELCOMED_KEY = "xmsg.welcomed";

/** A wait only announces itself when it passes 300 ms: no flash for the fast path. */
function DelayedLoading() {
  const [show, setShow] = useState(false);
  useEffect(() => {
    const id = setTimeout(() => setShow(true), 300);
    return () => clearTimeout(id);
  }, []);
  return <div className="center">{show && <span className="muted">loading…</span>}</div>;
}

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
  if (checking) return <DelayedLoading />;
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
  return <AuthScreen mode={mode} setMode={setMode} name={name} setName={setName} email={email} setEmail={setEmail} password={password} setPassword={setPassword} passwordAgain={passwordAgain} setPasswordAgain={setPasswordAgain} code={code} setCode={setCode} error={error} busy={busy} providers={providers} joining={joinCode ? (joinWorkspace ?? "workspace") : undefined} onSubmit={(e) => void submit(e)} />;
}

type Gate =
  | { phase: "loading" }
  | { phase: "setup"; workspace: Workspace }
  | { phase: "waiting" }
  | { phase: "error" }
  | { phase: "ready"; initial: ChatState; initialChannel: string | null; workspace: Workspace | undefined; workspaces: Workspace[]; fresh: boolean };

async function resolveGate(me: string): Promise<Gate> {
  try {
    const token = await session.accessToken();
    // An invite link: one confirmation for a signed-in person, absorbed before the rest.
    if (joinCode) {
      await api.acceptInvite(token, joinCode).catch(() => undefined);
      history.replaceState(null, "", location.pathname);
    }
    const workspaces = await api.myWorkspaces(token);
    const saved = localStorage.getItem(WORKSPACE_KEY);
    const workspace = workspaces.find((w) => w.workspace_id === saved) ?? workspaces[0];
    if (workspace && workspace.role !== "member" && workspace.setup_completed === false) return { phase: "setup", workspace };
    let fresh = false;
    if (workspace) {
      const joined = await api.joinDefaultChannels(token, workspace.workspace_id).catch(() => [] as string[]);
      fresh = joined.length > 0 && !localStorage.getItem(WELCOMED_KEY);
    }
    const initial = await loadState(me);
    const defaults = workspace ? await api.defaultChannels(token, workspace.workspace_id).catch(() => []) : [];
    const hasChannels = Object.values(initial.channels).some((c) => !c.left);
    if (!hasChannels && workspace?.setup_completed === false) return { phase: "waiting" };
    return { phase: "ready", initial, initialChannel: defaults[0]?.channelId ?? null, workspace, workspaces, fresh };
  } catch {
    return { phase: "error" };
  }
}

function Chat({ me, theme, toggleTheme, onSignOut }: { me: string; theme: Theme; toggleTheme: () => void; onSignOut: () => void }) {
  const [gate, setGate] = useState<Gate>({ phase: "loading" });
  const resolve = useCallback(() => { setGate({ phase: "loading" }); void resolveGate(me).then(setGate); }, [me]);
  useEffect(resolve, [resolve]);
  if (gate.phase === "loading") return <DelayedLoading />;
  if (gate.phase === "setup") return <Setup gate={gate} onDone={resolve} />;
  if (gate.phase === "waiting") return <WaitingScreen />;
  if (gate.phase === "error")
    return <div className="center"><div className="modal-form"><p className="error">couldn't reach the server · <button className="link" onClick={resolve}>[ retry ]</button></p></div></div>;
  return <ChatView me={me} initial={gate.initial} initialChannel={gate.initialChannel} workspace={gate.workspace} workspaces={gate.workspaces} fresh={gate.fresh} theme={theme} toggleTheme={toggleTheme} onSignOut={onSignOut} />;
}

function Setup({ gate, onDone }: { gate: Extract<Gate, { phase: "setup" }>; onDone: () => void }) {
  const [workspaceName, setWorkspaceName] = useState(gate.workspace.name);
  const [channelName, setChannelName] = useState("general");
  const [error, setError] = useState(""); const [busy, setBusy] = useState(false);
  const [inviteLink, setInviteLink] = useState("");
  const submit = async (e: FormEvent) => {
    e.preventDefault(); setBusy(true); setError("");
    try {
      const token = await session.accessToken();
      const { channel_id } = await api.createChannel(token, "public", channelName.trim());
      await api.addDefaultChannel(token, gate.workspace.workspace_id, channel_id, false);
      await api.updateWorkspace(token, gate.workspace.workspace_id, { name: workspaceName.trim(), setup_completed: true });
      const { code } = await api.createInvite(token, gate.workspace.workspace_id);
      setInviteLink(`${location.origin}/?join=${code}&ws=${gate.workspace.slug}`);
    } catch (err) { setError(errorText(err)); setBusy(false); }
  };
  if (inviteLink)
    return (
      <main className="auth-shell">
        <section className="auth-terminal">
          <header><strong>xmsg · invite people</strong><span>{location.host}</span></header>
          <div className="invite-step">
            <p className="muted">a chat is useless alone — share this link, or send it later from an empty channel:</p>
            <InviteLinkAction onCreate={() => Promise.resolve(inviteLink)} onLater={onDone} />
          </div>
        </section>
      </main>
    );
  return <SetupScreen workspaceName={workspaceName} setWorkspaceName={setWorkspaceName} channelName={channelName} setChannelName={setChannelName} error={error} busy={busy} onSubmit={(e) => void submit(e)} />;
}

function useNarrow(): boolean {
  const [narrow, setNarrow] = useState(() => matchMedia("(max-width: 900px)").matches);
  useEffect(() => {
    const media = matchMedia("(max-width: 900px)");
    const changed = () => setNarrow(media.matches);
    media.addEventListener("change", changed);
    return () => media.removeEventListener("change", changed);
  }, []);
  return narrow;
}

function ChatView({ me, initial, initialChannel, workspace, workspaces, fresh, theme, toggleTheme, onSignOut }: { me: string; initial: ChatState; initialChannel: string | null; workspace: Workspace | undefined; workspaces: Workspace[]; fresh: boolean; theme: Theme; toggleTheme: () => void; onSignOut: () => void }) {
  const notifyRef = useRef<(channel: Channel, msgSeq: number, mentioned: boolean) => void>(() => undefined);
  const { state, status, syncError, send, upload, nameChannel, markRead, setMuted, retry, typingIn, notifyTyping, react, openThread, watchPresence, presenceOf, presenceKnown, view, editMessage, preloadRecent } = useChat(me, initial, { onIncoming: (c, s, m) => notifyRef.current(c, s, m) });
  const names = useNames(me);
  const [hits, setHits] = useState<SearchHit[] | null>(null); const [searchNote, setSearchNote] = useState("");
  const [threadOf, setThreadOf] = useState<number | null>(null); const [inCall, setInCall] = useState<string | null>(null); const [declined, setDeclined] = useState<Set<string>>(new Set());
  const [selected, setSelected] = useState<string | null>(() => (initialChannel && initial.channels[initialChannel] ? initialChannel : new URLSearchParams(location.search).get("channel")));
  const [main, setMain] = useState<"chat" | "admin">("chat");
  const [modal, setModal] = useState<"none" | "channel" | "dm" | "settings" | "info">("none");
  const [switcher, setSwitcher] = useState(false);
  const [shortcuts, setShortcuts] = useState(false);
  const narrow = useNarrow();
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [people, setPeople] = useState<{ id: string; name: string }[]>([]);
  const [editReq, setEditReq] = useState<{ seq: number; nonce: number }>({ seq: 0, nonce: 0 });
  const [welcome, setWelcome] = useState(fresh);
  const pendingInitial = useRef<string | null>(initialChannel && !initial.channels[initialChannel] ? initialChannel : null);
  // First-unread marker per channel, set when the channel is opened.
  const markers = useRef(new Map<string, number>());
  const leaveCall = useCallback(() => setInCall(null), []);
  const channels = useMemo(() => Object.values(state.channels).filter((c) => !c.left), [state.channels]);
  const rooms = channels.filter((c) => c.kind !== "dm" && c.kind !== "group_dm"); const dms = channels.filter((c) => c.kind === "dm" || c.kind === "group_dm");
  const calls = channels.filter((c) => c.call);
  const current = selected ? state.channels[selected] : undefined;
  const currentTitle = current ? channelTitle(current, me, names) : "";
  const myRole = workspace?.role ?? "member";
  const isStaff = myRole === "owner" || myRole === "admin";
  const messages = current ? (state.messages[current.id] ?? []) : [];
  const ringers = Object.values(state.channels).filter((c) => c.id !== inCall && c.call && ringing(c, me) && !declined.has(c.call.callId));

  // Notifications and badge: DMs and mentions, muted channels stay quiet, unfocused only.
  notifyRef.current = (channel, _msgSeq, mentioned) => {
    if (!isDesktop || document.hasFocus()) return;
    if (channel.muted || channel.notifyLevel === "nothing") return;
    if (channel.notifyLevel === "mentions" && !mentioned) return;
    void notify(channel.name ? `#${channel.name}` : "xmsg", mentioned ? "you were mentioned" : "new direct message");
  };
  const totalUnread = unreadTotal(channels);
  useEffect(() => {
    document.title = totalUnread > 0 ? `(${totalUnread}) xmsg` : "xmsg";
    void setBadge(totalUnread);
  }, [totalUnread]);

  // Preload the five most recent channels: their missed events arrive in the background.
  useEffect(() => {
    if (channels.length === 0) return;
    preloadRecent([...channels].sort((a, b) => b.msgSeq - a.msgSeq).slice(0, 5).map((c) => c.id));
  }, [channels.length]); // eslint-disable-line react-hooks/exhaustive-deps

  // Welcome line: once, for someone who just joined the defaults; any action dismisses it.
  useEffect(() => {
    if (!welcome) return;
    const dismiss = () => { setWelcome(false); try { localStorage.setItem(WELCOMED_KEY, "1"); } catch { /* private mode */ } };
    const timer = setTimeout(() => {
      window.addEventListener("keydown", dismiss, { once: true });
      window.addEventListener("pointerdown", dismiss, { once: true });
    }, 500);
    return () => { clearTimeout(timer); window.removeEventListener("keydown", dismiss); window.removeEventListener("pointerdown", dismiss); };
  }, [welcome]);

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
  const openThreadFor = useCallback((message: Message) => {
    if (!current) return; setThreadOf(message.msgSeq); openThread(current.id, { msgSeq: message.msgSeq, messageId: messageIdOf(message) });
  }, [current, openThread]); // eslint-disable-line react-hooks/exhaustive-deps
  const inviteTo = useCallback((channel: Channel) => {
    const user = prompt("User ID to invite")?.trim();
    if (user) void session.accessToken().then((token) => api.join(token, channel.id, user)).catch((e) => alert(errorText(e)));
  }, []);
  const createInviteLink = useCallback(async () => {
    if (!workspace) throw new Error("no workspace");
    const { code } = await api.createInvite(await session.accessToken(), workspace.workspace_id);
    return `${location.origin}/?join=${code}&ws=${workspace.slug}`;
  }, [workspace]);

  // The people list for the switcher and @ autocomplete: all members, fetched once.
  const peopleLoaded = useRef(false);
  const loadPeople = useCallback(() => {
    if (peopleLoaded.current || !workspace) return;
    peopleLoaded.current = true;
    void (async () => {
      try {
        const token = await session.accessToken();
        const all: AdminMember[] = [];
        let after: string | undefined;
        for (;;) {
          const page = await api.workspaceMembers(token, workspace.workspace_id, after);
          all.push(...page.members);
          if (!page.next) break;
          after = page.next;
        }
        setPeople(all.map((m) => ({ id: m.user_id, name: m.name })));
        all.forEach((m) => names.remember(m));
      } catch { /* autocomplete just has channel members */ }
    })();
  }, [workspace, names]);
  useEffect(loadPeople, [loadPeople]);

  // Call ring: a visible line with answer/decline, and a short sound per new ring.
  const seenRings = useRef(new Set<string>());
  useEffect(() => {
    for (const c of ringers) {
      const callId = c.call!.callId;
      if (seenRings.current.has(callId)) continue;
      seenRings.current.add(callId);
      playRing();
    }
  }, [ringers]);

  const answer = (channel: Channel) => { setSelected(channel.id); setInCall(channel.id); };
  const decline = (channel: Channel) => setDeclined((d) => new Set(d).add(channel.call!.callId));

  const pickPerson = useCallback((id: string) => {
    setMain("chat");
    const existing = dms.find((c) => c.kind === "dm" && c.members.includes(id));
    if (existing) { setSelected(existing.id); return; }
    void session.accessToken().then((token) => api.openDm(token, [id])).then(({ channel_id }) => setSelected(channel_id)).catch((e) => alert(errorText(e)));
  }, [dms]);
  const doSearch = useCallback(async (q: string) => {
    setMain("chat");
    setSearchNote("");
    try { setHits(await api.search(await session.accessToken(), q)); } catch (err) { setHits([]); setSearchNote(errorText(err)); }
  }, []);

  const selectChannel = (id: string) => {
    markers.current.set(id, state.channels[id]?.readSeq ?? 0);
    setSelected(id); setThreadOf(null); setHits(null); setMain("chat");
  };
  if (selected && !markers.current.has(selected)) markers.current.set(selected, state.channels[selected]?.readSeq ?? 0);

  const editLast = () => {
    if (!current) return;
    const last = messages.filter((m) => m.authorId === me && m.msgSeq > 0 && !m.deleted).at(-1);
    if (last) setEditReq({ seq: last.msgSeq, nonce: Date.now() });
  };

  const moveChannel = (dir: 1 | -1) => {
    if (channels.length === 0) return;
    const index = selected ? channels.findIndex((c) => c.id === selected) : -1;
    const next = channels[(index + dir + channels.length) % channels.length]!;
    selectChannel(next.id);
  };

  const fieldFocused = () => {
    const el = document.activeElement;
    return el instanceof HTMLElement && (el.tagName === "INPUT" || el.tagName === "TEXTAREA" || el.tagName === "SELECT" || el.isContentEditable);
  };
  useEffect(() => {
    const keys = (e: KeyboardEvent) => {
      if (e.ctrlKey && e.key.toLowerCase() === "k") { e.preventDefault(); setSwitcher(true); return; }
      if (e.ctrlKey && e.key.toLowerCase() === "n") { e.preventDefault(); setModal("channel"); return; }
      if (e.ctrlKey && e.key.toLowerCase() === "t") { e.preventDefault(); const roots = current ? state.messages[current.id] ?? [] : []; const root = [...roots].reverse().find((m) => m.msgSeq > 0 && messageIdOf(m)); if (root) openThreadFor(root); return; }
      if (e.ctrlKey && e.key.toLowerCase() === "i") { e.preventDefault(); if (current && isStaff && current.kind !== "dm") inviteTo(current); return; }
      if (e.altKey && e.key === "ArrowDown") { e.preventDefault(); moveChannel(1); return; }
      if (e.altKey && e.key === "ArrowUp") { e.preventDefault(); moveChannel(-1); return; }
      if (e.key === "Escape") {
        if (switcher || shortcuts) return; // those close themselves
        if (modal !== "none") { setModal("none"); return; }
        if (current) markRead(current.id);
        setThreadOf(null); setHits(null); setInCall(null); setMain("chat");
        return;
      }
      if (fieldFocused() || e.ctrlKey || e.metaKey || e.altKey) return;
      if (e.key === "?") { setShortcuts(true); return; }
      const ringingChannel = ringers[0];
      if (ringingChannel && e.key.toLowerCase() === "a") { answer(ringingChannel); return; }
      if (ringingChannel && e.key.toLowerCase() === "x") { decline(ringingChannel); return; }
    };
    window.addEventListener("keydown", keys); return () => window.removeEventListener("keydown", keys);
  }, [current, state.messages, openThreadFor, isStaff, inviteTo, selected, channels, switcher, shortcuts, modal, ringers]); // eslint-disable-line react-hooks/exhaustive-deps

  const currentRoot = current && threadOf !== null ? (state.messages[current.id] ?? []).find((m) => m.msgSeq === threadOf) : undefined;
  const closePanels = () => { setThreadOf(null); setInCall(null); };
  const membersList = (current?.members ?? []).map((id) => ({ id, name: id === me ? "you" : names.label(id) }));
  const channelsList = rooms.filter((c) => c.name).map((c) => ({ id: c.id, name: c.name }));
  const commands = [{ name: "huddle", hint: "start a huddle" }];

  return <div className="terminal-app"><StatusLine workspace={workspace?.slug ?? ""} channel={currentTitle} status={status} /><div className={`workspace-grid ${currentRoot || inCall ? "with-panel" : ""}`}>
    <div className={`sidebar-wrap ${narrow && sidebarOpen ? "open" : ""}`}><Sidebar rooms={rooms.map((channel) => ({ channel, title: channelTitle(channel, me, names) }))} dms={dms.map((channel) => { const other = channel.kind === "dm" ? channel.members.find((m) => m !== me) : undefined; return { channel, title: channelTitle(channel, me, names), presence: other && presenceKnown(other) ? presenceOf(other) : undefined }; })} calls={calls.map((channel) => ({ channel, title: channel.call?.huddle ? `huddle-${channel.id.slice(-4)}` : channelTitle(channel, me, names) }))} selected={selected} onSelect={(id) => { selectChannel(id); setSidebarOpen(false); }} onNewChannel={() => setModal("channel")} onNewDm={() => setModal("dm")} onAdmin={isStaff ? () => setMain("admin") : undefined} meName={names.label(me)} onOpenSettings={() => setModal("settings")} /></div>
      <main className="main-terminal">{main === "admin" && workspace ? <AdminScreen workspace={workspace} myRole={myRole} me={me} rooms={rooms} onClose={() => setMain("chat")} /> : <>
        {narrow && <button className="link narrow-menu" onClick={() => setSidebarOpen((v) => !v)}>[ menu ]</button>}
        {status === "offline" && <p className="note-text offline-note">can't reach {location.host} · retrying{isDesktop && <> · <button className="link" onClick={() => setModal("settings")}>change server</button></>}</p>}
        {syncError && <p className="note-text">couldn't load history · <button className="link" onClick={() => location.reload()}>retry</button></p>}
        <div id="screen-share-slot" />
        {ringers.map((c) => <div key={c.id} className="call-ring" role="alert"><span>{names.label(c.call!.participants[0]!)} is calling</span><button onClick={() => answer(c)}>[a] answer</button><button onClick={() => decline(c)}>[x] decline</button></div>)}
        {hits !== null ? <SearchResults hits={hits} note={searchNote} me={me} names={names} channelName={(id) => state.channels[id] ? channelTitle(state.channels[id]!, me, names) : "unknown"} onPick={(hit) => { setHits(null); selectChannel(hit.channelId); setThreadOf(hit.reply ? hit.rootMsgSeq : null); const root = hit.reply ? (state.messages[hit.channelId] ?? []).find((m) => m.msgSeq === hit.rootMsgSeq) : undefined; if (root?.messageId) openThread(hit.channelId, { msgSeq: root.msgSeq, messageId: root.messageId }); }} onClose={() => setHits(null)} /> : current ? <section className="conversation" onDragOver={(e) => { if (e.dataTransfer.types.includes("Files")) { e.preventDefault(); e.currentTarget.classList.add("drop-target"); } }} onDragLeave={(e) => e.currentTarget.classList.remove("drop-target")} onDrop={(e) => e.currentTarget.classList.remove("drop-target")}>
          <ConversationHeader title={currentTitle} onCall={() => { setInCall(current.id); setThreadOf(null); }} onOpenInfo={() => setModal("info")} />
          {welcome && <p className="welcome-line">welcome · ^K to find anyone · ? for shortcuts</p>}
          <MessageList
            channel={current}
            messages={messages}
            me={me}
            names={names}
            newMarker={selected ? (markers.current.get(selected) ?? 0) : 0}
            presenceOf={presenceOf}
            presenceKnown={presenceKnown}
            onOpenThread={openThreadFor}
            onReact={(m, emoji, add) => react(current.id, m.msgSeq, emoji, add)}
            onEdit={(m, body) => { editMessage(current.id, m.msgSeq, body); return Promise.resolve(); }}
            onDelete={(m) => { if (confirm("Delete this message for everyone?")) void session.accessToken().then((token) => api.deleteMessage(token, current.id, m.msgSeq)).catch((e) => alert(errorText(e))); }}
            onRetry={retry}
            onCaughtUp={() => markRead(current.id)}
            editRequest={editReq}
            emptyExtra={<li className="start-of-channel"><span className="muted">this is the start of #{current.name || current.id.slice(-6)}</span>{isStaff && current.kind !== "dm" && <span className="start-actions"><button className="link" onClick={() => inviteTo(current)}>invite people → ^I</button><InviteLinkAction onCreate={createInviteLink} /></span>}</li>}
          />
          <div className="typing">{typingIn(current.id).length > 0 && `${typingIn(current.id).map((id) => names.label(id)).join(", ")} typing…`}</div>
          <Prompt channel={currentTitle} members={current.members} me={me} names={names} draftKey={draftKey(current.id)} membersList={membersList} channelsList={channelsList} commands={commands} onSend={(body, files) => send(current.id, body, undefined, files)} onUpload={(file) => upload(current.id, file)} onTyping={() => notifyTyping(current.id)} onCommand={(command) => { if (command === "/huddle") { setInCall(current.id); setThreadOf(null); } }} onEditLast={editLast} readOnly={!canPost(current, me)} />
        </section> : <div className="center muted">{channels.length ? "select a conversation" : "create a channel or start a DM"}</div>}</>}</main>
    {currentRoot && current && !inCall && <ThreadPanel root={currentRoot} replies={state.messages[threadKey(current.id, currentRoot.msgSeq)] ?? []} me={me} names={names} channel={currentTitle} draftKey={draftKey(current.id, currentRoot.msgSeq)} membersList={membersList} channelsList={channelsList} commands={commands} onSend={(body, files) => send(current.id, body, { msgSeq: currentRoot.msgSeq, messageId: messageIdOf(currentRoot) }, files)} onUpload={(file) => upload(current.id, file)} onRetry={retry} onClose={() => setThreadOf(null)} />}
    {inCall && state.channels[inCall] && <Suspense fallback={<aside className="right-panel"><p className="muted">starting call…</p></aside>}><CallPanel key={inCall} channelId={inCall} title={channelTitle(state.channels[inCall]!, me, names)} label={(id) => names.label(id)} onLeave={leaveCall} onClose={closePanels} /></Suspense>}
  </div><KeyHints inCall={Boolean(inCall)} />
  {switcher && <Switcher channels={channels.map((c) => ({ id: c.id, title: channelTitle(c, me, names), unread: c.muted ? 0 : unread(c) }))} people={people} me={me} onPickChannel={selectChannel} onPickPerson={pickPerson} onSearch={(q) => void doSearch(q)} onClose={() => setSwitcher(false)} />}
  {shortcuts && <ShortcutsPanel onClose={() => setShortcuts(false)} />}
  {modal === "channel" || modal === "dm" ? <CreatePanel kind={modal} onClose={() => setModal("none")} onCreated={(id, name) => { if (name) nameChannel(id, name); selectChannel(id); setModal("none"); }} /> : null}
  {modal === "settings" && <SettingsPanel me={me} names={names} theme={theme} onToggleTheme={toggleTheme} workspaces={workspaces} currentWorkspaceId={workspace?.workspace_id ?? ""} serverInitial={isDesktop ? serverUrl() : undefined} onSaveProfile={async (name, avatar) => names.remember(await api.updateMe(await session.accessToken(), { name, avatar_url: avatar || null }))} onSaveServer={isDesktop ? (url) => { if (!validServerUrl(url)) return alert("Use an https URL (or http for localhost / 127.0.0.1)"); saveServerUrl(url); location.reload(); } : undefined} onAcceptInvite={async (code) => { await api.acceptInvite(await session.accessToken(), code); }} onSignOut={onSignOut} onClose={() => setModal("none")} />}
  {modal === "info" && current && <ChannelInfoPanel channel={current} title={currentTitle} me={me} names={names} onMute={() => setMuted(current.id, !current.muted)} onInvite={current.kind !== "dm" ? () => inviteTo(current) : undefined} onLeave={current.kind !== "dm" ? () => { void session.accessToken().then((token) => api.remove(token, current.id, me)).catch((e) => alert(errorText(e))); } : undefined} onClose={() => setModal("none")} />}
  </div>;
}

/** The admin view's data and actions, behind the [admin] sidebar entry. */
function AdminScreen({ workspace, myRole, me, rooms, onClose }: { workspace: Workspace; myRole: "owner" | "admin" | "member"; me: string; rooms: Channel[]; onClose: () => void }) {
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

/** Two soft beeps for an incoming DM call (existing call events carry the ring). */
function playRing(): void {
  try {
    const ctx = new AudioContext();
    for (let i = 0; i < 2; i++) {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.frequency.value = 880;
      gain.gain.setValueAtTime(0.0001, ctx.currentTime + i * 0.4);
      gain.gain.exponentialRampToValueAtTime(0.08, ctx.currentTime + i * 0.4 + 0.03);
      gain.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + i * 0.4 + 0.3);
      osc.connect(gain).connect(ctx.destination);
      osc.start(ctx.currentTime + i * 0.4);
      osc.stop(ctx.currentTime + i * 0.4 + 0.35);
    }
  } catch { /* no audio device: the visible ring line still shows */ }
}
