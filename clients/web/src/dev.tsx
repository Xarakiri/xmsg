// SPDX-License-Identifier: Apache-2.0
import { useState } from "react";
import { createRoot } from "react-dom/client";
import { Directory, type Channel, type Message, type Workspace } from "@xmsg/client-core";
import { AdminPanel, AuthScreen, ConversationHeader, KeyHints, MessageRow, Prompt, SettingsPanel, SetupScreen, Sidebar, StatusLine, ThreadPanel, WaitingScreen, channelTitle, type AdminMember, type Theme } from "./components.tsx";
import "./styles.css";
import "./dev.css";

const me = "1001";
const names = new Directory(async () => []);
for (const [user_id, name] of [["1001", "you"], ["1002", "aiko"], ["1003", "marcus"], ["1004", "dmitri"], ["1005", "sara"]] as const) names.remember({ user_id, name, avatar_url: null });
const baseChannel = { eventSeq: 1, syncedSeq: 1, membershipSeq: 1, readSeq: 1, muted: false, notifyLevel: "all" as const, left: false, mentions: [] };
const gateway: Channel = { ...baseChannel, id: "9001", kind: "public", name: "gateway", members: ["1001", "1002", "1003", "1004"], msgSeq: 4 };
const dm: Channel = { ...baseChannel, id: "9002", kind: "dm", name: "", members: ["1001", "1002"], msgSeq: 3 };
const huddle: Channel = { ...gateway, call: { callId: "call-1", huddle: true, participants: ["1001", "1002", "1004"], startedAt: Date.now() - 600000 } };
const messages: Message[] = [
  { key: "1", channelId: gateway.id, authorId: "1002", body: "gateway deploy is ready for review", msgSeq: 1, messageId: "101", status: "sent", at: Date.now() - 2400000, replyCount: 2, reactions: { "👍": { count: 2, mine: false, seq: 1 } } },
  { key: "2", channelId: gateway.id, authorId: "1003", body: "can we publish the full spike report?", msgSeq: 2, messageId: "102", status: "sent", at: Date.now() - 1800000 },
  { key: "3", channelId: gateway.id, authorId: "1004", body: "shipping phase 1b to eu-west", msgSeq: 3, messageId: "103", status: "sent", at: Date.now() - 900000 },
  { key: "4", channelId: gateway.id, authorId: me, body: "great run — dashboards are updated", msgSeq: 4, messageId: "104", status: "sent", at: Date.now() - 300000 },
];
const replies: Message[] = [
  { ...messages[0]!, key: "r1", msgSeq: 1, body: "raw csv is in the shared drive" },
  { ...messages[0]!, key: "r2", msgSeq: 2, authorId: me, body: "svg please — adding it to the deck" },
];
const noFiles = async () => { throw new Error("fixtures are read-only"); };
const noop = () => {};
const workspace: Workspace = { workspace_id: "1", name: "acme", slug: "acme", role: "owner", setup_completed: true };
const adminMembers: AdminMember[] = [
  { user_id: "1001", name: "you", avatar_url: null, role: "owner" },
  { user_id: "1002", name: "aiko", avatar_url: null, role: "admin" },
  { user_id: "1003", name: "marcus", avatar_url: null, role: "member" },
  { user_id: "1004", name: "dmitri", avatar_url: null, role: "member" },
];
const defaults = [{ channelId: "9001", name: "general" }, { channelId: "9003", name: "sequencer" }];
const publicChannels = [...defaults, { channelId: "9004", name: "random" }, { channelId: "9005", name: "incidents" }];

type Screen = "channel" | "dm" | "huddle" | "share" | "login" | "register" | "setup" | "waiting" | "settings" | "admin-workspace" | "admin-channels" | "admin-members" | "admin-invites";
type ChatScreen = "channel" | "dm" | "huddle" | "share";
const screens: Screen[] = ["channel", "dm", "huddle", "share", "login", "register", "setup", "waiting", "settings", "admin-workspace", "admin-channels", "admin-members", "admin-invites"];

function DevApp() {
  const [screen, setScreen] = useState<Screen>(() => { const fromUrl = new URLSearchParams(location.search).get("screen"); return (screens as string[]).includes(fromUrl ?? "") ? (fromUrl as Screen) : "channel"; }); const [theme, setTheme] = useState<Theme>("dark");
  document.documentElement.dataset.theme = theme;
  return <><nav className="dev-nav"><strong>minimal cli / fixtures</strong>{screens.map((item) => <button className={item === screen ? "active" : ""} key={item} onClick={() => setScreen(item)}>{item}</button>)}<span /><button onClick={() => setTheme(theme === "dark" ? "light" : "dark")}>{theme} → {theme === "dark" ? "light" : "dark"}</button></nav><div className="dev-stage">{screen === "login" || screen === "register" ? <><StatusLine workspace="" channel="not connected" status="offline" /><AuthScreen mode={screen === "login" ? "in" : "up"} setMode={noop} name="Aiko Tanaka" setName={noop} email="aiko@acme.dev" setEmail={noop} password="correct-horse" setPassword={noop} passwordAgain="correct-horse" setPasswordAgain={noop} code="" setCode={noop} error="" busy={false} providers={[]} onSubmit={(e) => e.preventDefault()} /></> : screen === "setup" ? <><StatusLine workspace="" channel="not connected" status="offline" /><SetupScreen workspaceName="" setWorkspaceName={noop} channelName="general" setChannelName={noop} error="" busy={false} onSubmit={(e) => e.preventDefault()} /></> : screen === "waiting" ? <><StatusLine workspace="" channel="not connected" status="offline" /><WaitingScreen /></> : screen === "settings" ? <div className="terminal-app"><StatusLine workspace="acme" channel="#gateway" status="online" /><div className="center"><SettingsPanel me={me} names={names} theme={theme} onToggleTheme={noop} workspaces={[workspace]} currentWorkspaceId="1" onSaveProfile={() => Promise.resolve()} onAcceptInvite={() => Promise.resolve()} onSignOut={noop} onClose={noop} /></div></div> : screen.startsWith("admin") ? <AdminFixture key={screen} tab={screen.slice(6) as "workspace" | "channels" | "members" | "invites"} /> : <ChatFixture screen={screen as ChatScreen} />}</div></>;
}

function AdminFixture({ tab }: { tab: "workspace" | "channels" | "members" | "invites" }) {
  const [initialTab] = useState(tab);
  return <div className="terminal-app"><StatusLine workspace="acme" channel="admin" status="online" /><div className="workspace-grid"><Sidebar rooms={[{ channel: gateway, title: "#general" }]} dms={[]} calls={[]} selected={null} onSelect={noop} onNewChannel={noop} onNewDm={noop} onAdmin={noop} meName="you" onOpenSettings={noop} /><main className="main-terminal"><AdminPanel key={initialTab} workspace={workspace} myRole="owner" me={me} members={adminMembers} defaults={defaults} publicChannels={publicChannels} onRename={() => Promise.resolve()} onAddDefault={() => Promise.resolve()} onRemoveDefault={() => Promise.resolve()} onSetRole={() => Promise.resolve()} onRemoveMember={() => Promise.resolve()} onCreateInvite={() => Promise.resolve("acme-invite-7f3d9")} onClose={noop} /></main></div><KeyHints /></div>;
}

function ChatFixture({ screen }: { screen: ChatScreen }) {
  const current = screen === "dm" ? dm : screen === "huddle" || screen === "share" ? huddle : gateway;
  const title = channelTitle(current, me, names);
  const rooms = [gateway, { ...gateway, id: "9003", name: "sequencer", msgSeq: 13 }, { ...gateway, id: "9004", name: "random" }];
  return <div className="terminal-app"><StatusLine workspace="acme" channel={title} status="online" /><div className={`workspace-grid ${screen !== "dm" ? "with-panel" : ""}`}><Sidebar rooms={rooms.map((channel) => ({ channel, title: channelTitle(channel, me, names) }))} dms={[{ channel: dm, title: "@aiko", presence: "online" }]} calls={screen === "huddle" || screen === "share" ? [{ channel: huddle, title: "huddle-01" }] : []} selected={current.id} onSelect={noop} onNewChannel={noop} onNewDm={noop} onAdmin={noop} meName="you" onOpenSettings={noop} /><main className="main-terminal">{screen === "share" && <SharePreview />}<section className="conversation"><ConversationHeader title={title} onCall={noop} onOpenInfo={noop} /><ol className="message-list">{messages.map((message) => <MessageRow key={message.key} message={{ ...message, channelId: current.id }} me={me} names={names} onOpenThread={noop} onReact={noop} />)}</ol><Prompt channel={title} members={current.members} me={me} names={names} onSend={noop} onUpload={noFiles} /></section></main>{screen === "channel" && <ThreadPanel root={messages[0]!} replies={replies} me={me} names={names} channel={title} onSend={noop} onUpload={noFiles} onRetry={noop} onClose={noop} />}{(screen === "huddle" || screen === "share") && <aside className="right-panel call-panel"><header className="panel-title">◉ HUDDLE-01<button className="link">×</button></header><div className="call-status connected"><i />3 in call</div><div className="call-content"><section className="participant-list"><h3>— participants —</h3>{["aiko", "dmitri", "you"].map((name, index) => <div className="participant-row" key={name}><div><span className={index === 0 ? "speaker" : ""}>{name}</span><span className="muted"> · {index === 0 ? "speaking" : index === 2 ? "muted" : "live"}</span></div>{index < 2 && <div className="level-meter">{"▮".repeat(index === 0 ? 11 : 3)}{"▯".repeat(index === 0 ? 5 : 13)}</div>}</div>)}</section>{screen === "share" && <><section className="share-section"><h3>— screen share —</h3><span>dmitri <span className="muted">sharing</span></span></section><div className="call-tiles">{["aiko", "dmitri"].map((name) => <div className="call-tile fixture-camera" key={name}><span>{name.slice(0, 1).toUpperCase()}</span><span className="call-name">{name}</span></div>)}</div></>}</div><div className="call-controls"><button className="link">[m] mute</button><button className="link">[v] cam</button><button className="link">[s] share</button><button className="link">[q] leave</button></div></aside>}</div><KeyHints inCall={screen === "huddle" || screen === "share"} /></div>;
}
function SharePreview() {
  return <section className="screen-share fixture-screen-share"><header><span className="error">● LIVE</span> dmitri is sharing <strong>deployment-dashboard</strong></header><div className="fake-dashboard"><div><span>deployment</span><strong>phase 1b</strong></div><div className="fake-chart">╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲</div><div className="fake-bars">▂▃▅▃▆▅▇▆▅▇▆▇█▇▆▅▆▅▃</div></div></section>;
}

createRoot(document.getElementById("root")!).render(<DevApp />);
