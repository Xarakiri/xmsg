// SPDX-License-Identifier: Apache-2.0
import { useEffect, useRef, useState } from "react";
import { Participant, Room, RoomEvent, Track, type RemoteTrack } from "livekit-client";
import { api, session } from "./session.ts";
import { Avatar } from "./components.tsx";

type Status = "connecting" | "connected" | "error";

function Tile({ participant, label }: { participant: Participant; label: string }) {
  const video = useRef<HTMLVideoElement>(null);
  const screen = participant.getTrackPublication(Track.Source.ScreenShare);
  const camera = participant.getTrackPublication(Track.Source.Camera);
  const shown = screen?.track && !screen.isMuted ? screen : camera?.track && !camera.isMuted ? camera : undefined;
  const track = shown?.track;
  useEffect(() => { const el = video.current; if (!track || !el) return; track.attach(el); return () => { track.detach(el); }; }, [track]);
  return <div className={`call-tile ${participant.isSpeaking ? "speaking" : ""}`}>{track ? <video ref={video} autoPlay playsInline muted={participant.isLocal} /> : <Avatar id={participant.identity} name={label} size="large" />}<span className="call-name">{label}{!participant.isMicrophoneEnabled && " · muted"}{shown?.source === Track.Source.ScreenShare && " · screen"}</span></div>;
}

export function CallPanel({ channelId, title, label, onLeave, onClose }: { channelId: string; title: string; label: (userId: string) => string; onLeave: () => void; onClose: () => void }) {
  const [room] = useState(() => new Room({ adaptiveStream: true, dynacast: true }));
  const [status, setStatus] = useState<Status>("connecting"); const [error, setError] = useState(""); const [, setTick] = useState(0);
  const controls = useRef({ mic: () => {}, camera: () => {}, screen: () => {}, leave: onLeave });
  useEffect(() => {
    const changed = () => setTick((n) => n + 1); let connected = false; const audio = new Map<string, HTMLMediaElement>();
    const subscribed = (track: RemoteTrack) => { if (track.kind !== Track.Kind.Audio) return changed(); const el = track.attach(); el.hidden = true; document.body.appendChild(el); audio.set(track.sid ?? "", el); };
    const unsubscribed = (track: RemoteTrack) => { const el = audio.get(track.sid ?? ""); if (el) { track.detach(el); el.remove(); audio.delete(track.sid ?? ""); } changed(); };
    room.on(RoomEvent.TrackSubscribed, subscribed).on(RoomEvent.TrackUnsubscribed, unsubscribed).on(RoomEvent.ParticipantConnected, changed).on(RoomEvent.ParticipantDisconnected, changed).on(RoomEvent.TrackMuted, changed).on(RoomEvent.TrackUnmuted, changed).on(RoomEvent.LocalTrackPublished, changed).on(RoomEvent.LocalTrackUnpublished, changed).on(RoomEvent.ActiveSpeakersChanged, changed).on(RoomEvent.Disconnected, () => connected && onLeave());
    let cancelled = false;
    session.accessToken().then((token) => api.joinCall(token, channelId)).then(async ({ url, token }) => { if (cancelled) return; await room.connect(url, token); connected = true; setStatus("connected"); await room.localParticipant.setMicrophoneEnabled(true).catch(() => setError("No microphone (or no permission)")); }).catch((e: unknown) => { if (!cancelled) { setStatus("error"); setError(e instanceof Error ? e.message : String(e)); } });
    return () => { cancelled = true; room.removeAllListeners(); for (const el of audio.values()) el.remove(); void room.disconnect(); };
  }, [room, channelId, onLeave]);
  const me = room.localParticipant;
  const toggle = (what: "mic" | "camera" | "screen") => {
    if (status !== "connected") return;
    const run = what === "mic" ? me.setMicrophoneEnabled(!me.isMicrophoneEnabled) : what === "camera" ? me.setCameraEnabled(!me.isCameraEnabled) : me.setScreenShareEnabled(!me.isScreenShareEnabled, { audio: true });
    run.then(() => setTick((n) => n + 1)).catch((e: unknown) => setError(e instanceof Error ? e.message : String(e)));
  };
  controls.current = { mic: () => toggle("mic"), camera: () => toggle("camera"), screen: () => toggle("screen"), leave: onLeave };
  useEffect(() => {
    const key = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement | null;
      if (target?.matches("input, textarea, select, [contenteditable=true]") || e.ctrlKey || e.metaKey || e.altKey) return;
      const action = { m: "mic", v: "camera", s: "screen" } as const;
      if (e.key.toLowerCase() in action) { e.preventDefault(); controls.current[action[e.key.toLowerCase() as keyof typeof action]](); }
      if (e.key.toLowerCase() === "q") { e.preventDefault(); controls.current.leave(); }
    };
    window.addEventListener("keydown", key); return () => window.removeEventListener("keydown", key);
  }, []);
  const participants: Participant[] = [me, ...room.remoteParticipants.values()];
  const sharing = participants.find((p) => p.getTrackPublication(Track.Source.ScreenShare)?.track);
  return <aside className={`right-panel call-panel ${sharing ? "has-screen" : ""}`} aria-label={`Call in ${title}`}><header className="panel-title">◉ {title.toUpperCase()}<button className="link" onClick={onClose}>×</button></header><div className={`call-status ${status}`}><i />{status === "connected" ? `${participants.length} in call` : status}</div>{error && <p className="error">{error}</p>}{status === "connected" && <div className="call-tiles">{participants.map((p) => <Tile key={p.identity} participant={p} label={p.isLocal ? "you" : label(p.identity)} />)}</div>}<div className="call-controls"><button onClick={() => toggle("mic")} disabled={status !== "connected"}>[m] {me.isMicrophoneEnabled ? "mute" : "unmute"}</button><button onClick={() => toggle("camera")} disabled={status !== "connected"}>[v] {me.isCameraEnabled ? "cam off" : "cam on"}</button><button onClick={() => toggle("screen")} disabled={status !== "connected"}>[s] {me.isScreenShareEnabled ? "stop share" : "share"}</button><button onClick={onLeave}>[q] leave</button></div></aside>;
}
