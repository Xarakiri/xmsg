// SPDX-License-Identifier: Apache-2.0
// Files in messages (ADR-0024): uploads waiting in the composer, and attachments in the
// list. The media service wants a bearer token, which <img src> and <a href> can't send,
// so files are fetched with it and shown through object URLs.

import { useEffect, useState } from "react";
import type { Attachment } from "@xmsg/client-core";
import { api, session } from "./session.ts";

export function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(bytes < 10 * 1024 ? 1 : 0)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

/** An object URL for a file (or its thumbnail), revoked when no longer shown. */
function useFileUrl(fileId: string, thumbnail: boolean): string | null | "error" {
  const [url, setUrl] = useState<string | null | "error">(null);
  useEffect(() => {
    let current: string | undefined;
    let cancelled = false;
    setUrl(null);
    session
      .accessToken()
      .then((token) => api.fileBlob(token, fileId, thumbnail))
      .then((blob) => {
        if (cancelled) return;
        current = URL.createObjectURL(blob);
        setUrl(current);
      })
      .catch(() => !cancelled && setUrl("error"));
    return () => {
      cancelled = true;
      if (current) URL.revokeObjectURL(current);
    };
  }, [fileId, thumbnail]);
  return url;
}

/** Saves the file under its name (fetched with the token, then handed to the browser). */
async function download(file: Attachment): Promise<void> {
  const blob = await api.fileBlob(await session.accessToken(), file.fileId);
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = file.name;
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 10_000);
}

/** Opens the full image in a new tab. */
async function openImage(file: Attachment): Promise<void> {
  // Opened first, filled after the fetch: browsers block windows opened later.
  const tab = window.open("", "_blank");
  const blob = await api.fileBlob(await session.accessToken(), file.fileId);
  const url = URL.createObjectURL(blob);
  if (tab) tab.location.href = url;
  else window.location.assign(url);
}

function ImagePreview({ file }: { file: Attachment }) {
  const url = useFileUrl(file.fileId, true);
  // Reserve the thumbnail's space up front, so the list doesn't jump when it loads.
  const scale = file.width && file.height ? Math.min(1, 400 / Math.max(file.width, file.height)) : 1;
  const size = file.width && file.height ? { width: Math.round(file.width * scale), aspectRatio: `${file.width} / ${file.height}` } : undefined;
  if (url === "error") return <FileChip file={file} />;
  return (
    <button type="button" className="image-preview" onClick={() => void openImage(file)} title={`${file.name} (${formatSize(file.size)})`} style={size}>
      {url ? <img src={url} alt={file.name} /> : <span className="muted small">Loading…</span>}
    </button>
  );
}

function FileChip({ file }: { file: Attachment }) {
  const [failed, setFailed] = useState(false);
  return (
    <button
      type="button"
      className="file-chip"
      onClick={() => download(file).catch(() => setFailed(true))}
      title={failed ? "Couldn't download; try again" : `Download ${file.name}`}
    >
      <span aria-hidden="true">📄</span> <span className="file-name">{file.name}</span>{" "}
      <span className="muted small">{failed ? "failed" : formatSize(file.size)}</span>
    </button>
  );
}

export function Attachments({ files }: { files: Attachment[] }) {
  return (
    <div className="attachments">
      {files.map((f) => (f.hasThumbnail ? <ImagePreview key={f.fileId} file={f} /> : <FileChip key={f.fileId} file={f} />))}
    </div>
  );
}

/** A file picked in the composer: uploading, ready to send, or failed. */
export interface PendingFile {
  id: string;
  name: string;
  size: number;
  attachment?: Attachment;
  error?: string;
}

export function PendingFiles({ files, onRemove }: { files: PendingFile[]; onRemove: (id: string) => void }) {
  if (files.length === 0) return null;
  return (
    <ul className="pending-files">
      {files.map((f) => (
        <li key={f.id} className={f.error ? "error" : ""}>
          <span className="file-name">{f.name}</span>{" "}
          <span className="muted small">{f.error ?? (f.attachment ? formatSize(f.size) : "uploading…")}</span>
          <button type="button" className="link" onClick={() => onRemove(f.id)} aria-label={`Remove ${f.name}`}>
            ×
          </button>
        </li>
      ))}
    </ul>
  );
}
