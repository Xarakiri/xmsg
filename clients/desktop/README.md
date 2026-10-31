<!-- SPDX-License-Identifier: Apache-2.0 -->
# xmsg desktop

Tauri 2 desktop app for xmsg. It bundles the [`@xmsg/web`](../web) build unchanged —
the same UI compiled with `VITE_XMSG_DESKTOP=1`, which switches it to talk to a remote
server instead of the same origin:

- **First run** shows an `xmsg · connect` screen asking for the server URL (prefilled
  with `https://3-13-72-4.sslip.io`). The URL is stored in localStorage and can be
  changed from `[ server ]` in the sidebar. Only `https` is accepted, or `http` for
  `localhost` / `127.0.0.1`.
- **REST** goes through `@tauri-apps/plugin-http` (native, so no CORS). The capability
  scopes it to `https://*` and localhost only.
- **WebSocket and LiveKit** stay in the webview, using URLs built from the saved server.
- **SSO and web push are hidden** (neither works in the desktop shell; no service
  worker is shipped).
- **Screen share** is hidden with a note when the webview lacks `getDisplayMedia`.
- **File downloads** use the native save dialog (`@tauri-apps/plugin-dialog`) and write
  through `@tauri-apps/plugin-fs`, scoped to Downloads/Documents/Desktop.

The `src-tauri` crate is deliberately **not** part of the repository's root Cargo
workspace; it builds and locks independently.

## Scripts

```sh
# from clients/
npm ci
npm run dev -w @xmsg/desktop     # vite dev server + tauri dev window
npm run build -w @xmsg/web       # the plain web build (unchanged behaviour)
npm run build -w @xmsg/desktop -- --bundles app   # .app bundle in src-tauri/target/release/bundle/osx/
```

`npm run build -w @xmsg/desktop` first builds `@xmsg/web` with `VITE_XMSG_DESKTOP=1`
(via `beforeBuildCommand` in `tauri.conf.json`), then compiles the Rust shell.

Icons are placeholders generated from an `x_` mark on the dark background colour:

```sh
python3 src-tauri/icons/gen_icons.py   # then re-run iconutil, or edit by hand
```
