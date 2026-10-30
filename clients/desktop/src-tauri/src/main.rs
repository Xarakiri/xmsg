// SPDX-License-Identifier: Apache-2.0
// xmsg desktop shell: hosts the @xmsg/web UI. REST goes through the native HTTP plugin
// (no CORS); WebSocket and LiveKit stay in the webview. Permissions are scoped in
// capabilities/default.json.

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_http::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_fs::init())
        .run(tauri::generate_context!())
        .expect("error while running xmsg");
}

fn main() {
    run();
}
