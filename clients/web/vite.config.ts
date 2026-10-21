// SPDX-License-Identifier: Apache-2.0
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The dev server is the single origin the browser talks to; it forwards to the local
// services (override with XMSG_AUTH_ORIGIN etc.), so the services need no CORS.
const env = (name: string, fallback: string) => process.env[name] ?? fallback;
const strip = (prefix: string) => (path: string) => path.slice(prefix.length);

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/auth": { target: env("XMSG_AUTH_ORIGIN", "http://127.0.0.1:8081"), rewrite: strip("/auth") },
      "/channels": { target: env("XMSG_CHANNELS_ORIGIN", "http://127.0.0.1:8082"), rewrite: strip("/channels") },
      "/ws": { target: env("XMSG_GATEWAY_ORIGIN", "ws://127.0.0.1:4000"), ws: true },
    },
  },
});
