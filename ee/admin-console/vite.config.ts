// SPDX-License-Identifier: LicenseRef-xmsg-EE
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Served at /admin by xmsg-admin-ee, on the same origin as the web app (it shares its
// sign-in). The dev server forwards to local services like the web app's does.
const env = (name: string, fallback: string) => process.env[name] ?? fallback;
const strip = (prefix: string) => (path: string) => path.slice(prefix.length);

export default defineConfig({
  base: "/admin/",
  plugins: [react()],
  server: {
    port: 5174,
    proxy: {
      "/admin/api": { target: env("XMSG_ADMIN_ORIGIN", "http://127.0.0.1:8096") },
      "/auth": { target: env("XMSG_AUTH_ORIGIN", "http://127.0.0.1:8081"), rewrite: strip("/auth") },
      "/users": { target: env("XMSG_USERS_ORIGIN", "http://127.0.0.1:8084"), rewrite: strip("/users") },
    },
  },
});
