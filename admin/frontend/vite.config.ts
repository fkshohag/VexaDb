import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// During `npm run dev`, Vite serves the SPA on :5173 and proxies /api to the
// Go backend on :8090, which in turn proxies to the VectorDB gateway.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8090",
        changeOrigin: true,
      },
      "/config.json": {
        target: "http://127.0.0.1:8090",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
    sourcemap: true,
  },
});
