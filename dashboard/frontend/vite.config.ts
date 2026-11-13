import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Embedded into `agave-validator` by `dashboard/build.rs`. Assets go under `assets/`, the prefix
// the server caches, referenced absolutely since unknown paths fall back to index.html.
export default defineConfig({
  plugins: [react()],
  base: "/",
  build: {
    outDir: "dist",
    emptyOutDir: true,
    assetsDir: "assets",
    // One bundle keeps the embedded asset table small and avoids a waterfall of requests.
    rollupOptions: {
      output: {
        manualChunks: undefined,
      },
    },
  },
  server: {
    proxy: {
      "/websocket": {
        target: "ws://127.0.0.1:11000",
        ws: true,
      },
    },
  },
});
