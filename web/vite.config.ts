import { defineConfig } from "vite";
import { resolve } from "node:path";

export default defineConfig({
  // Zerops serves the built artifact from the static service's document root.
  // Four entry points: the wall at /, the receipts ledger at /events/, Vandal
  // Patrol at /patrol/ and the standings at /leaderboard/.
  // Directory-style output means nginx serves each with no rewrite rules.
  build: {
    outDir: "dist",
    emptyOutDir: true,
    sourcemap: false,
    rollupOptions: {
      input: {
        main: resolve(__dirname, "index.html"),
        events: resolve(__dirname, "events/index.html"),
        patrol: resolve(__dirname, "patrol/index.html"),
        leaderboard: resolve(__dirname, "leaderboard/index.html"),
      },
    },
  },
  server: {
    // Container-correct dev settings: bind all interfaces, not loopback.
    host: "0.0.0.0",
    port: 5173,
  },
});
