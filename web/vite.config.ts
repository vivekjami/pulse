import { defineConfig } from "vite";
import { resolve } from "node:path";

export default defineConfig({
  // Zerops serves the built artifact from the static service's document root.
  // Two entry points: the wall at / and the receipts ledger at /events/.
  // Directory-style output means nginx serves /events/ with no rewrite rules.
  build: {
    outDir: "dist",
    emptyOutDir: true,
    sourcemap: false,
    rollupOptions: {
      input: {
        main: resolve(__dirname, "index.html"),
        events: resolve(__dirname, "events/index.html"),
      },
    },
  },
  server: {
    // Container-correct dev settings: bind all interfaces, not loopback.
    host: "0.0.0.0",
    port: 5173,
  },
});
