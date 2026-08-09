import { defineConfig } from "vite";

export default defineConfig({
  // Zerops serves the built artifact from the static service's document root.
  build: { outDir: "dist", emptyOutDir: true, sourcemap: false },
  server: {
    // Container-correct dev settings: bind all interfaces, not loopback.
    host: "0.0.0.0",
    port: 5173,
  },
});
