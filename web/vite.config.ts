import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "../go-collector/static",
    emptyOutDir: true,
  },
  server: {
    proxy: {
      "/api": "http://localhost:8081",
      "/healthz": "http://localhost:8081",
      "/metrics": "http://localhost:8081",
    },
  },
});
