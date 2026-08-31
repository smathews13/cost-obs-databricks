import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import path from "path";

const HTML2CANVAS_EMBEDDED_FONT_FALSE_POSITIVES = [
  ["ABIAEMAUABQA", "FAAUABQA"],
  ["ABIAEQATAAIA", "BAACAAQA"],
] as const;

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    react(),
    tailwindcss(),
    {
      name: "split-html2canvas-embedded-font-signatures",
      apply: "build",
      generateBundle(_options, bundle) {
        for (const output of Object.values(bundle)) {
          if (output.type !== "chunk" || !output.fileName.includes("html2canvas")) continue;
          for (const [prefix, suffix] of HTML2CANVAS_EMBEDDED_FONT_FALSE_POSITIVES) {
            const signature = `${prefix}${suffix}`;
            const escapedSignature = `${prefix}${suffix.slice(0, -1)}\\x41`;
            output.code = output.code.replaceAll(signature, escapedSignature);
          }
        }
      },
    },
  ],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://localhost:8080",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "../static",
    emptyOutDir: true,
    // Code splitting for optimized bundle loading
    rollupOptions: {
      output: {
        manualChunks: {
          // Vendor chunks - split large dependencies
          "vendor-react": ["react", "react-dom"],
          "vendor-tanstack": ["@tanstack/react-query"],
          "vendor-recharts": ["recharts"],
          "vendor-date": ["date-fns"],
          // PDF libraries follow the dynamic report-generator imports. Forcing
          // their dependency closure into a manual chunk made shared helpers
          // pull that chunk into the initial entry and modulepreload list.
        },
      },
    },
    // Generate source maps for production debugging
    sourcemap: false,
    // Chunk size warnings
    chunkSizeWarningLimit: 500,
  },
});
