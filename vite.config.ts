import { defineConfig } from "vite";

export default defineConfig({
  build: {
    lib: {
      entry: "src/index.ts",
      name: "usqldb",
      formats: ["es", "umd"],
      fileName: (format) => `usqldb.${format}.js`,
    },
    sourcemap: true,
    rollupOptions: {
      external: [
        "@jaepil/uqa",
        "libpg-query",
        "sql.js",
        "apache-arrow",
        "@duckdb/duckdb-wasm",
        "xterm",
        "highlight.js",
        "comlink",
      ],
      output: {
        globals: {
          "@jaepil/uqa": "uqa",
        },
      },
    },
  },
});
