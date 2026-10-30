import { defineConfig } from "vitest/config";
export default defineConfig({
  resolve: { alias: { "@": new URL("./src", import.meta.url).pathname } },
  test: {
    environment: "jsdom",
    restoreMocks: true,
    clearMocks: true,
    setupFiles: ["./tests/setup.ts"],
  },
});
