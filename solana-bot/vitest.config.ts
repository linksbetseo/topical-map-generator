import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: "unit",
          include: ["packages/*/test/**/*.test.ts", "apps/*/test/**/*.test.ts"],
          exclude: ["**/*.db.test.ts", "**/node_modules/**"],
        },
      },
      {
        test: {
          name: "db",
          include: ["packages/*/test/**/*.db.test.ts", "apps/*/test/**/*.db.test.ts"],
          fileParallelism: false,
          testTimeout: 30000,
        },
      },
    ],
  },
});
