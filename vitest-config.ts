import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

const rootDir = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  resolve: {
    alias: {
      '@': path.resolve(rootDir, 'src'),
      '@core': path.resolve(rootDir, 'src/core'),
      '@adapters': path.resolve(rootDir, 'src/adapters'),
    },
  },
});
