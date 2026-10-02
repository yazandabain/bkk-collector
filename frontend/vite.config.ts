import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  build: { chunkSizeWarningLimit: 1200 },
  test: { include: ['tests/**/*.test.ts', 'tests/**/*.test.tsx'] },
})
