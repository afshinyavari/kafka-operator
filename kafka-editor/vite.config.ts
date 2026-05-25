/// <reference types="vitest/config" />
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  // The packaged desktop app loads index.html over `file://`, which needs
  // relative asset URLs; the web build keeps absolute paths. `ELECTRON=true`
  // is set by the `build:spa:electron` script.
  base: process.env.ELECTRON === 'true' ? './' : '/',
  server: {
    // Dev proxy so the browser avoids CORS. The schema registry is reached
    // through the backend's own `/api/registry` proxy, so only `/api` is here.
    proxy: {
      // The Kafka Editor backend (Quarkus, port 8080).
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
    },
  },
  test: {
    environment: 'node',
    include: ['src/**/*.test.{ts,tsx}'],
  },
})
