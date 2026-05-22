/** @type {import('tailwindcss').Config} */
export default {
  content: ['./src/**/*.{astro,html,md,mdx,js,jsx,ts,tsx}'],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        bg: {
          DEFAULT: '#0a0a0c',
          raised: '#111114',
          inset: '#16161a',
        },
        fg: {
          DEFAULT: '#e7e7ea',
          muted: '#9ca0a8',
          dim: '#6b7077',
        },
        border: {
          DEFAULT: '#23252b',
          strong: '#33363d',
        },
        brand: {
          DEFAULT: '#5b8cff',
          bright: '#7da4ff',
          deep: '#3a6bdf',
        },
        signal: {
          ok: '#48d597',
          warn: '#f0b429',
          err: '#ef4444',
        },
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', '-apple-system', 'Segoe UI', 'Roboto', 'sans-serif'],
        display: ['"Instrument Serif"', 'Georgia', 'serif'],
        mono: ['"JetBrains Mono"', 'ui-monospace', 'SFMono-Regular', 'Menlo', 'monospace'],
      },
      maxWidth: {
        prose: '68ch',
        site: '1200px',
      },
      animation: {
        'pulse-slow': 'pulse 4s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'flow': 'flow 3s linear infinite',
      },
      keyframes: {
        flow: {
          '0%': { strokeDashoffset: '0' },
          '100%': { strokeDashoffset: '-20' },
        },
      },
    },
  },
};
