import { defineConfig } from 'astro/config';
import mdx from '@astrojs/mdx';
import sitemap from '@astrojs/sitemap';
import tailwind from '@astrojs/tailwind';

// Update this once a real domain is registered.
export const SITE_URL = 'https://example.com';

export default defineConfig({
  site: SITE_URL,
  integrations: [
    mdx(),
    tailwind({ applyBaseStyles: false }),
    sitemap(),
  ],
  markdown: {
    shikiConfig: { theme: 'github-dark-dimmed', wrap: true },
  },
  build: { format: 'directory' },
});
