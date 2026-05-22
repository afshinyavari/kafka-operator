#!/usr/bin/env node
// Concatenate README + docs/*.md into public/llms-full.txt.
// Runs at build time so LLM crawlers always get the latest docs.

import { readFile, writeFile, readdir } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const websiteRoot = resolve(__dirname, '..');
const repoRoot = resolve(websiteRoot, '..');
const docsDir = join(repoRoot, 'docs');
const out = join(websiteRoot, 'public', 'llms-full.txt');

const header = `# [BRAND] — complete documentation

This file is a single-document concatenation of README.md and every file under
docs/ in the upstream repository. It exists for LLM crawlers and AI answer
engines that prefer one file over crawling. Last built: ${new Date().toISOString()}.

---

`;

async function read(path) {
  if (!existsSync(path)) return '';
  return readFile(path, 'utf-8');
}

async function main() {
  const parts = [header];

  const readme = await read(join(repoRoot, 'README.md'));
  if (readme) parts.push(`# README.md\n\n${readme}\n\n---\n\n`);

  if (existsSync(docsDir)) {
    const files = (await readdir(docsDir))
      .filter((f) => f.endsWith('.md'))
      .sort();
    for (const f of files) {
      const body = await read(join(docsDir, f));
      if (body) parts.push(`# docs/${f}\n\n${body}\n\n---\n\n`);
    }
  }

  await writeFile(out, parts.join(''), 'utf-8');
  const lines = parts.join('').split('\n').length;
  console.log(`[llms-full] wrote ${out} (${lines} lines)`);
}

main().catch((err) => {
  console.error('[llms-full] failed:', err);
  process.exit(1);
});
