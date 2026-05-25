#!/usr/bin/env node
// Copy docs/*.md from the upstream repo into src/pages/docs/<slug>.md so
// Astro renders each as a static route under /docs/<slug>/. Adds layout
// frontmatter, extracts the title from the first H1, and rewrites
// inter-doc links so they resolve at /docs/<slug>/.

import { readFile, writeFile, readdir, rm, mkdir } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { join, dirname, resolve, basename } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const websiteRoot = resolve(__dirname, '..');
const repoRoot = resolve(websiteRoot, '..');
const docsSrc = join(repoRoot, 'docs');
const docsDest = join(websiteRoot, 'src', 'pages', 'docs');

// One-paragraph blurbs per doc — drives the /docs/ index page and metadata.
const blurbs = {
  'architecture':
    'Reconciler hierarchy, multi-cluster topology, KRaft quorum design, and rolling-update flow.',
  'api-reference':
    'Every CRD field, type, default, and constraint across all eleven custom resources.',
  'operations':
    'Upgrades, scaling, proxy and RBAC setup, monitoring, troubleshooting.',
  'security':
    'mTLS, OIDC, JWT-claim RBAC, the Apicurio RBAC proxy, secret handling.',
  'disaster-recovery':
    'Multi-cluster failure scenarios, recovery procedures, backup verification.',
  'kafka-client-oauth':
    'OAuth2 / OIDC client configuration for SASL/OAUTHBEARER and the proxy.',
  'upgrade':
    'Kafka version upgrades with downgrade protection and metadata-version gating.',
  'audit':
    'Unified JSON audit channel from the Kroxylicious proxy, Apicurio RBAC proxy, and kafka-editor. Stdout always on; opt-in Kafka topic sink.',
};

const niceTitles = {
  'architecture': 'Architecture',
  'api-reference': 'API reference',
  'operations': 'Operations',
  'security': 'Security',
  'disaster-recovery': 'Disaster recovery',
  'kafka-client-oauth': 'Kafka client OAuth',
  'upgrade': 'Upgrades',
  'audit': 'Audit logging',
};

function extractTitle(md, slug) {
  const h1 = md.match(/^#\s+(.+)$/m);
  return niceTitles[slug] ?? (h1 ? h1[1].trim() : slug);
}

function stripFirstH1(md) {
  return md.replace(/^#\s+.+\n+/m, '');
}

// Convert docs-relative markdown links into site URLs.
function rewriteLinks(md) {
  return md
    // [text](docs/foo.md#anchor) → [text](/docs/foo/#anchor)
    .replace(/\]\(docs\/([\w-]+)\.md(#[^)]+)?\)/g, '](/docs/$1/$2)')
    // [text](./foo.md#anchor) → [text](/docs/foo/#anchor)
    .replace(/\]\(\.\/([\w-]+)\.md(#[^)]+)?\)/g, '](/docs/$1/$2)')
    // [text](foo.md#anchor) → [text](/docs/foo/#anchor)
    .replace(/\]\(([\w-]+)\.md(#[^)]+)?\)/g, '](/docs/$1/$2)')
    // [text](../README.md) → [text](/)
    .replace(/\]\(\.\.\/README\.md(#[^)]+)?\)/g, '](/$1)')
    // [text](../docs/foo.md) → [text](/docs/foo/)
    .replace(/\]\(\.\.\/docs\/([\w-]+)\.md(#[^)]+)?\)/g, '](/docs/$1/$2)');
}

// Escape any literal characters that would confuse YAML in frontmatter.
function escapeYaml(s) {
  return s.replace(/"/g, '\\"');
}

async function clean(dir) {
  if (!existsSync(dir)) return;
  const files = await readdir(dir);
  for (const f of files) {
    if (f.endsWith('.md')) await rm(join(dir, f));
  }
}

async function main() {
  if (!existsSync(docsSrc)) {
    console.warn(`[sync-docs] ${docsSrc} not found, skipping`);
    return;
  }
  await mkdir(docsDest, { recursive: true });
  await clean(docsDest);

  const files = (await readdir(docsSrc))
    .filter((f) => f.endsWith('.md'))
    .sort();

  const index = [];

  for (const f of files) {
    const slug = basename(f, '.md');
    const raw = await readFile(join(docsSrc, f), 'utf-8');
    const title = extractTitle(raw, slug);
    const description = blurbs[slug] ?? '';
    const body = rewriteLinks(stripFirstH1(raw));

    const frontmatter =
      `---\n` +
      `layout: ../../layouts/DocsLayout.astro\n` +
      `title: "${escapeYaml(title)}"\n` +
      (description ? `description: "${escapeYaml(description)}"\n` : '') +
      `slug: "${slug}"\n` +
      `---\n\n`;

    const out = join(docsDest, `${slug}.md`);
    await writeFile(out, frontmatter + body, 'utf-8');
    index.push({ slug, title, description });
  }

  // Emit a JSON index so the /docs/index.astro page can list everything.
  const indexPath = join(websiteRoot, 'src', 'data', 'docs-index.json');
  await writeFile(indexPath, JSON.stringify(index, null, 2), 'utf-8');

  console.log(`[sync-docs] synced ${files.length} doc(s) → ${docsDest}`);
}

main().catch((err) => {
  console.error('[sync-docs] failed:', err);
  process.exit(1);
});
