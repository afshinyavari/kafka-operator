// Single source of truth for site-wide config. Update [BRAND] etc. here.

export const site = {
  brand: '[BRAND]',
  tagline: 'The multi-cluster Kafka platform for Kubernetes.',
  description:
    'A Kubernetes operator that runs Apache Kafka 4.x in KRaft mode across 3+ clusters with built-in OIDC RBAC, schema governance, mirror-maker, backup, rebalancing, and a UI. The full Kafka platform — declarative, multi-cluster, one YAML.',
  url: 'https://example.com',
  githubUrl: 'https://github.com/afshin-yavari/kafka-operator',
  contactEmail: 'afshin@yavari.se',
  twitter: '@yavari',
  license: 'Apache-2.0',
  keywords: [
    'kafka operator',
    'multi-cluster kafka',
    'kafka kubernetes',
    'kraft operator',
    'kafka mcs',
    'kubernetes multi-cluster services',
    'kafka cilium cluster mesh',
    'kafka submariner',
    'kafka istio multi-cluster',
    'kroxylicious',
    'apicurio',
    'kafka mirror maker',
    'kafka backup',
    'cruise control',
    'kafka rbac',
    'strimzi alternative',
  ],
};

export const nav = [
  { href: '/why/', label: 'Why' },
  { href: '/features/multi-cluster-kraft/', label: 'Features' },
  { href: '/architecture/', label: 'Architecture' },
  { href: '/docs/', label: 'Docs' },
  { href: site.githubUrl, label: 'GitHub', external: true },
];
