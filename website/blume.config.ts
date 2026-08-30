import { defineConfig } from 'blume'

export default defineConfig({
  title: 'goque',
  description:
    'A background job and scheduling library for Go: typed jobs with generics, pluggable backends, per-job retry policies, middleware, and a deterministic testing story.',
  github: {
    owner: 'swissy-dev',
    repo: 'goque',
    branch: 'main',
    dir: 'website',
  },
  theme: {
    accent: '#0f766e',
  },
  navigation: {
    repo: true,
    sidebar: [
      {
        label: 'Getting Started',
        items: ['/', '/installation', '/quick-start'],
      },
      {
        label: 'The Basics',
        items: ['/basics/jobs', '/basics/workers', '/basics/client', '/basics/enqueueing'],
      },
      {
        label: 'Configuring Jobs',
        items: [
          '/configuration/options',
          '/configuration/scheduling',
          '/configuration/retries',
          '/configuration/errors',
        ],
      },
      {
        label: 'Running Jobs',
        items: [
          '/running/queues',
          '/running/lifecycle',
          '/running/one-shot',
          '/running/middleware',
          '/running/observability',
        ],
      },
      {
        label: 'Testing',
        items: ['/testing', '/testing/patterns'],
      },
      {
        label: 'Reference',
        items: ['/reference/guarantees', '/reference/backends', '/reference/roadmap'],
      },
    ],
  },
})
