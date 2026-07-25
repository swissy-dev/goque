import { defineConfig } from 'vocs/config'

export default defineConfig({
  title: 'goque',
  titleTemplate: '%s · goque',
  description:
    'A background job and scheduling library for Go: typed jobs with generics, pluggable backends, per-job retry policies, middleware, and a deterministic testing story.',
  accentColor: 'light-dark(#0f766e, #2dd4bf)',
  socials: [{ icon: 'github', link: 'https://github.com/swissy-dev/goque' }],
  editLink: {
    link: 'https://github.com/swissy-dev/goque/edit/main/website/src/pages/:path',
    text: 'Suggest changes to this page',
  },
  topNav: [
    { text: 'Docs', link: '/', match: '/' },
    { text: 'GitHub', link: 'https://github.com/swissy-dev/goque', external: true },
  ],
  sidebar: [
    {
      text: 'Getting Started',
      items: [
        { text: 'Introduction', link: '/' },
        { text: 'Installation', link: '/installation' },
        { text: 'Quick Start', link: '/quick-start' },
      ],
    },
    {
      text: 'The Basics',
      items: [
        { text: 'Defining Jobs', link: '/basics/jobs' },
        { text: 'Workers', link: '/basics/workers' },
        { text: 'The Client', link: '/basics/client' },
        { text: 'Enqueueing Jobs', link: '/basics/enqueueing' },
      ],
    },
    {
      text: 'Configuring Jobs',
      items: [
        { text: 'Job Options', link: '/configuration/options' },
        { text: 'Scheduling & Priority', link: '/configuration/scheduling' },
        { text: 'Retries & Backoff', link: '/configuration/retries' },
        { text: 'Error Handling', link: '/configuration/errors' },
      ],
    },
    {
      text: 'Running Jobs',
      items: [
        { text: 'Queues & Concurrency', link: '/running/queues' },
        { text: 'Starting & Stopping', link: '/running/lifecycle' },
        { text: 'One-Shot Processing', link: '/running/one-shot' },
        { text: 'Middleware', link: '/running/middleware' },
        { text: 'Observability', link: '/running/observability' },
      ],
    },
    {
      text: 'Testing',
      items: [
        { text: 'Testing Jobs', link: '/testing' },
        { text: 'Testing Patterns', link: '/testing/patterns' },
      ],
    },
    {
      text: 'Reference',
      items: [
        { text: 'Lifecycle & Guarantees', link: '/reference/guarantees' },
        { text: 'Backends', link: '/reference/backends' },
        { text: 'Roadmap', link: '/reference/roadmap' },
      ],
    },
  ],
})
