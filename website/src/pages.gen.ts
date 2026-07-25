// deno-fmt-ignore-file
// biome-ignore format: generated types do not need formatting
// prettier-ignore
import type { PathsForPages } from 'waku/router'

// prettier-ignore
type Page =
  | { path: '/basics/client'; render: 'static' }
  | { path: '/basics/enqueueing'; render: 'static' }
  | { path: '/basics/jobs'; render: 'static' }
  | { path: '/basics/workers'; render: 'static' }
  | { path: '/configuration/errors'; render: 'static' }
  | { path: '/configuration/options'; render: 'static' }
  | { path: '/configuration/retries'; render: 'static' }
  | { path: '/configuration/scheduling'; render: 'static' }
  | { path: '/'; render: 'static' }
  | { path: '/installation'; render: 'static' }
  | { path: '/quick-start'; render: 'static' }
  | { path: '/reference/backends'; render: 'static' }
  | { path: '/reference/guarantees'; render: 'static' }
  | { path: '/reference/roadmap'; render: 'static' }
  | { path: '/running/lifecycle'; render: 'static' }
  | { path: '/running/middleware'; render: 'static' }
  | { path: '/running/observability'; render: 'static' }
  | { path: '/running/one-shot'; render: 'static' }
  | { path: '/running/queues'; render: 'static' }
  | { path: '/testing'; render: 'static' }
  | { path: '/testing/patterns'; render: 'static' }

// prettier-ignore
declare module 'waku/router' {
  interface RouteConfig {
    paths: PathsForPages<Page>
  }
  interface CreatePagesConfig {
    pages: Page
  }
}
