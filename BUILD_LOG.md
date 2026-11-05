# LineageIQ — Build Log

**Problem:** Data teams can't answer "where did this number come from?" or "what breaks if I change this table?" — broken dashboards and silent pipeline failures (the Monte Carlo / Datafold / dbt category).

**Approach:** A data-observability console on the shared core framework, indigo identity, custom 3D pipeline-flow hero.

## Real graph engine
- `ASSETS`: a 15-node warehouse DAG across raw→staging→mart→BI with real `up:[]` edges, columns, dbt-style tests, freshness and SLAs.
- `downstream()` / `upstream()`: BFS traversals over the edge set.
- `healthDeep()`: propagates failing tests / SLA breaches downstream so bad sources mark everything they feed.
- `buildIncidents()`: derives incidents from the actual test + freshness state, ranks by blast radius.
- `renderLineage()`: lays out upstream+downstream nodes by distance-from-focus and draws bezier SVG edges, hot-highlighting the focused node's edges.

## Custom 3D hero
`mountPipelineHero()` — 4 node layers along x (raw→BI) with additive edges and cyan particles flowing left→right along every edge (data moving through the pipeline), cursor-reactive. Renders vividly under headless swiftshader.

## Pages
index, assets, asset (detail + lineage graph, `?a=`), incidents, impact (BFS blast radius), settings.

## Testing (headless Chromium, swiftshader)
- Landing hero renders; 4 stat tiles. ✓
- Assets: 15 rows; layer filter (mart → 4). ✓
- Asset detail: lineage graph 8 nodes + 9 bezier edges, 4 columns with tests, freshness bars, incidents. ✓
- Incidents: 10 incidents with blast radius, severity filter. ✓
- Impact: BFS — ext_app_events → 7 downstream / 3 dashboards / 127 users. ✓
- Settings: theme, accents, export. ✓
- **0 JavaScript errors** across all pages.

## Fixes
- Removed a stray Devanagari glyph that slipped into a `--dim` color value.

## Status
Complete. 6 pages, 50+ features, real DAG + BFS impact, interactive lineage graph, 3D hero, 0 errors. Delivered as lineageiq.zip.
