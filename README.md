# LineageIQ — trust your data

When a dashboard looks wrong, the question is always the same: *what feeds this, and what did we break?* **LineageIQ** maps every table from raw ingest to BI dashboard, tracks freshness and tests, detects incidents, and traces the **blast radius** of any change — before it reaches the exec dashboard. Everything runs in the browser.

## Run it
Open `index.html`, or `python3 -m http.server 8080`. No build, no dependencies, fully offline.

## Pages
- **Home** — pitch + a live 3D pipeline-flow hero (data particles streaming raw → BI).
- **Assets** — the catalog: 15 assets across 4 layers (raw / staging / mart / BI), searchable and filterable, with health, freshness vs SLA, owners and row counts.
- **Asset detail** — an **interactive lineage DAG** (upstream + downstream), column-level schema with dbt-style tests (not_null / unique / accepted_values), a freshness sparkline, and related incidents.
- **Incidents** — freshness breaches, failing tests, volume anomalies and schema changes, each tied to an asset and ranked by **blast radius**.
- **Impact** — pick any asset and see the full downstream set (BFS): affected mart tables, dashboards/models, and the number of users hit.
- **Settings** — theme, accents, JSON export of the graph.

## The graph logic is real
- A proper warehouse DAG (`ASSETS` with `up:[...]` edges).
- `downstream()` / `upstream()` are real BFS traversals.
- **Health propagates**: `healthDeep()` marks an asset unhealthy if it or anything upstream has a failing test or breached SLA — so a bad raw table lights up everything it feeds.
- Incidents are derived from the actual tests and freshness values, then sorted by blast radius.
- The lineage graph on each asset is laid out by distance-from-focus and drawn with bezier SVG edges.

## Deploy
Static — Netlify drop, `vercel`, or GitHub Pages.

## Feature count
50+ — lineage graph, 15 assets, 4 layers, column tests, freshness SLAs, health propagation, incident feed, impact/blast-radius analysis, 3D hero, theming, export, offline. Full list on the home page.

*The real market need: broken dashboards and silent pipeline failures with no way to trace cause or impact (cf. Monte Carlo, Datafold, dbt). LineageIQ is a self-contained demonstration of data lineage + observability.*
