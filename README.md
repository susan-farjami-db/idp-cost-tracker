# IDP Cost Tracker — Declarative Automation Bundle

A reusable bundle that deploys the **Intelligent Document Processing (IDP) Cost Tracker** dashboard and its supporting Unity Catalog views to any Databricks workspace. Designed for customer deployments — one bundle, many `targets`.

## What gets deployed

- 5 views in `${var.catalog}.${var.schema}` (default schema: `idp_cost_tracking`):
  - `v_idp_usage_facts` — atomic facts with $ at list price
  - `v_idp_daily_summary` — daily rollup
  - `v_idp_by_workload` — JOB / PIPELINE / WAREHOUSE / NOTEBOOK / API_SDK attribution
  - `v_idp_by_user` — per-user chargeback
  - `v_idp_warehouse_queries` — DBSQL-originated AI function statements
- One **Lakeview dashboard**: `[<target>] Intelligent Document Processing (IDP) Cost Tracker` (3 pages: Cost Overview, By Workload, By User & Query) — with user + workspace cross-filters
- One **Job**: `[<target>] IDP cost tracking — setup views` — runs the setup notebook on the configured warehouse

## Customer prerequisites (one-time per workspace)

1. Unity Catalog enabled.
2. `system.billing` schema enabled on the metastore:
   ```bash
   databricks system-schemas enable --metastore-id <id> --schema-name billing
   ```
3. (Optional, but recommended for the warehouse-queries page)
   ```bash
   databricks system-schemas enable --metastore-id <id> --schema-name query
   ```
   Without this, `v_idp_warehouse_queries` returns no rows and the "By User & Query" page will look empty.
4. A serverless SQL warehouse (or any SQL warehouse the deploying user can query) — note its ID.
5. Permission to create a schema in the target catalog.
6. The Databricks CLI v0.205+ installed and authenticated (`databricks auth login` or a configured profile).

## First deploy (5 minutes)

1. Clone the repo and `cd` into it.
2. Open `databricks.yml` and edit the `_template` block: workspace host, catalog, warehouse ID. (Optionally rename `_template` to something meaningful like `prod` or your customer's short name.)
3. Validate, deploy, and run the setup job:
   ```bash
   databricks bundle validate -t _template --profile <profile>
   databricks bundle deploy   -t _template --profile <profile>
   databricks bundle run setup_idp_views -t _template --profile <profile>
   ```
4. Open the dashboard in the workspace — it queries the views you just created.

> **Note:** every bundle command must pass `-t <target>`. There is no default target on purpose, so you can't accidentally deploy to the wrong workspace.

## Adding more targets (multi-environment / multi-customer)

Open `databricks.yml`, add another block under `targets:`:

```yaml
targets:
  customer_name:
    mode: production
    workspace:
      host: https://<customer-workspace>.cloud.databricks.com
    variables:
      catalog: <customer_catalog>
      schema: idp_cost_tracking
      warehouse_id: <warehouse-id>
      parent_path: /Shared                 # or wherever they want the dashboard
      lookback_30d: "30"                   # optional override
      lookback_60d: "60"
      lookback_90d: "90"
```

The variables substitute throughout the dashboard JSON (`${var.catalog}`, `${var.lookback_30d}`, …) and the setup notebook (via job parameters). No need to edit anything else.

## Updating an existing deployment

```bash
# Edits to setup_views.py, dashboard.lvdash.json, or yaml files
databricks bundle deploy -t <target>

# If you changed view definitions, re-run the setup job
databricks bundle run setup_idp_views -t <target>
```

Dashboards are upserted by `display_name` — re-deploys update in place. Because the display name includes `[<target>]`, deploying to multiple targets from the same workspace user won't collide.

## What's parameterized

| Variable        | Purpose                                                          | Default          |
|-----------------|------------------------------------------------------------------|------------------|
| `catalog`       | Catalog hosting the IDP tracking schema                          | `main`           |
| `schema`        | Schema name for the 5 views                                      | `idp_cost_tracking` |
| `warehouse_id`  | SQL warehouse for setup job + dashboard queries                  | (per target)     |
| `parent_path`   | Workspace folder where the dashboard lands                       | `/Shared`        |
| `lookback_30d`  | Short window for KPIs, by_workload, top_users, top_workloads, etc. | `30`           |
| `lookback_60d`  | Trend line look-back                                              | `60`             |
| `lookback_90d`  | User-filter dropdown look-back                                    | `90`             |

## Layout

```
.
├── databricks.yml              # bundle config + variables + targets
├── resources/
│   ├── setup.job.yml           # one-shot job that creates the views
│   └── dashboard.yml           # Lakeview dashboard resource
├── src/
│   ├── setup_views.py          # parameterized notebook (creates schema + 5 views)
│   └── dashboard.lvdash.json   # templated dashboard with ${var.*} placeholders
├── LICENSE                     # Apache-2.0
└── README.md                   # this file
```

## Notes / caveats

- **Dashboard runs as the deployer.** `embed_credentials: true` is set on the dashboard, so its queries execute under the identity that deployed the bundle, not the viewer. This means viewers see results without needing direct access to `system.billing`. Flip to `false` if you want viewer-identity execution.
- **Cost is at list price.** `system.billing.list_prices` is the join source. For discounted/contract pricing, swap that join for the customer's contract-rate table.
- **`system.billing.usage` updates daily.** Cost numbers lag ~24h.
- **`API_SDK` workload bucket** = Databricks Connect / SDK / REST API direct invocations of the AI function endpoints (no notebook/job/pipeline shell, identified via `custom_tags['DatabricksScgwWorkloadType'] = 'Unspecified'`).
- **AI Functions billing is its own product** (`billing_origin_product = 'AI_FUNCTIONS'`) under SKU `ENTERPRISE_SERVERLESS_REAL_TIME_INFERENCE_<REGION>` — *not* serverless SQL compute. Don't confuse the two.
- **Counter widget titles need width ≥ 4 and no `value.format`** in Lakeview, otherwise the title is hidden. The dashboard is already configured correctly; keep this in mind if you tweak the layout.
