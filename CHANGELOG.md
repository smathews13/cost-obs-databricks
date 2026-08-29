# Release notes

> **Not Official Databricks Software**
> This application is maintained by Databricks field engineering and is not an official Databricks product. It is not covered by Databricks Support SLAs.

## v1.2 — 2026-08-27

### Outcomes

- A refreshed, consistent cost-obs experience across navigation, charts, filters, settings, and exported reports.
- Clearer semantic colors, stronger contrast, and steadier chart rendering make cost changes easier to interpret.
- A new customer-facing architecture PDF documents authentication, refresh behavior, managed data, and exact source lineage for every dashboard tab.

### Upgrade

Redeploy from Git. No new environment variables, migrations, or manual backfills are required.

## v1.1 — 2026-07-03

### Outcomes

- Apps and Tagging views load faster from new managed Delta aggregates, while live-query fallbacks preserve availability during setup or refresh.
- Cold-warehouse and temporary empty-result recovery was improved so valid data returns promptly after the warehouse becomes ready.
- Refresh coordination, workspace filtering, initial-page performance, accessibility, and chart stability were strengthened.

### Upgrade

Redeploy from Git. Existing settings remain compatible, and missing managed tables are created by the normal setup flow.
