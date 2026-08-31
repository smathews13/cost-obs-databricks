# Support Runbook — Cost Observability App

One-page incident reference. Start at the symptom that matches; follow the steps in order.

---

## Symptom: Dashboard shows $0 / zero counts / "—" for all metrics

**Root cause:** SP grants not applied after most recent deploy, OR readiness cache is stale.

**Steps:**
1. Check `GET /api/setup/readiness` → look at `overall` and `warehouse.granted`.
2. If `warehouse.granted: false` → the SP needs CAN_USE on the SQL warehouse. Grant via: SQL Warehouses → [warehouse name] → Permissions → Add the SP (cannot be done via SQL).
3. If any `core[]` item has `granted: false` → open **Settings → Permissions & Access**, copy the **Section 1 grant bundle**, and run it as a metastore admin.
4. After applying grants, call `GET /api/debug/run` as an app administrator to confirm all core checks pass.

**Key fact:** All SQL runs as the app service principal. Apply grants to the service principal ID reported by the current deployment.

---

## Symptom: KPI card shows "—" (not loading spinner, not 0)

**Root cause:** A specific system table grant is denied. "—" is the correct unavailable state — it means the app detected the denial and is not silently showing zero.

**Steps:**
1. Hover over the "—" card — the tooltip shows which table is missing (e.g. `query.history grant required`).
2. Go to **Settings → Permissions & Access** → copy the Section 1 grant bundle.
3. Run the grant as a metastore admin.
4. After ~30 seconds the card will refresh automatically (cache invalidation fires on grant apply).

---

## Symptom: Settings → Readiness shows "not_ready" after grants were applied

**Root cause:** Readiness cache has not been invalidated, or the grant targeted the wrong SP.

**Steps:**
1. In **Settings → Permissions & Access**, verify `SP Client ID` matches `DATABRICKS_CLIENT_ID` in the Apps UI → Environment Variables. If they differ, grants were applied to the wrong SP — re-apply to the current one.
2. Click **Re-check Readiness** (or **Run SP Grants** which also invalidates the cache).
3. If still stuck: call `GET /api/setup/readiness?refresh=true` to force a bypass-cache check.

---

## Symptom: "Drop Tables" button is greyed out / disabled

**Root cause:** The system is in a degraded state — one or more required tables have `exists: false`. This is intentional safety gate.

**Resolution:**
1. Call `GET /api/debug/run` as an app administrator to identify which tables are missing.
2. Fix the underlying issue (grant missing tables, rebuild) before dropping.
3. If you must drop in a degraded state (emergency recovery), note: the button is disabled to prevent deepening an existing outage. Use the API directly with caution: `DELETE /api/setup/tables` — this requires a separate service account token.

---

## Symptom: Authentication status is not service principal

**Root cause:** The deployment is inconsistent with the supported service-principal-only execution model.

**Resolution:** Call `GET /api/settings/auth-status` and verify the app reports service-principal mode. Confirm the bound warehouse resource and app service principal configuration; forwarded browser credentials must not be used for SQL.

---

## Symptom: Diagnostics check shows red for a warehouse check but warehouse exists

**Root cause:** SP identity mismatch, or warehouse is in STOPPED state.

**Steps:**
1. Verify the app has a SQL warehouse resource bound with key `sql-warehouse`.
2. Confirm the Apps environment receives `DATABRICKS_WAREHOUSE_ID` from that resource.
3. Verify the warehouse is running and the app service principal has `CAN_USE`.
4. Use **Settings → Permissions & Access** for the generated remediation guidance.

---

## API quick reference

| Endpoint | Purpose |
|---|---|
| `GET /api/setup/readiness` | Current readiness state (5-min cache) |
| `GET /api/setup/readiness?refresh=true` | Force live re-check, bypass cache |
| `GET /api/permissions/check` | Current user's table access (5-min cache) |
| `GET /api/permissions/check?refresh=true` | Force live re-check |
| `GET /api/settings/auth-status` | Current identity (auth mode, SP info) |
| `GET /api/debug/run` | Run all diagnostics and return typed results |

---

## Escalation path

1. **App admin** — apply SP grant bundle, check warehouse permissions.
2. **Metastore admin** — apply system table grants (`GRANT SELECT ON TABLE system.* TO ...`).
3. **Databricks Support** — if `system.*` tables are not visible to any principal in the workspace, the workspace may not have system table access enabled at the account level.
