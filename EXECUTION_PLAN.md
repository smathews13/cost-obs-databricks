# EXECUTION PLAN — cost-obs + gal-app
# Internal only. Last updated: 2026-04-14.
# Items are ordered by dependency and risk. Ship 1+2 together, 3+4 independently, 5+6 in parallel.

---

## ITEM 1: OAuth Fallback Messaging

**Objective:** When SP fallback fails on settings checks, surface a structured, actionable banner in the UI rather than silent nulls. The banner tells workspace admins exactly how to fix it.

**Files to change:**
- `server/routers/settings.py`
- `client/src/components/settings/SettingsConfig.tsx`

**Implementation tasks:**

1. In `server/routers/settings.py`, wrap the `/tables` endpoint body in a try/except that catches `PermissionError`, `databricks.sdk.errors.PermissionDenied`, `databricks.sdk.errors.NotFound`, and generic `Exception` from the DB call.
2. On catch, return a response shape like: `{"tables": [], "auth_error": {"code": "SP_PERMISSION_DENIED", "message": "The app service principal lacks SELECT permission on the settings catalog. Open the app as a workspace admin with SQL scope enabled, or run dba_deploy.sh to grant the required permissions."}}` — do NOT raise HTTP 500, return HTTP 200 with the error field so the frontend can handle it gracefully.
3. Also propagate `auth_error` through the `settings_store` layer if it reads from `execute_query` results — check if `/tables` calls `get_settings_tables()` or similar helper; if so, the try/except goes around the helper call at the router level, not inside the helper.
4. In `client/src/components/settings/SettingsConfig.tsx`, inspect the `tablesStatus` response after the tables fetch resolves. Add a conditional: if `tablesStatus?.auth_error` is present, render a `<Banner>` (or inline `<div>`) instead of (not alongside) the table list.
5. Banner content: "Service principal lacks permission to check table status. To fix: open this app as a Databricks workspace admin with SQL scope enabled — the app will auto-grant permissions on first authenticated load. Alternatively, run `dba_deploy.sh` manually." Include a copy-pasteable command or link to the dba_deploy.sh docs if available.
6. Banner should be visually distinct (amber/warning color, not red/error) since it's a fixable config state, not a crash.
7. Do not show the broken `⚠` or empty table rows when `auth_error` is present — suppress the table list entirely and show only the banner.

**Acceptance criteria:**
- With no `dba_deploy.sh` run and no workspace admin token, `/api/settings/tables` returns HTTP 200 with `auth_error` populated and `tables: []`.
- With `auth_error` present, SettingsConfig shows only the banner, no empty/null table rows.
- With a valid workspace admin token (sql scope), the normal table list renders and no banner appears.
- No console errors or unhandled promise rejections on the settings page in the SP-fallback-fail state.

---

## ITEM 2: Full OAuth Permission Unification

**Objective:** Every user-facing query runs as the logged-in user's OAuth token when available. SP is reserved for background jobs and the deploy script only. No more manual grant requirements for workspace admins running the app themselves.

**Files to change:**
- `server/routers/settings.py`
- `server/app.py`
- `server/db.py`
- `client/src/components/settings/SettingsConfig.tsx`

**Implementation tasks:**

1. **Audit `/tables`, `/config`, `/catalog` in `server/routers/settings.py`:** Verify none of these pass `no_cache=True` or explicitly skip the `_user_token` ContextVar path in `execute_query`. If any call bypasses the token, remove that bypass. These should flow through `execute_query` normally — the existing middleware already sets `_user_token` from `x-forwarded-access-token`.

2. **MV refresh token passthrough:**
   - In `server/routers/settings.py` `/refresh-mvs` POST handler: extract the user token from the request header (`request.headers.get("x-forwarded-access-token")`).
   - Pass it as an optional argument to whatever function triggers `_run_mv_refresh` in `server/app.py`.
   - In `server/app.py` `_run_mv_refresh`: add parameter `user_token: str | None = None`. At the top of the function body, if `user_token` is not None, call `_user_token.set(user_token)` before executing any MV refresh queries. This ensures the refresh runs as the triggering admin user, not the SP.
   - Add a log line: `logger.info(f"MV refresh triggered by user token: {'present' if user_token else 'SP fallback'}")`.

3. **`get_auth_status()` in `server/db.py`:**
   - Add function `get_auth_status() -> dict` that returns:
     ```python
     {
       "user_token_active": bool,     # whether _user_token ContextVar is set and non-None
       "identity": str,               # "user_oauth" | "service_principal"
       "locked_to_sp": bool,          # whether _lock_auth_mode has locked to SP
       "has_sql_scope": bool | None,  # attempt to decode token scope if present
     }
     ```
   - For `has_sql_scope`: decode the JWT (no verification needed, just base64 decode the payload) and check if `"sql"` is in the `scp` claim. Wrap in try/except, return None on failure.

4. **`GET /api/settings/auth-status` in `server/routers/settings.py`:**
   - New endpoint, no auth required beyond normal middleware.
   - Calls `get_auth_status()` from `server/db.py` and returns it as JSON.
   - This endpoint is called by the frontend on Settings page load.

5. **Frontend auth mode indicator in `client/src/components/settings/SettingsConfig.tsx`:**
   - On Settings page mount, call `GET /api/settings/auth-status`.
   - In the Connections section, add a small status row: "Auth mode: [User OAuth ✓] or [Service Principal ⚠]".
   - If `identity === "service_principal"` and `locked_to_sp === false`, show amber indicator with "Using service principal (no user token detected)".
   - If `locked_to_sp === true`, show amber indicator with "Locked to service principal (user token failed scope check)".
   - If `identity === "user_oauth"`, show green indicator with "Running as user OAuth token".
   - Keep the indicator small — one line, inline with the Connections section header or below it.

6. **Fallback lock surfacing:** The `_lock_auth_mode` mechanism is already implemented. Ensure `get_auth_status()` reads from wherever `_lock_auth_mode` stores its state (check `server/db.py` for the variable name). If it's a module-level bool or ContextVar, just read it in `get_auth_status()`.

**Acceptance criteria:**
- As a workspace admin with sql scope: auth-status returns `identity: "user_oauth"`, Connections section shows green indicator, MV refresh uses the user token.
- As a non-admin or no-token user: auth-status returns `identity: "service_principal"`, amber indicator shown.
- After token scope failure (lock engaged): `locked_to_sp: true` in auth-status response, UI shows "Locked to service principal" message.
- `/refresh-mvs` called by workspace admin triggers MV refresh that runs as that user's token, not SP.
- No regression on scheduler-triggered refreshes (they have no user token, should continue running as SP).

---

## ITEM 3: Contract Terms + Burn Down Tracking

**Objective:** Experimental feature behind a settings toggle — adds a Contract tab where customers can input their Databricks commit terms and track daily spend against pace.

**Files to change:**
- `server/routers/settings.py`
- `server/routers/billing.py`
- `server/materialized_views.py` (read-only verification — check if cumulative daily spend is queryable; do not add MVs)
- `client/src/App.tsx`
- `client/src/components/ContractBurndown.tsx` (new file)
- `client/src/components/settings/SettingsExperimental.tsx`
- `client/src/types/billing.ts`

**Implementation tasks:**

1. **Contract settings storage (server):**
   - In `server/routers/settings.py`, add `GET /api/settings/contract` and `POST /api/settings/contract`.
   - Storage path: `.settings/contract_settings.json` (same pattern as other local settings files in the project).
   - GET: read and return the JSON file; if not found, return `{"start_date": null, "end_date": null, "total_commit_usd": null, "notes": ""}`.
   - POST: validate that `start_date` and `end_date` are valid ISO dates, `total_commit_usd` is a positive number; write to file; return the saved object.
   - Data model:
     ```json
     {
       "start_date": "2024-01-01",
       "end_date": "2025-12-31",
       "total_commit_usd": 500000,
       "notes": "FY2025 Enterprise Agreement"
     }
     ```

2. **Contract burndown endpoint (server):**
   - In `server/routers/billing.py`, add `GET /api/billing/contract-burndown`.
   - Read contract terms from `.settings/contract_settings.json`. If not configured, return `{"configured": false}`.
   - Query `daily_usage_summary` MV for daily total spend from `start_date` to today. Check the existing MV schema to confirm the right column name for total daily spend — likely `total_dbu_cost` or similar.
   - Compute cumulative daily spend array: ordered by date, each day's value = sum of all prior days + today.
   - Compute ideal straight-line burn: for each calendar day in term, `(day_index / total_days) * total_commit_usd`.
   - Compute projected end: from today's average daily burn rate (`cumulative_to_date / days_elapsed`), project forward to `total_commit_usd`. Return as a date string.
   - Compute pace status: `actual_cumulative / ideal_cumulative_at_today`. If ratio < 0.95: `"under"` (green). If 0.95–1.10: `"on_pace"` (amber). If > 1.10: `"over"` (red).
   - Response shape:
     ```json
     {
       "configured": true,
       "contract": { ...contract_settings... },
       "kpis": {
         "total_commit_usd": 500000,
         "spent_to_date": 123456.78,
         "remaining": 376543.22,
         "days_elapsed": 105,
         "days_remaining": 256,
         "projected_end_date": "2025-11-15",
         "pace_status": "under"
       },
       "daily_series": [
         {"date": "2024-01-01", "actual_cumulative": 0, "ideal_cumulative": 0},
         ...
       ]
     }
     ```

3. **TypeScript types in `client/src/types/billing.ts`:**
   - Add `ContractTerms` interface: `{ start_date: string; end_date: string; total_commit_usd: number; notes: string }`.
   - Add `ContractBurndownResponse` interface matching the response shape above.
   - Add `ContractKPIs` interface.

4. **ContractBurndown component (`client/src/components/ContractBurndown.tsx`) — new file:**
   - Form section: inputs for start_date, end_date, total_commit_usd, notes. Save button calls POST `/api/settings/contract`. Show success/error toast.
   - KPI row: 6 KPI cards — Total Committed, Spent to Date, Remaining, Days Elapsed, Days Remaining, Projected End Date. Color-code Projected End Date: green if before end_date, red if after.
   - Burn chart: use whatever charting library is already in the project (check `package.json` — likely recharts or similar). Line chart with:
     - Series 1: "Actual Spend" — step line, cumulative daily spend.
     - Series 2: "Ideal Pace" — straight dashed line from 0 to total_commit.
     - Series 3: "Projected" — dashed line from today's actual value to projected end date at total_commit (draw from today forward only).
     - Color series: Actual = Databricks red (#FF3621), Ideal = gray, Projected = amber.
   - Pace badge: above the chart, show "AHEAD OF PACE", "ON PACE", or "BEHIND PACE" with appropriate color.
   - If `configured: false`, show an empty state: "No contract configured. Fill in the form above to start tracking."

5. **Experimental toggle in `client/src/components/settings/SettingsExperimental.tsx`:**
   - Add a new toggle row: "Contract Tracking" with a "Preview" badge.
   - Description: "Track Databricks contract burn-down against committed spend. Add contract terms in the Contract tab."
   - Toggle key: `enableContractTracking` in `localSettings`.
   - Add it after existing experimental features, before any footer.

6. **App.tsx — Contract tab:**
   - Import `ContractBurndown` component.
   - Add a "Contract" tab entry in the tab list, conditionally rendered only when `localSettings.enableContractTracking === true`.
   - Position: after the last existing tab or as a clearly grouped "experimental" tab.
   - Tab content: render `<ContractBurndown />`.

**Acceptance criteria:**
- Toggle off (default): Contract tab not visible anywhere in the UI.
- Toggle on: Contract tab appears. Empty state shows when no contract saved.
- After saving contract terms, page reloads the burndown data without a full refresh.
- Burn chart renders correctly with all 3 series. Actual line stops at today. Projected line starts at today.
- KPI cards show correct math: remaining = total - spent, projected end date is plausible.
- Pace status color logic matches thresholds (green < 0.95, amber 0.95–1.10, red > 1.10).
- POST `/api/settings/contract` with invalid dates returns a 422 with a clear error message.

---

## ITEM 4: MV Model Updates

**Objective:** Reduce operational risk and compute cost by trimming expensive lookback windows on query-history MVs, adding refresh timing instrumentation, and surfacing refresh staleness in the UI.

**Files to change:**
- `server/materialized_views.py`
- `server/app.py`
- `server/routers/settings.py`
- `client/src/components/settings/SettingsConfig.tsx`

**Implementation tasks:**

1. **Reduce lookback windows in `server/materialized_views.py`:**
   - Find `daily_query_stats` MV definition. Change `INTERVAL 1095 DAYS` (or `3 * 365` equivalent) to `INTERVAL 365 DAYS`.
   - Find `sql_tool_attribution` MV definition. Change `INTERVAL 1095 DAYS` to `INTERVAL 365 DAYS`.
   - DO NOT touch: `daily_usage_summary`, `daily_product_breakdown`, `daily_workspace_breakdown` — these stay at 1095 days.
   - Add code comments on the changed MVs: `# Lookback reduced from 1095→365 days (2026-04-14). Query history joins are expensive. 1y is sufficient for query analytics. NOTE: Lakebase is the planned destination for this pre-aggregated data; when migrated, replace Delta writes with Postgres writes and queries with Postgres reads.`
   - Add the same Lakebase note as a comment on the 3 core billing MVs too (without changing their windows).

2. **Refresh timing instrumentation in `server/app.py`:**
   - In `_run_mv_refresh` (or wherever the MV refresh loop runs), record `refresh_start = datetime.utcnow()` before the loop.
   - For each MV refresh call, record `mv_start = datetime.utcnow()` before and log after: `logger.info(f"MV {mv_name} refreshed in {(datetime.utcnow() - mv_start).total_seconds():.1f}s")`.
   - After all MVs complete, write a JSON log to `.settings/mv_refresh_log.json`:
     ```json
     {
       "last_refresh_utc": "2026-04-14T01:00:00Z",
       "duration_seconds": 142.3,
       "mv_timings": {
         "daily_usage_summary": 45.2,
         "daily_product_breakdown": 30.1,
         ...
       },
       "status": "success"
     }
     ```
   - On refresh failure, write `"status": "error"` and `"error": "<exception message>"` to the same file.
   - Use atomic write (write to `.settings/mv_refresh_log.json.tmp` then rename) to avoid partial reads.

3. **Expose refresh timing in `/tables` endpoint (`server/routers/settings.py`):**
   - In the `/tables` response, add a `refresh_status` field.
   - Read `.settings/mv_refresh_log.json` at request time (not cached — file is small).
   - Return:
     ```json
     {
       "refresh_status": {
         "last_refresh_utc": "2026-04-14T01:00:00Z",
         "duration_seconds": 142.3,
         "hours_since_refresh": 2.4,
         "stale": false,
         "status": "success"
       }
     }
     ```
   - `stale` = `hours_since_refresh > 26`. If log file doesn't exist, return `"refresh_status": null`.

4. **Frontend staleness indicator in `client/src/components/settings/SettingsConfig.tsx`:**
   - Below or next to the existing Refresh button (wherever it renders), add a "Last refreshed: X hours ago" label using `tablesStatus.refresh_status.hours_since_refresh`.
   - If `refresh_status.stale === true`, add a warning badge on or adjacent to the Refresh button. Badge text: "Stale (>26h)". Badge color: amber.
   - If `refresh_status === null`, show "Last refresh: unknown".
   - If `refresh_status.status === "error"`, show "Last refresh failed" in red.
   - Do not add a new API call — this data comes from the existing `/tables` fetch.

**Acceptance criteria:**
- `daily_query_stats` and `sql_tool_attribution` MVs have 365-day windows. The 3 core billing MVs still have 1095-day windows.
- After a successful refresh, `.settings/mv_refresh_log.json` exists and contains accurate timing data.
- `/api/settings/tables` returns `refresh_status` in every response (null if log missing, populated otherwise).
- Settings page shows "Last refreshed: X hours ago" without a separate API call.
- If last refresh was >26h ago, the Refresh button has a visible "Stale" indicator.
- MV refresh log write is atomic (tmp + rename).

---

## ITEM 5: Impeccable UI Updates — cost-obs

**Objective:** Apply Impeccable design treatment to cost-obs: replace generic AI-design patterns with confident, opinionated design using Databricks brand direction. Primary focus: type hierarchy, color, spacing rhythm, component specificity.

**Files to change:**
- `client/src/index.css`
- `client/src/components/SummaryCards.tsx`
- `client/src/components/SpendChart.tsx`
- `client/src/App.tsx`
- `client/src/components/settings/SettingsConfig.tsx`
- `client/src/components/settings/SettingsExperimental.tsx`

**Anti-patterns to eliminate (priority order):**
1. Flat type hierarchy — font sizes too close together, headings not distinct from body
2. Identical card grids — all KPI cards same weight, same shadow, same radius
3. Nested cards — cards inside cards in settings panels
4. Generic drop shadows everywhere — replace with borders or elevation through contrast
5. Purple/violet gradients — remove all; brand color is Databricks red #FF3621
6. Monotonous spacing — same `gap-4` everywhere; introduce spacing rhythm
7. Redundant information — heading repeated in body text

**Implementation tasks:**

1. **Run `/audit` as a skill invocation first** before touching any file. Read the audit output to get the specific list of violations found in this codebase. Use the audit output to prioritize.

2. **`client/src/index.css` — CSS foundation:**
   - Add/update CSS custom properties:
     ```css
     --color-brand: #FF3621;
     --color-brand-dark: #CC2A18;
     --color-brand-dim: rgba(255, 54, 33, 0.12);
     --font-display: /* add a display/heading font if not present, or use system-ui with heavier weight */;
     --text-display: 2.25rem; /* 36px — for page titles */
     --text-heading: 1.5rem;  /* 24px — section headings */
     --text-subheading: 1.125rem; /* 18px — card titles */
     --text-body: 0.875rem; /* 14px — standard */
     --text-caption: 0.75rem; /* 12px — labels, metadata */
     --space-xs: 4px;
     --space-sm: 8px;
     --space-md: 16px;
     --space-lg: 24px;
     --space-xl: 40px;
     --space-2xl: 64px;
     ```
   - Remove any purple/violet gradient variables.
   - Ensure body uses `var(--text-body)` as default font size.

3. **`client/src/components/SummaryCards.tsx` — KPI card redesign:**
   - Eliminate identical card grid. Introduce visual hierarchy: the primary KPI (total spend or most important metric) should be larger/heavier than secondary KPIs.
   - Replace uniform drop shadows with a left border accent (`border-left: 3px solid var(--color-brand)`) on the primary card, no border on secondary cards.
   - Increase the number value font size significantly vs the label: label at `var(--text-caption)`, value at `var(--text-heading)` or larger.
   - Remove card background shadows — use background color contrast instead (slightly off-white or bordered).
   - Add a subtle trend indicator (up/down arrow + delta %) visually distinct from the main value.

4. **`client/src/components/SpendChart.tsx` — chart styling:**
   - Replace the primary chart color with `#FF3621` (Databricks red).
   - If there are secondary series, use `#CC2A18` (darker red) or a neutral gray — not purple/blue unless they are semantically meaningful.
   - Increase axis label font size to match `var(--text-caption)`.
   - Remove chart card shadow if it has one — let the chart breathe with whitespace instead.
   - Ensure chart title (if any) uses `var(--text-heading)` weight.

5. **`client/src/App.tsx` — nav/tab design:**
   - Active tab indicator: use a thick bottom border in `var(--color-brand)` rather than a background fill or pill shape.
   - Tab text: inactive tabs should be visibly muted (60-70% opacity or lighter color), active tab at full weight.
   - App header: if there's a logo or app name, give it more visual weight. Remove any gradient backgrounds from the nav.
   - Tab spacing: ensure tabs have enough padding (`var(--space-md)` horizontal minimum).

6. **`client/src/components/settings/SettingsConfig.tsx` — reduce nested card feel:**
   - Identify any section that renders as a card inside a card. Convert inner cards to bordered sections or simple dividers instead.
   - Section headings should use `var(--text-subheading)` and be visually separated from content by spacing, not by a card border.
   - Table/list items in settings: use alternating row backgrounds or dividers, not individual card borders per row.

7. **`client/src/components/settings/SettingsExperimental.tsx` — same treatment:**
   - Remove card-in-card nesting if present.
   - Toggle rows: left-align label and description with right-aligned toggle control, separated by flex spacer — not wrapped in individual cards.

**Acceptance criteria:**
- No purple or violet as a primary/accent color anywhere in the UI.
- Databricks red (#FF3621) is the clear brand/accent color — visible in at least nav active state, primary KPI accent, and primary chart series.
- Type hierarchy is clearly distinguishable: display > heading > subheading > body > caption — at least 4px size difference between adjacent levels.
- KPI cards are not all identical — the primary metric is visually distinct.
- Settings panels have no card-in-card nesting.
- All chart series colors are intentional and not default library colors (no default blue/purple).
- `/audit` run after implementation shows fewer violations than before.

---

## ITEM 6: Impeccable UI Updates — gal-app

**Objective:** Apply Impeccable design treatment to gal-app with a gaming analytics brand direction — different palette from cost-obs, but same rigor: type hierarchy, spacing rhythm, color confidence, no generic AI-design patterns.

**Files to change (discover during audit — preliminary list):**
- `gal-app/client/src/index.css`
- `gal-app/client/src/App.tsx`
- `gal-app/client/src/components/GamesTab.tsx`
- `gal-app/client/src/components/LeaderboardTab.tsx`
- `gal-app/client/src/components/SeasonalityTab.tsx`
- `gal-app/client/src/components/SettingsPanel.tsx`
- `gal-app/client/src/components/CompareView.tsx`
- `gal-app/client/src/components/UpcomingReleases.tsx`

**Implementation tasks:**

1. **Audit first:** Read `gal-app/client/src/index.css` and all component files listed above. Inventory current colors, font sizes, spacing tokens, shadow usage. Run `/audit` skill on the gal-app frontend to get the specific violation list.

2. **Brand direction for gal-app (gaming analytics):**
   - Gaming analytics should feel data-dense, high-contrast, slightly editorial — NOT pastel or rounded.
   - Suggested palette: dark background with high-contrast data (not dark mode for dark mode's sake — but gaming data often reads better on dark). Alternatively, a confident light theme with bold accent.
   - Accent color: pick ONE bold accent — could be electric blue (#0066FF), green (#00CC66), or a platform-specific color. Do NOT use multiple competing accent colors.
   - Avoid: rainbow chart palettes, gradient hero sections, identical card grids, soft drop shadows.

3. **`gal-app/client/src/index.css` — CSS foundation:**
   - Same approach as cost-obs: define a clear type scale, spacing scale, and color token system.
   - Type scale tailored to data-dense gaming analytics: slightly smaller body text is OK (0.8125rem / 13px) since tables and leaderboards have dense data.
   - Add `--color-accent`, `--color-accent-dim`, `--color-surface`, `--color-surface-alt`, `--color-border`.

4. **`gal-app/client/src/App.tsx` — nav/tab:**
   - Same treatment as cost-obs: active tab bottom border accent, muted inactive tabs, no gradient nav.
   - Ensure app name/logo has visual weight.

5. **Component-level fixes (apply to each component after reading it):**
   - `GamesTab.tsx`: game cards are likely identical grids. Introduce hierarchy — featured/trending games larger, secondary games smaller. Remove uniform shadows.
   - `LeaderboardTab.tsx`: leaderboards should look like leaderboards — ranked list with position numbers visually prominent. Remove card wrapping around each row, use clean table or list with dividers.
   - `SeasonalityTab.tsx`: chart series colors — ensure they're intentional, not default library colors.
   - `SettingsPanel.tsx`: same as cost-obs settings — no nested cards, clean toggle rows.
   - `CompareView.tsx`: comparison views often have side-by-side layout — ensure columns are visually separated with a clear divider, not just spacing.
   - `UpcomingReleases.tsx`: release list — date should be visually prominent (it's the primary sorting key). Use `var(--text-subheading)` for game title, `var(--text-caption)` for metadata, release date as accent-colored badge.

6. **Read `gal-app/app.yaml`** to confirm what routes the app serves and whether there are any config-driven UI flags to be aware of before making changes.

**Acceptance criteria:**
- gal-app has a single, consistent accent color — not multiple competing ones.
- Type hierarchy is distinguishable: at least 4 size levels in use.
- Leaderboard looks like a ranked list, not a card grid.
- Game cards have visual hierarchy — not all identical.
- Chart series colors are intentional.
- No nested cards in SettingsPanel.
- `/audit` on gal-app shows fewer violations after implementation than before.
- `gal-app/app.yaml` is read and no config-driven constraints are violated.

---

## SEQUENCE + DEPLOYMENT NOTES

```
Items 1+2 (OAuth):
  - Build and test together (they share settings.py changes)
  - Deploy to both AWS default and Azure field-eng (per deploy targets rule)
  - Sequence: Item 1 first (safer, just error handling), then Item 2 (token passthrough)
  - Test: confirm auth-status endpoint returns correct identity before deploying Item 2

Item 3 (Contract):
  - Independent feature, no shared state with Items 1-2
  - Can build and deploy anytime after Item 1 is merged (shares settings.py but different endpoints)
  - Default off (behind experimental toggle) — zero risk to existing users
  - Deploy to both envs

Item 4 (MV model):
  - Deploy during off-hours (touches refresh scheduler and MV definitions)
  - The MV lookback reduction takes effect on the NEXT refresh cycle — not immediate
  - Verify the refresh log JSON is being written correctly before deploying the frontend changes
  - Deploy to both envs

Items 5+6 (Impeccable):
  - Pure frontend — no backend changes
  - Can be worked in parallel (cost-obs and gal-app are separate repos/apps)
  - Run /audit skill BEFORE making any changes in each app
  - Deploy independently per app; no coordination needed
  - cost-obs: deploy to both AWS and Azure field-eng as usual
  - gal-app: deploy per its own deploy process (check gal-app/deploy.sh or app.yaml)
```

## OPEN QUESTIONS / BLOCKERS

- Item 2: Confirm the exact variable/function name for `_lock_auth_mode` in `server/db.py` before implementing `get_auth_status()`.
- Item 3: Confirm charting library in `client/package.json` before designing the ContractBurndown chart (recharts vs chart.js vs victory vs other).
- Item 3: Confirm the column name for daily total spend in `daily_usage_summary` MV before writing the burndown query.
- Item 4: Confirm `.settings/` directory exists and is gitignored before writing `mv_refresh_log.json` there.
- Item 5: Run `/audit` first — do not guess which patterns are present; let the audit determine priority.
- Item 6: Read `gal-app/app.yaml` and `gal-app/client/src/index.css` before choosing the accent color — may already have brand tokens defined.
