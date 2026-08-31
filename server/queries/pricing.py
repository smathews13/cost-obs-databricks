"""Shared Databricks SQL fragments for historical list-price attribution."""

from __future__ import annotations

TEMPORAL_PRICE_JOIN_MARKER = "/* TEMPORAL_LIST_PRICE_JOIN */"
CURRENT_PRICE_JOIN_MARKER = "/* CURRENT_LIST_PRICE_JOIN */"


def current_list_price_join(
    usage_alias: str = "u",
    price_alias: str = "p",
) -> str:
    """Return the AWS-compatible current list-price join for live requests."""
    return f"""LEFT JOIN system.billing.list_prices {price_alias}
  ON {usage_alias}.sku_name = {price_alias}.sku_name
  AND {usage_alias}.cloud = {price_alias}.cloud
  AND {price_alias}.price_end_time IS NULL"""


def apply_current_list_price_join(sql: str) -> str:
    """Expand live-query price markers using the current price row."""
    return sql.replace(
        CURRENT_PRICE_JOIN_MARKER,
        current_list_price_join(),
    ).replace(
        TEMPORAL_PRICE_JOIN_MARKER,
        current_list_price_join(),
    )


def temporal_list_price_join(
    usage_alias: str = "u",
    price_alias: str = "p",
    *,
    include_account_id: bool = True,
    include_cloud: bool = True,
    include_usage_unit: bool = True,
) -> str:
    """Return a deterministic one-row temporal list-price join.

    Billing quantities are matched on every dimension available on both source
    contracts: account, SKU, cloud, usage unit, USD currency, and interval. Set
    a dimension flag to false only for a documented source projection that does
    not expose that column; never invent a replacement dimension.

    The usage
    start timestamp is in the half-open price interval [start, end); usage_date
    is only a defensive fallback. If source data contains overlapping rows, the
    most recently-started interval wins. ROW_NUMBER plus stable tie-breakers
    guarantees exactly one row and prevents overlap anomalies multiplying usage.
    """
    dimension_predicates = []
    if include_account_id:
        dimension_predicates.append(
            f"candidate.account_id = {usage_alias}.account_id"
        )
    if include_cloud:
        dimension_predicates.append(f"candidate.cloud = {usage_alias}.cloud")
    if include_usage_unit:
        dimension_predicates.append(
            f"candidate.usage_unit = {usage_alias}.usage_unit"
        )
    dimensions = "\n      AND ".join(dimension_predicates)
    if dimensions:
        dimensions = f"\n      AND {dimensions}"
    return f"""LEFT JOIN LATERAL (
  SELECT
    ranked.pricing,
    ranked.price_start_time,
    ranked.price_end_time
  FROM (
    SELECT
      candidate.pricing,
      candidate.price_start_time,
      candidate.price_end_time,
      ROW_NUMBER() OVER (
        ORDER BY
          candidate.price_start_time DESC,
          COALESCE(candidate.price_end_time, TIMESTAMP '9999-12-31 23:59:59') ASC,
          COALESCE(candidate.pricing.effective_list.default, candidate.pricing.default, 0) DESC,
          COALESCE(candidate.pricing.default, 0) DESC,
          TO_JSON(candidate.pricing) DESC
      ) AS price_rank
    FROM system.billing.list_prices candidate
    WHERE candidate.sku_name = {usage_alias}.sku_name{dimensions}
      AND candidate.currency_code = 'USD'
      AND COALESCE(
        {usage_alias}.usage_start_time,
        CAST({usage_alias}.usage_date AS TIMESTAMP)
      ) >= candidate.price_start_time
      AND (
        candidate.price_end_time IS NULL
        OR COALESCE(
          {usage_alias}.usage_start_time,
          CAST({usage_alias}.usage_date AS TIMESTAMP)
        ) < candidate.price_end_time
      )
  ) ranked
  WHERE ranked.price_rank = 1
) {price_alias} ON TRUE"""


def temporal_list_price_join_without_account(
    usage_alias: str = "u", price_alias: str = "p"
) -> str:
    """Safe variant for projections that genuinely omit ``account_id``.

    SKU, cloud, usage unit, USD currency, interval semantics, and deterministic
    row selection remain mandatory.
    """
    return temporal_list_price_join(
        usage_alias,
        price_alias,
        include_account_id=False,
    )


def apply_temporal_list_price_join(sql: str) -> str:
    """Replace every shared marker in a SQL template with the canonical join."""
    return sql.replace(TEMPORAL_PRICE_JOIN_MARKER, temporal_list_price_join())
