"""Regression coverage for the August dashboard feedback fixes."""

import asyncio
from unittest.mock import patch

from server.queries import BILLING_KPIS_FAST
from server.routers import tagging


def test_tag_key_drilldown_aggregates_all_values():
    with patch.object(tagging, "execute_query", return_value=[]) as execute:
        result = asyncio.run(
            tagging.get_top_objects_by_tag(
                tag_key="DataClassification",
                tag_value=None,
                start_date="2026-08-01",
                end_date="2026-08-28",
            )
        )

    params = execute.call_args.args[1]
    assert params["tag_key"] == "DataClassification"
    assert params["tag_value"] == ""
    assert result["tag_value"] is None
    assert ":tag_value = '' OR tv = :tag_value" in tagging.TOP_OBJECTS_BY_TAG


def test_tag_value_drilldown_remains_scoped():
    with patch.object(tagging, "execute_query", return_value=[]) as execute:
        asyncio.run(
            tagging.get_top_objects_by_tag(
                tag_key="DataClassification",
                tag_value="Confidential",
                start_date="2026-08-01",
                end_date="2026-08-28",
            )
        )

    assert execute.call_args.args[1]["tag_value"] == "Confidential"


def test_compute_kpi_query_counts_sql_warehouses():
    assert "COUNT(DISTINCT usage_metadata.cluster_id) as total_clusters" in BILLING_KPIS_FAST
    assert "usage_metadata.warehouse_id" in BILLING_KPIS_FAST
    assert "as sql_warehouses" in BILLING_KPIS_FAST
