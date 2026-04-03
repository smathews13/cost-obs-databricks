"""Cloud infrastructure pricing for AWS and Azure.

This module provides estimated pricing for compute instances used by Databricks
clusters across different cloud providers.
"""

from enum import Enum
from typing import Any


class CloudProvider(Enum):
    AWS = "AWS"
    AZURE = "AZURE"
    GCP = "GCP"
    UNKNOWN = "UNKNOWN"


# AWS EC2 On-Demand pricing (us-east-1, Linux, 2025)
# Source: https://aws.amazon.com/ec2/pricing/on-demand/
AWS_INSTANCE_PRICING: dict[str, float] = {
    # Storage optimized (i3, i3en, i4i)
    "i3.xlarge": 0.312,
    "i3.2xlarge": 0.624,
    "i3.4xlarge": 1.248,
    "i3.8xlarge": 2.496,
    "i3.16xlarge": 4.992,
    "i3en.xlarge": 0.452,
    "i3en.2xlarge": 0.904,
    "i3en.3xlarge": 1.356,
    "i3en.6xlarge": 2.712,
    "i3en.12xlarge": 5.424,
    "i4i.xlarge": 0.333,
    "i4i.2xlarge": 0.666,
    "i4i.4xlarge": 1.332,
    "i4i.8xlarge": 2.664,
    "i4i.16xlarge": 5.328,
    "i4i.32xlarge": 10.656,
    # General purpose (m5, m5d, m5n, m5dn, m6i, m6id, m7i)
    "m5.xlarge": 0.192,
    "m5.2xlarge": 0.384,
    "m5.4xlarge": 0.768,
    "m5.8xlarge": 1.536,
    "m5.12xlarge": 2.304,
    "m5.16xlarge": 3.072,
    "m5.24xlarge": 4.608,
    "m5d.xlarge": 0.226,
    "m5d.2xlarge": 0.452,
    "m5d.4xlarge": 0.904,
    "m5d.8xlarge": 1.808,
    "m5d.12xlarge": 2.712,
    "m5d.16xlarge": 3.616,
    "m5d.24xlarge": 5.424,
    "m5n.xlarge": 0.238,
    "m5n.2xlarge": 0.476,
    "m5n.4xlarge": 0.952,
    "m5n.8xlarge": 1.904,
    "m5dn.xlarge": 0.272,
    "m5dn.2xlarge": 0.544,
    "m5dn.4xlarge": 1.088,
    "m6i.xlarge": 0.192,
    "m6i.2xlarge": 0.384,
    "m6i.4xlarge": 0.768,
    "m6i.8xlarge": 1.536,
    "m6i.12xlarge": 2.304,
    "m6i.16xlarge": 3.072,
    "m6i.24xlarge": 4.608,
    "m6id.xlarge": 0.2259,
    "m6id.2xlarge": 0.4518,
    "m6id.4xlarge": 0.9036,
    "m6id.8xlarge": 1.8072,
    "m7i.xlarge": 0.2016,
    "m7i.2xlarge": 0.4032,
    "m7i.4xlarge": 0.8064,
    "m7i.8xlarge": 1.6128,
    "m7i.12xlarge": 2.4192,
    "m7i.16xlarge": 3.2256,
    "m7i.24xlarge": 4.8384,
    "m7i.48xlarge": 9.6768,
    # Memory optimized (r5, r5d, r6i, r7i)
    "r5.xlarge": 0.252,
    "r5.2xlarge": 0.504,
    "r5.4xlarge": 1.008,
    "r5.8xlarge": 2.016,
    "r5.12xlarge": 3.024,
    "r5.16xlarge": 4.032,
    "r5.24xlarge": 6.048,
    "r5d.xlarge": 0.288,
    "r5d.2xlarge": 0.576,
    "r5d.4xlarge": 1.152,
    "r5d.8xlarge": 2.304,
    "r6i.xlarge": 0.252,
    "r6i.2xlarge": 0.504,
    "r6i.4xlarge": 1.008,
    "r6i.8xlarge": 2.016,
    "r6id.xlarge": 0.2916,
    "r6id.2xlarge": 0.5832,
    "r6id.4xlarge": 1.1664,
    "r6id.8xlarge": 2.3328,
    "r7i.xlarge": 0.2646,
    "r7i.2xlarge": 0.5292,
    "r7i.4xlarge": 1.0584,
    "r7i.8xlarge": 2.1168,
    "r7i.12xlarge": 3.1752,
    "r7i.16xlarge": 4.2336,
    "r7i.24xlarge": 6.3504,
    "r7i.48xlarge": 12.7008,
    # Compute optimized (c5, c5d, c6i, c7i)
    "c5.xlarge": 0.17,
    "c5.2xlarge": 0.34,
    "c5.4xlarge": 0.68,
    "c5.9xlarge": 1.53,
    "c5.12xlarge": 2.04,
    "c5.18xlarge": 3.06,
    "c5d.xlarge": 0.192,
    "c5d.2xlarge": 0.384,
    "c5d.4xlarge": 0.768,
    "c5d.9xlarge": 1.728,
    "c6i.xlarge": 0.17,
    "c6i.2xlarge": 0.34,
    "c6i.4xlarge": 0.68,
    "c6i.8xlarge": 1.36,
    "c7i.xlarge": 0.1785,
    "c7i.2xlarge": 0.357,
    "c7i.4xlarge": 0.714,
    "c7i.8xlarge": 1.428,
    "c7i.12xlarge": 2.142,
    "c7i.16xlarge": 2.856,
    "c7i.24xlarge": 4.284,
    "c7i.48xlarge": 8.568,
    # GPU instances (p3, g4dn, g5, g6)
    "p3.2xlarge": 3.06,
    "p3.8xlarge": 12.24,
    "p3.16xlarge": 24.48,
    "g4dn.xlarge": 0.526,
    "g4dn.2xlarge": 0.752,
    "g4dn.4xlarge": 1.204,
    "g4dn.8xlarge": 2.176,
    "g4dn.12xlarge": 3.912,
    "g5.xlarge": 1.006,
    "g5.2xlarge": 1.212,
    "g5.4xlarge": 1.624,
    "g5.8xlarge": 2.448,
    "g5.12xlarge": 5.672,
    "g5.16xlarge": 4.096,
    "g5.24xlarge": 8.144,
    "g5.48xlarge": 16.288,
    "g6.xlarge": 0.8048,
    "g6.2xlarge": 0.9784,
    "g6.4xlarge": 1.3232,
    "g6.8xlarge": 2.2688,
    "g6.12xlarge": 4.6016,
    "g6.16xlarge": 4.3648,
    "g6.24xlarge": 6.7312,
    "g6.48xlarge": 13.4624,
}

# Azure VM Pay-As-You-Go pricing (East US, Linux, 2025)
# Source: https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/
AZURE_INSTANCE_PRICING: dict[str, float] = {
    # Storage optimized (Lsv2, Lsv3)
    "Standard_L4s": 0.312,
    "Standard_L8s": 0.624,
    "Standard_L16s": 1.248,
    "Standard_L32s": 2.496,
    "Standard_L8s_v2": 0.624,
    "Standard_L16s_v2": 1.248,
    "Standard_L32s_v2": 2.496,
    "Standard_L48s_v2": 3.744,
    "Standard_L64s_v2": 4.992,
    "Standard_L8s_v3": 0.668,
    "Standard_L16s_v3": 1.336,
    "Standard_L32s_v3": 2.672,
    "Standard_L48s_v3": 4.008,
    "Standard_L64s_v3": 5.344,
    "Standard_L80s_v3": 6.680,
    # General purpose (Dv3, Dv4, Dv5, Dv6 — East US, Linux)
    "Standard_D4s_v3": 0.192,
    "Standard_D8s_v3": 0.384,
    "Standard_D16s_v3": 0.768,
    "Standard_D32s_v3": 1.536,
    "Standard_D48s_v3": 2.304,
    "Standard_D64s_v3": 3.072,
    "Standard_D4ds_v4": 0.226,
    "Standard_D8ds_v4": 0.452,
    "Standard_D16ds_v4": 0.904,
    "Standard_D32ds_v4": 1.808,
    "Standard_D48ds_v4": 2.712,
    "Standard_D64ds_v4": 3.616,
    "Standard_D4s_v4": 0.192,
    "Standard_D8s_v4": 0.384,
    "Standard_D16s_v4": 0.768,
    "Standard_D32s_v4": 1.536,
    "Standard_D4ds_v5": 0.226,
    "Standard_D8ds_v5": 0.452,
    "Standard_D16ds_v5": 0.904,
    "Standard_D32ds_v5": 1.808,
    "Standard_D48ds_v5": 2.712,
    "Standard_D64ds_v5": 3.616,
    "Standard_D96ds_v5": 5.424,
    "Standard_D4s_v5": 0.192,
    "Standard_D8s_v5": 0.384,
    "Standard_D16s_v5": 0.768,
    "Standard_D32s_v5": 1.536,
    "Standard_D48s_v5": 2.304,
    "Standard_D64s_v5": 3.072,
    "Standard_D96s_v5": 4.608,
    # General purpose (Dv6, preview)
    "Standard_D4s_v6": 0.192,
    "Standard_D8s_v6": 0.384,
    "Standard_D16s_v6": 0.768,
    "Standard_D32s_v6": 1.536,
    "Standard_D48s_v6": 2.304,
    "Standard_D64s_v6": 3.072,
    # Memory optimized (Ev3, Ev4, Ev5, Ev6)
    "Standard_E4s_v3": 0.252,
    "Standard_E8s_v3": 0.504,
    "Standard_E16s_v3": 1.008,
    "Standard_E32s_v3": 2.016,
    "Standard_E48s_v3": 3.024,
    "Standard_E64s_v3": 4.032,
    "Standard_E4ds_v4": 0.288,
    "Standard_E8ds_v4": 0.576,
    "Standard_E16ds_v4": 1.152,
    "Standard_E32ds_v4": 2.304,
    "Standard_E48ds_v4": 3.456,
    "Standard_E4s_v4": 0.252,
    "Standard_E8s_v4": 0.504,
    "Standard_E16s_v4": 1.008,
    "Standard_E32s_v4": 2.016,
    "Standard_E4ds_v5": 0.288,
    "Standard_E8ds_v5": 0.576,
    "Standard_E16ds_v5": 1.152,
    "Standard_E32ds_v5": 2.304,
    "Standard_E48ds_v5": 3.456,
    "Standard_E64ds_v5": 4.608,
    "Standard_E96ds_v5": 6.912,
    "Standard_E4s_v5": 0.252,
    "Standard_E8s_v5": 0.504,
    "Standard_E16s_v5": 1.008,
    "Standard_E32s_v5": 2.016,
    "Standard_E48s_v5": 3.024,
    "Standard_E64s_v5": 4.032,
    "Standard_E96s_v5": 6.048,
    # Compute optimized (Fsv2)
    "Standard_F4s_v2": 0.169,
    "Standard_F8s_v2": 0.338,
    "Standard_F16s_v2": 0.677,
    "Standard_F32s_v2": 1.354,
    "Standard_F48s_v2": 2.031,
    "Standard_F64s_v2": 2.708,
    "Standard_F72s_v2": 3.045,
    # GPU — NC T4 v3, NC A100 v4, ND A100 v4, NVadsA10 v5
    "Standard_NC6": 0.90,
    "Standard_NC12": 1.80,
    "Standard_NC24": 3.60,
    "Standard_NC6s_v2": 2.07,
    "Standard_NC12s_v2": 4.14,
    "Standard_NC24s_v2": 8.28,
    "Standard_NC6s_v3": 3.06,
    "Standard_NC12s_v3": 6.12,
    "Standard_NC24s_v3": 12.24,
    "Standard_NC4as_T4_v3": 0.526,
    "Standard_NC8as_T4_v3": 0.752,
    "Standard_NC16as_T4_v3": 1.204,
    "Standard_NC64as_T4_v3": 4.352,
    "Standard_NC24ads_A100_v4": 3.673,
    "Standard_NC48ads_A100_v4": 7.346,
    "Standard_NC96ads_A100_v4": 14.692,
    "Standard_ND40rs_v2": 22.032,
    "Standard_ND96asr_v4": 27.197,
    "Standard_ND96amsr_A100_v4": 32.768,
    "Standard_NV4as_v4": 0.361,
    "Standard_NV8as_v4": 0.722,
    "Standard_NV16as_v4": 1.444,
    "Standard_NV32as_v4": 2.888,
    # Standard A series (legacy)
    "Standard_A4_v2": 0.191,
    "Standard_A8_v2": 0.40,
    "Standard_A4m_v2": 0.238,
    "Standard_A8m_v2": 0.50,
}

# Default hourly cost when instance type is not found
DEFAULT_HOURLY_COST = 0.50


def get_instance_pricing(instance_type: str | None, cloud: str) -> float:
    """Get the hourly pricing for an instance type on a given cloud.

    Args:
        instance_type: The instance type string (e.g., "m5.xlarge" for AWS, "Standard_D8s_v3" for Azure)
        cloud: The cloud provider ("AWS", "AZURE", "GCP")

    Returns:
        Hourly cost in USD. Returns DEFAULT_HOURLY_COST if not found.
    """
    if not instance_type:
        return DEFAULT_HOURLY_COST

    cloud_upper = cloud.upper() if cloud else ""

    if cloud_upper == "AWS":
        return AWS_INSTANCE_PRICING.get(instance_type, DEFAULT_HOURLY_COST)
    elif cloud_upper == "AZURE":
        return AZURE_INSTANCE_PRICING.get(instance_type, DEFAULT_HOURLY_COST)
    else:
        # Try both pricing tables for unknown cloud
        price = AWS_INSTANCE_PRICING.get(instance_type)
        if price:
            return price
        return AZURE_INSTANCE_PRICING.get(instance_type, DEFAULT_HOURLY_COST)


def get_instance_family(instance_type: str | None, cloud: str) -> str:
    """Extract the instance family from an instance type.

    Args:
        instance_type: The instance type string
        cloud: The cloud provider

    Returns:
        The instance family (e.g., "m5" for AWS, "D" for Azure)
    """
    if not instance_type:
        return "unknown"

    cloud_upper = cloud.upper() if cloud else ""

    if cloud_upper == "AZURE":
        # Azure format: Standard_<Family><Size>_v<version>
        # Examples: Standard_D8s_v3, Standard_E16ds_v4, Standard_NC6s_v3
        if instance_type.startswith("Standard_"):
            # Extract family letter(s) after "Standard_"
            family_part = instance_type[9:]  # Remove "Standard_"
            # Get the letter(s) before the first digit
            family = ""
            for char in family_part:
                if char.isdigit():
                    break
                family += char
            # Return with "Standard_" prefix to match frontend expectations
            return f"Standard_{family}" if family else "unknown"
        return "unknown"
    else:
        # AWS format: <family>.<size>
        # Examples: m5.xlarge, i3.2xlarge, g4dn.xlarge
        # Family is the full prefix before the dot (e.g., m5, i3, g4dn)
        parts = instance_type.split(".")
        if parts and parts[0]:
            return parts[0]
        return "unknown"


def get_cloud_display_name(cloud: str) -> str:
    """Get a display-friendly name for the cloud provider."""
    cloud_upper = cloud.upper() if cloud else ""

    if cloud_upper == "AWS":
        return "AWS"
    elif cloud_upper == "AZURE":
        return "Azure"
    elif cloud_upper == "GCP":
        return "GCP"
    else:
        return "Cloud"


def get_pricing_disclaimer(cloud: str) -> str:
    """Get a pricing disclaimer for the given cloud provider."""
    cloud_upper = cloud.upper() if cloud else ""

    if cloud_upper == "AWS":
        return "Infrastructure costs are estimated based on AWS EC2 On-Demand pricing (us-east-1, Linux, 2025). Reserved Instances and Spot can reduce actual costs by 30–72%. Does not include EBS storage, data transfer, or Databricks licensing fees."
    elif cloud_upper == "AZURE":
        return "Infrastructure costs are estimated based on Azure VM Pay-As-You-Go pricing (East US, Linux, 2025). Azure Hybrid Benefit, Reserved VMs, and Spot can reduce actual costs by 40–72%. Does not include Managed Disk storage, bandwidth, or Databricks licensing fees."
    elif cloud_upper == "GCP":
        return "Infrastructure costs are estimated based on GCP Compute Engine On-Demand pricing. Actual costs may vary based on region, committed use discounts, and preemptible VMs."
    else:
        return "Infrastructure costs are estimated based on standard cloud pricing. Actual costs may vary based on region and pricing model."


def generate_pricing_sql_values(cloud: str) -> str:
    """Generate SQL VALUES clause for instance pricing.

    Used to inject pricing into SQL queries.
    """
    cloud_upper = cloud.upper() if cloud else ""

    if cloud_upper == "AZURE":
        pricing = AZURE_INSTANCE_PRICING
    else:
        pricing = AWS_INSTANCE_PRICING

    values = []
    for instance_type, price in pricing.items():
        values.append(f"('{instance_type}', {price})")

    return ",\n    ".join(values)


def get_instance_families_for_cloud(cloud: str) -> list[str]:
    """Get the list of instance families for a cloud provider."""
    cloud_upper = cloud.upper() if cloud else ""

    if cloud_upper == "AZURE":
        return ["D", "E", "F", "L", "NC", "ND", "NV", "A"]
    else:
        return ["i3", "i3en", "i4i", "m5", "m5d", "m6i", "m7i", "r5", "r5d", "r6i", "r7i", "c5", "c5d", "c6i", "c7i", "g4dn", "g5", "g6", "p3"]


# Color mapping for instance families (used in UI)
INSTANCE_FAMILY_COLORS: dict[str, str] = {
    # AWS families
    "i3": "#3b82f6",
    "i3en": "#60a5fa",
    "m5": "#22c55e",
    "m5d": "#4ade80",
    "m6i": "#86efac",
    "r5": "#f59e0b",
    "r5d": "#fbbf24",
    "c5": "#a855f7",
    "c5d": "#c084fc",
    "g4dn": "#ec4899",
    "g5": "#f472b6",
    "p3": "#ef4444",
    # Azure families
    "L": "#3b82f6",
    "D": "#22c55e",
    "E": "#f59e0b",
    "F": "#a855f7",
    "NC": "#ec4899",
    "ND": "#ef4444",
    "A": "#6b7280",
    # Default
    "unknown": "#6b7280",
}
