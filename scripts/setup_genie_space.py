"""Setup script for Genie Space configuration."""
import os
import sys

def setup_genie_space():
    """Provide instructions and help set up Genie Space."""

    # Load environment variables
    env_file = ".env.local"
    if os.path.exists(env_file):
        with open(env_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ[key] = value

    host = os.environ.get("DATABRICKS_HOST", "your-databricks-workspace")

    print("=" * 80)
    print("Genie Space Setup for Cost Observability & Control")
    print("=" * 80)
    print()
    print("To enable the Genie chat feature, you need to create a Genie Space in Databricks.")
    print()
    print("📋 STEP 1: Create Genie Space")
    print("-" * 80)
    print(f"1. Visit: {host}/sql/genie")
    print("2. Click 'Create Space' button")
    print("3. Name it: 'Cost Observability & Control'")
    print("4. Select a SQL Warehouse")
    print()
    print("📋 STEP 2: Configure Tables")
    print("-" * 80)
    print("Add these system tables to your Genie Space:")
    print("  • system.billing.usage")
    print("  • system.billing.list_prices")
    print("  • system.compute.clusters")
    print()
    print("📋 STEP 3: Add Instructions")
    print("-" * 80)
    print("Add these instructions to guide the Genie assistant:")
    print()
    instructions = """This space provides access to Databricks system tables for cost analysis.

Key Tables:
- system.billing.usage: Usage records with DBU consumption, SKUs, and metadata
- system.billing.list_prices: Current pricing for all SKUs by cloud provider
- system.compute.clusters: Cluster configurations including instance types

Important Columns:
- usage_date: Date of usage
- sku_name: Product SKU (ALL_PURPOSE_COMPUTE, JOBS_COMPUTE, SERVERLESS_SQL, etc.)
- usage_quantity: DBUs consumed
- workspace_id: Workspace identifier
- usage_metadata: JSON with cluster_id, warehouse_id, notebook_path, etc.
- identity_metadata: JSON with run_as user information

Common Queries:
- Top expensive workspaces by total spend
- Daily spending trends over time periods
- SQL warehouse DBU consumption analysis
- Cost breakdown by product/SKU type
- Most expensive notebooks and users
- Interactive compute vs jobs compute attribution"""
    print(instructions)
    print()
    print("📋 STEP 4: Add Sample Questions")
    print("-" * 80)
    print("Add these sample questions to help users:")
    print("  • What are my top 5 most expensive workspaces this month?")
    print("  • Show me daily spending trends for the last 30 days")
    print("  • Which SQL warehouses consume the most DBUs?")
    print("  • What percentage of costs come from interactive compute?")
    print("  • Show me the most expensive notebooks by total spend")
    print()
    print("📋 STEP 5: Get Your Space ID")
    print("-" * 80)
    print("1. In your Genie Space, click the '...' menu")
    print("2. Copy the Space ID (looks like: 01f0fada534f16b19656a2e3ebd0f46e)")
    print("3. Run this script again with the Space ID:")
    print()
    print(f"   uv run python scripts/setup_genie_space.py <space-id>")
    print()
    print("=" * 80)

def update_env_with_space_id(space_id):
    """Update .env.local with Genie Space ID."""
    env_file = ".env.local"

    if not os.path.exists(env_file):
        print(f"❌ Error: {env_file} not found")
        return False

    with open(env_file, "r") as f:
        lines = f.readlines()

    # Check if GENIE_SPACE_ID already exists
    updated = False
    for i, line in enumerate(lines):
        if line.startswith("GENIE_SPACE_ID="):
            lines[i] = f"GENIE_SPACE_ID={space_id}\n"
            updated = True
            break

    # Add if not found
    if not updated:
        lines.append(f"\nGENIE_SPACE_ID={space_id}\n")

    with open(env_file, "w") as f:
        f.writelines(lines)

    print(f"✅ Updated {env_file} with GENIE_SPACE_ID={space_id}")
    print()
    print("📋 Next Steps:")
    print("1. Restart the watch server:")
    print("   pkill -f watch.sh")
    print("   nohup ./watch.sh > /tmp/databricks-app-watch.log 2>&1 &")
    print()
    print("2. Open the app and click the 'Genie Assistant' tab")
    print("3. Start asking questions about your cost data!")
    print()
    return True

if __name__ == "__main__":
    if len(sys.argv) > 1:
        space_id = sys.argv[1]
        update_env_with_space_id(space_id)
    else:
        setup_genie_space()
