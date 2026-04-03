"""Create a Genie Space configured with system tables for cost observability."""
import os
import json
import requests
from databricks.sdk import WorkspaceClient

def create_cost_genie_space():
    """Create a Genie Space with system billing tables."""
    # Load environment variables from .env.local
    env_file = ".env.local"
    if os.path.exists(env_file):
        with open(env_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ[key] = value

    w = WorkspaceClient()

    # Get first available warehouse
    warehouses = list(w.warehouses.list())
    if not warehouses:
        raise ValueError("No SQL warehouses found. Please create a warehouse first.")

    warehouse_id = warehouses[0].id
    print(f"Using warehouse: {warehouses[0].name} ({warehouse_id})")

    # Instructions for the Genie space
    instructions = """This Genie Space provides access to Databricks system tables for cost analysis.

Tables available:
- system.billing.usage: All usage records with DBU consumption, SKUs, metadata
- system.billing.list_prices: Current pricing for all SKUs by cloud provider
- system.compute.clusters: Cluster configurations including instance types
- system.lakeflow.pipelines: Pipeline configurations for DLT execution mode

Common queries:
- "What are my top 5 most expensive workspaces this month?"
- "Show me daily spending trends for the last 30 days"
- "Which SQL warehouses consume the most DBUs?"
- "What percentage of costs come from interactive compute?"
- "Show me the most expensive notebooks by total spend"
- "Show me batch vs streaming costs for DLT pipelines"

Key columns:
- usage_date: Date of usage
- sku_name: Product SKU (e.g., ALL_PURPOSE_COMPUTE, JOBS_COMPUTE, SERVERLESS_SQL)
- usage_quantity: DBUs consumed
- workspace_id: Workspace identifier
- billing_origin_product: Product taxonomy (SQL, JOBS, DLT)
- usage_metadata: Contains cluster_id, warehouse_id, notebook_path, etc.
- identity_metadata: Contains run_as user information
"""

    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")

    if not host or not token:
        raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Create payload with serialized_space as JSON string
    serialized_space = json.dumps({
        "instructions": instructions
    })

    payload = {
        "warehouse_id": warehouse_id,
        "display_name": "Cost Observability & Control",
        "serialized_space": serialized_space
    }

    # Create the Genie space
    try:
        response = requests.post(
            f"{host}/api/2.0/genie/spaces",
            headers=headers,
            json=payload
        )

        if response.status_code != 200:
            print(f"Error response: {response.text}")
            response.raise_for_status()

        space_data = response.json()

        space_id = space_data.get("space_id")

        print(f"\n✅ Genie Space created successfully!")
        print(f"Space ID: {space_id}")
        print(f"Title: {payload['display_name']}")
        print(f"Warehouse: {warehouse_id}")

        # Update .env.local with the space ID
        env_file = ".env.local"
        if os.path.exists(env_file):
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

            print(f"\n✅ Updated {env_file} with GENIE_SPACE_ID")

        print(f"\n📝 Next steps:")
        print(f"1. Visit the Genie Space: {host}/genie/{space_id}")
        print(f"2. Add these system tables:")
        print(f"   - system.billing.usage")
        print(f"   - system.billing.list_prices")
        print(f"   - system.compute.clusters")
        print(f"   - system.lakeflow.pipelines")
        print(f"3. Add sample questions")
        print(f"4. Restart the watch server")

        return space_id

    except Exception as e:
        print(f"❌ Error creating Genie Space: {e}")
        raise

if __name__ == "__main__":
    create_cost_genie_space()
