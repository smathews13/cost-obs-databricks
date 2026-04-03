"""Import the Cost Reporting Genie Space using the pre-configured JSON."""
import os
import json
import requests
from databricks.sdk import WorkspaceClient

def import_genie_space():
    """Import Genie Space using the Databricks Genie Import API."""
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

    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")

    if not host or not token:
        raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")

    # Load the Genie Space configuration JSON
    config_file = "genie_space_config.json"
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"{config_file} not found. Please ensure it exists in the project root.")

    with open(config_file, "r") as f:
        genie_config = json.load(f)

    # Add warehouse_id to the configuration
    genie_config["warehouse_id"] = warehouse_id

    print(f"\n📦 Importing Genie Space configuration...")
    print(f"Title: {genie_config['title']}")
    print(f"Warehouse: {warehouse_id}")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Use the Genie Create Space API
    try:
        response = requests.post(
            f"{host}/api/2.0/genie/spaces",
            headers=headers,
            json=genie_config
        )

        if response.status_code not in [200, 201]:
            print(f"Error response: {response.text}")
            response.raise_for_status()

        space_data = response.json()
        space_id = space_data.get("space_id")

        print(f"\n✅ Genie Space imported successfully!")
        print(f"Space ID: {space_id}")
        print(f"Title: {genie_config['title']}")

        # Update .env.local with the space ID
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

            print(f"\n✅ Updated {env_file} with GENIE_SPACE_ID={space_id}")

        # Grant the app's service principal CAN_RUN on the Genie Space
        try:
            app_name = "cost-obs"
            app_info = requests.get(
                f"{host}/api/2.0/apps/{app_name}",
                headers=headers,
            ).json()
            sp_client_id = app_info.get("service_principal_client_id", "")
            if sp_client_id:
                perm_resp = requests.patch(
                    f"{host}/api/2.0/permissions/genie/{space_id}",
                    headers=headers,
                    json={"access_control_list": [{"service_principal_name": sp_client_id, "permission_level": "CAN_RUN"}]},
                )
                if perm_resp.status_code == 200:
                    print(f"\n✅ Granted CAN_RUN to app service principal ({sp_client_id})")
                else:
                    print(f"\n⚠️  Could not grant permissions to service principal: {perm_resp.text[:200]}")
        except Exception as perm_err:
            print(f"\n⚠️  Could not auto-grant permissions: {perm_err}")

        print(f"\n📝 Next steps:")
        print(f"1. Visit the Genie Space: {host}/genie/{space_id}")
        print(f"2. The space is pre-configured with:")
        print(f"   - system.billing.usage")
        print(f"   - system.billing.list_prices")
        print(f"   - system.billing.account_prices")
        print(f"   - system.access.workspaces_latest")
        print(f"3. Restart the watch server:")
        print(f"   pkill -f watch.sh")
        print(f"   nohup ./watch.sh > /tmp/databricks-app-watch.log 2>&1 &")
        print(f"4. Open the app and use the Genie Assistant tab!")

        return space_id

    except Exception as e:
        print(f"❌ Error importing Genie Space: {e}")
        print(f"\nTroubleshooting:")
        print(f"1. Ensure you have access to the Genie API (may require preview access)")
        print(f"2. Check that your token has permission to create Genie Spaces")
        print(f"3. Verify the warehouse_id is valid")
        raise

if __name__ == "__main__":
    import_genie_space()
