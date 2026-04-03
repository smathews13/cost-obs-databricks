#!/bin/bash
set -e

echo "=== Cost Observability & Control Setup ==="

# Check for uv
if ! command -v uv &> /dev/null; then
    echo "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
fi

# Check for bun
if ! command -v bun &> /dev/null; then
    echo "Installing bun..."
    curl -fsSL https://bun.sh/install | bash
fi

# Install Python dependencies
echo "Installing Python dependencies..."
uv sync

# Install frontend dependencies
echo "Installing frontend dependencies..."
cd client && bun install && cd ..

# Create .env.local if it doesn't exist
if [ ! -f .env.local ]; then
    echo "Creating .env.local..."
    cat > .env.local << 'EOF'
# Databricks Configuration
# DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
# DATABRICKS_TOKEN=your-token
# DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
EOF
    echo "Created .env.local - please configure your Databricks credentials"
fi

echo ""
echo "=== Setup Complete ==="
echo "Run ./watch.sh to start the development server"
