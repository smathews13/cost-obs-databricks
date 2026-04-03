#!/bin/bash
# =============================================================================
# Databricks App Deployment Script (Multi-Cloud)
# =============================================================================
# A robust, repeatable deployment script for Databricks Apps on AWS and Azure.
#
# Usage:
#   ./dba_deploy.sh                    # Deploy using .env.local (default)
#   ./dba_deploy.sh --target azure     # Deploy using .env.azure
#   ./dba_deploy.sh --target aws       # Deploy using .env.local (explicit)
#   ./dba_deploy.sh --status           # Check deploy progress from log
#   ./dba_deploy.sh --all              # Deploy to all configured targets
#   ./dba_deploy.sh my-app-name        # Override app name (legacy)
#
# Environment files:
#   .env.local   — Default / AWS target
#   .env.azure   — Azure target(s)
#   .env.<name>  — Custom targets
#
# Prerequisites:
#   - Databricks CLI installed and configured
#   - Environment file with DATABRICKS_HOST and DATABRICKS_TOKEN
#   - app.yaml in the project root
# =============================================================================

set -e

# =============================================================================
# Deploy log file — all progress written here for live monitoring
# =============================================================================
DEPLOY_LOG="/tmp/cost-obs-deploy.log"

# Trap unexpected exits so they show up in the log
trap 'ec=$?; echo "[$(date "+%H:%M:%S")] [ERROR] Script exited unexpectedly (exit code $ec)" >> "$DEPLOY_LOG"; echo "DEPLOY_DONE|FAILED|$(date "+%H:%M:%S")|" >> "$DEPLOY_LOG"' ERR

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions — write to both stdout and log file
log_info()    { local msg="[INFO] $1";    echo -e "${BLUE}${msg}${NC}"; echo "[$(date '+%H:%M:%S')] ${msg}" >> "$DEPLOY_LOG"; }
log_success() { local msg="[SUCCESS] $1"; echo -e "${GREEN}${msg}${NC}"; echo "[$(date '+%H:%M:%S')] ${msg}" >> "$DEPLOY_LOG"; }
log_warn()    { local msg="[WARN] $1";    echo -e "${YELLOW}${msg}${NC}"; echo "[$(date '+%H:%M:%S')] ${msg}" >> "$DEPLOY_LOG"; }
log_error()   { local msg="[ERROR] $1";   echo -e "${RED}${msg}${NC}"; echo "[$(date '+%H:%M:%S')] ${msg}" >> "$DEPLOY_LOG"; }

# Step tracking — writes structured markers for easy parsing
TOTAL_STEPS=7
step_start()    { echo "STEP_START|$1|$TOTAL_STEPS|$2|$(date '+%H:%M:%S')" >> "$DEPLOY_LOG"; log_info "Step $1/$TOTAL_STEPS: $2..."; }
step_complete() { echo "STEP_DONE|$1|$TOTAL_STEPS|$2|$(date '+%H:%M:%S')" >> "$DEPLOY_LOG"; log_success "Step $1/$TOTAL_STEPS: $2 complete"; }
step_fail()     { echo "STEP_FAIL|$1|$TOTAL_STEPS|$2|$(date '+%H:%M:%S')|$3" >> "$DEPLOY_LOG"; log_error "Step $1/$TOTAL_STEPS: $2 FAILED — $3"; }
deploy_done()   { echo "DEPLOY_DONE|$1|$(date '+%H:%M:%S')|$2" >> "$DEPLOY_LOG"; }

# =============================================================================
# --status flag: parse log and show current state
# =============================================================================
if [ "$1" = "--status" ]; then
    if [ ! -f "$DEPLOY_LOG" ]; then
        echo "No deploy log found at $DEPLOY_LOG — no deployment has been run yet."
        exit 0
    fi
    echo "=== Deploy Status ==="
    # Show last step marker
    LAST_STEP=$(grep -E '^STEP_(START|DONE|FAIL)\|' "$DEPLOY_LOG" | tail -1)
    DONE_LINE=$(grep -E '^DEPLOY_DONE\|' "$DEPLOY_LOG" | tail -1)
    if [ -n "$DONE_LINE" ]; then
        RESULT=$(echo "$DONE_LINE" | cut -d'|' -f2)
        TIME=$(echo "$DONE_LINE" | cut -d'|' -f3)
        URL=$(echo "$DONE_LINE" | cut -d'|' -f4)
        if [ "$RESULT" = "SUCCESS" ]; then
            echo "FINISHED: Deploy succeeded at $TIME"
            echo "URL: $URL"
        else
            echo "FINISHED: Deploy FAILED at $TIME"
        fi
    elif [ -n "$LAST_STEP" ]; then
        TYPE=$(echo "$LAST_STEP" | cut -d'|' -f1)
        STEP_NUM=$(echo "$LAST_STEP" | cut -d'|' -f2)
        STEP_TOTAL=$(echo "$LAST_STEP" | cut -d'|' -f3)
        STEP_NAME=$(echo "$LAST_STEP" | cut -d'|' -f4)
        STEP_TIME=$(echo "$LAST_STEP" | cut -d'|' -f5)
        case "$TYPE" in
            STEP_START) echo "IN PROGRESS: Step $STEP_NUM/$STEP_TOTAL — $STEP_NAME (started $STEP_TIME)" ;;
            STEP_DONE)  echo "IN PROGRESS: Step $STEP_NUM/$STEP_TOTAL — $STEP_NAME done ($STEP_TIME), next step pending" ;;
            STEP_FAIL)  REASON=$(echo "$LAST_STEP" | cut -d'|' -f6); echo "FAILED at Step $STEP_NUM/$STEP_TOTAL — $STEP_NAME: $REASON ($STEP_TIME)" ;;
        esac
    else
        echo "Deploy started but no step markers yet."
    fi
    echo "--- Last 10 log lines ---"
    tail -10 "$DEPLOY_LOG"
    exit 0
fi

# =============================================================================
# --all flag: deploy to all configured targets sequentially
# =============================================================================
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT_PATH="$SCRIPT_DIR/$(basename "$0")"

if [ "$1" = "--all" ]; then
    echo -e "${BLUE}[INFO] Deploying to all configured targets...${NC}"
    FAILED=0

    # Always deploy default (AWS)
    echo -e "\n${BLUE}========== Deploying: AWS (default) ==========${NC}\n"
    bash "$SCRIPT_PATH" || FAILED=$((FAILED + 1))

    # Deploy any .env.<target> files (except .env.local and .env.example)
    for envfile in .env.azure .env.gcp; do
        if [ -f "$envfile" ]; then
            TARGET="${envfile#.env.}"
            echo -e "\n${BLUE}========== Deploying: ${TARGET} ==========${NC}\n"
            bash "$SCRIPT_PATH" --target "$TARGET" || FAILED=$((FAILED + 1))
        fi
    done

    if [ $FAILED -eq 0 ]; then
        echo -e "\n${GREEN}[SUCCESS] All deployments completed!${NC}"
    else
        echo -e "\n${YELLOW}[WARN] $FAILED deployment(s) had issues.${NC}"
    fi
    exit $FAILED
fi

# =============================================================================
# Parse arguments: --target <name> and/or app name
# =============================================================================
TARGET=""
APP_NAME=""
CLI_PROFILE=""

while [ $# -gt 0 ]; do
    case "$1" in
        --target)
            TARGET="$2"
            shift 2
            ;;
        *)
            APP_NAME="$1"
            shift
            ;;
    esac
done

# =============================================================================
# Configuration — load environment based on target
# =============================================================================

# Fresh log for this deploy run
echo "=== Deploy started at $(date '+%Y-%m-%d %H:%M:%S') ===" > "$DEPLOY_LOG"

# Determine which env file to source
if [ -n "$TARGET" ]; then
    ENV_FILE=".env.${TARGET}"
    if [ ! -f "$ENV_FILE" ]; then
        log_error "Environment file not found: $ENV_FILE"
        log_error "Create it with DATABRICKS_HOST and DATABRICKS_TOKEN for the ${TARGET} target."
        exit 1
    fi
else
    ENV_FILE=".env.local"
fi

if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
    export DATABRICKS_HOST
    export DATABRICKS_TOKEN
    # Export CLI profile if set in env file
    if [ -n "$DATABRICKS_CLI_PROFILE" ]; then
        CLI_PROFILE="$DATABRICKS_CLI_PROFILE"
    fi
else
    log_error "$ENV_FILE not found. Create it with DATABRICKS_HOST and DATABRICKS_TOKEN."
    exit 1
fi

# Detect cloud from host URL
CLOUD="unknown"
HOST_LOWER=$(echo "$DATABRICKS_HOST" | tr '[:upper:]' '[:lower:]')
if echo "$HOST_LOWER" | grep -q "azuredatabricks.net"; then
    CLOUD="azure"
elif echo "$HOST_LOWER" | grep -q "gcp.databricks.com"; then
    CLOUD="gcp"
else
    CLOUD="aws"
fi

# Hide databricks.yml during deployment so the CLI uses DATABRICKS_HOST from the
# environment directly. Without this, the CLI reads databricks.yml and uses the
# literal "${DATABRICKS_HOST}" string as the host for workspace/apps commands.
if [ -f "databricks.yml" ]; then
    mv databricks.yml databricks.yml.deploy-bak
    trap 'mv databricks.yml.deploy-bak databricks.yml 2>/dev/null' EXIT
fi

# Build CLI args (profile flag if needed)
CLI_ARGS=""
if [ -n "$CLI_PROFILE" ]; then
    CLI_ARGS="--profile $CLI_PROFILE"
fi

# If no DATABRICKS_TOKEN but a CLI profile is set, extract the token from
# ~/.databrickscfg so curl-based API calls (Lakebase, SQL statements) work.
if [ -z "$DATABRICKS_TOKEN" ] && [ -n "$CLI_PROFILE" ]; then
    DATABRICKS_TOKEN=$(python3 -c "
import configparser, os, sys
cfg = configparser.ConfigParser()
cfg.read(os.path.expanduser('~/.databrickscfg'))
print(cfg.get(sys.argv[1], 'token', fallback=''))
" "$CLI_PROFILE" 2>/dev/null || echo "")
    if [ -n "$DATABRICKS_TOKEN" ]; then
        export DATABRICKS_TOKEN
        log_info "Using token from CLI profile: ${CLI_PROFILE}"
    else
        log_warn "No DATABRICKS_TOKEN in env or CLI profile — curl-based API calls may fail"
    fi
fi

# Get app name from argument, env file, or app.yaml
if [ -z "$APP_NAME" ]; then
    APP_NAME="${DATABRICKS_APP_NAME:-$(grep "^name:" app.yaml | awk '{print $2}')}"
fi

if [ -z "$APP_NAME" ]; then
    log_error "Could not determine app name. Provide as argument or ensure app.yaml has 'name:' field."
    exit 1
fi

# Verify CLI is available
if ! command -v databricks &> /dev/null; then
    log_error "Databricks CLI not found. Install with: brew install databricks"
    exit 1
fi

# Workspace path for the app
WORKSPACE_PATH="/Workspace/Users/${DATABRICKS_USER:-$(databricks current-user me $CLI_ARGS 2>/dev/null | jq -r '.userName')}/apps/${APP_NAME}"

log_info "=============================================="
log_info "Deploying: ${APP_NAME}"
log_info "Cloud: $(echo "$CLOUD" | tr '[:lower:]' '[:upper:]')"
log_info "Host: ${DATABRICKS_HOST}"
log_info "Workspace: ${WORKSPACE_PATH}"
log_info "Env file: ${ENV_FILE}"
log_info "=============================================="

# =============================================================================
# Step 0: Pre-flight validation & resource setup
# =============================================================================
step_start 0 "Pre-flight validation & resource setup"

# Determine which app.yaml to use for pre-flight validation
PREFLIGHT_YAML="app.yaml"
if [ -n "$TARGET" ] && [ -f "app.${TARGET}.yaml" ]; then
    PREFLIGHT_YAML="app.${TARGET}.yaml"
elif [ -f "app.${CLOUD}.yaml" ]; then
    PREFLIGHT_YAML="app.${CLOUD}.yaml"
fi

# Validate app.yaml doesn't use DATABRICKS_HTTP_PATH: auto
HTTP_PATH_VALUE=$(grep -A1 'DATABRICKS_HTTP_PATH' "$PREFLIGHT_YAML" | grep 'value:' | awk '{print $2}')
if [ "$HTTP_PATH_VALUE" = "auto" ] || [ -z "$HTTP_PATH_VALUE" ]; then
    log_error "app.yaml has DATABRICKS_HTTP_PATH set to 'auto' or empty."
    log_error "Set it to an explicit warehouse path like: /sql/1.0/warehouses/{id}"
    log_error "The app's service principal cannot create warehouses."
    exit 1
fi

# Extract warehouse ID from HTTP path
WAREHOUSE_ID=$(echo "$HTTP_PATH_VALUE" | sed 's|/sql/1.0/warehouses/||')
if [ -z "$WAREHOUSE_ID" ]; then
    log_error "Could not extract warehouse ID from HTTP path: $HTTP_PATH_VALUE"
    exit 1
fi

log_info "  Warehouse ID: $WAREHOUSE_ID"
log_info "  HTTP Path: $HTTP_PATH_VALUE"

# Extract app description from app.yaml
APP_DESCRIPTION=$(grep "^description:" app.yaml | sed 's/^description: //')

# Ensure app exists (create if needed) and set resources
log_info "  Checking if app exists..."
APP_EXISTS=$(databricks apps get "$APP_NAME" $CLI_ARGS 2>&1) || true
if echo "$APP_EXISTS" | grep -qi "does not exist\|not found"; then
    log_info "  App does not exist. Creating..."
    CREATE_JSON=$(cat <<EOF
{
  "name": "${APP_NAME}",
  "description": "${APP_DESCRIPTION}",
  "resources": [{
    "name": "sql-warehouse",
    "description": "SQL Warehouse for cost observability queries",
    "sql_warehouse": {
      "id": "${WAREHOUSE_ID}",
      "permission": "CAN_USE"
    }
  }]
}
EOF
)
    CREATE_RESULT=$(databricks apps create --json "$CREATE_JSON" --no-wait $CLI_ARGS 2>&1) || true
    if echo "$CREATE_RESULT" | grep -qi "error"; then
        log_error "  Failed to create app: $CREATE_RESULT"
        exit 1
    fi
    log_success "  App created with SQL warehouse resource"
    # Wait for app to be ready
    log_info "  Waiting for app to initialize..."
    sleep 15
else
    # App exists - update resources
    log_info "  Setting app resources (SQL warehouse CAN_USE)..."
    RESOURCE_JSON=$(cat <<EOF
{
  "description": "${APP_DESCRIPTION}",
  "resources": [{
    "name": "sql-warehouse",
    "description": "SQL Warehouse for cost observability queries",
    "sql_warehouse": {
      "id": "${WAREHOUSE_ID}",
      "permission": "CAN_USE"
    }
  }]
}
EOF
)
    UPDATE_RESULT=$(databricks apps update "$APP_NAME" --json "$RESOURCE_JSON" $CLI_ARGS 2>&1) || true
    if echo "$UPDATE_RESULT" | grep -qi "error"; then
        log_warn "  Could not update app resources: $UPDATE_RESULT"
    else
        log_success "  App resources set (SQL warehouse CAN_USE on $WAREHOUSE_ID)"
    fi
fi

step_complete 0 "Pre-flight validation & resource setup"

# =============================================================================
# Lakebase setup — create project if needed, get endpoint, update deploy yaml
# =============================================================================
# We always work from a temp copy of app.yaml so the source file is never
# mutated.  If Lakebase setup succeeds we inject the database resource and
# ENDPOINT_NAME; on failure we just use the original yaml unchanged.
# =============================================================================

# Determine app.yaml source now (also used in step 4 upload)
APP_YAML_SOURCE="app.yaml"
if [ -n "$TARGET" ] && [ -f "app.${TARGET}.yaml" ]; then
    APP_YAML_SOURCE="app.${TARGET}.yaml"
elif [ -f "app.${CLOUD}.yaml" ]; then
    APP_YAML_SOURCE="app.${CLOUD}.yaml"
fi

LAKEBASE_PROJECT_ID="${DATABRICKS_LAKEBASE_PROJECT:-cost-obs-app}"
DEPLOY_YAML=$(mktemp "/tmp/cost-obs-deploy.XXXXXX")
cleanup_deploy_yaml() { rm -f "$DEPLOY_YAML"; }
trap cleanup_deploy_yaml EXIT
cp "$APP_YAML_SOURCE" "$DEPLOY_YAML"

log_info "----------------------------------------------"
log_info "Lakebase project: ${LAKEBASE_PROJECT_ID}"

# ── Check if project already exists ──────────────────────────────────────────
LB_STATUS_JSON=$(curl -s \
    -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
    "${DATABRICKS_HOST}/api/2.0/postgres/projects/${LAKEBASE_PROJECT_ID}" 2>/dev/null)
LB_STATE=$(echo "$LB_STATUS_JSON" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(d.get('status',{}).get('state','MISSING'))" \
    2>/dev/null || echo "MISSING")

# ── Create project if it doesn't exist ───────────────────────────────────────
LB_HAS_PROJECT=$(echo "$LB_STATUS_JSON" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print('yes' if d.get('name','').startswith('projects/') else 'no')
except: print('no')
" 2>/dev/null || echo "no")

LAKEBASE_READY=false
if [ "$LB_HAS_PROJECT" = "yes" ]; then
    # Project already exists — ready immediately, no polling needed
    LAKEBASE_READY=true
    log_success "  Lakebase project already exists"
else
    log_info "  Creating Lakebase project '${LAKEBASE_PROJECT_ID}'..."
    CREATE_RESP=$(curl -s -X POST \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        "${DATABRICKS_HOST}/api/2.0/postgres/projects?project_id=${LAKEBASE_PROJECT_ID}" 2>/dev/null)
    log_info "  Create response: $(echo "$CREATE_RESP" | head -c 300)"

    # Poll until ready (up to 5 min) — project is ready when GET returns a name field
    log_info "  Waiting for Lakebase to be ready..."
    for _i in $(seq 1 30); do
        LB_STATUS_JSON=$(curl -s \
            -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
            "${DATABRICKS_HOST}/api/2.0/postgres/projects/${LAKEBASE_PROJECT_ID}" 2>/dev/null)
        LB_HAS_NAME=$(echo "$LB_STATUS_JSON" | python3 -c \
            "import sys,json; d=json.load(sys.stdin); print('yes' if d.get('name','').startswith('projects/') else 'no')" \
            2>/dev/null || echo "no")
        if [ "$LB_HAS_NAME" = "yes" ]; then
            LAKEBASE_READY=true
            log_success "  Lakebase project ready"
            break
        fi
        log_info "  Lakebase not ready yet (attempt $_i/30)..."
        sleep 10
    done
fi

# ── Get endpoint name and update deploy yaml ──────────────────────────────────
if [ "$LAKEBASE_READY" = "true" ]; then
    # Get branch from project status, then list endpoints
    BRANCH_ID=$(echo "$LB_STATUS_JSON" | python3 -c "
import sys, json
try:
    branch_path = json.load(sys.stdin).get('status', {}).get('default_branch', '')
    # format: projects/cost-obs-app/branches/production
    print(branch_path.split('/')[-1] if branch_path else 'production')
except: print('production')
" 2>/dev/null || echo "production")

    ENDPOINTS_JSON=$(curl -s \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        "${DATABRICKS_HOST}/api/2.0/postgres/projects/${LAKEBASE_PROJECT_ID}/branches/${BRANCH_ID}/endpoints" \
        2>/dev/null)
    ENDPOINT_ID=$(echo "$ENDPOINTS_JSON" | python3 -c "
import sys, json
try:
    endpoints = json.load(sys.stdin).get('endpoints', [])
    name = endpoints[0]['name'] if endpoints else ''
    print(name.split('/')[-1] if name else 'primary')
except: print('primary')
" 2>/dev/null || echo "primary")

    ENDPOINT_NAME="projects/${LAKEBASE_PROJECT_ID}/branches/${BRANCH_ID}/endpoints/${ENDPOINT_ID}"
    log_success "  Lakebase endpoint: $ENDPOINT_NAME"

    # Get the endpoint host from the endpoint detail API
    ENDPOINT_DETAIL_JSON=$(curl -s \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        "${DATABRICKS_HOST}/api/2.0/postgres/projects/${LAKEBASE_PROJECT_ID}/branches/${BRANCH_ID}/endpoints/${ENDPOINT_ID}" \
        2>/dev/null)
    PGHOST=$(echo "$ENDPOINT_DETAIL_JSON" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    # Databricks Lakebase API: status.hosts.host (preferred — direct read-write endpoint)
    # or status.hosts.read_write_pooled_host (PgBouncer pooler, also valid)
    hosts = d.get('status', {}).get('hosts', {})
    host = (hosts.get('host')
            or hosts.get('read_write_pooled_host')
            or d.get('spec', {}).get('host')
            or d.get('host')
            or d.get('endpoint_url')
            or d.get('pghost')
            or '')
    print(host)
except: print('')
" 2>/dev/null || echo "")

    if [ -n "$PGHOST" ]; then
        log_success "  Lakebase host: $PGHOST"
    else
        log_warn "  Could not determine PGHOST from endpoint API — Databricks may inject it via database resource"
        # Log the response for debugging
        log_info "  Endpoint detail response: $(echo "$ENDPOINT_DETAIL_JSON" | head -c 500)"
    fi

    # Inject database resource + ENDPOINT_NAME + PG* vars into the temp deploy yaml
    python3 - "$DEPLOY_YAML" "$LAKEBASE_PROJECT_ID" "$ENDPOINT_NAME" "$PGHOST" <<'PYEOF'
import sys, re

yaml_file, project_id, endpoint_name, pghost = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
with open(yaml_file) as f:
    content = f.read()


def set_env_var(content: str, name: str, value: str) -> str:
    """Set or add an env var in the yaml content."""
    uncommented_pattern = rf'^  - name: {name}\s*$'
    commented_pattern = rf'^\s*#\s*- name: {name}'
    if re.search(uncommented_pattern, content, re.MULTILINE):
        content = re.sub(
            rf'(  - name: {name}\s*\n    value: ).*',
            rf'\g<1>{value}',
            content,
        )
    elif re.search(commented_pattern, content, re.MULTILINE):
        # Match commented block: leading spaces + # - name: NAME ... newline ... leading spaces + # value: ...
        content = re.sub(
            rf'[ \t]*#[ \t]*- name: {name}[^\n]*\n[ \t]*#[ \t]*value:[^\n]*',
            f'  - name: {name}\n    value: {value}',
            content,
        )
    else:
        content = re.sub(
            r'^command:',
            f'  - name: {name}\n    value: {value}\n\ncommand:',
            content,
            flags=re.MULTILINE,
        )
    return content


content = set_env_var(content, 'ENDPOINT_NAME', endpoint_name)

if pghost:
    content = set_env_var(content, 'PGHOST', pghost)
    content = set_env_var(content, 'PGDATABASE', 'databricks_postgres')
    content = set_env_var(content, 'PGPORT', '5432')
    content = set_env_var(content, 'PGSSLMODE', 'require')

# ── database resource ────────────────────────────────────────────────────────
if not re.search(r'^\s+database:', content, re.MULTILINE):
    db_block = (
        f"  - name: lakebase\n"
        f"    description: Lakebase database for persistent app state\n"
        f"    database:\n"
        f"      instance_name: {project_id}\n"
        f"      permission: CAN_CONNECT\n"
    )
    content = re.sub(r'^resources:\n', f'resources:\n{db_block}', content, flags=re.MULTILINE)

with open(yaml_file, 'w') as f:
    f.write(content)
print(f'Updated {yaml_file}: database resource + ENDPOINT_NAME={endpoint_name}, PGHOST={pghost or "(from resource)"}')
PYEOF

    if [ $? -eq 0 ]; then
        log_success "  Deploy yaml updated with Lakebase config"
        APP_YAML_SOURCE="$DEPLOY_YAML"
    else
        log_warn "  Could not patch deploy yaml — continuing without Lakebase"
    fi
else
    log_warn "  Lakebase not ready — permissions will fall back to Delta table"
fi
log_info "----------------------------------------------"

# =============================================================================
# Permissions table setup — run as the deployer (admin), not the app SP.
# The app SP may lack CREATE TABLE/GRANT rights in a customer workspace.
# We use the deployer's DATABRICKS_TOKEN + the SQL warehouse to create the
# table and grant the SP the minimum needed access.
# =============================================================================

# Parse catalog and schema from the source yaml
PERM_CATALOG=$(python3 -c "
import re, sys
with open(sys.argv[1]) as f:
    content = f.read()
m = re.search(r'name:\s*COST_OBS_CATALOG\s*\n\s*value:\s*(\S+)', content)
print(m.group(1) if m else 'main')
" "$APP_YAML_SOURCE" 2>/dev/null || echo "main")

PERM_SCHEMA=$(python3 -c "
import re, sys
with open(sys.argv[1]) as f:
    content = f.read()
m = re.search(r'name:\s*COST_OBS_SCHEMA\s*\n\s*value:\s*(\S+)', content)
print(m.group(1) if m else 'cost_obs')
" "$APP_YAML_SOURCE" 2>/dev/null || echo "cost_obs")

log_info "Setting up permissions table (${PERM_CATALOG}.${PERM_SCHEMA}.app_user_permissions)..."

# Helper: execute a SQL statement via the Statement Execution API.
# SQL is passed via stdin to avoid shell escaping issues with backticks/quotes.
_exec_sql() {
    echo "$1" | python3 -c "
import sys, json, urllib.request
stmt         = sys.stdin.read().strip()
warehouse_id = sys.argv[1]
host         = sys.argv[2].rstrip('/')
token        = sys.argv[3]
payload = json.dumps({'warehouse_id': warehouse_id, 'statement': stmt, 'wait_timeout': '30s'})
req = urllib.request.Request(
    host + '/api/2.0/sql/statements',
    data=payload.encode(),
    headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'},
    method='POST',
)
try:
    with urllib.request.urlopen(req, timeout=35) as resp:
        d = json.loads(resp.read())
    state = d.get('status', {}).get('state', 'UNKNOWN')
    if state == 'SUCCEEDED':
        print('SUCCEEDED')
    else:
        err = d.get('status', {}).get('error', {}).get('message', state)
        print('FAILED: ' + str(err)[:300])
except Exception as e:
    print('FAILED: ' + str(e))
" "$WAREHOUSE_ID" "$DATABRICKS_HOST" "$DATABRICKS_TOKEN" 2>/dev/null
}

# Step A: create the table
CREATE_RESULT=$(_exec_sql "CREATE TABLE IF NOT EXISTS \`${PERM_CATALOG}\`.\`${PERM_SCHEMA}\`.\`app_user_permissions\` (role STRING NOT NULL, email STRING NOT NULL, updated_at TIMESTAMP) USING DELTA")
if echo "$CREATE_RESULT" | grep -q "^SUCCEEDED"; then
    log_success "  Permissions table ready"
else
    log_warn "  Permissions table create: $CREATE_RESULT"
fi

# Step B: SP grants deferred to post-deploy (after Step 6) so the SP exists on fresh deploys.
# The _grant_sp_access function is called below after the app is deployed and running.
log_info "  (SP access grants will run after deployment completes)"
log_info "----------------------------------------------"

# =============================================================================
# Step 1: Build Frontend
# =============================================================================
step_start 1 "Build frontend"

if [ -d "client" ]; then
    cd client
    if command -v bun &> /dev/null; then
        bun run build 2>&1 || { log_error "Frontend build failed"; exit 1; }
    elif command -v npm &> /dev/null; then
        npm run build 2>&1 || { log_error "Frontend build failed"; exit 1; }
    else
        log_error "Neither bun nor npm found for frontend build"
        exit 1
    fi
    cd ..

    # Copy to static
    rm -rf static
    cp -r client/dist static
    # Include app thumbnail for Databricks Apps listing
    if [ -f "thumbnail.png" ]; then
        cp thumbnail.png static/thumbnail.png
    fi
    log_success "Frontend built and copied to static/"
else
    log_warn "No client directory found, skipping frontend build"
fi

step_complete 1 "Build frontend"

# =============================================================================
# Step 2: Generate requirements.txt
# =============================================================================
step_start 2 "Generate requirements.txt"

if command -v uv &> /dev/null; then
    uv pip compile pyproject.toml -o requirements.txt 2>&1 && log_success "requirements.txt generated" \
        || log_warn "uv pip compile failed (network issue?), using existing requirements.txt"
else
    log_warn "uv not found, using existing requirements.txt"
fi

step_complete 2 "Generate requirements.txt"

# =============================================================================
# Step 3: Clean workspace (remove problematic files)
# =============================================================================
step_start 3 "Clean workspace"

# Remove .venv if it exists (causes permission errors)
databricks workspace delete "${WORKSPACE_PATH}/.venv" --recursive $CLI_ARGS 2>/dev/null || true
# Remove node_modules if it exists
databricks workspace delete "${WORKSPACE_PATH}/node_modules" --recursive $CLI_ARGS 2>/dev/null || true
# Remove __pycache__ directories
databricks workspace delete "${WORKSPACE_PATH}/.git" --recursive $CLI_ARGS 2>/dev/null || true

log_success "Workspace cleaned"

step_complete 3 "Clean workspace"

# =============================================================================
# Step 4: Sync files to workspace (selective - no .venv, node_modules, etc.)
# =============================================================================
step_start 4 "Sync files to workspace"

# Essential directories to sync
DIRS_TO_SYNC="server static"

for dir in $DIRS_TO_SYNC; do
    if [ -d "$dir" ]; then
        log_info "  Syncing $dir/..."
        databricks workspace import-dir "$dir" "${WORKSPACE_PATH}/$dir" --overwrite $CLI_ARGS 2>&1 || {
            log_warn "  Failed to sync $dir, trying delete first..."
            databricks workspace delete "${WORKSPACE_PATH}/$dir" --recursive $CLI_ARGS 2>/dev/null || true
            databricks workspace import-dir "$dir" "${WORKSPACE_PATH}/$dir" --overwrite $CLI_ARGS 2>&1 || {
                log_error "Failed to sync $dir"
                exit 1
            }
        }
    fi
done

# Essential files to sync
FILES_TO_SYNC="requirements.txt pyproject.toml"

for file in $FILES_TO_SYNC; do
    if [ -f "$file" ]; then
        log_info "  Syncing $file..."
        databricks workspace import "${WORKSPACE_PATH}/$file" --file "$file" --format AUTO --overwrite $CLI_ARGS 2>&1 || {
            log_warn "  Retry without overwrite..."
            databricks workspace delete "${WORKSPACE_PATH}/$file" $CLI_ARGS 2>/dev/null || true
            databricks workspace import "${WORKSPACE_PATH}/$file" --file "$file" --format AUTO $CLI_ARGS 2>&1 || {
                log_error "Failed to sync $file"
                exit 1
            }
        }
    fi
done

# Sync app.yaml — use DEPLOY_YAML (temp file with Lakebase env vars injected)
log_info "  Syncing app.yaml (from $DEPLOY_YAML)..."
databricks workspace import "${WORKSPACE_PATH}/app.yaml" --file "$DEPLOY_YAML" --format AUTO --overwrite $CLI_ARGS 2>&1 || {
    log_warn "  Retry without overwrite..."
    databricks workspace delete "${WORKSPACE_PATH}/app.yaml" $CLI_ARGS 2>/dev/null || true
    databricks workspace import "${WORKSPACE_PATH}/app.yaml" --file "$DEPLOY_YAML" --format AUTO $CLI_ARGS 2>&1 || {
        log_error "Failed to sync app.yaml"
        exit 1
    }
}

log_success "Files synced to workspace"

step_complete 4 "Sync files to workspace"

# =============================================================================
# Step 5: Deploy the app
# =============================================================================
step_start 5 "Trigger deployment"

# Check if there's a pending deployment
PENDING=$(databricks apps get "$APP_NAME" $CLI_ARGS 2>/dev/null | jq -r '.pending_deployment.status.state // "none"' || echo "none")
if [ "$PENDING" = "IN_PROGRESS" ]; then
    log_warn "Deployment already in progress. Waiting up to 5 minutes..."
    for i in {1..30}; do
        sleep 10
        PENDING=$(databricks apps get "$APP_NAME" $CLI_ARGS 2>/dev/null | jq -r '.pending_deployment.status.state // "none"' || echo "none")
        if [ "$PENDING" != "IN_PROGRESS" ]; then
            log_info "Previous deployment finished with state: $PENDING"
            break
        fi
        echo -n "."
    done
    echo ""
fi

# Trigger deployment
log_info "Triggering deployment..."
DEPLOY_RESULT=$(databricks apps deploy "$APP_NAME" --source-code-path "$WORKSPACE_PATH" --no-wait $CLI_ARGS 2>&1) || true
log_info "Deploy command output: $DEPLOY_RESULT"
if echo "$DEPLOY_RESULT" | grep -qi "error"; then
    log_error "Deployment trigger failed: $DEPLOY_RESULT"
    step_fail 5 "Trigger deployment" "$DEPLOY_RESULT"
    deploy_done "FAILED" ""
    exit 1
fi
log_success "Deployment triggered"

step_complete 5 "Trigger deployment"

# =============================================================================
# Step 6: Wait for deployment to complete
# =============================================================================
step_start 6 "Wait for deployment to complete"
MAX_WAIT=300  # 5 minutes
WAIT_INTERVAL=10
ELAPSED=0

while [ $ELAPSED -lt $MAX_WAIT ]; do
    APP_JSON=$(databricks apps get "$APP_NAME" $CLI_ARGS 2>/dev/null) || true
    STATUS=$(echo "$APP_JSON" | jq -r '.pending_deployment.status.state // .active_deployment.status.state // "UNKNOWN"')
    MESSAGE=$(echo "$APP_JSON" | jq -r '.pending_deployment.status.message // .active_deployment.status.message // "No message"')

    case "$STATUS" in
        "SUCCEEDED")
            APP_URL=$(echo "$APP_JSON" | jq -r '.url')
            log_success "Deployment SUCCEEDED!"
            log_success "App URL: $APP_URL"
            step_complete 6 "Wait for deployment to complete"
            break
            ;;
        "FAILED")
            log_error "Deployment FAILED!"
            log_error "Message: $MESSAGE"
            step_fail 6 "Wait for deployment to complete" "$MESSAGE"
            deploy_done "FAILED" ""
            exit 1
            ;;
        "IN_PROGRESS")
            log_info "[${ELAPSED}s] Deploying... $MESSAGE"
            ;;
        *)
            log_warn "[${ELAPSED}s] Unknown status: $STATUS"
            ;;
    esac

    sleep $WAIT_INTERVAL
    ELAPSED=$((ELAPSED + WAIT_INTERVAL))
done

if [ $ELAPSED -ge $MAX_WAIT ]; then
    log_error "Deployment timed out after ${MAX_WAIT}s"
    step_fail 6 "Wait for deployment to complete" "Timed out after ${MAX_WAIT}s"
    deploy_done "FAILED" ""
    exit 1
fi

# =============================================================================
# Post-deploy: Grant SP access to required resources
# Runs after deployment so the SP exists (critical for fresh / first-time deploys).
# =============================================================================
log_info "----------------------------------------------"
log_info "Post-deploy: Granting app SP access..."

SP_NUM_ID=$(databricks apps get "$APP_NAME" $CLI_ARGS 2>/dev/null | jq -r '.service_principal_id // empty' || echo "")
if [ -n "$SP_NUM_ID" ]; then
    SP_APP_ID=$(databricks service-principals get "$SP_NUM_ID" $CLI_ARGS 2>/dev/null \
        | jq -r '.applicationId // .userName // empty' || echo "")
    if [ -n "$SP_APP_ID" ]; then
        # Permissions table access
        GRANT_RESULT=$(_exec_sql "GRANT SELECT, MODIFY ON TABLE \`${PERM_CATALOG}\`.\`${PERM_SCHEMA}\`.\`app_user_permissions\` TO \`${SP_APP_ID}\`")
        if echo "$GRANT_RESULT" | grep -q "^SUCCEEDED"; then
            log_success "  Granted SP (${SP_APP_ID}) SELECT/MODIFY on permissions table"
        else
            log_warn "  SP permissions table GRANT: $GRANT_RESULT"
        fi
        # Add SP to workspace admins group (enables system.query.history access)
        ADMINS_GROUP_ID=$(databricks api get "/api/2.0/preview/scim/v2/Groups?filter=displayName+eq+admins" $CLI_ARGS 2>/dev/null \
            | python3 -c "import json,sys; gs=json.load(sys.stdin).get('Resources',[]); print(next((g['id'] for g in gs if g.get('displayName')=='admins'), ''))" 2>/dev/null || echo "")
        if [ -n "$ADMINS_GROUP_ID" ]; then
            ADMINS_PATCH=$(databricks api patch "/api/2.0/preview/scim/v2/Groups/${ADMINS_GROUP_ID}" $CLI_ARGS \
                --json "{\"schemas\":[\"urn:ietf:params:scim:api:messages:2.0:PatchOp\"],\"Operations\":[{\"op\":\"add\",\"path\":\"members\",\"value\":[{\"value\":\"${SP_NUM_ID}\"}]}]}" 2>/dev/null \
                | python3 -c "import json,sys; d=json.load(sys.stdin); print('ok' if 'id' in d else d.get('detail','?'))" 2>/dev/null || echo "?")
            if [ "$ADMINS_PATCH" = "ok" ]; then
                log_success "  Added SP to workspace admins group (enables system.query.history access)"
            else
                log_warn "  Admins group patch: $ADMINS_PATCH"
            fi
        else
            log_warn "  Could not find workspace admins group — skipping system table access grant"
        fi
        # Grant CAN_USE on the SQL warehouse
        WH_GRANT=$(curl -s -X PATCH \
            -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
            -H "Content-Type: application/json" \
            "${DATABRICKS_HOST}/api/2.0/permissions/sql/warehouses/${WAREHOUSE_ID}" \
            -d "{\"access_control_list\": [{\"service_principal_name\": \"${SP_APP_ID}\", \"permission_level\": \"CAN_USE\"}]}" 2>/dev/null \
            | python3 -c "import json,sys; d=json.load(sys.stdin); print('ok' if 'object_id' in d else d.get('message','?'))" 2>/dev/null || echo "?")
        if [ "$WH_GRANT" = "ok" ]; then
            log_success "  Granted SP (${SP_APP_ID}) CAN_USE on warehouse ${WAREHOUSE_ID}"
        else
            log_warn "  Warehouse CAN_USE grant: $WH_GRANT"
        fi
        # Grant CAN_MANAGE on Lakebase project so SP can generate DB credentials
        LB_GRANT=$(curl -s -X PATCH \
            -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
            -H "Content-Type: application/json" \
            "${DATABRICKS_HOST}/api/2.0/permissions/database-projects/${LAKEBASE_PROJECT_ID}" \
            -d "{\"access_control_list\": [{\"service_principal_name\": \"${SP_APP_ID}\", \"permission_level\": \"CAN_MANAGE\"}]}" 2>/dev/null \
            | python3 -c "import json,sys; d=json.load(sys.stdin); print('ok' if 'object_id' in d else d.get('message','?'))" 2>/dev/null || echo "?")
        if [ "$LB_GRANT" = "ok" ]; then
            log_success "  Granted SP (${SP_APP_ID}) CAN_MANAGE on Lakebase project ${LAKEBASE_PROJECT_ID}"
        else
            log_warn "  Lakebase CAN_MANAGE grant: $LB_GRANT"
        fi
    else
        log_warn "  Could not resolve SP application ID (numeric ID: ${SP_NUM_ID})"
    fi
else
    log_warn "  Could not get app SP numeric ID — skipping grants"
fi
log_info "----------------------------------------------"

# =============================================================================
# Step 7: Post-deployment verification
# =============================================================================
step_start 7 "Post-deployment verification"
log_info "  Waiting 30s for app to initialize..."
sleep 30

APP_URL=$(databricks apps get "$APP_NAME" $CLI_ARGS 2>/dev/null | jq -r '.url' || echo "")

# Helper to call app endpoints via dba_client.py if available, or curl directly
verify_endpoint() {
    local endpoint="$1"
    local description="$2"
    local url="${APP_URL}${endpoint}"

    if [ -f "dba_client.py" ]; then
        RESULT=$(DATABRICKS_APP_NAME="$APP_NAME" python3 dba_client.py "$endpoint" 2>&1)
    else
        RESULT=$(curl -s --max-time 30 "$url" 2>&1)
    fi

    if echo "$RESULT" | grep -qi "error\|timeout\|504\|502\|500"; then
        log_warn "  $description: ISSUE - $RESULT"
        return 1
    else
        # Show first 100 chars of response
        PREVIEW=$(echo "$RESULT" | head -c 100)
        log_success "  $description: OK - $PREVIEW"
        return 0
    fi
}

VERIFY_FAILURES=0

verify_endpoint "/api/health" "Health check" || VERIFY_FAILURES=$((VERIFY_FAILURES + 1))
verify_endpoint "/api/settings/config" "Settings config" || VERIFY_FAILURES=$((VERIFY_FAILURES + 1))
verify_endpoint "/api/permissions/check" "Permissions check" || VERIFY_FAILURES=$((VERIFY_FAILURES + 1))
verify_endpoint "/api/billing/summary" "Billing data" || VERIFY_FAILURES=$((VERIFY_FAILURES + 1))

step_complete 7 "Post-deployment verification"

# NOTE: Thumbnail must be set via the Databricks UI.
# The REST API PATCH wipes app resources (SQL warehouse permissions),
# and the app URL requires OAuth so Databricks can't fetch the image anyway.

log_info "=============================================="
if [ $VERIFY_FAILURES -eq 0 ]; then
    log_success "All verification checks passed!"
    log_success "App URL: $APP_URL"
    deploy_done "SUCCESS" "$APP_URL"
else
    log_warn "$VERIFY_FAILURES verification check(s) had issues."
    log_warn "App URL: $APP_URL"
    log_warn "The app may need more time to initialize. Try again in a minute."
    deploy_done "SUCCESS" "$APP_URL"
fi
log_info "=============================================="
