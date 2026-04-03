#!/bin/bash
# =============================================================================
# Sync internal repo to public mirror (smathews13/cost-obs-databricks)
# Usage: bash sync-mirror.sh
# =============================================================================

set -e

INTERNAL="/Users/sam.mathews/GIt/cost-obs-app"
MIRROR="/tmp/cost-obs-mirror"
MIRROR_REMOTE="https://github.com/smathews13/cost-obs-databricks.git"

echo "[INFO] Syncing to public mirror..."

# Build frontend so static/ assets are always up to date in the public repo
echo "[INFO] Building frontend..."
cd "$INTERNAL/client"
if command -v bun &> /dev/null; then
    bun run build 2>&1 | tail -3
elif command -v npm &> /dev/null; then
    npm run build 2>&1 | tail -3
fi
cd "$INTERNAL"
cp -r client/dist/* static/
cp client/public/databricks.svg static/ 2>/dev/null || true
cp client/public/dbfavicon.png static/ 2>/dev/null || true
cp thumbnail.png static/ 2>/dev/null || true
echo "[INFO] Frontend built"

# Copy app.yaml.example → app.yaml in the mirror so git deployments
# pre-populate env var fields automatically (no secrets — just structure)
cp "$INTERNAL/app.yaml.example" "$MIRROR/app.yaml"

# Ensure mirror clone exists
if [ ! -d "$MIRROR/.git" ]; then
    echo "[INFO] Cloning mirror repo..."
    gh auth switch --user smathews13 2>/dev/null || true
    git clone "$MIRROR_REMOTE" "$MIRROR"
    cd "$MIRROR"
    git config user.email "sync@databricks.com"
    git config user.name "Mirror Sync"
    gh auth switch --user sam-mathews_data 2>/dev/null || true
fi

# Sync files (excludes secrets and build artifacts)
rsync -a \
    --exclude='.git' \
    --exclude='node_modules' \
    --exclude='client/node_modules' \
    --exclude='.venv' \
    --exclude='venv' \
    --exclude='static' \
    --exclude='*.pid' \
    --exclude='*.deploy-bak' \
    --exclude='.databricks' \
    --exclude='.env' \
    --exclude='.env.*' \
    --exclude='app.yaml' \
    --exclude='app.*.yaml' \
    --exclude='.settings' \
    --exclude='.github' \
    --delete \
    "$INTERNAL/" "$MIRROR/"

cd "$MIRROR"

git add -A

if git diff --cached --quiet; then
    echo "[INFO] No changes to sync."
    exit 0
fi

COMMIT_MSG=$(git -C "$INTERNAL" log -1 --pretty=format:"%s")
git commit -m "sync: ${COMMIT_MSG}"

echo "[INFO] Pushing to public mirror..."
gh auth switch --user smathews13
git push origin main
gh auth switch --user sam-mathews_data

echo "[SUCCESS] Mirror synced: $MIRROR_REMOTE"
