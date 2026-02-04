#!/usr/bin/env bash
set -euo pipefail

# Safe project snapshot (NO secrets)
# Output: snapshots/<timestamp>/

ts="$(date -u +%Y-%m-%dT%H-%M-%SZ)"
out="snapshots/${ts}"
mkdir -p "$out"

echo "==> Snapshot dir: $out"

# Basic system info (useful when restoring)
{
  echo "UTC_TIMESTAMP=${ts}"
  echo "HOSTNAME=$(hostname)"
  echo "USER=$(whoami)"
  echo "KERNEL=$(uname -a)"
  echo "DATE_LOCAL=$(date)"
} > "$out/system.txt" 2>/dev/null || true

# Docker info (if installed)
command -v docker >/dev/null 2>&1 && {
  docker version > "$out/docker-version.txt" 2>/dev/null || true
  docker compose version > "$out/docker-compose-version.txt" 2>/dev/null || true
  docker ps -a > "$out/docker-ps-a.txt" 2>/dev/null || true
  docker images > "$out/docker-images.txt" 2>/dev/null || true
} || true

# Git info
git rev-parse HEAD > "$out/git-head.txt" 2>/dev/null || true
git status --porcelain > "$out/git-status-porcelain.txt" 2>/dev/null || true
git remote -v > "$out/git-remotes.txt" 2>/dev/null || true

# Copy key repo files (safe)
# NOTE: do NOT copy .env or backups/
safe_paths=(
  "docker-compose.yml"
  "compose.yml"
  "Dockerfile"
  ".gitignore"
  ".env.example"
  "README.md"
  "docs"
  "scripts"
)

for p in "${safe_paths[@]}"; do
  if [ -e "$p" ]; then
    cp -a "$p" "$out/" 2>/dev/null || true
  fi
done

# Also capture a tree view (helps to restore structure)
( command -v tree >/dev/null 2>&1 && tree -a -I ".git|.venv|backups|snapshots" ) > "$out/tree.txt" 2>/dev/null || true
( find . -maxdepth 3 -print | sed 's|^\./||' | grep -Ev '^(\.git|\.venv|backups|snapshots)($|/)' ) > "$out/find.txt" 2>/dev/null || true

# Create an archive for easy download/copy
tar -czf "${out}.tar.gz" -C snapshots "${ts}"

echo "==> Done:"
echo " - ${out}/"
echo " - ${out}.tar.gz"
