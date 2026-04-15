#!/bin/bash

DRIVE_PATH="/mnt/g/My Drive/Personal/EGX_CoPilot"

mkdir -p "$DRIVE_PATH"

rsync -av --delete \
    --exclude='.git' \
    --exclude='__pycache__' \
    --exclude='.env' \
    --exclude='data/memory.json' \
    --exclude='node_modules' \
    --exclude='.venv' \
    --exclude='*.log' \
    ./ "$DRIVE_PATH"

echo "✅ Synced to Google Drive: $DRIVE_PATH"