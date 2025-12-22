#!/bin/bash

REPO_DIR="/root/UNI-BIG-DATA-COURSE-PROJECT-SERVER"
SERVICE_NAME="ghcnd-api"
BRANCH="${1:-main}"  # default is main

echo "Deploying branch: $BRANCH locally"

# Navigate to repo
cd "$REPO_DIR" || { echo 'Repo directory not found!'; exit 1; }

# Fetch and reset to latest
echo "Fetching latest changes from branch '$BRANCH'..."
git fetch origin "$BRANCH"
git reset --hard "origin/$BRANCH"

# Restart service
echo "Restarting service..."
sudo systemctl restart "$SERVICE_NAME"

# Show status
sudo systemctl status "$SERVICE_NAME" --no-pager

echo "Deployment of branch '$BRANCH' completed!"
