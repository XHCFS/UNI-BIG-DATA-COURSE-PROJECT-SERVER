#!/bin/bash
SERVER_IP="123.45.67.89"         # PLACEHOLDER, IP NOT HERE
SSH_USER="root"                 
REMOTE_REPO_DIR="/root/UNI-BIG-DATA-COURSE-PROJECT-SERVER"
SERVICE_NAME="ghcnd-api"

BRANCH="${1:-main}"  # default is main

echo "Deploying branch: $BRANCH to $SERVER_IP"

ssh "$SSH_USER@$SERVER_IP" "bash -s" <<ENDSSH
cd "$REMOTE_REPO_DIR" || { echo 'Repo directory not found!'; exit 1; }

echo "Fetching latest changes from branch '$BRANCH'..."
git fetch origin "$BRANCH"
git reset --hard "origin/$BRANCH"

echo "Restarting service..."
sudo systemctl restart "$SERVICE_NAME"

sudo systemctl status "$SERVICE_NAME" --no-pager
echo "Deployment of branch '$BRANCH' completed!"
ENDSSH

