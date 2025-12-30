## Running the API

This document explains how to run the FastAPI + Spark service, both manually and as a `systemd` service.

### Manual Run (Development)

From the project root, with the Python environment activated:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

The API will be available at:

- Swagger UI: `http://<server>:8000/docs`
- ReDoc: `http://<server>:8000/redoc`

### Production Run via systemd

You can manage the API as a long-running service using `systemd`.

#### Unit File

Create `/etc/systemd/system/ghcnd-api.service`:

```ini
[Unit]
Description=GHCND FastAPI + Spark API
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/UNI-BIG-DATA-COURSE-PROJECT-SERVER

# Load pyenv environment and start Uvicorn
ExecStart=/bin/bash -c 'source /opt/pyenv/bin/activate && uvicorn app.main:app --host 0.0.0.0 --port 8000'

Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

Adjust:

- `User` – the account that should run the service
- `WorkingDirectory` – project root
- `source /opt/pyenv/bin/activate` – activation command for your Python environment

#### Enable and Start

```bash
sudo systemctl daemon-reload
sudo systemctl enable ghcnd-api
sudo systemctl start ghcnd-api
sudo systemctl status ghcnd-api
```

Tail logs:

```bash
journalctl -u ghcnd-api -f
```

### Deployment Helper Scripts

Two shell scripts are provided to streamline deployment:

- `deploy.sh`: Deploys a given Git branch to a remote server via SSH, updates the repository, and restarts the `ghcnd-api` service.
- `local_deploy.sh`: Performs the same steps on the local machine for a specified branch (default `main`).

Both scripts assume:

- The repository lives at `/root/UNI-BIG-DATA-COURSE-PROJECT-SERVER`.
- The `ghcnd-api` systemd service is configured and enabled.



