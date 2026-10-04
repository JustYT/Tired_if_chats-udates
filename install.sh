#!/bin/sh
set -eu

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    echo 'Usage: ./install.sh CORPORATE_LOGIN [PORT]' >&2
    exit 2
fi
owner=$1
port=${2:-8765}
case "$owner" in
    ''|*[!a-z0-9._-]*) echo 'Invalid corporate login' >&2; exit 2 ;;
esac
case "$owner" in
    [a-z]*) ;;
    *) echo 'Login must start with a lowercase letter' >&2; exit 2 ;;
esac
if [ "${#owner}" -lt 2 ] || [ "${#owner}" -gt 49 ]; then
    echo 'Login length must be 2-49 characters' >&2
    exit 2
fi
case "$port" in
    ''|*[!0-9]*) echo 'Invalid port' >&2; exit 2 ;;
esac
if [ "$port" -lt 1024 ] || [ "$port" -gt 65535 ]; then
    echo 'Port must be 1024-65535' >&2
    exit 2
fi
if [ "$(uname -s)" != Linux ]; then
    echo 'This installer requires Linux and user systemd' >&2
    exit 1
fi
if ! command -v systemctl >/dev/null 2>&1; then
    echo 'systemctl is required' >&2
    exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
    echo 'python3 is required' >&2
    exit 1
fi
if ! python3 -c 'import sys; assert sys.version_info >= (3, 10)' >/dev/null 2>&1; then
    echo 'Python 3.10 or newer is required' >&2
    exit 1
fi
root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
install_dir=${CHAT_STUDIO_INSTALL_DIR:-"$HOME/.local/opt/chat-studio"}
data_dir=${CHAT_STUDIO_DATA_DIR:-"$HOME/.local/share/chat-studio"}
unit_dir="$HOME/.config/systemd/user"
if [ -e "$unit_dir/chat-studio.service" ]; then
    echo "Service already exists: $unit_dir/chat-studio.service" >&2
    exit 1
fi
if [ -e "$install_dir" ]; then
    echo "Installation directory already exists: $install_dir" >&2
    exit 1
fi
if [ -e "$data_dir/studio.sqlite3" ]; then
    echo "Data directory already contains a database: $data_dir" >&2
    exit 1
fi
if [ -e "$data_dir/updates.json" ] || [ -e "$data_dir/update_deploy_key" ]; then
    echo "Data directory already contains update credentials: $data_dir" >&2
    exit 1
fi
if [ -f "$root/updates.json" ] || [ -f "$root/updates/deploy_key" ]; then
    if [ ! -f "$root/updates.json" ] || [ ! -f "$root/updates/deploy_key" ]; then
        echo 'Incomplete update configuration in archive' >&2
        exit 1
    fi
    if ! command -v git >/dev/null 2>&1 || ! command -v systemd-run >/dev/null 2>&1; then
        echo 'Git and systemd-run are required for automatic updates' >&2
        exit 1
    fi
fi
case "$install_dir$data_dir" in
    *[[:space:]]*|*\;*|*\"*|*\\*) echo 'Unsupported path characters' >&2; exit 2 ;;
esac
"$root/check-splitty.sh"
umask 077
mkdir -p "$install_dir" "$data_dir" "$unit_dir"
cp -R "$root/app" "$install_dir/app"
cp "$root/update.sh" "$install_dir/update.sh"
chmod 700 "$install_dir/update.sh"
if [ -f "$root/updates.json" ] || [ -f "$root/updates/deploy_key" ]; then
    cp "$root/updates.json" "$data_dir/updates.json"
    cp "$root/updates/deploy_key" "$data_dir/update_deploy_key"
    chmod 600 "$data_dir/updates.json" "$data_dir/update_deploy_key"
    python3 - "$data_dir/updates.json" "$port" <<'PY'
import json
import pathlib
import sys
path = pathlib.Path(sys.argv[1])
config = json.loads(path.read_text())
config['port'] = int(sys.argv[2])
path.write_text(json.dumps(config))
PY
fi
mkdir -p "$install_dir/site"
python3 - "$root/wheels/websockets-15.0.1-py3-none-any.whl" "$install_dir/site" <<'PY'
import sys
import zipfile
with zipfile.ZipFile(sys.argv[1]) as wheel:
    wheel.extractall(sys.argv[2])
PY
cat > "$unit_dir/chat-studio.service" <<EOF
[Unit]
Description=Саммаризатор чатов (single corporate user)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$install_dir
Environment=PYTHONDONTWRITEBYTECODE=1
Environment=PYTHONPATH=$install_dir:$install_dir/site
Environment=CHAT_STUDIO_OWNER=$owner
Environment=CHAT_STUDIO_DATA=$data_dir
Environment=CHAT_STUDIO_PORT=$port
ExecStart=/usr/bin/python3 -m app.server
Restart=on-failure
RestartSec=5
NoNewPrivileges=true
UMask=0077

[Install]
WantedBy=default.target
EOF
systemctl --user daemon-reload
systemctl --user enable --now chat-studio.service
ready=0
attempt=0
while [ "$attempt" -lt 50 ]; do
    if python3 - "$port" <<'PY'
import sys
import urllib.request
try:
    with urllib.request.urlopen('http://127.0.0.1:' + sys.argv[1] + '/api/health', timeout=1) as response:
        assert response.status == 200
except Exception:
    raise SystemExit(1)
PY
    then
        ready=1
        break
    fi
    attempt=$((attempt + 1))
    sleep .2
done
if [ "$ready" -ne 1 ]; then
    echo 'Service did not become ready; inspect systemctl --user status chat-studio.service' >&2
    exit 1
fi
echo "Installed for $owner on VM loopback port $port"
echo 'Import your own credentials with ./connect-env.sh from your Stefania shell.'
