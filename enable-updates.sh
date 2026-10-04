#!/bin/sh
set -eu

if [ "$#" -ne 0 ]; then
    echo 'Usage: ./enable-updates.sh' >&2
    exit 2
fi
root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
unit="$HOME/.config/systemd/user/chat-studio.service"
if [ ! -f "$unit" ] || [ ! -f "$root/updates.json" ] || [ ! -f "$root/updates/deploy_key" ]; then
    echo 'Run this from the complete bootstrap archive as the installed service user.' >&2
    exit 1
fi
install_dir=$(sed -n 's/^WorkingDirectory=//p' "$unit")
data_dir=$(sed -n 's/^Environment=CHAT_STUDIO_DATA=//p' "$unit")
port=$(sed -n 's/^Environment=CHAT_STUDIO_PORT=//p' "$unit")
case "$port" in
    ''|*[!0-9]*) echo 'Invalid service port' >&2; exit 1 ;;
esac
if [ "$port" -lt 1024 ] || [ "$port" -gt 65535 ] ||
   [ ! -f "$install_dir/update.sh" ] || [ ! -f "$data_dir/studio.sqlite3" ]; then
    echo 'Existing installation is incomplete; no update credentials were changed.' >&2
    exit 1
fi
python3 - "$data_dir/studio.sqlite3" <<'PY'
import sqlite3
import sys
try:
    with sqlite3.connect(f'file:{sys.argv[1]}?mode=ro', uri=True) as db:
        active = db.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('queued','collecting','summarizing','sending')").fetchone()[0]
except sqlite3.Error:
    raise SystemExit('Cannot verify active jobs; no update credentials were changed.')
if active:
    raise SystemExit('Wait for the current summarization to finish before enabling updates.')
PY
if [ -e "$data_dir/updates.json" ] && [ -e "$data_dir/update_deploy_key" ]; then
    echo 'Update credentials are already present; checking access.'
elif [ -e "$data_dir/updates.json" ] || [ -e "$data_dir/update_deploy_key" ]; then
    echo 'Incomplete existing update configuration; no files were overwritten.' >&2
    exit 1
else
    umask 077
    config_tmp=$(mktemp "$data_dir/.updates.json.XXXXXX")
    key_tmp=$(mktemp "$data_dir/.update_deploy_key.XXXXXX")
    trap 'rm -f "$config_tmp" "$key_tmp"' EXIT HUP INT TERM
    cp "$root/updates.json" "$config_tmp"
    cp "$root/updates/deploy_key" "$key_tmp"
    chmod 600 "$config_tmp" "$key_tmp"
    python3 - "$config_tmp" "$port" <<'PY'
import json
import pathlib
import sys
path = pathlib.Path(sys.argv[1])
config = json.loads(path.read_text())
config['port'] = int(sys.argv[2])
path.write_text(json.dumps(config))
PY
    mv "$key_tmp" "$data_dir/update_deploy_key"
    if ! mv "$config_tmp" "$data_dir/updates.json"; then
        rm -f "$data_dir/update_deploy_key"
        exit 1
    fi
    trap - EXIT HUP INT TERM
fi
CHAT_STUDIO_DATA="$data_dir"
CHAT_STUDIO_INSTALL_DIR="$install_dir"
export CHAT_STUDIO_DATA CHAT_STUDIO_INSTALL_DIR
if ! "$install_dir/update.sh" check >/dev/null; then
    echo 'Credentials were installed, but the update repository is unreachable; check SSH/GitHub access.' >&2
    exit 1
fi
systemctl --user restart chat-studio.service
python3 - "$port" <<'PY'
import json
import sys
import time
from urllib.request import urlopen
url = f'http://127.0.0.1:{sys.argv[1]}/api/bootstrap'
for _ in range(40):
    try:
        with urlopen(url, timeout=2) as response:
            if json.load(response)['updates']['configured']:
                print('Automatic updates are configured and visible in the dashboard.')
                break
    except (OSError, ValueError, KeyError):
        pass
    time.sleep(.25)
else:
    raise SystemExit('Service did not confirm automatic updates; inspect its status.')
PY
