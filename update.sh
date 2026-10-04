#!/bin/sh
set -eu
install_dir=${CHAT_STUDIO_INSTALL_DIR:-"$HOME/.local/opt/chat-studio"}
export PYTHONPATH="$install_dir:$install_dir/site"
exec /usr/bin/python3 -m app.update "$@"
