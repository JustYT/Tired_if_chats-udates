#!/bin/sh
set -eu

if [ -z "${MESSENGER_TOKEN:-}" ]; then
    echo 'MESSENGER_TOKEN must be present in this shell' >&2
    exit 1
fi
if [ -n "${CHAT_STUDIO_BOT_TOKEN:-}" ] && [ -z "${CHAT_STUDIO_BOT_LOGIN:-}" ]; then
    echo 'CHAT_STUDIO_BOT_LOGIN is required with CHAT_STUDIO_BOT_TOKEN' >&2
    exit 1
fi
systemctl --user import-environment MESSENGER_TOKEN
if [ -n "${WIKI_TOKEN:-}" ]; then
    systemctl --user import-environment WIKI_TOKEN
fi
if [ -n "${ANTHROPIC_AUTH_TOKEN:-}" ]; then
    systemctl --user import-environment ANTHROPIC_AUTH_TOKEN
fi
if [ -n "${ANTHROPIC_BASE_URL:-}" ]; then
    systemctl --user import-environment ANTHROPIC_BASE_URL
fi
if [ -n "${CHAT_STUDIO_BOT_TOKEN:-}" ]; then
    systemctl --user import-environment CHAT_STUDIO_BOT_TOKEN CHAT_STUDIO_BOT_LOGIN
fi
systemctl --user restart chat-studio.service
echo 'Credentials imported into the user systemd runtime; service restarted.'
