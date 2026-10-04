#!/usr/bin/env python3
"""Import the caller's own credentials into a matching remote user service."""
import argparse
import json
import os
import re
import shlex
import subprocess

NAMES = ('MESSENGER_TOKEN', 'WIKI_TOKEN', 'ANTHROPIC_AUTH_TOKEN',
         'ANTHROPIC_BASE_URL', 'CHAT_STUDIO_BOT_TOKEN', 'CHAT_STUDIO_BOT_LOGIN')
REMOTE = r'''
import json, os, pathlib, re, subprocess, sys
payload = json.load(sys.stdin)
allowed = {'owner', 'tokens'}
names = {'MESSENGER_TOKEN', 'WIKI_TOKEN', 'ANTHROPIC_AUTH_TOKEN',
         'ANTHROPIC_BASE_URL', 'CHAT_STUDIO_BOT_TOKEN', 'CHAT_STUDIO_BOT_LOGIN'}
if set(payload) != allowed or set(payload['tokens']) - names:
    raise SystemExit(2)
unit = pathlib.Path.home() / '.config/systemd/user/chat-studio.service'
try:
    text = unit.read_text()
except OSError:
    raise SystemExit(3)
match = re.search(r'^Environment=CHAT_STUDIO_OWNER=([a-z][a-z0-9._-]{1,48})$', text, re.M)
if not match or match.group(1) != payload['owner']:
    raise SystemExit(4)
os.environ.update(payload['tokens'])
if subprocess.run(['systemctl', '--user', 'import-environment', *payload['tokens']],
                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
    raise SystemExit(5)
if subprocess.run(['systemctl', '--user', 'restart', 'chat-studio.service'],
                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
    raise SystemExit(6)
print('connected')
'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', required=True)
    parser.add_argument('--ssh-user', required=True)
    parser.add_argument('--owner', required=True)
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z0-9.-]+', args.host):
        parser.error('Invalid host')
    for value in (args.ssh_user, args.owner):
        if not re.fullmatch(r'[a-z][a-z0-9._-]{1,48}', value):
            parser.error('Invalid login')
    tokens = {name: os.environ[name] for name in NAMES if os.environ.get(name)}
    if not tokens.get('MESSENGER_TOKEN'):
        parser.error('MESSENGER_TOKEN is required in this shell')
    if tokens.get('CHAT_STUDIO_BOT_TOKEN') and not tokens.get('CHAT_STUDIO_BOT_LOGIN'):
        parser.error('CHAT_STUDIO_BOT_LOGIN is required with a bot token')
    command = ['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
               '-o', 'ConnectTimeout=12', f'{args.ssh_user}@{args.host}',
               'python3 -c ' + shlex.quote(REMOTE)]
    try:
        result = subprocess.run(command, input=json.dumps({'owner': args.owner, 'tokens': tokens}),
                                text=True, capture_output=True, timeout=45)
    except subprocess.TimeoutExpired:
        raise SystemExit('Connection timed out') from None
    if result.returncode or result.stdout.strip() != 'connected':
        raise SystemExit(f'Connection failed (code {result.returncode}); check SSH and service owner')
    print('Credentials imported into the remote user systemd runtime.')


if __name__ == '__main__':
    main()
