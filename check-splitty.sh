#!/bin/sh
set -eu

# The installer and its user systemd service run under the same Linux account.
# Check that account's actual network path, not only the daemon's presence.
if ! command -v splitty >/dev/null 2>&1; then
    echo 'Splitty check failed: splitty CLI is missing.' >&2
    exit 1
fi
if ! status=$(splitty status 2>/dev/null) ||
   ! printf '%s\n' "$status" | grep -Eq '^status:[[:space:]]*OK[[:space:]]*$'; then
    echo 'Splitty check failed: splitty status is not OK.' >&2
    exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
    echo 'Splitty check failed: curl is required for the official connectivity test.' >&2
    exit 1
fi
if ! response=$(curl --noproxy '*' --silent --show-error --fail --max-time 10 \
    https://proxy-check.sec.yandex.net 2>/dev/null) ||
   ! printf '%s\n' "$response" | grep -Fq 'Splitty is properly configured and ready to go!'; then
    echo 'Splitty check failed: proxy-check did not confirm access for this Linux user.' >&2
    echo 'Check the corporate network/VPN and Splitty enrolment, then retry.' >&2
    exit 1
fi
echo 'Splitty check passed for the installing Linux user.'
