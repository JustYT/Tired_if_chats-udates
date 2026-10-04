"""Small standalone client for the per-attempt Flow Messenger broker."""

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request

POLICY_ENV = "STEFANIA_FLOW_MESSENGER_RESTRICTED"
URL_ENV = "STEFANIA_FLOW_MESSENGER_URL"
CAPABILITY_ENV = "STEFANIA_FLOW_MESSENGER_TOKEN"
RESTRICTED_CREDENTIAL = "stefania-flow-use-broker"
MAX_BYTES = 16 * 1024 * 1024


def restricted() -> bool:
    return os.environ.get(POLICY_ENV) == "1" or any(
        os.environ.get(name) == RESTRICTED_CREDENTIAL
        for name in ("MESSENGER_TOKEN", "MESSENGER_ROBOT_TOKEN")
    )


def reject_direct_client(token: str) -> None:
    if restricted() or token == RESTRICTED_CREDENTIAL:
        raise PermissionError(
            "Flow: прямой клиент Мессенджера запрещен. Используй scripts/api.py; "
            "доступ проверяет посредник текущего запуска."
        )


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def invoke(kind: str, request: dict) -> dict:
    url = os.environ.get(URL_ENV, "")
    capability = os.environ.get(CAPABILITY_ENV, "")
    if not re.fullmatch(
        r"http://127\.0\.0\.1:[1-9][0-9]{0,4}/invoke", url
    ) or not re.fullmatch(r"[A-Za-z0-9_-]{40,100}", capability):
        raise PermissionError(
            "Flow: отсутствует ограниченный доступ к Мессенджеру; "
            "прямой fallback запрещен"
        )
    data = json.dumps({"kind": kind, "request": request}, ensure_ascii=False).encode(
        "utf-8"
    )
    if len(data) > 1024 * 1024:
        raise ValueError("Flow: запрос Мессенджера слишком большой")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer " + capability,
        },
        method="POST",
    )
    try:
        with opener.open(req, timeout=120) as response:
            raw = response.read(MAX_BYTES + 1)
    except urllib.error.HTTPError as exc:
        if exc.code == 400:
            raise ValueError(
                "Flow: неверные параметры Мессенджера; account, environment и "
                "expected_login задаются на верхнем уровне JSON, "
                "параметры операции — в args"
            ) from None
        if exc.code in (401, 403):
            raise PermissionError(
                "Flow: операция или адресат Мессенджера не разрешены"
            ) from None
        raise RuntimeError(
            "Flow: ошибка посредника Мессенджера; outcome=unknown, "
            "не повторяй отправку вслепую"
        ) from None
    except (OSError, urllib.error.URLError):
        raise RuntimeError(
            "Flow: посредник Мессенджера недоступен; outcome=unknown, "
            "прямой fallback запрещен"
        ) from None
    if len(raw) > MAX_BYTES:
        raise RuntimeError("Flow: ответ Мессенджера слишком большой; outcome=unknown")
    result = json.loads(raw)
    if not isinstance(result, dict) or "result" not in result:
        raise RuntimeError("Flow: некорректный ответ посредника; outcome=unknown")
    return result["result"]
