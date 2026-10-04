#!/usr/bin/env python3
"""Stable JSON facade for the user Messenger API.

The module is importable through :func:`invoke`.  Its executable adapter reads
one JSON object from stdin and writes one JSON object to stdout, so agents do
not need to generate task-specific Python files.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from copy import deepcopy
from typing import Any, Callable, Dict, Optional

try:
    from .client import MessengerUserClient
    from . import flow_transport
except ImportError:  # executable adapter: scripts/api.py
    from client import MessengerUserClient
    import flow_transport


DIRECT_OPERATIONS = frozenset(
    {
        "identity",
        "diagnose",
        "list_chats",
        "list_folders",
        "list_private_chats",
        "find_user",
        "resolve_login",
        "resolve_logins",
        "read_history",
        "mark_chat_read",
        "read_new",
        "last_from",
        "message_info",
        "threads_list",
        "read_thread",
        "thread_dump",
        "reactions",
        "unread_count",
        "edit_history",
        "media_messages",
        "chat_members",
        "resolve_invite",
        "send_message",
        "edit_message",
        "delete_message",
        "pin_message",
        "unpin_message",
        "create_poll",
        "create_group",
        "manage_members",
        "wait_reply",
        "download_media",
        "press_button",
        "send_image_with_caption",
    }
)
HIGH_LEVEL_OPERATIONS = frozenset({"resolve_login_full", "send_to_login"})
LOCAL_OPERATIONS = frozenset({"capabilities"})
OPERATIONS = DIRECT_OPERATIONS | HIGH_LEVEL_OPERATIONS | LOCAL_OPERATIONS
# Мутации, которым фасад разрешает auto_confirm: подтвердить свежий preview
# тем же fingerprint без человеко-итерации (автономные сценарии, SHARINGML-835).
# download_media сознательно вне списка: побочный эффект — локальный файл.
MUTATING_OPERATIONS = frozenset(
    {
        "send_message",
        "send_image_with_caption",
        "edit_message",
        "delete_message",
        "pin_message",
        "unpin_message",
        "create_poll",
        "create_group",
        "manage_members",
        "mark_chat_read",
        "press_button",
        "send_to_login",
    }
)
_LOGIN_RE = re.compile(r"^[a-z][a-z0-9._-]{1,48}$")
_TOKEN_PATTERN = re.compile(r"(?:y[01]_|t[01]_|AQAD-)[A-Za-z0-9._~+/=-]+")


class MessengerUserApiError(RuntimeError):
    """A safe, caller-facing facade error."""


def _safe_error(error: BaseException) -> str:
    text = str(error)
    token = os.environ.get("MESSENGER_TOKEN", "")
    if token:
        text = text.replace(token, "***")
    text = _TOKEN_PATTERN.sub("***", text)
    return text[:500]


def _request(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("request должен быть JSON-объектом")
    allowed = {"operation", "account", "environment", "expected_login", "args"}
    extra = set(value) - allowed
    if extra:
        raise ValueError("неизвестные поля request: %s" % ", ".join(sorted(extra)))
    operation = value.get("operation")
    if operation not in OPERATIONS:
        raise ValueError("operation не входит в allowlist")
    args = value.get("args", {})
    if not isinstance(args, dict):
        raise ValueError("args должен быть JSON-объектом")
    return {
        "operation": operation,
        "account": value.get("account", "work"),
        "environment": value.get("environment", "prod"),
        "expected_login": value.get("expected_login"),
        "args": dict(args),
    }


def _normalize_login(value: Any) -> str:
    login = str(value or "").strip().lower().lstrip("@")
    if not _LOGIN_RE.fullmatch(login):
        raise ValueError("некорректный staff-логин")
    return login


def capabilities() -> Dict[str, Any]:
    """Return the stable facade inventory without reading a token or using network."""

    return {
        "facade_version": 1,
        "subject": "user",
        "token_env": "MESSENGER_TOKEN",
        "operations": sorted(OPERATIONS),
        "high_level_operations": sorted(HIGH_LEVEL_OPERATIONS),
        "protocol": {
            "input": "one_json_object_on_stdin",
            "output": "one_json_object_on_stdout",
            "mutation_confirmation": "confirmation_request",
        },
    }


def resolve_login_full(
    client: MessengerUserClient,
    login: str,
    *,
    max_pages: int = 100,
) -> Optional[Dict[str, Any]]:
    """Resolve a login across all paginated private chats, then People search."""

    normalized = _normalize_login(login)
    if not isinstance(max_pages, int) or not 1 <= max_pages <= 100:
        raise ValueError("max_pages должен быть в диапазоне 1..100")
    first_page = client.list_private_chats(limit=100, cursor=None)
    matches = [
        user
        for user in first_page.get("private_chats", [])
        if user.get("login") == normalized and user.get("to_guid")
    ]
    if not matches:
        search = client.find_user(normalized, limit=20)
        matches = [
            user
            for user in search.get("users", [])
            if user.get("login") == normalized and user.get("guid")
        ]
    cursor = first_page.get("next_cursor")
    seen_cursors = set()
    pages_left = max_pages - 1
    while not matches and cursor and pages_left > 0 and cursor not in seen_cursors:
        seen_cursors.add(cursor)
        page = client.list_private_chats(limit=100, cursor=cursor)
        matches = [
            user
            for user in page.get("private_chats", [])
            if user.get("login") == normalized and user.get("to_guid")
        ]
        cursor = page.get("next_cursor")
        pages_left -= 1
    unique = {}
    for user in matches:
        guid = user.get("to_guid") or user.get("guid")
        if guid:
            unique[guid] = {
                "guid": guid,
                "login": normalized,
                "display_name": user.get("display_name"),
                "chat_id": user.get("chat_id"),
            }
    if len(unique) > 1:
        raise MessengerUserApiError("логин резолвится неоднозначно")
    return next(iter(unique.values()), None)


def _send_to_login(
    client: MessengerUserClient,
    args: Dict[str, Any],
    *,
    account: str,
    environment: str,
    expected_login: Optional[str],
    auto_confirm: bool = False,
) -> Dict[str, Any]:
    allowed = {
        "login",
        "text",
        "mention_guids",
        "payload_id",
        "confirm_fingerprint",
        "recipient_guid",
        "verify_limit",
        "verify_attempts",
    }
    extra = set(args) - allowed
    if extra:
        raise ValueError(
            "неизвестные аргументы send_to_login: %s" % ", ".join(sorted(extra))
        )
    login = _normalize_login(args.get("login"))
    recipient_guid = args.get("recipient_guid")
    if recipient_guid is not None:
        recipient = {
            "login": login,
            "guid": recipient_guid,
            "display_name": None,
        }
    else:
        recipient = resolve_login_full(client, login)
        if recipient is None:
            raise MessengerUserApiError("точный логин не найден в Мессенджере")
    result = client.send_message(
        args.get("text"),
        to_guid=recipient["guid"],
        mention_guids=args.get("mention_guids"),
        payload_id=args.get("payload_id"),
        confirm_fingerprint=args.get("confirm_fingerprint"),
    )
    safe_recipient = {
        "login": recipient["login"],
        "guid": recipient["guid"],
        "display_name": recipient.get("display_name"),
    }
    if result.get("status") == "preview_not_applied":
        confirm_args = {
            "login": login,
            "recipient_guid": recipient["guid"],
            "text": args.get("text"),
            "payload_id": result["request"]["payload_id"],
            "confirm_fingerprint": result["fingerprint"],
        }
        if args.get("mention_guids") is not None:
            confirm_args["mention_guids"] = args["mention_guids"]
        if args.get("verify_limit") is not None:
            confirm_args["verify_limit"] = args["verify_limit"]
        if args.get("verify_attempts") is not None:
            confirm_args["verify_attempts"] = args["verify_attempts"]
        confirmation_request = {
            "operation": "send_to_login",
            "account": account,
            "environment": environment,
            "args": confirm_args,
        }
        if expected_login is not None:
            confirmation_request["expected_login"] = expected_login
        if auto_confirm:
            confirmed = invoke(confirmation_request, client=client)
            return {**confirmed, "auto_confirmed": True}
        return {
            **result,
            "recipient": safe_recipient,
            "confirmation_request": confirmation_request,
        }

    verification = {
        "checked": False,
        "found": False,
        "timestamp": None,
        "matched_by": None,
        "attempts": 0,
    }
    if result.get("outcome") == "confirmed":
        verify_limit = args.get("verify_limit", 50)
        if not isinstance(verify_limit, int) or not 1 <= verify_limit <= 500:
            raise ValueError("verify_limit должен быть в диапазоне 1..500")
        verify_attempts = args.get("verify_attempts", 3)
        if not isinstance(verify_attempts, int) or not 1 <= verify_attempts <= 5:
            raise ValueError("verify_attempts должен быть в диапазоне 1..5")
        actor = client.identity()
        message_info = result.get("message_info") or {}
        expected_timestamp = (
            message_info.get("Timestamp")
            or message_info.get("TimestampMcs")
            or message_info.get("timestamp")
        )
        expected_payload_id = result.get("payload_id")
        matches = []
        matched_by = None
        attempts = 0
        for attempts in range(1, verify_attempts + 1):
            history = client.read_history(to_guid=recipient["guid"], limit=verify_limit)
            messages = history.get("messages", [])
            if expected_timestamp is not None:
                matches = [
                    message
                    for message in messages
                    if message.get("author_guid") == actor["guid"]
                    and str(message.get("ts")) == str(expected_timestamp)
                ]
                matched_by = "timestamp"
            if not matches and expected_payload_id:
                matches = [
                    message
                    for message in messages
                    if message.get("author_guid") == actor["guid"]
                    and message.get("payload_id") == expected_payload_id
                ]
                matched_by = "payload_id"
            if matches:
                break
            if attempts < verify_attempts:
                time.sleep(0.25 * attempts)
        verification = {
            "checked": True,
            "found": bool(matches),
            "timestamp": matches[-1].get("ts") if matches else None,
            "matched_by": matched_by if matches else None,
            "attempts": attempts,
        }
    return {**result, "recipient": safe_recipient, "verification": verification}


def invoke(
    request: Dict[str, Any],
    *,
    client: Optional[MessengerUserClient] = None,
    client_factory: Callable[..., MessengerUserClient] = MessengerUserClient.from_env,
) -> Dict[str, Any]:
    """Invoke one allowlisted operation and return a JSON-serializable result."""

    parsed = _request(request)
    if parsed["operation"] == "capabilities":
        if parsed["args"]:
            raise ValueError("capabilities не принимает args")
        return capabilities()
    if client is None and flow_transport.restricted():
        return flow_transport.invoke("user", request)
    if client is None:
        client = client_factory(
            account=parsed["account"],
            environment=parsed["environment"],
            expected_login=parsed["expected_login"],
        )
    operation = parsed["operation"]
    args = parsed["args"]
    auto_confirm = bool(args.pop("auto_confirm", False))
    if auto_confirm:
        if operation not in MUTATING_OPERATIONS:
            raise ValueError("auto_confirm применим только к мутациям")
        if args.get("confirm_fingerprint"):
            raise ValueError(
                "auto_confirm и confirm_fingerprint взаимно исключают друг друга"
            )
    if operation == "resolve_login_full":
        extra = set(args) - {"login", "max_pages"}
        if extra:
            raise ValueError("неизвестные аргументы resolve_login_full")
        return {
            "operation": operation,
            "recipient": resolve_login_full(
                client,
                args.get("login"),
                max_pages=args.get("max_pages", 100),
            ),
        }
    if operation == "send_to_login":
        return _send_to_login(
            client,
            args,
            account=parsed["account"],
            environment=parsed["environment"],
            expected_login=parsed["expected_login"],
            auto_confirm=auto_confirm,
        )
    method = getattr(client, operation)
    result = method(**args)
    if isinstance(result, dict) and result.get("status") == "preview_not_applied":
        confirm_args = deepcopy(args)
        preview_request = result.get("request") or {}
        payload_id = preview_request.get("payload_id") or preview_request.get(
            "PayloadId"
        )
        action_id = preview_request.get("action_id") or preview_request.get("ActionId")
        if payload_id is not None and operation in {
            "send_message",
            "send_image_with_caption",
            "create_poll",
        }:
            confirm_args["payload_id"] = payload_id
        if action_id is not None:
            confirm_args["action_id"] = action_id
        if operation == "mark_chat_read":
            for field in ("target_timestamp", "target_seq_no"):
                confirm_args[field] = preview_request[field]
        confirm_args["confirm_fingerprint"] = result["fingerprint"]
        confirmation_request = {
            "operation": operation,
            "account": parsed["account"],
            "environment": parsed["environment"],
            "args": confirm_args,
        }
        if parsed["expected_login"] is not None:
            confirmation_request["expected_login"] = parsed["expected_login"]
        if auto_confirm:
            confirmed = invoke(confirmation_request, client=client)
            return {**confirmed, "auto_confirmed": True}
        result = {**result, "confirmation_request": confirmation_request}
    return result


def main() -> int:
    try:
        raw = sys.stdin.read()
        if not raw.strip():
            raise ValueError("передай один JSON request через stdin")
        request = json.loads(raw)
        result = invoke(request)
        payload = {"ok": True, "result": result}
        code = 0
    except Exception as error:  # safe CLI boundary
        payload = {
            "ok": False,
            "error": {
                "type": type(error).__name__,
                "message": _safe_error(error),
            },
        }
        code = 1
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
