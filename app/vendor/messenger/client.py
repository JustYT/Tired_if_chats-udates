"""API-first client for user-authorized Yandex Messenger operations.

The module intentionally has no CLI entry point. Import ``MessengerUserClient``
from a short task script and call the typed methods directly.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import inspect
import json
import os
import re
import ssl
import struct
import time
import zlib
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

try:
    import fcntl
except ImportError:  # Windows: story lock fails closed
    fcntl = None  # type: ignore[assignment]

try:
    from .flow_transport import reject_direct_client
except ImportError:
    from flow_transport import reject_direct_client

OAUTH_CLIENT_ID = "bef24ec2889b481bb39af0b430099845"
OAUTH_URL = (
    "https://oauth.yandex.ru/authorize?response_type=token&client_id=" + OAUTH_CLIENT_ID
)
WS_URL = "wss://push.yandex.ru/v2/subscribe/websocket"
PROFILES = {
    "prod": {
        "registry": "https://messenger.360.yandex.ru/api/",
        "service": "messenger-prod:version5",
    },
    "alpha": {
        "registry": "https://api.messenger.alpha.yandex.ru/api/",
        "service": "messenger:version5",
    },
}
ACCOUNT_TOKENS = {
    "work": "MESSENGER_TOKEN",
    "personal": "MESSENGER_TOKEN",
}
REGISTRY_READ_METHODS = frozenset(
    {
        "request_user",
        "get_chats_info",
        "get_users_data",
        "get_suggest",
        "search",
        "get_chat_members",
        "get_media_messages",
        "get_buckets",
    }
)
REGISTRY_WRITE_METHODS = frozenset(
    {"create_private_chat", "create_chat", "update_members"}
)
FANOUT_READ_PATHS = frozenset(
    {
        "history",
        "message_info",
        "list_reactions",
        "unread_count",
        "edit_history",
    }
)
FANOUT_WRITE_PATHS = frozenset({"push"})
MESSENGER_OPERATIONS = frozenset(
    {
        "ListChats",
        "ListPrivateChats",
        "FindUser",
        "ResolveLogin",
        "ResolveLogins",
        "ReadHistory",
        "MarkChatRead",
        "SendMessage",
        "CreateGroup",
        "ManageMembers",
        "Diagnose",
        "ThreadsList",
        "ReadThread",
        "MessageInfo",
        "MediaMessages",
        "Reactions",
        "UnreadCount",
        "EditHistory",
        "ChatMembers",
        "ReadNew",
        "LastFrom",
        "ThreadDump",
        "EditMessage",
        "DeleteMessage",
        "PinMessage",
        "UnpinMessage",
        "CreatePoll",
        "WaitReply",
        "DownloadMedia",
        "ResolveInvite",
        "PressButton",
        "ListFolders",
        "SendImageWithCaption",
    }
)

_MAX_REGISTRY_BYTES = 16 * 1024 * 1024
_MAX_WS_BYTES = 16 * 1024 * 1024
_MAX_TEXT = 6000
# Web client composer limit (messageLengthLimit), not the text send_message cap.
_MAX_CAPTION = 4096
# Web client maxFileSize for media_upload.
_MAX_IMAGE_BYTES = 52428800
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_STORY_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_IMAGE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,80}\.png$")
_MAX_DOWNLOAD_BYTES = 100 * 1024 * 1024
_MAX_REDIRECTS = 3
_MEDIA_HOST = "files.messenger.yandex.ru"
_DOWNLOAD_SUFFIXES = (".yandex.ru", ".yandex.net")
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.I,
)
_LOGIN_RE = re.compile(r"^[a-z][a-z0-9._-]{1,48}$")
_FILE_ID_RE = re.compile(r"^[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*$")
_INVITE_RE = re.compile(
    r"(?:^|/join/)([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12})(?:$|[/?#&\s])",
    re.I,
)
# Пауза вежливости между People search вызовами. Серверного rate-limit на
# directory-поиск нет (проверено 2026-09-20, SHARINGML-835); пауза защищает
# от шторма вызовов, не блокируя пакетный резолв логинов.
_PEOPLE_SEARCH_INTERVAL = 1.0


class MessengerError(RuntimeError):
    """Base error whose message never contains response bodies or tokens."""


class AuthenticationError(MessengerError):
    """The OAuth token was rejected or belongs to the wrong account class."""


class PermissionDenied(MessengerError):
    """The identity is valid, but this method or resource is not permitted."""


class ConfirmationError(MessengerError):
    """A mutating call lacks the exact fingerprint from its preview."""


class OperationUncertain(MessengerError):
    """A mutation may have reached the server and must be reconciled."""


@dataclass(frozen=True)
class MutationPreview:
    operation: str
    account: str
    environment: str
    request: Dict[str, Any]
    fingerprint: str
    status: str = "preview_not_applied"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _stable_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _push_outcome(response: Dict[str, Any]) -> tuple[str, Any]:
    """Classify a Fanout mutation without treating error MessageInfo as success."""
    commit_status = response.get("CommitStatus")
    status = commit_status if commit_status is not None else response.get("Status")
    if response.get("Details"):
        return "rejected", status
    if status in {"FullyCommitted", "Duplicate", 1, 2}:
        return "confirmed", status
    if response.get("MessageInfo") and status in {None, 0}:
        return "confirmed", status
    if response.get("MessageInfo"):
        return "unknown", status
    if status is not None:
        return "rejected", status
    return "unknown", status


def _push_rejection_reason(response: Dict[str, Any]) -> Optional[str]:
    """Expose a stable category without leaking the server's free-form body."""
    if not response.get("Details"):
        return None
    details = str(response["Details"]).lower()
    if "no right" in details or "permission" in details or "forbidden" in details:
        return "permission_denied"
    return "server_rejected"


def _ssl_context() -> ssl.SSLContext:
    candidates = (
        os.environ.get("REQUESTS_CA_BUNDLE"),
        os.environ.get("SSL_CERT_FILE"),
        Path.home()
        / "AppData"
        / "Local"
        / "Stefania"
        / "certs"
        / "YandexInternalCA.pem",
        Path("/etc/ssl/certs/YandexInternalCA.pem"),
    )
    cafile = next(
        (str(path) for path in candidates if path and Path(path).is_file()),
        None,
    )
    return ssl.create_default_context(cafile=cafile)


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError("%s должен быть положительным целым" % name)
    try:
        result = int(value)
    except (TypeError, ValueError) as error:
        raise ValueError("%s должен быть положительным целым" % name) from error
    if result <= 0:
        raise ValueError("%s должен быть положительным целым" % name)
    return result


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    result = _positive_int(value, name)
    if not minimum <= result <= maximum:
        raise ValueError("%s должен быть в диапазоне %d..%d" % (name, minimum, maximum))
    return result


def _chat_id(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw or len(raw) > 512 or any(ord(char) < 32 for char in raw):
        raise ValueError("некорректный chat_id")
    parsed = urllib.parse.urlparse(raw)
    if parsed.scheme or parsed.netloc:
        if (
            parsed.scheme != "https"
            or parsed.hostname not in {"messenger.360.yandex.ru", "yandex.ru"}
            or parsed.username is not None
            or parsed.password is not None
            or parsed.port not in (None, 443)
        ):
            raise ValueError("разрешена только HTTPS-ссылка web-клиента Мессенджера")
        decoded = urllib.parse.unquote(raw)
        if "/join/" in decoded.lower():
            raise ValueError("invite-ссылка не является chat_id")
        for marker in ("#/chats/", "/chats/"):
            if marker in decoded:
                raw = decoded.split(marker, 1)[1]
                break
        else:
            raise ValueError("в ссылке нет chat_id")
    raw = raw.split("?", 1)[0].split("#", 1)[0].strip("/")
    if not raw or any(char.isspace() or ord(char) < 32 for char in raw):
        raise ValueError("некорректный chat_id")
    return raw


def private_chat_id(first_guid: str, second_guid: str) -> str:
    if not _UUID_RE.fullmatch(first_guid) or not _UUID_RE.fullmatch(second_guid):
        raise ValueError("guid должен быть UUID")
    return "_".join(sorted((first_guid.lower(), second_guid.lower())))


def thread_chat_id(parent_chat_id: str, timestamp: int) -> str:
    parts = _chat_id(parent_chat_id).split("/")
    if len(parts) != 3:
        raise ValueError("треды поддерживаются только для группового chat_id n/k/id")
    try:
        namespace = int(parts[0]) + 100
    except ValueError as error:
        raise ValueError("некорректный групповой chat_id") from error
    return "%d/%s/%s_%d" % (namespace, parts[1], parts[2], timestamp)


def _murmur2_32(data: bytes, seed: int = 0) -> int:
    multiplier = 0x5BD1E995
    result = (seed ^ len(data)) & 0xFFFFFFFF
    index = 0
    remaining = len(data)
    while remaining >= 4:
        chunk = (
            data[index]
            | (data[index + 1] << 8)
            | (data[index + 2] << 16)
            | (data[index + 3] << 24)
        )
        chunk = (chunk * multiplier) & 0xFFFFFFFF
        chunk ^= chunk >> 24
        chunk = (chunk * multiplier) & 0xFFFFFFFF
        result = (result * multiplier) & 0xFFFFFFFF
        result ^= chunk
        index += 4
        remaining -= 4
    if remaining == 3:
        result ^= data[index + 2] << 16
    if remaining >= 2:
        result ^= data[index + 1] << 8
    if remaining >= 1:
        result ^= data[index]
        result = (result * multiplier) & 0xFFFFFFFF
    result ^= result >> 13
    result = (result * multiplier) & 0xFFFFFFFF
    result ^= result >> 15
    return result & 0xFFFFFFFF


def _msgpack_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    if len(encoded) < 32:
        return bytes([0xA0 | len(encoded)]) + encoded
    if len(encoded) <= 255:
        return bytes([0xD9, len(encoded)]) + encoded
    raise ValueError("fanout path слишком длинный")


def _fanout_frame(path: str, payload: bytes, request_id: int = 1) -> bytes:
    xiva = bytes([0x93, 0x00, request_id]) + _msgpack_string(path)
    messenger = struct.pack("<I", 5) + struct.pack("<Q", _murmur2_32(payload))
    return b"\x01" + xiva + messenger + payload


def _extract_fanout_json(frame: bytes) -> Dict[str, Any]:
    # Бинарный xiva/msgpack-заголовок может случайно содержать байт ``{``.
    # Перебираем все brace-кандидаты и принимаем только реально декодируемый
    # JSON-объект. Это поддерживает и обычный ``{"...``, и пустой ответ ``{}``.
    start = 0
    while True:
        start = frame.find(b"{", start)
        if start < 0:
            raise MessengerError("fanout вернул DATA-фрейм без JSON-объекта")
        try:
            data = json.loads(frame[start:].decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            start += 1
            continue
        if not isinstance(data, dict):
            raise MessengerError("fanout вернул JSON не объект")
        return data


def _login_candidate(value: Any) -> Optional[str]:
    normalized = str(value or "").strip().lower().lstrip("@")
    if normalized.endswith("@yandex-team.ru"):
        normalized = normalized.split("@", 1)[0]
    return normalized if _LOGIN_RE.fullmatch(normalized) else None


def _login_from_user(user: Dict[str, Any]) -> Optional[str]:
    employee = user.get("employee_info") or {}
    for candidate in (user.get("nickname"), employee.get("nickname")):
        login = _login_candidate(candidate)
        if login:
            return login
    for contact in user.get("contacts") or []:
        if not isinstance(contact, dict):
            continue
        value = str(contact.get("value") or "").strip().lower()
        if contact.get("type") == "email" and value.endswith("@yandex-team.ru"):
            login = _login_candidate(value)
            if login:
                return login
    return None


def _button_payload(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, str) or not raw:
        return None
    try:
        value = json.loads(base64.b64decode(raw))
    except (ValueError, TypeError):
        return None
    return value if isinstance(value, dict) else None


def _buttons(plain: Dict[str, Any], include_wire: bool = False) -> List[Dict[str, Any]]:
    holder = plain.get("SuggestButtonsHolder") or {}
    if holder.get("LayoutSuggestButtons"):
        rows = [
            row.get("Buttons") or []
            for row in holder["LayoutSuggestButtons"].get("ButtonRows") or []
        ]
    elif holder.get("SuggestButtons"):
        rows = [holder["SuggestButtons"].get("Buttons") or []]
    else:
        rows = []
    result = []
    for row_number, row in enumerate(rows):
        for button in row:
            directives = button.get("Directives") or []
            action = next(
                (
                    directive
                    for directive in directives
                    if directive.get("Type") == "server_action"
                ),
                None,
            )
            if action is None:
                action = next(
                    (
                        directive
                        for directive in directives
                        if directive.get("Type") == "client_action"
                        and directive.get("Name") == "send_message"
                    ),
                    {},
                )
            decoded = _button_payload(action.get("Payload")) or {}
            item = {
                "index": len(result),
                "row": row_number,
                "text": " ".join(str(button.get("Text") or "").split()),
                "id": button.get("Id"),
                "callback_data": decoded.get("callback_data"),
            }
            if include_wire:
                item["action_type"] = action.get("Type")
                item["action_name"] = action.get("Name")
                item["payload_b64"] = action.get("Payload")
                item["action_payload"] = decoded
            result.append(item)
    return result


def render_message(message: Dict[str, Any]) -> Dict[str, Any]:
    server = message.get("ServerMessage") or message
    client = server.get("ClientMessage") or {}
    info = server.get("ServerMessageInfo") or {}
    plain = client.get("Plain") or {}
    sender = info.get("From") or {}
    text = (plain.get("Text") or {}).get("MessageText")
    if plain.get("Gallery") and plain["Gallery"].get("Text"):
        text = plain["Gallery"]["Text"]
    media = []
    for key in ("Image", "File", "Voice"):
        if plain.get(key):
            media.append({"kind": key.lower(), **plain[key]})
    for item in (plain.get("Gallery") or {}).get("Items") or []:
        media.append({"kind": "gallery_item", **item})
    rendered = {
        "ts": info.get("Timestamp"),
        "seq": info.get("SeqNo"),
        "version": info.get("Version"),
        "author": sender.get("DisplayName"),
        "author_guid": sender.get("Guid"),
        "text": text,
        "payload_id": plain.get("PayloadId"),
        "deleted": bool(info.get("Deleted") or plain.get("Deleted")),
    }
    if media:
        rendered["media"] = media
    buttons = _buttons(plain)
    if buttons:
        rendered["buttons"] = buttons
    reactions = server.get("Reactions") or []
    if reactions:
        rendered["reactions"] = [
            {"type": reaction.get("Type"), "count": reaction.get("Count")}
            for reaction in reactions
        ]
    return rendered


def _default_image_receipt_dir() -> Path:
    return Path.home() / ".stefania" / "flow" / "image-receipts"


def _private_dir(path: Path) -> Path:
    path.mkdir(parents=True, mode=0o700, exist_ok=True)
    if path.is_symlink() or not path.is_dir():
        raise ValueError("каталог квитанций недоступен")
    os.chmod(path, 0o700)
    return path


def _png_geometry(data: bytes) -> tuple[int, int]:
    if not data.startswith(_PNG_SIGNATURE) or len(data) < 33:
        raise ValueError("файл не PNG")
    length = int.from_bytes(data[8:12], "big")
    if data[12:16] != b"IHDR" or length != 13:
        raise ValueError("PNG без IHDR")
    expected = int.from_bytes(data[29:33], "big")
    actual = zlib.crc32(data[12:29]) & 0xFFFFFFFF
    if actual != expected:
        raise ValueError("CRC IHDR не сошелся")
    width, height, bit_depth, color = struct.unpack(">IIBB", data[16:26])
    if (
        width < 1
        or height < 1
        or width > 16384
        or height > 16384
        or bit_depth != 8
        or color not in (0, 2, 3, 4, 6)
    ):
        raise ValueError("некорректный PNG")
    if b"acTL" in data:
        raise ValueError("анимированный PNG не поддержан")
    return width, height


def _read_local_png(path: str) -> Dict[str, Any]:
    if not isinstance(path, str) or not path or path != path.strip():
        raise ValueError("нужен абсолютный путь к PNG")
    lowered = path.lower()
    if "://" in path or lowered.startswith("http:") or lowered.startswith("https:"):
        raise ValueError("удаленный URL изображения не принимается")
    candidate = Path(path)
    if not candidate.is_absolute():
        raise ValueError("нужен абсолютный путь к PNG")
    cursor = candidate
    while True:
        if cursor.is_symlink():
            raise ValueError("симлинк в пути изображения не принимается")
        if cursor.parent == cursor:
            break
        cursor = cursor.parent
    if not candidate.is_file():
        raise ValueError("PNG не найден")
    size = candidate.stat().st_size
    if size < 33 or size > _MAX_IMAGE_BYTES:
        raise ValueError("размер PNG вне лимита media_upload 50 МиБ")
    data = candidate.read_bytes()
    if len(data) != size:
        raise ValueError("PNG изменился во время чтения")
    width, height = _png_geometry(data)
    filename = (
        candidate.name if _IMAGE_NAME_RE.fullmatch(candidate.name) else "image.png"
    )
    return {
        "path": str(candidate),
        "filename": filename,
        "data": data,
        "size": size,
        "sha256": hashlib.sha256(data).hexdigest(),
        "width": width,
        "height": height,
    }


def _load_receipt(path: Path) -> Optional[Dict[str, Any]]:
    if not path.is_file() or path.is_symlink():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise MessengerError("квитанция истории повреждена")
    if not isinstance(value, dict):
        raise MessengerError("квитанция истории повреждена")
    return value


def _save_receipt(path: Path, record: Dict[str, Any]) -> None:
    blob = json.dumps(record, ensure_ascii=False, sort_keys=True).encode("utf-8")
    temporary = path.with_suffix(".json.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(descriptor, blob)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    os.chmod(path, 0o600)


class _StoryLock:
    def __init__(self, directory: Path, story_id: str) -> None:
        self._path = directory / f"{story_id}.lock"
        self._handle = None

    def __enter__(self) -> "_StoryLock":
        if fcntl is None:
            raise MessengerError("блокировка истории недоступна")
        self._handle = open(self._path, "a+", encoding="utf-8")
        fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, *_args: Any) -> None:
        if self._handle is not None:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            self._handle.close()
            self._handle = None


def _raw_timestamp(raw: Dict[str, Any]) -> Optional[int]:
    server = raw.get("ServerMessage") or {}
    info = server.get("ServerMessageInfo") or {}
    timestamp = info.get("Timestamp")
    return timestamp if isinstance(timestamp, int) else None


def _push_timestamp(response: Dict[str, Any]) -> Optional[int]:
    info = response.get("MessageInfo") or {}
    if not isinstance(info, dict):
        return None
    for key in ("Timestamp", "timestamp"):
        value = info.get(key)
        if isinstance(value, int) and value > 0:
            return value
    nested = info.get("ServerMessageInfo") or {}
    if isinstance(nested, dict) and isinstance(nested.get("Timestamp"), int):
        return nested["Timestamp"]
    return None


def _matching_gallery(
    raw: Dict[str, Any],
    *,
    chat_id: str,
    payload_id: str,
    text: str,
    file_id: Optional[str],
    author_guid: str,
) -> Optional[Dict[str, Any]]:
    server = raw.get("ServerMessage") or {}
    info = server.get("ServerMessageInfo") or {}
    plain = (server.get("ClientMessage") or {}).get("Plain") or {}
    mentions = plain.get("MentionedUserIds") or []
    gallery = plain.get("Gallery") or {}
    items = gallery.get("Items") or []
    image: Dict[str, Any] = {}
    if len(items) == 1 and isinstance(items[0], dict):
        candidate = items[0].get("Image") or {}
        if isinstance(candidate, dict):
            image = candidate
    file_info = image.get("FileInfo") or {}
    author_ok = (info.get("From") or {}).get("Guid") == author_guid
    text_ok = gallery.get("Text") == text
    attachment_ok = (
        bool(file_id) and file_info.get("Id2") == file_id and len(items) == 1
    )
    mentions_absent = not mentions and not plain.get("SuggestButtonsHolder")
    payload_ok = plain.get("PayloadId") == payload_id
    if not (author_ok and text_ok and attachment_ok and mentions_absent and payload_ok):
        return None
    return {
        "chat_id": chat_id,
        "timestamp": _raw_timestamp(raw),
        "author_ok": True,
        "text_ok": True,
        "chat_ok": True,
        "attachment_ok": True,
        "mentions_absent": True,
    }


class MessengerUserClient:
    """Direct API client for one explicitly selected Messenger identity."""

    def __init__(
        self,
        token: str,
        *,
        account: str = "work",
        environment: str = "prod",
        expected_login: Optional[str] = None,
        enforce_account_class: bool = True,
        timeout: float = 30,
    ) -> None:
        reject_direct_client(token)
        if account not in ACCOUNT_TOKENS:
            raise ValueError("account должен быть work или personal")
        if environment not in PROFILES:
            raise ValueError("environment должен быть prod или alpha")
        if not isinstance(token, str) or not token.strip():
            raise AuthenticationError("OAuth-токен Мессенджера не задан")
        self._token = token.strip()
        self.account = account
        self.environment = environment
        self.expected_login = expected_login.strip().lower() if expected_login else None
        self.enforce_account_class = enforce_account_class
        self.timeout = float(timeout)
        self._identity: Optional[Dict[str, Any]] = None
        self._suggest_last_call = 0.0

    @classmethod
    def from_env(
        cls,
        account: str = "work",
        *,
        environment: str = "prod",
        expected_login: Optional[str] = None,
        enforce_account_class: bool = True,
    ) -> "MessengerUserClient":
        if account not in ACCOUNT_TOKENS:
            raise ValueError("account должен быть work или personal")
        variable = ACCOUNT_TOKENS[account]
        token = os.environ.get(variable, "")
        if not token.strip():
            raise AuthenticationError(
                "%s не задан; получи OAuth yamb:all через setup Стефании" % variable
            )
        return cls(
            token,
            account=account,
            environment=environment,
            expected_login=expected_login,
            enforce_account_class=enforce_account_class,
        )

    @property
    def profile(self) -> Dict[str, str]:
        return PROFILES[self.environment]

    def _opener(self) -> urllib.request.OpenerDirector:
        return urllib.request.build_opener(
            _NoRedirect(), urllib.request.HTTPSHandler(context=_ssl_context())
        )

    def registry(
        self,
        method: str,
        params: Dict[str, Any],
        *,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        if method not in REGISTRY_READ_METHODS | REGISTRY_WRITE_METHODS:
            raise ValueError("Registry method не входит в allowlist")
        if not isinstance(params, dict):
            raise ValueError("params должен быть JSON-объектом")
        extra_headers = dict(extra_headers or {})
        if set(extra_headers) - {"X-Ya-Organization-Id"}:
            raise ValueError("extra_headers содержит запрещенный заголовок")
        body = urllib.parse.urlencode(
            {
                "request": json.dumps(
                    {"method": method, "params": params},
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            }
        ).encode("utf-8")
        retry_safe = method in REGISTRY_READ_METHODS
        attempts = 3 if retry_safe else 1
        for attempt in range(attempts):
            request = urllib.request.Request(
                self.profile["registry"],
                data=body,
                method="POST",
                headers={
                    "Authorization": "OAuth " + self._token,
                    "X-Application-Id": "Yamb-web",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                    **extra_headers,
                },
            )
            try:
                with self._opener().open(request, timeout=self.timeout) as response:
                    raw = response.read(_MAX_REGISTRY_BYTES + 1)
                    status = response.status
            except urllib.error.HTTPError as error:
                status = error.code
                raw = b""
            except (urllib.error.URLError, TimeoutError, OSError) as error:
                if retry_safe and attempt + 1 < attempts:
                    time.sleep((0.5, 1.0)[attempt])
                    continue
                if retry_safe:
                    raise MessengerError(
                        "Registry %s: network_error, outcome=not_confirmed" % method
                    ) from error
                raise OperationUncertain(
                    "Registry %s: network_error, outcome=unknown; reconcile" % method
                ) from error
            if status in (302, 401):
                raise AuthenticationError(
                    "Registry отклонил OAuth для %s: HTTP %d" % (method, status)
                )
            if status == 403:
                raise PermissionDenied(
                    "Registry запретил %s для текущего субъекта/ресурса: HTTP 403"
                    % method
                )
            if status == 429 or status >= 500:
                if retry_safe and attempt + 1 < attempts:
                    time.sleep((0.5, 1.0)[attempt])
                    continue
                error_cls = MessengerError if retry_safe else OperationUncertain
                raise error_cls(
                    "Registry %s: HTTP %d, outcome=%s"
                    % (method, status, "not_confirmed" if retry_safe else "unknown")
                )
            if not 200 <= status < 300:
                raise MessengerError("Registry %s: HTTP %d" % (method, status))
            if len(raw) > _MAX_REGISTRY_BYTES:
                raise MessengerError("Registry response превышает лимит")
            try:
                value = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, ValueError) as error:
                raise MessengerError("Registry вернул некорректный JSON") from error
            if not isinstance(value, dict):
                raise MessengerError("Registry вернул JSON не объект")
            if value.get("status") == "error":
                raise MessengerError("Registry %s: status=error" % method)
            data = value.get("data", value)
            if not isinstance(data, dict):
                raise MessengerError("Registry %s: data не объект" % method)
            return data
        raise AssertionError("unreachable")

    async def _fanout_async(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            import websockets
        except ImportError as error:
            raise MessengerError("нужен pip-пакет websockets") from error
        identity = self.identity()
        query = urllib.parse.urlencode(
            {
                "service": self.profile["service"],
                "client": "stefania-messenger-user",
                "user": str(identity["uid"]),
                "session": str(uuid.uuid4()),
            }
        )
        headers = {"Authorization": "OAuth " + self._token}
        try:
            parameters = inspect.signature(websockets.connect).parameters
        except (TypeError, ValueError):
            parameters = {}
        header_name = (
            "extra_headers"
            if "extra_headers" in parameters and "additional_headers" not in parameters
            else "additional_headers"
        )
        connection = websockets.connect(
            WS_URL + "?" + query,
            ssl=_ssl_context(),
            open_timeout=self.timeout,
            close_timeout=5,
            max_size=_MAX_WS_BYTES,
            **{header_name: headers},
        )
        frame = _fanout_frame(path, _stable_json(payload))
        try:
            async with connection as websocket:
                sent = False
                for _ in range(15):
                    message = await asyncio.wait_for(
                        websocket.recv(), timeout=self.timeout
                    )
                    if isinstance(message, str):
                        try:
                            operation = json.loads(message).get("operation")
                        except (ValueError, AttributeError):
                            operation = None
                        if not sent and operation in {"ping", "subscribed"}:
                            await websocket.send(frame)
                            sent = True
                        continue
                    if message and message[0] == 1:
                        return _extract_fanout_json(message)
        except asyncio.TimeoutError as error:
            raise MessengerError("fanout timeout") from error
        raise MessengerError("fanout не вернул DATA-фрейм")

    def fanout(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if path not in FANOUT_READ_PATHS | FANOUT_WRITE_PATHS:
            raise ValueError("fanout path не входит в allowlist")
        if not isinstance(payload, dict):
            raise ValueError("payload должен быть JSON-объектом")
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise MessengerError(
                "синхронный client нельзя вызвать из активного asyncio loop"
            )
        try:
            return asyncio.run(self._fanout_async(path, payload))
        except (AuthenticationError, ConfirmationError, OperationUncertain):
            raise
        except Exception as error:
            if path == "push":
                raise OperationUncertain(
                    "fanout push: outcome=unknown; reconcile by MessageInfo/history"
                ) from error
            if isinstance(error, MessengerError):
                raise
            raise MessengerError("fanout %s failed" % path) from error

    def identity(self, refresh: bool = False) -> Dict[str, Any]:
        if self._identity is not None and not refresh:
            return dict(self._identity)
        data = self.registry("request_user", {})
        user = data.get("user")
        if not isinstance(user, dict) or not user.get("uid") or not user.get("guid"):
            raise AuthenticationError("request_user не вернул uid/guid")
        is_org = bool(user.get("organizations"))
        login = _login_from_user(user) or user.get("login")
        if self.enforce_account_class:
            if self.account == "work" and not is_org:
                raise AuthenticationError(
                    "MESSENGER_TOKEN принадлежит личному, а не рабочему аккаунту"
                )
            if self.account == "personal" and is_org:
                raise AuthenticationError(
                    "MESSENGER_TOKEN принадлежит рабочему, а не личному аккаунту"
                )
        if self.expected_login and str(login or "").lower() != self.expected_login:
            raise AuthenticationError("OAuth принадлежит неожиданному логину")
        organizations = user.get("organizations") or []
        org_id = None
        if organizations and isinstance(organizations[0], dict):
            org_id = (
                organizations[0].get("id")
                or organizations[0].get("org_id")
                or organizations[0].get("organization_id")
            )
        self._identity = {
            "uid": user["uid"],
            "guid": user["guid"],
            "login": login,
            "display_name": user.get("display_name") or user.get("public_name"),
            "is_org_account": is_org,
            "organization_id": str(org_id) if org_id else None,
        }
        return dict(self._identity)

    def diagnose(self) -> Dict[str, Any]:
        identity = self.identity(refresh=True)
        return {
            "authenticated": True,
            "account": self.account,
            "environment": self.environment,
            "identity": {
                "login": identity.get("login"),
                "display_name": identity.get("display_name"),
                "is_org_account": identity["is_org_account"],
            },
            "registry": self.profile["registry"],
            "fanout_service": self.profile["service"],
            "operations": sorted(MESSENGER_OPERATIONS),
        }

    def _preview(self, operation: str, request: Dict[str, Any]) -> MutationPreview:
        if operation not in MESSENGER_OPERATIONS:
            raise ValueError("неизвестная операция")
        identity = self.identity()
        envelope = {
            "operation": operation,
            "account": self.account,
            "environment": self.environment,
            "actor_guid": identity["guid"],
            "actor_login": identity.get("login"),
            "request": request,
        }
        fingerprint = hashlib.sha256(_stable_json(envelope)).hexdigest()
        return MutationPreview(
            operation=operation,
            account=self.account,
            environment=self.environment,
            request={
                **request,
                "_actor": {
                    "guid": identity["guid"],
                    "login": identity.get("login"),
                },
            },
            fingerprint=fingerprint,
        )

    @staticmethod
    def _confirmed(
        preview: MutationPreview, confirm_fingerprint: Optional[str]
    ) -> bool:
        if confirm_fingerprint is None:
            return False
        if not isinstance(confirm_fingerprint, str) or not hmac.compare_digest(
            preview.fingerprint, confirm_fingerprint
        ):
            raise ConfirmationError(
                "fingerprint не совпал: параметры изменились после превью"
            )
        return True

    def _resolve_chat(
        self, chat_id: Optional[str] = None, to_guid: Optional[str] = None
    ) -> str:
        if bool(chat_id) == bool(to_guid):
            raise ValueError("передай ровно один из chat_id или to_guid")
        if to_guid:
            return private_chat_id(self.identity()["guid"], to_guid)
        return _chat_id(chat_id)

    def list_chats(
        self,
        limit: int = 50,
        *,
        name_contains: Optional[str] = None,
        cursor: Optional[str] = None,
    ) -> Dict[str, Any]:
        limit = _bounded_int(limit, "limit", 1, 500)
        fetch = max(limit, 500) if name_contains else limit
        params: Dict[str, Any] = {"limit": fetch}
        if cursor:
            params["chat_id_offset"] = _chat_id(cursor)
        chats = self.registry("get_chats_info", params).get("chats") or []
        needle = str(name_contains or "").lower()
        result = []
        for chat in chats:
            haystack = " ".join(
                str(chat.get(key) or "") for key in ("name", "description", "chat_id")
            ).lower()
            if needle and needle not in haystack:
                continue
            result.append(chat)
            if len(result) >= limit:
                break
        return {
            "chats": result,
            "count": len(result),
            "next_cursor": chats[-1].get("chat_id") if len(chats) >= fetch else None,
        }

    def list_folders(self) -> Dict[str, Any]:
        identity = self.identity()
        organization_id = identity.get("organization_id")
        if not organization_id:
            raise MessengerError(
                "папки доступны только для рабочего аккаунта с organization_id"
            )

        data = self.registry("get_buckets", {"version": 0})
        buckets = data.get("buckets", [])
        if not isinstance(buckets, list):
            raise MessengerError("get_buckets вернул buckets не массив")

        folder_bucket = None
        for bucket in buckets:
            if not isinstance(bucket, dict):
                raise MessengerError("get_buckets вернул некорректный bucket")
            if (
                bucket.get("bucket_name") == "folders"
                or bucket.get("name") == "folders"
            ):
                folder_bucket = bucket
                break
        if folder_bucket is None:
            return {
                "organization_id": organization_id,
                "folders": [],
                "main_folder": {},
                "version": None,
            }
        if "bucket_value" not in folder_bucket:
            raise MessengerError("folders bucket не содержит bucket_value")

        value = folder_bucket["bucket_value"]
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except ValueError as error:
                raise MessengerError(
                    "folders bucket содержит некорректный JSON"
                ) from error
        if not isinstance(value, dict):
            raise MessengerError("folders bucket имеет некорректный формат")

        organization = value.get(str(organization_id))
        if organization is None:
            return {
                "organization_id": organization_id,
                "folders": [],
                "main_folder": {},
                "version": folder_bucket.get("version"),
            }
        if not isinstance(organization, dict):
            raise MessengerError("folders bucket содержит некорректную организацию")

        custom_folders = organization.get("custom_folders", [])
        if not isinstance(custom_folders, list):
            raise MessengerError("custom_folders должен быть массивом")
        main_folder = organization.get("main_folder", {})
        if not isinstance(main_folder, dict):
            raise MessengerError("main_folder должен быть объектом")

        list_fields = (
            "included_chat_ids",
            "included_type_ids",
            "excluded_chat_ids",
            "excluded_type_ids",
            "pinned_chat_ids",
        )
        folders = []
        for folder in custom_folders:
            if not isinstance(folder, dict):
                raise MessengerError("custom_folders содержит не объект")
            rendered = {key: folder.get(key) for key in ("id", "name", "icon")}
            for key in list_fields:
                items = folder.get(key, [])
                if not isinstance(items, list):
                    raise MessengerError("%s должен быть массивом" % key)
                rendered[key] = list(items)
            folders.append(rendered)

        return {
            "organization_id": organization_id,
            "folders": folders,
            "main_folder": dict(main_folder),
            "version": folder_bucket.get("version"),
        }

    def list_private_chats(
        self, limit: int = 50, *, cursor: Optional[str] = None
    ) -> Dict[str, Any]:
        limit = _bounded_int(limit, "limit", 1, 500)
        params: Dict[str, Any] = {"limit": min(limit, 100)}
        if cursor:
            if not _UUID_RE.fullmatch(cursor):
                raise ValueError("cursor должен быть guid UUID")
            params["guid_offset"] = cursor
        users = self.registry("get_users_data", params).get("users") or []
        me = self.identity()["guid"]
        chats = []
        for user in users:
            guid = user.get("guid")
            if not guid or guid == me:
                continue
            chats.append(
                {
                    "chat_id": private_chat_id(me, guid),
                    "to_guid": guid,
                    "login": _login_from_user(user),
                    "display_name": user.get("display_name") or user.get("public_name"),
                }
            )
        return {
            "private_chats": chats,
            "count": len(chats),
            "next_cursor": (
                users[-1].get("guid") if len(users) >= params["limit"] else None
            ),
        }

    def find_user(self, query: str, limit: int = 10) -> Dict[str, Any]:
        query = str(query or "").strip()
        if not query:
            raise ValueError("query не должен быть пустым")
        limit = _bounded_int(limit, "limit", 1, 50)
        wait = _PEOPLE_SEARCH_INTERVAL - (time.monotonic() - self._suggest_last_call)
        if self._suggest_last_call and wait > 0:
            time.sleep(wait)
        self._suggest_last_call = time.monotonic()
        # Без X-Ya-Organization-Id Registry search ищет только по своим
        # контактам и только по display_name (SHARINGML-835). С заголовком
        # организации поиск покрывает директорию организации (users_pvp),
        # глобальный каталог (users_global) и матчит по nickname=логину.
        extra_headers = {}
        org_id = self.identity().get("organization_id")
        if org_id:
            extra_headers["X-Ya-Organization-Id"] = str(org_id)
        data = self.registry(
            "search",
            {"query": query, "entities": ["users"]},
            extra_headers=extra_headers or None,
        )
        items = (data.get("users") or {}).get("items") or []
        users: List[Dict[str, Any]] = []
        seen: set = set()
        for item in items:
            user = item.get("data", item)
            guid = user.get("guid")
            if not guid or guid in seen:
                continue
            seen.add(guid)
            users.append(
                {
                    "guid": guid,
                    "login": _login_from_user(user),
                    "display_name": user.get("display_name") or user.get("public_name"),
                    "type": item.get("type"),
                }
            )
            if len(users) >= limit:
                break
        return {"users": users, "count": len(users)}

    def resolve_login(self, login: str) -> Optional[Dict[str, Any]]:
        normalized = str(login or "").strip().lower().lstrip("@")
        if not _LOGIN_RE.fullmatch(normalized):
            raise ValueError("некорректный staff-логин")
        private = self.list_private_chats(limit=100)
        for user in private["private_chats"]:
            if user.get("login") == normalized:
                return user
        result = self.find_user(normalized, limit=20)
        exact = [user for user in result["users"] if user.get("login") == normalized]
        if len(exact) > 1:
            raise MessengerError("логин резолвится неоднозначно")
        return exact[0] if exact else None

    def resolve_logins(
        self,
        logins: Sequence[str],
        *,
        search_missing: bool = True,
        max_search: int = 10,
    ) -> Dict[str, Any]:
        if not logins or len(logins) > 50:
            raise ValueError("logins должен содержать 1..50 значений")
        private = self.list_private_chats(limit=100)
        known = {
            user.get("login"): user
            for user in private["private_chats"]
            if user.get("login")
        }
        found: Dict[str, str] = {}
        missing = []
        for raw_login in logins:
            login = str(raw_login or "").strip().lower().lstrip("@")
            if not _LOGIN_RE.fullmatch(login):
                raise ValueError("некорректный staff-логин: %r" % raw_login)
            if login in known:
                found[login] = known[login]["to_guid"]
            else:
                missing.append(login)
        max_search = _bounded_int(max_search, "max_search", 1, 20)
        still_missing = []
        for position, login in enumerate(missing):
            if not search_missing or position >= max_search:
                still_missing.append(login)
                continue
            result = self.find_user(login, limit=20)
            exact = [
                user
                for user in result["users"]
                if user.get("login") == login and user.get("guid")
            ]
            if len(exact) == 1:
                found[login] = exact[0]["guid"]
            else:
                still_missing.append(login)
        return {
            "found": found,
            "missing": still_missing,
            "complete": not still_missing,
            "hint": (
                "People search вызывается последовательно с паузой вежливости; "
                "сверх max_search логины остаются в missing."
            ),
        }

    def read_history(
        self,
        *,
        chat_id: Optional[str] = None,
        to_guid: Optional[str] = None,
        limit: int = 30,
        before_ts: Optional[int] = None,
    ) -> Dict[str, Any]:
        resolved = self._resolve_chat(chat_id, to_guid)
        limit = _bounded_int(limit, "limit", 1, 500)
        payload: Dict[str, Any] = {"ChatId": resolved, "Limit": limit}
        if before_ts is not None:
            payload["MaxTimestamp"] = _positive_int(before_ts, "before_ts")
        response = self.fanout("history", payload)
        chats = response.get("Chats") or []
        raw_messages = chats[0].get("Messages") or [] if chats else []
        messages = sorted(
            (render_message(message) for message in raw_messages),
            key=lambda item: item.get("ts") or 0,
        )
        oldest = min((item["ts"] for item in messages if item.get("ts")), default=None)
        return {
            "chat_id": resolved,
            "messages": messages,
            "count": len(messages),
            "next_before_ts": oldest,
        }

    def _chat_history(self, chat_id, *, limit=1, before_ts=None):
        payload = {"ChatId": chat_id, "Limit": limit}
        if before_ts is not None:
            payload["MaxTimestamp"] = before_ts
        response = self.fanout("history", payload)
        rooms = [r for r in response.get("Chats", []) if r.get("ChatId") == chat_id]
        if len(rooms) != 1 or rooms[0].get("Status") not in (None, 0, "Success"):
            raise MessengerError("history did not return a successful exact chat")
        return rooms[0]

    @staticmethod
    def _read_state(room):
        fields = {
            "timestamp": "LastSeenByMeTsMcs",
            "seq_no": "LastSeenByMeSeqNo",
            "latest_timestamp": "LastTsMcs",
            "latest_seq_no": "LastSeqNo",
        }
        state = {}
        for key, field in fields.items():
            value = room.get(field)
            if isinstance(value, bool) or not str(value).isdigit():
                raise MessengerError(
                    "history missing or invalid read boundary: " + field
                )
            state[key] = int(value)
        if (
            state["timestamp"] > state["latest_timestamp"]
            or state["seq_no"] > state["latest_seq_no"]
        ):
            raise MessengerError("history read boundary exceeds latest message")
        version_field = (
            "LastSeenByMeVersion2"
            if "LastSeenByMeVersion2" in room
            else "LastSeenByMeVersion"
        )
        version = room.get(version_field)
        if version is not None and (
            isinstance(version, bool) or not str(version).isdigit()
        ):
            raise MessengerError(
                "history missing or invalid read boundary: " + version_field
            )
        state["version"] = int(version) if version is not None else None
        return state

    def mark_chat_read(
        self,
        chat_id: str,
        *,
        target_timestamp: Optional[int] = None,
        target_seq_no: Optional[int] = None,
        confirm_fingerprint: Optional[str] = None,
        verify_attempts: int = 3,
    ) -> Dict[str, Any]:
        """Preview a pinned SeenMarker, push once, then verify history separately."""
        chat_id = _chat_id(chat_id)
        verify_attempts = _bounded_int(verify_attempts, "verify_attempts", 1, 5)
        if (target_timestamp is None) != (target_seq_no is None):
            raise ValueError("provide both target_timestamp and target_seq_no")
        room = self._chat_history(chat_id)
        before = self._read_state(room)
        if target_timestamp is None:
            target_timestamp = before["latest_timestamp"]
            target_seq_no = before["latest_seq_no"]
        else:
            target_timestamp = _positive_int(target_timestamp, "target_timestamp")
            target_seq_no = _positive_int(target_seq_no, "target_seq_no")
            if (
                target_timestamp > before["latest_timestamp"]
                or target_seq_no > before["latest_seq_no"]
            ):
                raise ValueError("target is beyond the latest chat message")
            info = self.message_info(chat_id, target_timestamp)
            message = info.get("message") or {}
            if (
                not info.get("found")
                or int(message.get("ts") or 0) != target_timestamp
                or int(message.get("seq") or 0) != target_seq_no
            ):
                raise ValueError(
                    "target timestamp and sequence do not identify a chat message"
                )
        request = {
            "chat_id": chat_id,
            "target_timestamp": target_timestamp,
            "target_seq_no": target_seq_no,
        }
        result = {"operation": "MarkChatRead", **request, "before": before}
        if (
            before["timestamp"] >= target_timestamp
            and before["seq_no"] >= target_seq_no
        ):
            return {
                **result,
                "outcome": "confirmed",
                "status": "already_read",
                "applied": False,
                "verification": {
                    "checked": True,
                    "target_read": True,
                    "attempts": 0,
                    "read_state": before,
                },
            }
        preview = self._preview("MarkChatRead", request)
        if not self._confirmed(preview, confirm_fingerprint):
            return preview.to_dict()
        marker = {
            "ChatId": chat_id,
            "Timestamp": target_timestamp,
            "SeqNo": target_seq_no,
        }
        if before["version"] is not None:
            marker["Version"] = before["version"]
        # Version may be absent in prod; the documented legacy marker omits it.
        push_outcome, status = "unknown", None
        try:
            response = self.fanout(
                "push", {"ClientMessage": {"SeenMarker": marker}, "UserIp": "::1"}
            )
            push_outcome, status = _push_outcome(response)
        except OperationUncertain:
            # Reconcile only; never resend an uncertain mutation.
            pass
        verification = {"checked": False, "target_read": False, "attempts": 0}
        for attempt in range(1, verify_attempts + 1):
            verification["attempts"] = attempt
            try:
                after = self._read_state(self._chat_history(chat_id))
                verification.update(
                    checked=True,
                    read_state=after,
                    target_read=(
                        after["timestamp"] >= target_timestamp
                        and after["seq_no"] >= target_seq_no
                    ),
                )
                verification.pop("error", None)
                if verification["target_read"]:
                    break
            except MessengerError:
                verification["error"] = "history_verification_failed"
            if attempt < verify_attempts:
                time.sleep(0.25 * attempt)
        outcome = (
            "confirmed"
            if verification["target_read"]
            else "rejected" if push_outcome == "rejected" else "unknown"
        )
        return {
            **result,
            "outcome": outcome,
            "push_outcome": push_outcome,
            "commit_status": status,
            "verification": verification,
        }

    def message_info(self, chat_id: str, timestamp: int) -> Dict[str, Any]:
        resolved = _chat_id(chat_id)
        ts = _positive_int(timestamp, "timestamp")
        response = self.fanout("message_info", {"ChatId": resolved, "Timestamp": ts})
        raw = response.get("Message")
        return {
            "found": bool(raw),
            "chat_id": resolved,
            "timestamp": ts,
            "message": render_message(raw) if raw else None,
            "raw_message": raw,
        }

    def threads_list(self, chat_id: str, limit: int = 200) -> Dict[str, Any]:
        resolved = _chat_id(chat_id)
        history = self.fanout(
            "history",
            {"ChatId": resolved, "Limit": _bounded_int(limit, "limit", 1, 500)},
        )
        raw_messages = (history.get("Chats") or [{}])[0].get("Messages") or []
        threads = []
        for raw in raw_messages:
            info = (raw.get("ServerMessage") or {}).get("ServerMessageInfo") or {}
            state = info.get("ThreadState")
            if state:
                timestamp = info.get("Timestamp")
                threads.append(
                    {
                        "parent_ts": timestamp,
                        "thread_chat_id": thread_chat_id(resolved, timestamp),
                        "parent": render_message(raw),
                        "state": state,
                    }
                )
        return {"chat_id": resolved, "threads": threads, "count": len(threads)}

    def read_thread(
        self,
        chat_id: str,
        parent_ts: int,
        limit: int = 100,
        before_ts: Optional[int] = None,
    ) -> Dict[str, Any]:
        parent = _chat_id(chat_id)
        timestamp = _positive_int(parent_ts, "parent_ts")
        thread = thread_chat_id(parent, timestamp)
        result = self.read_history(chat_id=thread, limit=limit, before_ts=before_ts)
        return {**result, "parent_chat_id": parent, "parent_ts": timestamp}

    def media_messages(
        self,
        chat_id: str,
        *,
        media_types: Optional[Sequence[str]] = None,
        pivot: int = 9999999999999999,
        limit: int = 30,
    ) -> Dict[str, Any]:
        allowed = {"image", "gallery", "file", "link", "important"}
        types = list(media_types or [])
        if any(media_type not in allowed for media_type in types):
            raise ValueError("неизвестный media_type")
        params: Dict[str, Any] = {
            "chat_id": _chat_id(chat_id),
            "pivot_id": _positive_int(pivot, "pivot"),
            "prev": _bounded_int(limit, "limit", 1, 500),
            "next": 0,
        }
        if types:
            params["types"] = types
        data = self.registry("get_media_messages", params)
        messages = [render_message(message) for message in data.get("messages") or []]
        oldest = min((item["ts"] for item in messages if item.get("ts")), default=None)
        return {
            "chat_id": params["chat_id"],
            "messages": messages,
            "count": len(messages),
            "next_pivot": oldest if (data.get("info") or {}).get("has_prev") else None,
        }

    def reactions(
        self, chat_id: str, timestamp: int, limit: int = 1000
    ) -> Dict[str, Any]:
        payload = {
            "ChatId": _chat_id(chat_id),
            "Timestamp": _positive_int(timestamp, "timestamp"),
            "Limit": _bounded_int(limit, "limit", 1, 5000),
            "Mode": 2,
        }
        response = self.fanout("list_reactions", payload)
        return {
            "chat_id": payload["ChatId"],
            "timestamp": payload["Timestamp"],
            "reactions": response.get("UserReactions") or [],
            "reads_version": response.get("ReadsVersion"),
        }

    def unread_count(
        self,
        chat_id: Optional[str] = None,
        *,
        page_size: int = 100,
        max_pages: int = 100,
    ) -> Dict[str, Any]:
        """Keep the API counter; derive chat message counts from a history snapshot."""
        resolved = _chat_id(chat_id) if chat_id else None
        if resolved:
            parts = resolved.split("/")
            if len(parts) == 3 and parts[0].isdigit() and int(parts[0]) >= 100:
                raise ValueError("unread_count supports only main chats, not threads")
            page_size = _bounded_int(page_size, "page_size", 1, 500)
            max_pages = _bounded_int(max_pages, "max_pages", 1, 1000)
        response = self.fanout("unread_count", {"ChatId": resolved} if resolved else {})
        result = {
            "chat_id": resolved,
            "unread_count": response.get("UnreadCount"),
            "chat_unread_count": response.get("ChatUnreadCount"),
            "last_unread_ts": response.get("LastUnreadTsMcs"),
        }
        if resolved:
            result.update(self._count_unread_messages(resolved, page_size, max_pages))
        return result

    def _count_unread_messages(self, chat_id, page_size, max_pages):
        # Count only main-chat messages; thread replies have separate read boundaries.
        before, marker, upper = None, None, None
        found, complete = {}, False
        for pages in range(1, max_pages + 1):
            payload = {"ChatId": chat_id, "Limit": page_size}
            if before is not None:
                payload["MaxTimestamp"] = before
            response = self.fanout("history", payload)
            rooms = [r for r in response.get("Chats", []) if r.get("ChatId") == chat_id]
            if len(rooms) != 1 or rooms[0].get("Status") not in (None, 0, "Success"):
                raise MessengerError("history did not return a successful exact chat")
            room = rooms[0]
            if marker is None:
                bounds = []
                for field in ("LastSeenByMeTsMcs", "LastTsMcs"):
                    value = room.get(field)
                    if isinstance(value, bool) or not str(value).isdigit():
                        raise MessengerError(
                            "history missing or invalid boundary: " + field
                        )
                    bounds.append(int(value))
                marker, upper = bounds
                if marker > upper:
                    raise MessengerError("history read marker exceeds latest message")
                if marker == upper:
                    complete = True
                    break
            # Keep the first page's boundaries even if another client reads meanwhile.
            times = []
            for raw in room.get("Messages", []):
                server = raw.get("ServerMessage") or raw
                info = server.get("ServerMessageInfo") or {}
                timestamp = _positive_int(info.get("Timestamp"), "message timestamp")
                if before is not None and timestamp > before:
                    raise MessengerError("history pagination ignored its boundary")
                times.append(timestamp)
                if marker < timestamp <= upper:
                    plain = (server.get("ClientMessage") or {}).get("Plain") or {}
                    version = int(info.get("Version") or 0)
                    deleted = bool(info.get("Deleted") or plain.get("Deleted"))
                    if timestamp not in found or version >= found[timestamp][0]:
                        found[timestamp] = (version, deleted)
            if not times or min(times) <= marker:
                complete = True
                break
            next_before = min(times) - 1
            if before is not None and next_before >= before:
                raise MessengerError("history pagination stalled")
            before = next_before
        count = sum(not deleted for _, deleted in found.values())
        return {
            "chat_unread_count": count if complete else None,
            "chat_count_complete": complete,
            "chat_count_observed": count,
            "chat_count_source": "history",
            "chat_count_scope": "main_chat",
            "read_marker": marker,
            "snapshot_upper": upper,
            "pages": pages,
        }

    def edit_history(
        self, chat_id: str, since_ts: int, limit: int = 50
    ) -> Dict[str, Any]:
        payload = {
            "ChatId": _chat_id(chat_id),
            "MinTimestamp": _positive_int(since_ts, "since_ts"),
            "Limit": _bounded_int(limit, "limit", 1, 500),
        }
        response = self.fanout("edit_history", payload)
        messages = [
            render_message(message) for message in response.get("Messages") or []
        ]
        return {
            "chat_id": payload["ChatId"],
            "messages": messages,
            "count": len(messages),
        }

    def chat_members(
        self,
        chat_id: str,
        *,
        limit: int = 10000,
        page_limit: int = 100,
        max_pages: int = 100,
        cursor: Optional[str] = None,
    ) -> Dict[str, Any]:
        resolved = _chat_id(chat_id)
        limit = _bounded_int(limit, "limit", 1, 10000)
        page_limit = _bounded_int(page_limit, "page_limit", 1, 100)
        max_pages = _bounded_int(max_pages, "max_pages", 1, 200)
        if cursor and not _UUID_RE.fullmatch(cursor):
            raise ValueError("cursor должен быть guid UUID")
        members: List[Dict[str, Any]] = []
        seen = set()
        current = cursor
        stopped_by = "complete"
        pages = 0
        while len(members) < limit and pages < max_pages:
            params: Dict[str, Any] = {"chat_id": resolved, "limit": page_limit}
            if current:
                params["guid_offset"] = current
            page = self.registry("get_chat_members", params).get("users")
            pages += 1
            if not isinstance(page, list):
                stopped_by = "bad_response"
                break
            if not page:
                stopped_by = "stale_cursor" if pages == 1 and cursor else "complete"
                break
            added = 0
            for member in page:
                guid = member.get("guid")
                if guid and guid in seen:
                    continue
                if guid:
                    seen.add(guid)
                members.append(member)
                added += 1
                if len(members) >= limit:
                    stopped_by = "limit"
                    break
            if len(members) >= limit or len(page) < page_limit:
                break
            next_cursor = page[-1].get("guid")
            if not next_cursor or next_cursor == current or not added:
                stopped_by = "cursor_stall"
                break
            current = next_cursor
        else:
            if pages >= max_pages:
                stopped_by = "max_pages"
        truncated = stopped_by != "complete"
        return {
            "chat_id": resolved,
            "members": members,
            "count": len(members),
            "pages": pages,
            "truncated": truncated,
            "stopped_by": stopped_by,
            "next_cursor": members[-1].get("guid") if truncated and members else None,
        }

    def read_new(
        self, chat_id: str, *, cursor: Optional[int] = None, limit: int = 100
    ) -> Dict[str, Any]:
        """Stateless delta read: the caller owns and persists ``next_cursor``."""
        history = self.read_history(chat_id=chat_id, limit=limit)
        boundary = int(cursor or 0)
        messages = [
            message
            for message in history["messages"]
            if int(message.get("ts") or 0) > boundary
        ]
        next_cursor = max(
            [boundary]
            + [int(message.get("ts") or 0) for message in history["messages"]]
        )
        return {
            "chat_id": history["chat_id"],
            "messages": messages,
            "count": len(messages),
            "cursor": boundary,
            "next_cursor": next_cursor,
        }

    def last_from(
        self, chat_id: str, author_guid: str, limit: int = 100
    ) -> Dict[str, Any]:
        if not _UUID_RE.fullmatch(str(author_guid or "")):
            raise ValueError("author_guid должен быть UUID")
        history = self.read_history(chat_id=chat_id, limit=limit)
        matching = [
            message
            for message in history["messages"]
            if message.get("author_guid") == author_guid
        ]
        return {
            "chat_id": history["chat_id"],
            "message": matching[-1] if matching else None,
            "found": bool(matching),
        }

    def thread_dump(
        self, chat_id: str, parent_ts: int, limit: int = 500
    ) -> Dict[str, Any]:
        thread = self.read_thread(chat_id, parent_ts, limit=limit)
        lines = ["# Тред", ""]
        for message in thread["messages"]:
            author = message.get("author") or message.get("author_guid") or "unknown"
            text = message.get("text") or "[без текста]"
            lines.extend(("## %s" % author, "", str(text), ""))
        return {**thread, "markdown": "\n".join(lines).rstrip() + "\n"}

    def send_message(
        self,
        text: str,
        *,
        chat_id: Optional[str] = None,
        to_guid: Optional[str] = None,
        thread_ts: Optional[int] = None,
        mention_guids: Optional[Sequence[str]] = None,
        payload_id: Optional[str] = None,
        confirm_fingerprint: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not isinstance(text, str) or not text or len(text) > _MAX_TEXT:
            raise ValueError("text должен содержать 1..6000 символов")
        parent = self._resolve_chat(chat_id, to_guid)
        target = parent
        if thread_ts is not None:
            if to_guid:
                raise ValueError("тред нельзя адресовать через to_guid")
            target = thread_chat_id(parent, _positive_int(thread_ts, "thread_ts"))
        mentions = list(mention_guids or [])
        if any(not _UUID_RE.fullmatch(guid) for guid in mentions):
            raise ValueError("mention_guids должны быть UUID")
        payload_id = payload_id or str(uuid.uuid4())
        request = {
            "chat_id": target,
            "parent_chat_id": parent,
            "to_guid": to_guid,
            "text": text,
            "mention_guids": mentions,
            "payload_id": payload_id,
        }
        preview = self._preview("SendMessage", request)
        if not self._confirmed(preview, confirm_fingerprint):
            return preview.to_dict()
        if to_guid:
            self.registry("create_private_chat", {"guid": to_guid})
        plain: Dict[str, Any] = {
            "ChatId": target,
            "Text": {"MessageText": text},
            "PayloadId": payload_id,
        }
        if mentions:
            plain["MentionedUserIds"] = mentions
        response = self.fanout(
            "push", {"ClientMessage": {"Plain": plain}, "UserIp": "::1"}
        )
        outcome, status = _push_outcome(response)
        return {
            "operation": "SendMessage",
            "outcome": outcome,
            "reason": _push_rejection_reason(response),
            "chat_id": target,
            "commit_status": status,
            "message_info": response.get("MessageInfo"),
            "payload_id": payload_id,
        }

    def send_image_with_caption(
        self,
        *,
        chat_id: str,
        path: str,
        text: str,
        story_id: str,
        payload_id: Optional[str] = None,
        confirm_fingerprint: Optional[str] = None,
        receipt_dir: Optional[str] = None,
    ) -> Dict[str, Any]:
        """One PNG plus caption. Preview does not upload or push.

        The wire message is a one-item Gallery, the same shape the web client
        uses when a single image has a caption. Caption length follows the web
        composer limit (4096), not ``_MAX_TEXT``.
        """
        if not isinstance(text, str) or not text or len(text) > _MAX_CAPTION:
            raise ValueError("подпись должна содержать 1..4096 символов")
        if "://" in str(chat_id or ""):
            raise ValueError("chat_id изображения не может быть URL")
        chat = _chat_id(chat_id)
        if not _STORY_ID_RE.fullmatch(str(story_id or "")):
            raise ValueError("некорректный story_id")
        image = _read_local_png(path)
        payload = payload_id or str(uuid.uuid4())
        if not _UUID_RE.fullmatch(payload):
            raise ValueError("payload_id должен быть UUID")
        request = {
            "chat_id": chat,
            "story_id": story_id,
            "text": text,
            "text_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "payload_id": payload,
            "file_sha256": image["sha256"],
            "file_size": image["size"],
            "width": image["width"],
            "height": image["height"],
            "filename": image["filename"],
        }
        preview = self._preview("SendImageWithCaption", request)
        if not self._confirmed(preview, confirm_fingerprint):
            result = preview.to_dict()
            result["upload"] = "not_performed"
            result["delivery"] = "not_performed"
            receipt = self._image_receipt_hint(request, receipt_dir)
            if receipt and receipt.get("state") == "verified":
                result["already_verified"] = True
            return result
        return self._commit_image_with_caption(request, path, receipt_dir)

    def _image_receipt_hint(
        self, request: Dict[str, Any], receipt_dir: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        directory = self._image_receipt_directory(receipt_dir, create=False)
        if directory is None or not directory.is_dir():
            return None
        return _load_receipt(directory / f"{request['story_id']}.json")

    def _image_receipt_directory(
        self, receipt_dir: Optional[str], *, create: bool
    ) -> Optional[Path]:
        if receipt_dir is None:
            directory = _default_image_receipt_dir()
        else:
            directory = Path(receipt_dir)
            if not directory.is_absolute():
                raise ValueError("receipt_dir должен быть абсолютным")
        if not create and not directory.exists():
            return directory
        return _private_dir(directory)

    def _commit_image_with_caption(
        self, request: Dict[str, Any], path: str, receipt_dir: Optional[str]
    ) -> Dict[str, Any]:
        directory = self._image_receipt_directory(receipt_dir, create=True)
        assert directory is not None
        story_id = request["story_id"]
        receipt_path = directory / f"{story_id}.json"
        with _StoryLock(directory, story_id):
            current = _load_receipt(receipt_path)
            if current and current.get("story_id") not in (None, story_id):
                raise MessengerError("квитанция истории повреждена")
            author = self.identity()["guid"]
            if current and current.get("author_guid") not in (None, author):
                raise PermissionDenied("квитанция принадлежит другому автору")
            if current:
                self._require_same_image_receipt(current, request)
                if current.get("state") not in {
                    "verified",
                    "uploaded",
                    "sent",
                    "unknown",
                    "rejected",
                }:
                    raise MessengerError("квитанция истории повреждена")
            if current and current.get("state") == "verified":
                return self._image_result(
                    request, current, outcome="duplicate", duplicate=True
                )
            if current and current.get("state") in {
                "uploaded",
                "sent",
                "unknown",
                "rejected",
            }:
                return self._reconcile_image_receipt(
                    request, current, receipt_path, directory
                )
            image = _read_local_png(path)
            if (
                image["sha256"] != request["file_sha256"]
                or image["size"] != request["file_size"]
            ):
                raise ConfirmationError("файл изменился после preview")
            record = {
                "story_id": story_id,
                "payload_id": request["payload_id"],
                "chat_id": request["chat_id"],
                "text_sha256": request["text_sha256"],
                "file_sha256": request["file_sha256"],
                "author_guid": author,
                "state": "uploaded",
                "file_id": None,
                "timestamp": None,
            }
            try:
                file_id = self._upload_messenger_png(
                    request["chat_id"],
                    request["payload_id"],
                    request["filename"],
                    image["data"],
                )
            except OperationUncertain:
                record["state"] = "unknown"
                _save_receipt(receipt_path, record)
                return self._image_result(
                    request, record, outcome="unknown", checked=True
                )
            except MessengerError:
                record["state"] = "rejected"
                _save_receipt(receipt_path, record)
                return self._image_result(request, record, outcome="rejected")
            record["file_id"] = file_id
            _save_receipt(receipt_path, record)
            try:
                response = self.fanout(
                    "push",
                    {
                        "ClientMessage": {"Plain": self._image_plain(request, file_id)},
                        "UserIp": "::1",
                    },
                )
            except OperationUncertain:
                record["state"] = "unknown"
                _save_receipt(receipt_path, record)
                return self._reconcile_image_receipt(
                    request, record, receipt_path, directory
                )
            outcome, status = _push_outcome(response)
            record["commit_status"] = status
            if outcome == "rejected":
                record["state"] = "rejected"
                _save_receipt(receipt_path, record)
                return self._image_result(request, record, outcome="rejected")
            if outcome != "confirmed":
                record["state"] = "unknown"
                _save_receipt(receipt_path, record)
                return self._reconcile_image_receipt(
                    request, record, receipt_path, directory
                )
            record["state"] = "sent"
            record["timestamp"] = _push_timestamp(response)
            _save_receipt(receipt_path, record)
            return self._reconcile_image_receipt(
                request, record, receipt_path, directory
            )

    def _require_same_image_receipt(
        self, current: Dict[str, Any], request: Dict[str, Any]
    ) -> None:
        if (
            current.get("payload_id") != request["payload_id"]
            or current.get("chat_id") != request["chat_id"]
            or current.get("text_sha256") != request["text_sha256"]
            or current.get("file_sha256") != request["file_sha256"]
        ):
            raise ValueError("история уже зафиксирована с другим содержимым")

    def _reconcile_image_receipt(
        self,
        request: Dict[str, Any],
        record: Dict[str, Any],
        receipt_path: Path,
        directory: Path,
    ) -> Dict[str, Any]:
        if record.get("state") == "rejected" or not record.get("file_id"):
            outcome = "rejected" if record.get("state") == "rejected" else "unknown"
            return self._image_result(request, record, outcome=outcome)
        found = self._find_delivered_image(request, record)
        if not found:
            record["state"] = "unknown"
            _save_receipt(receipt_path, record)
            return self._image_result(request, record, outcome="unknown", checked=True)
        record["state"] = "verified"
        record["timestamp"] = found.get("timestamp") or record.get("timestamp")
        record["bytes_match"] = self._delivered_bytes_match(
            record["file_id"], request["file_sha256"], request["file_size"], directory
        )
        _save_receipt(receipt_path, record)
        return self._image_result(
            request,
            record,
            outcome="confirmed",
            verification=found,
        )

    def _image_plain(self, request: Dict[str, Any], file_id: str) -> Dict[str, Any]:
        return {
            "ChatId": request["chat_id"],
            "PayloadId": request["payload_id"],
            "Gallery": {
                "Text": request["text"],
                "Items": [
                    {
                        "Image": {
                            "Width": request["width"],
                            "Height": request["height"],
                            "Animated": False,
                            "FileInfo": {
                                "Id2": file_id,
                                "Name": request["filename"],
                                "Size": request["file_size"],
                                "Source": 0,
                            },
                        }
                    }
                ],
            },
        }

    def _find_delivered_image(
        self, request: Dict[str, Any], record: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        author = record.get("author_guid") or self.identity()["guid"]
        before = None
        for _page in range(3):
            payload: Dict[str, Any] = {"ChatId": request["chat_id"], "Limit": 50}
            if before is not None:
                payload["MaxTimestamp"] = before
            response = self.fanout("history", payload)
            rooms = [
                room
                for room in response.get("Chats") or []
                if isinstance(room, dict) and room.get("ChatId") == request["chat_id"]
            ]
            messages = rooms[0].get("Messages") or [] if rooms else []
            oldest = None
            for raw in messages:
                matched = _matching_gallery(
                    raw,
                    chat_id=request["chat_id"],
                    payload_id=request["payload_id"],
                    text=request["text"],
                    file_id=record.get("file_id"),
                    author_guid=author,
                )
                if matched:
                    return matched
                timestamp = _raw_timestamp(raw)
                if isinstance(timestamp, int) and (
                    oldest is None or timestamp < oldest
                ):
                    oldest = timestamp
            if not messages or oldest is None or oldest == before:
                break
            before = oldest
        return None

    def _delivered_bytes_match(
        self, file_id: str, expected_sha256: str, size: int, directory: Path
    ) -> Optional[bool]:
        try:
            remote = self._remote_file_sha256(file_id, size, directory)
        except MessengerError:
            return None
        if not remote:
            return None
        return remote == expected_sha256

    def _remote_file_sha256(
        self, file_id: str, size: int, directory: Path
    ) -> Optional[str]:
        target = directory / f".verify-{uuid.uuid4().hex}.bin"
        preview = self.download_media(
            file_id, target=str(target), max_bytes=max(size, 1)
        )
        if preview.get("status") != "preview_not_applied":
            raise MessengerError("сверка вложения не построила preview")
        try:
            result = self.download_media(
                file_id,
                target=str(target),
                max_bytes=max(size, 1),
                confirm_fingerprint=preview["fingerprint"],
            )
        finally:
            try:
                if target.exists() and not target.is_symlink():
                    target.unlink()
            except OSError:
                pass
        sha = result.get("sha256")
        return sha if isinstance(sha, str) else None

    def _upload_messenger_png(
        self, chat_id: str, payload_id: str, filename: str, data: bytes
    ) -> str:
        parts = [urllib.parse.quote(part, safe="") for part in chat_id.split("/")]
        parts.append(urllib.parse.quote(payload_id, safe=""))
        parts.append(urllib.parse.quote(filename, safe=""))
        request = urllib.request.Request(
            f"https://{_MEDIA_HOST}/media_upload/{'/'.join(parts)}",
            data=data,
            method="POST",
            headers={
                "Authorization": "OAuth " + self._token,
                "Content-Type": "image/png",
                "Accept": "application/json",
            },
        )
        opener = urllib.request.build_opener(_NoRedirect)
        try:
            with opener.open(request, timeout=self.timeout) as response:
                raw = response.read(_MAX_REGISTRY_BYTES + 1)
                status = response.status
        except urllib.error.HTTPError as error:
            if error.code in (301, 302, 303, 307, 308):
                raise MessengerError("media upload redirect запрещен") from error
            if error.code in (401, 403):
                raise PermissionDenied("media upload запрещен") from error
            raise MessengerError(f"media upload HTTP {error.code}") from error
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            raise OperationUncertain(
                "media upload: outcome=unknown; reconcile by PayloadId"
            ) from error
        if not 200 <= status < 300:
            raise MessengerError(f"media upload HTTP {status}")
        if len(raw) > _MAX_REGISTRY_BYTES:
            raise MessengerError("media upload response превышает лимит")
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as error:
            raise MessengerError("media upload вернул некорректный JSON") from error
        if not isinstance(value, dict):
            raise MessengerError("media upload вернул JSON не объект")
        body = value.get("data", value)
        if not isinstance(body, dict):
            raise MessengerError("media upload не вернул file_id")
        file_id = body.get("file_id")
        if not _FILE_ID_RE.fullmatch(str(file_id or "")):
            raise MessengerError("media upload не вернул file_id")
        return str(file_id)

    def _image_result(
        self,
        request: Dict[str, Any],
        record: Dict[str, Any],
        *,
        outcome: str,
        duplicate: bool = False,
        verification: Optional[Dict[str, Any]] = None,
        checked: Optional[bool] = None,
    ) -> Dict[str, Any]:
        if checked is None:
            checked = verification is not None or outcome in {"confirmed", "duplicate"}
        repeated = outcome == "duplicate"
        return {
            "operation": "SendImageWithCaption",
            "outcome": outcome,
            "state": record.get("state"),
            "chat_id": request["chat_id"],
            "story_id": request["story_id"],
            "payload_id": request["payload_id"],
            "file_id": record.get("file_id"),
            "timestamp": record.get("timestamp"),
            "author_guid": record.get("author_guid"),
            "duplicate_suppressed": duplicate or outcome == "duplicate",
            "verification": {
                "checked": checked,
                "found": outcome in {"confirmed", "duplicate"} or bool(verification),
                "author_ok": repeated
                or bool(verification and verification.get("author_ok")),
                "chat_ok": repeated
                or bool(verification and verification.get("chat_ok")),
                "text_ok": repeated
                or bool(verification and verification.get("text_ok")),
                "attachment_ok": repeated
                or bool(verification and verification.get("attachment_ok")),
                "mentions_absent": repeated
                or bool(verification and verification.get("mentions_absent")),
                "bytes_match": record.get("bytes_match"),
            },
        }

    def _plain_mutation(
        self,
        operation: str,
        plain: Dict[str, Any],
        confirm_fingerprint: Optional[str],
    ) -> Dict[str, Any]:
        preview = self._preview(operation, plain)
        if not self._confirmed(preview, confirm_fingerprint):
            return preview.to_dict()
        response = self.fanout("push", {"ClientMessage": {"Plain": plain}})
        info = response.get("MessageInfo")
        outcome, status = _push_outcome(response)
        return {
            "operation": operation,
            "outcome": outcome,
            "reason": _push_rejection_reason(response),
            "message_info": info,
            "status": status,
        }

    def edit_message(
        self,
        chat_id: str,
        timestamp: int,
        text: str,
        *,
        confirm_fingerprint: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not isinstance(text, str) or not text or len(text) > _MAX_TEXT:
            raise ValueError("text должен содержать 1..6000 символов")
        resolved = _chat_id(chat_id)
        ts = _positive_int(timestamp, "timestamp")
        current = self.message_info(resolved, ts)
        message = current.get("message")
        if not message:
            raise MessengerError("редактируемое сообщение не найдено")
        if message.get("author_guid") != self.identity()["guid"]:
            raise PermissionDenied("можно редактировать только свое сообщение")
        payload_id = message.get("payload_id")
        if not payload_id:
            raise MessengerError("у сообщения отсутствует исходный PayloadId")
        plain = {
            "ChatId": resolved,
            "Timestamp": ts,
            "Text": {"MessageText": text},
            # Current web client reuses the original message id for edits.
            # A fresh PayloadId is rejected by production Fanout with Status=15.
            "PayloadId": payload_id,
        }
        return self._plain_mutation("EditMessage", plain, confirm_fingerprint)

    def delete_message(
        self,
        chat_id: str,
        timestamp: int,
        *,
        confirm_fingerprint: Optional[str] = None,
    ) -> Dict[str, Any]:
        # Deletion is encoded by the current web client as an intentionally
        # sparse Plain message. ``Deleted`` and ``PayloadId`` make prod reject
        # the request with Status=15.
        plain = {
            "ChatId": _chat_id(chat_id),
            "Timestamp": _positive_int(timestamp, "timestamp"),
        }
        return self._plain_mutation("DeleteMessage", plain, confirm_fingerprint)

    def pin_message(
        self,
        chat_id: str,
        timestamp: int,
        *,
        confirm_fingerprint: Optional[str] = None,
    ) -> Dict[str, Any]:
        pin = {
            "ChatId": _chat_id(chat_id),
            "Timestamp": _positive_int(timestamp, "timestamp"),
        }
        preview = self._preview("PinMessage", pin)
        if not self._confirmed(preview, confirm_fingerprint):
            return preview.to_dict()
        response = self.fanout("push", {"ClientMessage": {"Pin": pin}})
        outcome, status = _push_outcome(response)
        return {
            "operation": "PinMessage",
            "outcome": outcome,
            "reason": _push_rejection_reason(response),
            "message_info": response.get("MessageInfo"),
            "status": status,
        }

    def unpin_message(
        self,
        chat_id: str,
        *,
        confirm_fingerprint: Optional[str] = None,
    ) -> Dict[str, Any]:
        unpin = {"ChatId": _chat_id(chat_id)}
        preview = self._preview("UnpinMessage", unpin)
        if not self._confirmed(preview, confirm_fingerprint):
            return preview.to_dict()
        response = self.fanout("push", {"ClientMessage": {"Pin": unpin}})
        outcome, status = _push_outcome(response)
        return {
            "operation": "UnpinMessage",
            "outcome": outcome,
            "reason": _push_rejection_reason(response),
            "message_info": response.get("MessageInfo"),
            "status": status,
        }

    def create_poll(
        self,
        chat_id: str,
        question: str,
        options: Sequence[str],
        *,
        payload_id: Optional[str] = None,
        confirm_fingerprint: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not isinstance(question, str) or not question.strip():
            raise ValueError("question не должен быть пустым")
        answers = list(options)
        if not 2 <= len(answers) <= 20 or any(
            not isinstance(option, str) or not option.strip() for option in answers
        ):
            raise ValueError("options должен содержать 2..20 непустых строк")
        plain = {
            "ChatId": _chat_id(chat_id),
            "Poll": {"Title": question, "Answers": answers},
            "PayloadId": payload_id or str(uuid.uuid4()),
        }
        return self._plain_mutation("CreatePoll", plain, confirm_fingerprint)

    def create_group(
        self,
        name: str,
        members: Sequence[str],
        *,
        public: bool = False,
        confirm_fingerprint: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("name не должен быть пустым")
        if not isinstance(public, bool):
            raise ValueError("public должен быть boolean")
        member_list = list(dict.fromkeys(members))
        if any(not _UUID_RE.fullmatch(guid) for guid in member_list):
            raise ValueError("members должны быть guid UUID")
        request = {"name": name, "members": member_list, "public": public}
        preview = self._preview("CreateGroup", request)
        if not self._confirmed(preview, confirm_fingerprint):
            return preview.to_dict()
        params = {"name": name, "members": member_list}
        if public:
            params["public"] = True
        organization_id = self.identity().get("organization_id")
        extra_headers = (
            {"X-Ya-Organization-Id": organization_id} if organization_id else None
        )
        data = self.registry("create_chat", params, extra_headers=extra_headers)
        chat_id = data.get("chat_id")
        verified_members = None
        verify_error = None
        if chat_id and member_list:
            try:
                actual = self.chat_members(chat_id)
                present = {member.get("guid") for member in actual.get("members") or []}
                verified_members = {
                    "added": [guid for guid in member_list if guid in present],
                    "missing": [guid for guid in member_list if guid not in present],
                    "complete": not actual.get("truncated"),
                }
            except Exception as error:
                verify_error = type(error).__name__
        public_flag = data.get("public", data.get("is_public"))
        return {
            "operation": "CreateGroup",
            "outcome": "confirmed" if chat_id else "unknown",
            "chat": data,
            "members_verified": verified_members,
            "verify_error": verify_error,
            "public_confirmed": (
                bool(public_flag) == public if public_flag is not None else None
            ),
        }

    def manage_members(
        self,
        chat_id: str,
        *,
        add: Sequence[str] = (),
        remove: Sequence[str] = (),
        confirm_fingerprint: Optional[str] = None,
    ) -> Dict[str, Any]:
        added = list(dict.fromkeys(add))
        removed = list(dict.fromkeys(remove))
        if not added and not removed:
            raise ValueError("передай add и/или remove")
        if set(added) & set(removed):
            raise ValueError("один guid нельзя одновременно добавить и удалить")
        if any(not _UUID_RE.fullmatch(guid) for guid in added + removed):
            raise ValueError("add/remove должны быть guid UUID")
        request = {"chat_id": _chat_id(chat_id), "add": added, "remove": removed}
        preview = self._preview("ManageMembers", request)
        if not self._confirmed(preview, confirm_fingerprint):
            return preview.to_dict()
        params: Dict[str, Any] = {"chat_id": request["chat_id"]}
        if added:
            params["add"] = added
        if removed:
            params["remove"] = removed
        organization_id = self.identity().get("organization_id")
        extra_headers = (
            {"X-Ya-Organization-Id": organization_id} if organization_id else None
        )
        data = self.registry("update_members", params, extra_headers=extra_headers)
        verified = None
        verify_error = None
        try:
            actual = self.chat_members(request["chat_id"])
            present = {member.get("guid") for member in actual.get("members") or []}
            verified = {
                "added_present": [guid for guid in added if guid in present],
                "added_missing": [guid for guid in added if guid not in present],
                "removed_absent": [guid for guid in removed if guid not in present],
                "removed_present": [guid for guid in removed if guid in present],
                "complete": not actual.get("truncated"),
            }
        except Exception as error:
            verify_error = type(error).__name__
        return {
            "operation": "ManageMembers",
            "outcome": "confirmed" if data.get("version") else "unknown",
            "chat_id": request["chat_id"],
            "version": data.get("version"),
            "members_verified": verified,
            "verify_error": verify_error,
        }

    def wait_reply(
        self,
        chat_id: str,
        *,
        cursor: Optional[int] = None,
        author_guid: Optional[str] = None,
        timeout_s: int = 60,
        poll_every_s: int = 5,
    ) -> Dict[str, Any]:
        timeout_s = _bounded_int(timeout_s, "timeout_s", 1, 120)
        poll_every_s = _bounded_int(poll_every_s, "poll_every_s", 1, 30)
        if author_guid and not _UUID_RE.fullmatch(author_guid):
            raise ValueError("author_guid должен быть UUID")
        baseline = self.read_new(chat_id, cursor=cursor, limit=100)
        boundary = baseline["next_cursor"] if cursor is None else int(cursor)
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            time.sleep(min(poll_every_s, max(deadline - time.monotonic(), 0)))
            result = self.read_new(chat_id, cursor=boundary, limit=100)
            for message in result["messages"]:
                if not author_guid or message.get("author_guid") == author_guid:
                    return {
                        "status": "found",
                        "message": message,
                        "next_cursor": result["next_cursor"],
                    }
            boundary = result["next_cursor"]
        return {"status": "timeout", "next_cursor": boundary}

    def resolve_invite(self, invite: str) -> Dict[str, Any]:
        match = _INVITE_RE.search(str(invite or "").strip())
        if not match:
            raise ValueError("некорректная invite-ссылка или hash")
        invite_hash = match.group(1).lower()
        chats = self.registry("get_chats_info", {"invite_hash": invite_hash}).get(
            "chats"
        )
        if not isinstance(chats, list):
            raise MessengerError("get_chats_info не вернул chats")
        return {
            "invite_hash": invite_hash,
            "found": bool(chats),
            "chat": chats[0] if chats else None,
        }

    def download_media(
        self,
        file_id: str,
        destination: Optional[str] = None,
        *,
        target: Optional[str] = None,
        max_bytes: int = 50 * 1024 * 1024,
        confirm_fingerprint: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not _FILE_ID_RE.fullmatch(str(file_id or "")):
            raise ValueError("некорректный file_id")
        if (destination is None) == (target is None):
            raise ValueError("нужен ровно один target или destination")
        max_bytes = _bounded_int(max_bytes, "max_bytes", 1, _MAX_DOWNLOAD_BYTES)
        requested_target = Path(target or destination or "").expanduser()
        if not requested_target.is_absolute():
            requested_target = Path.cwd() / requested_target
        try:
            target_parent = requested_target.parent.resolve(strict=True)
        except FileNotFoundError:
            target_parent = None
            target_path = Path(os.path.abspath(requested_target))
        else:
            target_path = target_parent / requested_target.name
        request_data = {
            "file_id": file_id,
            "target": str(target_path),
            "max_bytes": max_bytes,
        }
        preview = self._preview("DownloadMedia", request_data)
        if not self._confirmed(preview, confirm_fingerprint):
            return preview.to_dict()
        if target_parent is None:
            raise ValueError("родительский каталог target не существует")
        if not target_parent.is_dir():
            raise ValueError("родитель target должен быть каталогом")
        if os.path.lexists(target_path):
            raise FileExistsError("target уже существует")
        url = "https://%s/file_shortterm/%s" % (
            _MEDIA_HOST,
            urllib.parse.quote(file_id, safe="/._-"),
        )
        headers = {"Authorization": "OAuth " + self._token}
        response = None
        for hop in range(_MAX_REDIRECTS + 1):
            request = urllib.request.Request(url, headers=headers, method="GET")
            try:
                response = self._opener().open(request, timeout=self.timeout)
                break
            except urllib.error.HTTPError as error:
                if error.code not in {301, 302, 303, 307, 308}:
                    raise MessengerError("download HTTP %d" % error.code) from error
                if hop >= _MAX_REDIRECTS:
                    raise MessengerError("слишком много media redirects") from error
                location = error.headers.get("Location")
                next_url = urllib.parse.urljoin(url, location or "")
                parsed = urllib.parse.urlparse(next_url)
                if (
                    parsed.scheme != "https"
                    or not parsed.hostname
                    or not parsed.hostname.endswith(_DOWNLOAD_SUFFIXES)
                    or parsed.username is not None
                    or parsed.password is not None
                    or parsed.port not in (None, 443)
                ):
                    raise MessengerError("media redirect host запрещен") from error
                url = next_url
                headers = {}
        if response is None:
            raise MessengerError("download response отсутствует")
        total = 0
        digest = hashlib.sha256()
        created_target = False
        descriptor: Optional[int] = None
        try:
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(target_path, flags, 0o600)
            created_target = True
            with response, os.fdopen(descriptor, "wb") as output:
                descriptor = None
                while True:
                    chunk = response.read(min(1024 * 1024, max_bytes - total + 1))
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > max_bytes:
                        raise MessengerError("файл превышает max_bytes")
                    digest.update(chunk)
                    output.write(chunk)
                output.flush()
                os.fsync(output.fileno())
        except Exception:
            if descriptor is not None:
                os.close(descriptor)
            try:
                if created_target:
                    target_path.unlink()
            except OSError:
                pass
            raise
        return {
            "operation": "DownloadMedia",
            "outcome": "confirmed",
            "path": str(target_path),
            "bytes": total,
            "sha256": digest.hexdigest(),
        }

    def press_button(
        self,
        chat_id: str,
        timestamp: int,
        *,
        button_text: Optional[str] = None,
        button_index: Optional[int] = None,
        button_id: Optional[str] = None,
        action_id: Optional[str] = None,
        confirm_fingerprint: Optional[str] = None,
    ) -> Dict[str, Any]:
        selectors = [
            value
            for value in (button_text, button_index, button_id)
            if value is not None
        ]
        if len(selectors) != 1:
            raise ValueError("нужен ровно один button_text/button_index/button_id")
        resolved = _chat_id(chat_id)
        timestamp = _positive_int(timestamp, "timestamp")
        info = self.message_info(resolved, timestamp)
        raw = info.get("raw_message")
        if not raw:
            raise MessengerError("сообщение с кнопкой не найдено")
        server = raw.get("ServerMessage") or {}
        message_info = server.get("ServerMessageInfo") or {}
        plain = (server.get("ClientMessage") or {}).get("Plain") or {}
        buttons = _buttons(plain, include_wire=True)
        if button_index is not None:
            index = (
                _bounded_int(int(button_index) + 1, "button_index", 1, len(buttons)) - 1
            )
            selected = buttons[index]
        elif button_id is not None:
            matches = [button for button in buttons if button.get("id") == button_id]
            if len(matches) != 1:
                raise MessengerError("button_id не найден или неоднозначен")
            selected = matches[0]
        else:
            normalized = " ".join(str(button_text or "").split()).lower()
            exact = [
                button
                for button in buttons
                if str(button.get("text") or "").lower() == normalized
            ]
            if len(exact) != 1:
                raise MessengerError("button_text не найден или неоднозначен")
            selected = exact[0]
        if not selected.get("payload_b64"):
            raise MessengerError(
                "кнопка не содержит server_action или client_action/send_message"
            )
        action_type = selected.get("action_type")
        action_name = selected.get("action_name")
        action_payload = selected.get("action_payload") or {}
        if action_type == "client_action" and action_name == "send_message":
            outgoing_text = action_payload.get("text")
            if (
                not isinstance(outgoing_text, str)
                or not outgoing_text
                or len(outgoing_text) > _MAX_TEXT
            ):
                raise MessengerError("send_message кнопка содержит некорректный text")
        elif action_type != "server_action":
            raise MessengerError("тип действия кнопки не поддерживается")
        context = {
            "Timestamp": timestamp,
            "ElementId": selected.get("id"),
            "Version": message_info.get("Version"),
            "AuthorGuid": (message_info.get("From") or {}).get("Guid"),
            "PayloadId": plain.get("PayloadId"),
        }
        context = {key: value for key, value in context.items() if value is not None}
        request_data = {
            "chat_id": resolved,
            "message_context": context,
            "button": {
                "index": selected["index"],
                "id": selected.get("id"),
                "text": selected.get("text"),
                "callback_data": selected.get("callback_data"),
                "action_type": action_type,
                "action_name": action_name,
                "payload_sha256": hashlib.sha256(
                    selected["payload_b64"].encode("ascii")
                ).hexdigest(),
            },
            "action_id": action_id or str(uuid.uuid4()),
        }
        if action_type == "client_action":
            request_data["outgoing_text"] = outgoing_text
        preview = self._preview("PressButton", request_data)
        if not self._confirmed(preview, confirm_fingerprint):
            result = preview.to_dict()
            result["buttons"] = [
                {key: button.get(key) for key in ("index", "id", "text")}
                for button in buttons
            ]
            return result
        if action_type == "client_action":
            callback_data = action_payload.get("callback_data")
            custom_payload = base64.b64encode(
                json.dumps(
                    {"callback_data": callback_data},
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).decode("ascii")
            payload = {
                "ClientMessage": {
                    "Plain": {
                        "ChatId": resolved,
                        "Text": {"MessageText": outgoing_text},
                        "PayloadId": request_data["action_id"],
                        "CustomPayload": custom_payload,
                    }
                }
            }
        else:
            payload = {
                "ClientMessage": {
                    "BotRequest": {
                        "ChatId": resolved,
                        "ActionId": request_data["action_id"],
                        "MessageContext": context,
                        "ServerAction": {
                            "Name": action_name or "button_action",
                            "Payload": selected["payload_b64"],
                        },
                    }
                },
                "UserIp": "::1",
            }
        response = self.fanout("push", payload)
        outcome, status = _push_outcome(response)
        return {
            "operation": "PressButton",
            "outcome": outcome,
            "reason": _push_rejection_reason(response),
            "commit_status": status,
            "button": request_data["button"],
            "message_info": response.get("MessageInfo"),
        }


def capability_matrix() -> Dict[str, Any]:
    """Complete machine-readable high- and low-level API contract."""
    return {
        "operations": sorted(MESSENGER_OPERATIONS),
        "count": len(MESSENGER_OPERATIONS),
        "registry": {
            "read_methods": sorted(REGISTRY_READ_METHODS),
            "write_methods": sorted(REGISTRY_WRITE_METHODS),
            "count": len(REGISTRY_READ_METHODS | REGISTRY_WRITE_METHODS),
        },
        "fanout": {
            "read_paths": sorted(FANOUT_READ_PATHS),
            "write_paths": sorted(FANOUT_WRITE_PATHS),
            "count": len(FANOUT_READ_PATHS | FANOUT_WRITE_PATHS),
        },
        "accounts": sorted(ACCOUNT_TOKENS),
        "environments": sorted(PROFILES),
        "auth": "OAuth yamb:all",
        "interface": "python_api",
        "runtime_dependency_on_skillstore": False,
    }
