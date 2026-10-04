"""Telemost routes use server identities, never names or model-generated URLs.

Routes mirror the client's pathname routing in helpers/history.ts
(getPathByChatId/getPathByGuid/buildRouteUrl). A thread is its own chat route.
"""
import re
from urllib.parse import quote, urlsplit, unquote, parse_qsl, urlencode
from .vendor.messenger.client import thread_chat_id

BASE = 'https://telemost.360.yandex.ru'
GUID = r'[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}'
PRIVATE = re.compile(GUID + '_' + GUID + r'\Z')
GROUP = re.compile(r'\d+/\d+/[A-Za-z0-9_-]+\Z')


def account_link(url, uid):
    """Select the authenticated report owner's account, never a fixed deployment UID."""
    if not re.fullmatch(r'[1-9]\d{0,19}', str(uid)):
        return url
    parsed = urlsplit(url)
    if parsed.scheme != 'https' or parsed.netloc != 'telemost.360.yandex.ru':
        return url
    query = parse_qsl(parsed.query, keep_blank_values=True)
    if any(key == 'selectedUid' for key, _ in query):
        return url
    # Preserve the original query bytes and invitation fragment.
    extra = urlencode({'selectedUid': str(uid)})
    return parsed._replace(query=parsed.query + ('&' if parsed.query else '') + extra).geturl()


def thread_room(chat_id, timestamp):
    if PRIVATE.fullmatch(chat_id):
        return f'110/0/{chat_id}_{timestamp}'
    return thread_chat_id(chat_id, timestamp)


def message_link(message):
    chat_id, ts = message.get('chat_id', ''), message.get('ts')
    if not (GROUP.fullmatch(chat_id) or PRIVATE.fullmatch(chat_id)) or type(ts) is not int or ts <= 0:
        return None
    if message.get('thread'):
        parent = message['thread']
        if type(parent) is not int or parent <= 0:
            return None
        chat_id = thread_room(chat_id, parent)
    return account_link(f'{BASE}/chats/{quote(chat_id, safe="")}/{ts}', message.get('account_uid'))


def person_link(message):
    guid = message.get('author_guid')
    if not isinstance(guid, str) or not re.fullmatch(GUID, guid):
        return None
    return account_link(f'{BASE}/user/{guid}', message.get('account_uid'))


def plain(text):
    """One line of literal Markdown, including literal HTML and link delimiters."""
    return re.sub(r'([\\`*_{}\[\]()<>#!|+])', r'\\\1', ' '.join(text.split()))


def link(label, url):
    return f'[{plain(label)}]({url})' if url else plain(label)


URL = re.compile(r'https?://[^\s<>"\)\]]+')
PERSON = re.compile(r'\{\{(s\d+)\}\}')


def telemost_url(url):
    """Translate verified legacy UI routes only; leave documents/API URLs intact.

    Same conversion as convertLegacyHashRouteToPathname in the official client.
    This also preserves the two timestamps in invitation links to thread replies.
    """
    parsed = urlsplit(url)
    if (parsed.scheme != 'https' or parsed.netloc != 'messenger.360.yandex.ru'
            or parsed.path not in ('', '/') or parsed.query):
        return url
    route = parsed.fragment
    if re.fullmatch(r'/(?:chats/[^/?#]+(?:/\d+)?|user/'+GUID+r')', route):
        return BASE + route
    invite = re.fullmatch(r'/join/('+GUID+r'(?:/\d+){0,2})', route)
    if invite:
        return BASE + '/join#' + invite.group(1)
    return url


def safe_url(url, account_uid=None):
    try:
        parsed = urlsplit(url)
        if parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username or parsed.password:
            return None
        url = account_link(telemost_url(url), account_uid)
        return quote(url, safe=':/?#@!$&\'*+,;=%~_-')
    except ValueError:
        return None


def document_label(url):
    """A readable fallback when the model did not supply a contextual label."""
    parsed = urlsplit(url)
    filename = unquote(parsed.path.rstrip('/').rsplit('/', 1)[-1])
    if re.search(r'\.(pdf|docx?|xlsx?|pptx?|csv|txt|zip|png|jpe?g)\Z', filename, re.I) and len(filename) <= 120:
        return filename
    if parsed.hostname in ('disk.yandex.ru', 'disk.360.yandex.ru', 'yadi.sk'):
        return 'Файл на Диске'
    if parsed.hostname == 'wiki.yandex-team.ru':
        return 'Документ в Wiki'
    return 'Открыть документ'


def body(text, sources):
    # Only our placeholders and evidence-checked HTTP links become markup.
    pattern = re.compile(r'(?P<person>\{\{s\d+\}\})|\[(?P<label>(?:\\.|[^\]\\\n])+)\]\((?P<href>https?://[^\s)]+)\)|(?P<url>'+URL.pattern+')')
    accounts = {str(m['account_uid']) for m in sources.values() if m.get('account_uid')}
    account_uid = next(iter(accounts)) if len(accounts) == 1 else None
    out = []; pos = 0
    for match in pattern.finditer(text):
        out.append(plain(text[pos:match.start()]))
        # Preserve spaces at token boundaries; plain() intentionally strips them.
        if text[pos:match.start()] and text[match.start()-1].isspace():
            out.append(' ')
        if match.group('person'):
            message = sources[match.group('person')[2:-2]]
            out.append(link(message.get('author') or 'Участник', person_link(message)))
        elif match.group('href'):
            url = match.group('href')
            label = re.sub(r'\\([\\`*_{}\[\]()<>#!|+])',r'\1',match.group('label'))
            if not label.strip() or 'http://' in label or 'https://' in label:
                label = document_label(url)
            out.append(link(label, safe_url(url, account_uid)))
        else:
            url = match.group().rstrip('.,;:!?')
            out.append(link(document_label(url), safe_url(url, account_uid)) + match.group()[len(url):])
        pos = match.end()
        if pos < len(text) and text[pos].isspace():
            out.append(' ')
    out.append(plain(text[pos:]))
    return ' '.join(''.join(out).split())
