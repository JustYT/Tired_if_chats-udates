"""Stefania identity, directory and connection adapters."""
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from .store import now
from .read_state import mark_target
from .vendor.messenger.client import MessengerUserClient, private_chat_id
from .vendor.wiki.wiki_client import WikiClient, WikiAPIError

TELEMOST_MEETING = re.compile(
    r'https://(?:telemost\.360\.yandex\.ru|telemost\.yandex\.ru)/(?:j/|join(?:/|#))', re.I)
TELEMOST_MEETING_TITLE = re.compile(
    r'(?=.*\bвстреч[а-яё]*\b)(?=.*\bтелемост[а-яё]*\b)', re.I)


@dataclass(frozen=True)
class Principal:
    login: str


class EnvironmentCredentials:
    """Single-user deployment adapter; tokens never come from an HTTP request.

    A future SSO deployment can replace this with a vault-backed provider while
    preserving principal-scoped storage and adapter interfaces.
    """
    def __init__(self, owner):
        self.owner = owner

    def get(self, principal, service):
        if principal.login != self.owner:
            raise PermissionError('Профиль не подключён')
        return os.environ.get({'messenger': 'MESSENGER_TOKEN', 'wiki': 'WIKI_TOKEN'}[service], '')


class Integrations:
    def __init__(self, credentials):
        self.credentials = credentials
        self.lock = threading.RLock()
        self.statuses = {}
        self.identity = {}
        self.pool = ThreadPoolExecutor(max_workers=3, thread_name_prefix='integration')
        self.operations = threading.BoundedSemaphore(1)

    def messenger(self, principal):
        token = self.credentials.get(principal, 'messenger')
        if not token:
            raise RuntimeError('Подключите Мессенджер через Стефанию')
        return MessengerUserClient(token, account='work', expected_login=principal.login, timeout=15)

    def wiki(self, principal):
        token = self.credentials.get(principal, 'wiki')
        if not token:
            raise RuntimeError('Подключите Wiki через Стефанию')
        return WikiClient(token=token, timeout=15)

    def mark_read(self,principal,target,cancel):
        return mark_target(self.messenger(principal),target,cancel)

    def cached(self, principal):
        with self.lock:
            return dict(self.statuses.get(principal.login, {}))

    def _record(self, principal, key, value):
        value = {**value, 'checked_at': now()}
        with self.lock:
            self.statuses.setdefault(principal.login, {})[key] = value
        return value

    def check_messenger(self, principal):
        try:
            identity = self.messenger(principal).identity()
            with self.lock:
                self.identity[principal.login] = identity.get('display_name') or principal.login
            state = {'state': 'ok', 'label': 'Подключён', 'detail': identity['login']}
        except Exception as error:
            state = self.failure(error, 'Мессенджер')
        return self._record(principal, 'messenger', state)

    def check_bot(self, principal, login):
        if not login:
            return self._record(principal, 'bot', {'state': 'missing', 'label': 'Не выбран', 'detail': 'Укажите логин личного бота', 'lookup_login': login})
        try:
            client = self.messenger(principal)
            actor = client.identity()
            users = client.find_user(login, limit=20)['users']
            matches = [u for u in users if u.get('login') == login]
            if len(matches) != 1:
                raise ValueError('Не удалось однозначно найти бота по логину')
            user = matches[0]
            state = {'state': 'found', 'label': 'Найден', 'detail': user.get('display_name') or login,
                     'login': login, 'guid': user['guid'], 'chat_id': private_chat_id(actor['guid'], user['guid']),
                     'delivery_verified': False,
                     'note': 'Диалог найден. Токен отправки подключён.' if os.environ.get('CHAT_STUDIO_BOT_TOKEN') and login==os.environ.get('CHAT_STUDIO_BOT_LOGIN') else 'Токен отправки от этого бота не подключён.'}
        except Exception as error:
            state = self.failure(error, 'Бот')
        return self._record(principal, 'bot', {**state, 'lookup_login': login})

    def check_wiki(self, principal, slug):
        try:
            client = self.wiki(principal)
            me = client.me()
            if me.get('username') != principal.login:
                raise PermissionError('Wiki подключена под другим пользователем')
            root = 'users/' + me['username']
            parent = client.get_page(slug=root)
            state = {'state': 'ok', 'label': 'Подключена', 'detail': root, 'parent_id': parent['id'],
                     'url': 'https://wiki.yandex-team.ru/' + root + '/', 'target': slug,
                     'write_verified': False, 'note': 'Личный раздел доступен. Саммари создаются отдельными страницами и наследуют доступы папки.'}
        except Exception as error:
            state = self.failure(error, 'Wiki')
        return self._record(principal, 'wiki', {**state, 'target': slug})

    @staticmethod
    def failure(error, service):
        # Never return exception bodies, transport headers or credentials to UI/logs.
        if isinstance(error, WikiAPIError):
            detail = f'Wiki API: {error.status}. Проверьте доступ через Стефанию.'
        elif isinstance(error, PermissionError):
            detail = 'Подключён другой пользователь или нет доступа'
        elif isinstance(error, ValueError):
            detail = 'Не найдено точного совпадения по логину'
        else:
            detail = 'Проверьте подключение Стефании и корпоративную сеть'
        return {'state': 'error', 'label': 'Нет подключения', 'detail': detail}

    def check_all(self, principal, destinations):
        futures = [self.pool.submit(self.check_messenger, principal),
                   self.pool.submit(self.check_bot, principal, destinations['bot_login']),
                   self.pool.submit(self.check_wiki, principal, destinations['wiki_slug'])]
        for future in futures:
            future.result()
        return self.cached(principal)

    @staticmethod
    def collect_pages(method, result_key, limit):
        result, cursor, seen = [], None, set()
        for _ in range(200):
            page = method(limit=limit, cursor=cursor)
            result.extend(page[result_key])
            cursor = page.get('next_cursor')
            if not cursor:
                return result
            if cursor in seen:
                raise RuntimeError('Повтор курсора; список не заменён')
            seen.add(cursor)
        raise RuntimeError('Превышен предел страниц; список не заменён')

    @staticmethod
    def merge_catalog(profiles, catalog, actor):
        # Match Messenger's isChatInExternalOrganization: absent/null
        # organization_ids means external. An empty array does NOT mean external.
        # The private directory only supplies names for external 1:1 chats.
        names = {r['chat_id']: r for r in profiles}
        org, me = actor.get('organization_id'), actor['guid']
        result = {}
        for row in catalog:
            is_telemost = bool(TELEMOST_MEETING.search(str(row.get('description') or '')) or
                               TELEMOST_MEETING_TITLE.search(str(row.get('name') or '')))
            organizations = row.get('organization_ids')
            external = organizations is None
            if not external and str(org) not in {str(x) for x in organizations}:
                continue
            chat_id = row['chat_id']
            if row.get('private'):
                if not external:
                    continue
                if chat_id == me + '_' + me:
                    continue  # Notes to self are not a 1:1 conversation.
                person = names.get(chat_id, {})
                if person.get('to_guid') == me:
                    continue
                result[chat_id] = {'id': chat_id, 'kind': 'external',
                    'title': person.get('display_name') or person.get('login') or row.get('name') or 'Внешний диалог',
                    'nickname': person.get('login') or '', 'members': 2,
                    'is_telemost': is_telemost}
            else:
                result[chat_id] = {'id': chat_id, 'kind': 'external' if external else 'group',
                    'title': row.get('name') or 'Группа без названия', 'nickname': '',
                    'members': row.get('members_count') or 0, 'is_telemost': is_telemost}
        return list(result.values())

    def chats(self, principal):
        # Independent streams run concurrently; cursors remain sequential.
        def private():
            client = self.messenger(principal)
            client.identity()
            return self.collect_pages(client.list_private_chats, 'private_chats', 100)
        def catalog():
            client = self.messenger(principal)
            actor = client.identity()
            return actor, self.collect_pages(client.list_chats, 'chats', 100)
        one, two = self.pool.submit(private), self.pool.submit(catalog)
        profiles = one.result()
        actor, rows = two.result()
        return self.merge_catalog(profiles, rows, actor)
