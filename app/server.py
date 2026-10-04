"""Chat Summarizer: local-only HTTP service, reached through an SSH tunnel."""
import copy
import getpass
import json
import mimetypes
import os
import re
import secrets
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, unquote
from . import __version__
from .store import Store, Conflict
from .engine import Engine
from .codex import CodexAuth, ModelRouter
from .history import RunError
from .jobs import ACTIVE
from .integrations import Principal, Integrations, EnvironmentCredentials
from .update import Updates, UpdateError
from .splitty import SplittyProbe

STATIC = Path(__file__).parent / 'static'
CONTENT_ITEMS = [
    {'id': 'topics', 'title': 'Ключевые темы', 'description': 'Что обсуждали и к каким решениям пришли'},
    {'id': 'questions', 'title': 'Вопросы и ответы', 'description': 'Озвученные вопросы, ответы и вопросы без ответа'},
    {'id': 'releases', 'title': 'Релизы', 'description': 'Что выпустили и какие изменения анонсировали'},
    {'id': 'launches', 'title': 'Запуски', 'description': 'Новые проекты, эксперименты и инициативы'},
    {'id': 'documents', 'title': 'Новые документы', 'description': 'Опубликованные документы и ссылки на них'},
    {'id': 'discussions', 'title': 'Активные обсуждения', 'description': 'Популярные сообщения с длинными обсуждениями'},
]


def fields(value, expected):
    if not isinstance(value, dict) or set(value) != set(expected):
        raise ValueError('Неверная структура настроек')


def boolean(value):
    if type(value) is not bool:
        raise ValueError('Ожидался переключатель')


def clock(value):
    if not isinstance(value, str) or not re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d', value):
        raise ValueError('Укажите время в формате ЧЧ:ММ')


def validate(payload, login):
    fields(payload, ['settings', 'selected', 'revision'])
    if type(payload['revision']) is not int or payload['revision'] < 0:
        raise ValueError('Неверная версия настроек')
    selected = payload['selected']
    if not isinstance(selected, list) or len(selected) > 10000 or any(not isinstance(x,str) or not x or len(x)>512 for x in selected):
        raise ValueError('Неверный список чатов')
    if len(selected) != len(set(selected)):
        raise ValueError('В выборе есть дубли')
    config = payload['settings']
    fields(config, ['summary', 'destinations', 'schedule'])
    summary = config['summary']
    if isinstance(summary, dict) and 'compression' in summary:
        raise ValueError('Настройки разделены на ежедневные и недельные. Обновите страницу перед сохранением.')
    fields(summary, ['provider', 'model', 'daily', 'weekly', 'mark_read'])
    if summary['provider'] not in ['stefania', 'codex']:
        raise ValueError('Неизвестный формат или провайдер')
    if not isinstance(summary['model'], str) or len(summary['model']) > 100 or not re.fullmatch(r'[a-zA-Z0-9._:/ -]*', summary['model']):
        raise ValueError('Некорректное название модели')
    for kind in ('daily','weekly'):
        mode = summary[kind]
        fields(mode, ['compression'] if kind == 'daily' else ['compression', 'group_by'])
        if type(mode['compression']) is not int or not 0 <= mode['compression'] <= 100:
            raise ValueError('Сжатие должно быть от 0 до 100')
    if summary['weekly']['group_by'] not in ('days', 'chats'):
        raise ValueError('Неизвестная группировка недельного саммари')
    boolean(summary['mark_read'])
    dest = config['destinations']
    fields(dest, ['bot', 'wiki', 'bot_login', 'wiki_slug'])
    boolean(dest['bot']); boolean(dest['wiki'])
    if not isinstance(dest['bot_login'], str) or (dest['bot_login'] and not re.fullmatch(r'[a-z][a-z0-9._-]{1,48}', dest['bot_login'])):
        raise ValueError('Укажите staff-логин бота')
    if dest['bot'] and not dest['bot_login']:
        raise ValueError('Для отправки в бот нужен его логин')
    slug = dest['wiki_slug']
    if not isinstance(slug, str) or len(slug)>300 or not slug.startswith(f'users/{login}/') or not re.fullmatch(r'[a-zA-Z0-9/_-]+', slug):
        raise ValueError('Wiki: укажите страницу внутри своего раздела users/' + login + '/')
    if any(not s for s in slug.split('/')):
        raise ValueError('Wiki: в адресе есть пустой сегмент')
    schedule = config['schedule']
    fields(schedule, ['timezone', 'daily', 'weekly'])
    if schedule['timezone'] != 'Europe/Moscow':
        raise ValueError('Используется московское время')
    daily, weekly = schedule['daily'], schedule['weekly']
    fields(daily, ['enabled', 'days', 'times', 'bot', 'wiki'])
    fields(weekly, ['enabled', 'day', 'time', 'bot', 'wiki'])
    boolean(daily['enabled']); boolean(weekly['enabled'])
    for mode in (daily, weekly):
        boolean(mode['bot']); boolean(mode['wiki'])
        if mode['enabled'] and not (mode['bot'] or mode['wiki']):
            raise ValueError('Для включённого расписания выберите бот или Wiki')
    if (daily['bot'] or weekly['bot']) and not dest['bot_login']:
        raise ValueError('Для отправки в бот нужен его логин')
    if not isinstance(daily['days'],list) or any(type(x) is not int or x not in range(7) for x in daily['days']) or len(set(daily['days'])) != len(daily['days']):
        raise ValueError('Неверные дни недели')
    if daily['enabled'] and not daily['days']:
        raise ValueError('Выберите хотя бы один день')
    if not isinstance(daily['times'], list) or len(daily['times']) != 7:
        raise ValueError('Укажите время для каждого дня')
    for t in daily['times']: clock(t)
    if type(weekly['day']) is not int or weekly['day'] not in range(7):
        raise ValueError('Неверный день недельного отчёта')
    clock(weekly['time'])
    return copy.deepcopy(config), selected, payload['revision']


class Application:
    def __init__(self, data_path, owner, bot_login='', integrations=None, codex=None, splitty=None):
        if not re.fullmatch(r'[a-z][a-z0-9._-]{1,48}', owner):
            raise ValueError('Invalid configured corporate login')
        self.principal = Principal(owner)
        self.store = Store(data_path)
        self.store.ensure(owner, bot_login)
        self.integrations = integrations or Integrations(EnvironmentCredentials(owner))
        self.codex = codex or CodexAuth(Path(data_path).parent/'codex-profiles')
        self.engine = Engine(self.store, self.integrations, self.principal,
                             model=ModelRouter(self.codex, owner), splitty=splitty or SplittyProbe())
        self.stefania = self.engine.model.stefania
        self.csrf = secrets.token_urlsafe(32)
        self.operation_lock = threading.Lock()
        self.operation = None
        self.updates = None
        self.update_status = {'current': __version__, 'latest': None, 'available': False,
                              'checking': False, 'installing': False,
                              'configured': False, 'error': ''}
        if (Path(data_path).parent / 'updates.json').is_file():
            try:
                self.updates = Updates(Path(data_path).parent, Path(__file__).resolve().parent.parent)
                self.update_status['configured'] = True
                threading.Thread(target=self._update_loop, daemon=True).start()
            except UpdateError as error:
                self.update_status['error'] = str(error)

    def _update_loop(self):
        while True:
            self.check_updates()
            threading.Event().wait(6 * 3600)

    def check_updates(self):
        if not self.updates:
            raise UpdateError('Обновления не настроены')
        self.update_status['checking'] = True
        try:
            self.update_status.update(self.updates.check())
            self.update_status['error'] = ''
        except (UpdateError, OSError, subprocess.TimeoutExpired) as error:
            self.update_status['error'] = 'Не удалось проверить обновления'
        finally:
            self.update_status['checking'] = False

    def bootstrap(self):
        login = self.principal.login
        snap = self.store.snapshot(login)
        connections = self.integrations.cached(self.principal)
        dest = snap['settings']['destinations']
        if connections.get('bot', {}).get('lookup_login') != dest['bot_login']:
            connections.pop('bot', None)
        if connections.get('wiki', {}).get('target') != dest['wiki_slug']:
            connections.pop('wiki', None)
        splitty = self.engine.splitty.check()
        return {**snap, 'user': {'login': login, 'display_name': self.integrations.identity.get(login, login)},
                'csrf': self.csrf, 'connections': connections, 'splitty': splitty,
                'codex': self.codex.status(login),
                'capabilities': {'summarization': True, 'sending': True, 'scheduler': True,
                                 'test_sends_immediately': True},
                'readiness': self.engine.readiness(snap,splitty_state=splitty), 'jobs': self.engine.jobs.list(login),
                'content_items': CONTENT_ITEMS, 'operation': self.operation,
                'updates': {**self.update_status,
                            'last_result': self.updates.state() if self.updates else None}}

    def perform(self, name, action):
        if not self.operation_lock.acquire(blocking=False):
            raise Conflict('Проверка или обновление уже выполняется')
        self.operation = name
        try:
            return action()
        finally:
            self.operation = None
            self.operation_lock.release()

    def post(self, route, body):
        principal = self.principal
        if route == '/api/settings':
            settings, selected, revision = validate(body, principal.login)
            self.store.save(principal.login, settings, selected, revision)
        elif route == '/api/chats/refresh':
            self.perform('sync', lambda: self.store.replace_chats(principal.login, self.integrations.chats(principal)))
        elif route == '/api/connections/check':
            dest = self.store.snapshot(principal.login)['settings']['destinations']
            def check_connections():
                self.engine.splitty.check(fresh=True)
                self.integrations.check_all(principal, dest)
            self.perform('check', check_connections)
        elif route == '/api/updates/check':
            fields(body, [])
            self.perform('update-check', self.check_updates)
        elif route == '/api/updates/install':
            fields(body, ['version'])
            tag = body['version']
            if (not self.updates or self.update_status['installing'] or
                    tag != self.update_status['latest'] or not self.update_status['available']):
                raise UpdateError('Сначала проверьте доступную версию')
            if any(job['status'] in ACTIVE for job in self.engine.jobs.list(principal.login)):
                raise Conflict('Дождитесь окончания текущей саммаризации')
            self.perform('update-install', lambda: self.updates.launch_apply(tag))
            self.update_status['installing'] = True
        elif route == '/api/control/stop':
            self.engine.stop()
        elif route == '/api/codex/login':
            fields(body, [])
            self.perform('codex-login', lambda: self.codex.start(principal.login))
        elif route == '/api/codex/models':
            fields(body, [])
            return 200, self.codex.models(principal.login, refresh=True)
        elif route == '/api/stefania/models':
            fields(body, [])
            return 200, self.stefania.models(refresh=True)
        elif route == '/api/codex/logout':
            fields(body, [])
            def disconnect():
                # Stop a Codex job before revoking its login so no result can be
                # delivered after disconnect. Other providers stay independent.
                with self.engine.gate:
                    current = self.store.snapshot(principal.login)['settings']['summary']['provider']
                    active = any(j['status'] in ACTIVE and self.engine.jobs.get(principal.login, j['id'])['snapshot']['settings']['summary']['provider']=='codex'
                                 for j in self.engine.jobs.list(principal.login))
                    if current=='codex' or active: self.engine.stop()
                    self.codex.logout(principal.login)
            self.perform('codex-logout', disconnect)
        elif route in ['/api/control/start', '/api/control/test-today', '/api/control/test-last-week']:
            fields(body, ['request_id'])
            if not isinstance(body['request_id'],str) or not re.fullmatch(r'[a-zA-Z0-9-]{16,80}',body['request_id']):
                raise ValueError('Invalid request id')
            kind='weekly' if route.endswith('/test-last-week') else 'today'
            self.engine.launch(kind,request_id=body['request_id'],arm=route.endswith('/start'))
        else:
            return 404, {'error': 'Метод не найден'}
        return 200, self.bootstrap()


def handler(app):
    class Handler(BaseHTTPRequestHandler):
        server_version = 'ChatSummarizer'
        sys_version = ''
        def log_message(self, fmt, *args):
            # Do not log request payloads, query strings or personal chat metadata.
            pass

        def headers_ok(self):
            port = self.server.server_port
            hosts = {f'localhost:{port}', f'127.0.0.1:{port}'}
            if self.headers.get('Host') not in hosts:
                self.json(403, {'error': 'Откройте сервис через локальный SSH-туннель'})
                return False
            if self.headers.get('Sec-Fetch-Site') == 'cross-site':
                self.json(403, {'error': 'Межсайтовый запрос отклонён'})
                return False
            origin = self.headers.get('Origin')
            if origin and origin not in {'http://' + h for h in hosts}:
                self.json(403, {'error': 'Неверный источник запроса'})
                return False
            return True

        def send(self, status, body, kind):
            self.send_response(status)
            self.send_header('Content-Type', kind)
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('X-Frame-Options', 'DENY')
            self.send_header('Referrer-Policy', 'no-referrer')
            self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; font-src 'self'; connect-src 'self'; frame-ancestors 'none'; form-action 'self'; base-uri 'none'")
            self.end_headers()
            try: self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError): pass

        def json(self, status, value):
            self.send(status, json.dumps(value, ensure_ascii=False).encode(), 'application/json; charset=utf-8')

        def do_GET(self):
            if not self.headers_ok(): return
            path = urlsplit(self.path).path
            if path == '/api/bootstrap':
                self.json(200, app.bootstrap()); return
            if path == '/api/codex/models':
                self.json(200, app.codex.models(app.principal.login)); return
            if path == '/api/stefania/models':
                self.json(200, app.stefania.models()); return
            if path == '/api/health':
                self.json(200, {'status': 'ok', 'version': __version__, 'summarization': True}); return
            if path.startswith('/api/'):
                self.json(404, {'error': 'Метод не найден'}); return
            target = (STATIC / unquote(path).lstrip('/')).resolve() if path != '/' else STATIC/'index.html'
            if not target.is_relative_to(STATIC.resolve()) or not target.is_file():
                self.json(404, {'error': 'Файл не найден'}); return
            kind = mimetypes.guess_type(str(target))[0] or 'application/octet-stream'
            if kind.startswith('text/') or kind == 'application/javascript': kind += '; charset=utf-8'
            self.send(200, target.read_bytes(), kind)

        def do_POST(self):
            if not self.headers_ok(): return
            if not secrets.compare_digest(self.headers.get('X-CSRF-Token',''), app.csrf):
                self.json(403, {'error': 'Сессия сервера обновилась. Перезагрузите страницу.'}); return
            if self.headers.get('Content-Type','').split(';')[0] != 'application/json':
                self.json(415, {'error': 'Ожидался JSON'}); return
            try:
                length = int(self.headers.get('Content-Length','0'))
                if not 0 < length <= 2*1024*1024:
                    self.json(413, {'error': 'Слишком большой запрос'}); return
                self.connection.settimeout(15)
                data = json.loads(self.rfile.read(length))
                if not isinstance(data,dict): raise ValueError('Ожидался объект')
                status, result = app.post(urlsplit(self.path).path, data)
                self.json(status,result)
            except Conflict as error:
                self.json(409, {'error': str(error)})
            except RunError as error:
                self.json(502, {'error': str(error)})
            except UpdateError as error:
                self.json(502, {'error': str(error)})
            except (ValueError, TypeError, KeyError):
                self.json(400, {'error': 'Некорректные настройки. Проверьте поля и время.'})
            except Exception:
                self.json(502, {'error': 'Не удалось получить данные. Проверьте подключения и повторите.'})
    return Handler


def main():
    os.umask(0o077)
    owner = os.environ.get('CHAT_STUDIO_OWNER', getpass.getuser())
    data = Path(os.environ.get('CHAT_STUDIO_DATA', str(Path.home()/'.local/share/chat-studio')))
    app = Application(data/'studio.sqlite3', owner, os.environ.get('CHAT_STUDIO_BOT_LOGIN',''))
    app.engine.serve()
    port = int(os.environ.get('CHAT_STUDIO_PORT','8765'))
    server = ThreadingHTTPServer(('127.0.0.1',port), handler(app))
    server.daemon_threads = True
    print('Chat Summarizer ready on loopback', flush=True)
    server.serve_forever()


if __name__ == '__main__': main()
