"""Profile-scoped Codex subscription login and text-only App Server adapter.

Credentials are managed exclusively by Codex in a private service profile.
No desktop configuration, API keys or corporate credentials enter the child.
"""
import copy
import json
import os
import queue
import re
import signal
import stat
import subprocess
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit
from . import __version__

from .history import RunError, checkpoint
from .summarizer import SYSTEM, StefaniaModel, compression_instruction, scope_instruction

FAILURE = 'Codex недоступен. Проверьте подключение, выбранную модель и лимит подписки.'


def sandbox_command(binary, home):
    actual = Path(binary).resolve(strict=True)
    info = actual.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
        raise RunError('Codex должен быть установлен администратором и защищён от записи.')
    if not all(Path(p).is_file() for p in ('/usr/bin/bwrap', '/usr/bin/prlimit')):
        raise RunError('На сервере не настроена изоляция Codex.')
    args = ['/usr/bin/prlimit', '--nofile=512:512', '--nproc=1024:1024', '--cpu=960:960',
            '--', '/usr/bin/bwrap', '--unshare-user', '--unshare-pid', '--unshare-ipc',
            '--unshare-uts', '--die-with-parent', '--new-session',
            '--ro-bind', '/usr', '/usr', '--ro-bind', '/lib', '/lib']
    if Path('/lib64').exists(): args += ['--ro-bind', '/lib64', '/lib64']
    # Nix binaries use their immutable runtime closure, not the host user home.
    if actual.is_relative_to('/nix/store'):
        args += ['--dir', '/nix', '--ro-bind', '/nix/store', '/nix/store']
    args += ['--proc', '/proc', '--dev', '/dev', '--tmpfs', '/tmp', '--dir', '/etc',
             '--ro-bind', '/etc/resolv.conf', '/etc/resolv.conf',
             '--ro-bind', '/etc/ssl', '/etc/ssl',
             '--ro-bind', str(actual.parent), '/opt/codex',
             '--dir', '/home', '--bind', str(home), '/home/codex',
             '--tmpfs', '/workspace', '--chdir', '/workspace', '--',
             '/opt/codex/' + actual.name, 'app-server', '--listen', 'stdio://',
             '-c', 'cli_auth_credentials_store="file"']
    env = {'HOME': '/home/codex', 'CODEX_HOME': '/home/codex',
           'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8', 'TMPDIR': '/tmp'}
    return args, env


class Rpc:
    def __init__(self, binary, home):
        args, env = sandbox_command(binary, home)
        self.lock = threading.RLock()
        self.closed = False
        self.next_id = 0
        self.waiters = {}
        self.events = queue.Queue(maxsize=2048)
        self.started = threading.Event()
        self.child = None
        # Linux PDEATHSIG follows the spawning *thread*. A short-lived HTTP
        # handler cannot own a --die-with-parent device-login process.
        self.reader = threading.Thread(target=self._launch, args=(args, env, home), daemon=True)
        self.reader.start()
        if not self.started.wait(10) or self.child is None: raise RunError(FAILURE)

    def _launch(self, args, env, home):
        try:
            self.child = subprocess.Popen(args, cwd=home, env=env, start_new_session=True,
                                          stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                          stderr=subprocess.DEVNULL)
        except Exception:
            self.closed = True
            self.started.set()
            return
        self.started.set()
        self._read()

    def _write(self, value):
        with self.lock:
            if self.closed: raise RunError(FAILURE)
            try:
                self.child.stdin.write((json.dumps(value, ensure_ascii=False)+'\n').encode())
                self.child.stdin.flush()
            except Exception: raise RunError(FAILURE) from None

    def _read(self):
        try:
            while True:
                line = self.child.stdout.readline(4*1024*1024+1)
                if not line or len(line)>4*1024*1024: break
                message = json.loads(line)
                if 'id' in message and message.get('method'):
                    self._write({'id': message['id'], 'error': {'code': -32601, 'message': 'Unsupported request'}})
                elif 'id' in message:
                    with self.lock:
                        waiter = self.waiters.get(message['id'])
                        if waiter: waiter.put_nowait(message)
                elif message.get('method') in ('account/login/completed', 'item/completed', 'turn/completed'):
                    self.events.put_nowait(message)
        except Exception:
            pass  # Never expose raw diagnostics, tokens, message content or signed URLs.
        finally:
            with self.lock:
                self.closed = True
                for waiter in self.waiters.values():
                    if waiter.empty(): waiter.put_nowait({'error': True})

    def request(self, method, params=None, timeout=20, cancel=None):
        waiter = queue.Queue(maxsize=1)
        with self.lock:
            self.next_id += 1
            ident = self.next_id
            self.waiters[ident] = waiter
        try:
            self._write({'id': ident, 'method': method, 'params': params or {}})
            deadline = time.monotonic()+timeout
            while time.monotonic()<deadline:
                if cancel is not None: checkpoint(cancel)
                try: result = waiter.get(timeout=min(.1, max(.001, deadline-time.monotonic())))
                except queue.Empty: continue
                if 'error' in result: raise RunError(FAILURE)
                return result.get('result', {})
            raise RunError('Codex не ответил вовремя. Попробуйте подключиться снова.')
        finally:
            with self.lock: self.waiters.pop(ident, None)

    def notify(self, method):
        self._write({'method': method, 'params': {}})

    def close(self):
        with self.lock: self.closed = True
        try: os.killpg(self.child.pid, signal.SIGTERM)
        except ProcessLookupError: pass
        try: self.child.wait(timeout=3)
        except subprocess.TimeoutExpired:
            try: os.killpg(self.child.pid, signal.SIGKILL)
            except ProcessLookupError: pass
            self.child.wait(timeout=3)
        self.reader.join(timeout=1)
        self.child.stdin.close()
        self.child.stdout.close()


class Profile:
    def __init__(self, home):
        self.home = home
        self.lock = threading.RLock()
        self.processes = set()
        self.pending = None
        self.epoch = 0
        self.cached = None
        self.cached_at = 0
        self.models = None
        self.models_at = 0


class CodexAuth:
    def __init__(self, root, binary=None, rpc_factory=Rpc):
        self.root = Path(root)
        self.binary = binary or os.environ.get('CHAT_STUDIO_CODEX_BIN', str(Path.home()/'.nix-profile/bin/codex'))
        self.rpc_factory = rpc_factory
        self.profiles = {}
        self.lock = threading.Lock()

    def profile(self, login):
        if not re.fullmatch(r'[a-z][a-z0-9._-]{1,48}', login): raise ValueError('Invalid profile')
        with self.lock:
            if login not in self.profiles:
                home = self.root / login
                if self.root.is_symlink() or home.is_symlink(): raise RunError('Недопустимый каталог Codex.')
                home.mkdir(mode=0o700, parents=True, exist_ok=True)
                home.chmod(0o700)
                self.profiles[login] = Profile(home)
            return self.profiles[login]

    def _open(self, p):
        rpc = self.rpc_factory(self.binary, p.home)
        p.processes.add(rpc)
        try:
            rpc.request('initialize', {'clientInfo': {'name': 'chat_studio', 'title': 'Саммаризатор чатов', 'version': __version__}})
            rpc.notify('initialized')
            return rpc
        except Exception:
            self._close(p, rpc)
            raise

    def _close(self, p, rpc):
        rpc.close()
        p.processes.discard(rpc)

    @staticmethod
    def _account(rpc):
        account = rpc.request('account/read', {'refreshToken': False}).get('account')
        if not isinstance(account, dict) or account.get('type') != 'chatgpt':
            return {'available': True, 'connected': False}
        return {'available': True, 'connected': True,
                'email': str(account.get('email') or '')[:254], 'plan': str(account.get('planType') or '')[:80]}

    def _cache(self, p, value):
        p.cached = value
        p.cached_at = time.monotonic()
        return copy.deepcopy(value)

    def _models(self, p, rpc, refresh=False):
        if not refresh and p.models is not None and time.monotonic()-p.models_at<300:
            return copy.deepcopy(p.models)
        result, seen, cursor = [], set(), None
        deadline = time.monotonic()+30
        for _ in range(20):
            remaining = deadline-time.monotonic()
            if remaining<=0: raise RunError(FAILURE)
            params = {'limit': 100, 'includeHidden': False}
            if cursor: params['cursor'] = cursor
            page = rpc.request('model/list', params, timeout=min(20, remaining))
            if not isinstance(page.get('data'), list): raise RunError(FAILURE)
            for row in page['data']:
                if row.get('hidden') or 'text' not in row.get('inputModalities', ['text', 'image']): continue
                model = row.get('model')
                if not isinstance(model, str) or not re.fullmatch(r'[A-Za-z0-9._:/ -]{1,100}', model): continue
                if any(m['model']==model for m in result): continue
                efforts = [e['reasoningEffort'] for e in row.get('supportedReasoningEfforts', [])
                           if e.get('reasoningEffort') in ('none','minimal','low','medium','high','xhigh','max','ultra')]
                result.append({'model': model, 'label': str(row.get('displayName') or model)[:120],
                               'is_default': row.get('isDefault') is True, 'efforts': efforts,
                               'default_effort': row.get('defaultReasoningEffort')})
            cursor = page.get('nextCursor')
            if not cursor: break
            if not isinstance(cursor, str) or cursor in seen: raise RunError(FAILURE)
            seen.add(cursor)
        else: raise RunError(FAILURE)
        p.models, p.models_at = result, time.monotonic()
        return copy.deepcopy(result)

    def models(self, login, refresh=False):
        p = self.profile(login)
        with p.lock:
            if p.pending: return {'models': [], 'error': 'Сначала завершите вход в Codex.'}
            rpc = None
            try:
                rpc = self._open(p)
                if not self._account(rpc)['connected']:
                    p.models = None
                    return {'models': [], 'error': 'Войдите в Codex, чтобы выбрать модель.'}
                return {'models': self._models(p, rpc, refresh)}
            except Exception:
                return {'models': [], 'error': 'Не удалось загрузить модели Codex. Обновите список.'}
            finally:
                if rpc: self._close(p, rpc)

    def status(self, login):
        p = self.profile(login)
        with p.lock:
            if p.pending:
                return {'available': True, 'connected': False, 'pending': copy.deepcopy(p.pending['public'])}
            if p.cached is not None and time.monotonic()-p.cached_at<10:
                return copy.deepcopy(p.cached)
            rpc = None
            try:
                rpc = self._open(p)
                return self._cache(p, self._account(rpc))
            except Exception:
                return self._cache(p, {'available': False, 'connected': False,
                                      'error': 'Подключение Codex недоступно. Проверьте настройку сервера.'})
            finally:
                if rpc: self._close(p, rpc)

    def start(self, login):
        p = self.profile(login)
        with p.lock:
            if p.pending: return self.status(login)
            rpc = self._open(p)
            try:
                current = self._account(rpc)
                if current['connected']:
                    self._close(p, rpc)
                    return self._cache(p, current)
                response = rpc.request('account/login/start', {'type': 'chatgptDeviceCode'}, timeout=45)
                url, code, ident = (response.get(k) for k in ('verificationUrl', 'userCode', 'loginId'))
                parsed = urlsplit(url) if isinstance(url, str) else None
                if (not parsed or parsed.scheme!='https' or parsed.netloc!='auth.openai.com'
                        or parsed.query or parsed.fragment or not isinstance(code, str)
                        or not re.fullmatch(r'[-A-Za-z0-9]{4,32}', code) or not isinstance(ident, str) or not ident):
                    raise RunError('Codex вернул некорректный ответ для входа.')
                pending = {'rpc': rpc, 'login_id': ident, 'public': {'url': url, 'code': code, 'expires_at': time.time()+900}}
                p.pending = pending
                p.cached = None
                threading.Thread(target=self._watch, args=(p, pending), daemon=True).start()
                return self.status(login)
            except Exception:
                self._close(p, rpc)
                raise

    def _watch(self, p, pending):
        rpc = pending['rpc']
        completed = False
        while time.time()<pending['public']['expires_at'] and not rpc.closed:
            try: event = rpc.events.get(timeout=.2)
            except queue.Empty: continue
            if event.get('method')=='account/login/completed' and event.get('params', {}).get('loginId')==pending['login_id']:
                completed = bool(event['params'].get('success'))
                break
        with p.lock:
            if p.pending is not pending: return
            p.pending = None
            try:
                value = self._account(rpc) if completed else {'available': True, 'connected': False,
                    'error': 'Вход не завершён или код истёк. Получите новый код.'}
                self._cache(p, value)
            except Exception:
                self._cache(p, {'available': True, 'connected': False, 'error': FAILURE})
            finally: self._close(p, rpc)

    def logout(self, login):
        p = self.profile(login)
        with p.lock:
            p.epoch += 1
            p.pending = None
            p.models = None
            # Kill every refresher before deleting this profile's credentials.
            for rpc in list(p.processes): self._close(p, rpc)
            rpc = None
            try:
                rpc = self._open(p)
                rpc.request('account/logout')
            except Exception: pass
            finally:
                if rpc: self._close(p, rpc)
            (p.home/'auth.json').unlink(missing_ok=True)
            return self._cache(p, {'available': True, 'connected': False})

    def generate(self, login, prompt, settings, cancel):
        p = self.profile(login)
        with p.lock:
            checkpoint(cancel)
            epoch = p.epoch
            rpc = self._open(p)
        try:
            if not self._account(rpc)['connected']: raise RunError('Войдите в личную подписку Codex в настройках саммари.')
            with p.lock: catalog = self._models(p, rpc)
            requested = settings.get('model', '').strip()
            selected = next((m for m in catalog if m['model']==requested), None) if requested else next((m for m in catalog if m['is_default']), None)
            if not selected: raise RunError('Выбранная модель недоступна в Codex. Обновите список моделей в настройках.')
            model = selected['model']
            requested_effort = settings.get('reasoning_effort', 'auto')
            effort = ('high' if 'high' in selected['efforts'] else selected['default_effort']) if requested_effort == 'auto' else requested_effort
            if effort not in selected['efforts']:
                raise RunError('Выбранный уровень рассуждения недоступен для модели. Обновите настройки модели.')
            config = {'model_reasoning_effort': effort, 'features.shell_tool': False,
                      'features.exec_tool': False, 'features.multi_agent': False,
                      'web_search': 'disabled'}
            params = {'cwd': '/workspace', 'ephemeral': True, 'approvalPolicy': 'never', 'sandbox': 'read-only',
                      'baseInstructions': SYSTEM+'\n'+scope_instruction(settings)+'\nНе используй инструменты, файлы, сеть и других агентов.\n'+compression_instruction(settings),
                      'config': config}
            if model: params['model'] = model
            started = rpc.request('thread/start', params, timeout=45, cancel=cancel)
            if model and started.get('model')!=model: raise RunError('Codex не подтвердил выбранную модель. Саммари не запущено.')
            if started.get('reasoningEffort')!=effort: raise RunError('Codex не подтвердил настройки генерации.')
            thread_id = started.get('thread', {}).get('id')
            if not isinstance(thread_id, str) or not thread_id: raise RunError(FAILURE)
            turn = {'threadId': thread_id, 'effort': effort, 'input': [{'type': 'text', 'text': prompt, 'text_elements': []}]}
            if model: turn['model'] = model
            response = rpc.request('turn/start', turn, timeout=45, cancel=cancel)
            turn_id = response.get('turn', {}).get('id')
            if not turn_id: raise RunError(FAILURE)
            deadline, answer = time.monotonic()+720, ''
            while time.monotonic()<deadline:
                checkpoint(cancel)
                if p.epoch!=epoch: raise RunError('Подключение Codex отключено. Запуск отменён.')
                try: event = rpc.events.get(timeout=.2)
                except queue.Empty:
                    if rpc.closed: raise RunError(FAILURE)
                    continue
                params = event.get('params', {})
                if params.get('threadId')!=thread_id: continue
                if event['method']=='item/completed' and params.get('turnId')==turn_id:
                    item = params.get('item', {})
                    if item.get('type')=='agentMessage' and isinstance(item.get('text'), str): answer=item['text']
                if event['method']=='turn/completed' and params.get('turn', {}).get('id')==turn_id:
                    if params['turn'].get('status')!='completed' or not answer.strip(): raise RunError(FAILURE)
                    checkpoint(cancel)
                    with p.lock:
                        if p.epoch!=epoch: raise RunError('Подключение Codex отключено. Запуск отменён.')
                        text = re.sub(r'^```(?:json)?\s*|\s*```$', '', answer.strip())
                        try: return json.loads(text)
                        except Exception: raise RunError('Codex вернул неверный формат. Саммари не отправлено.') from None
            raise RunError('Codex не завершил саммари за отведённое время.')
        finally:
            with p.lock:
                if rpc in p.processes: self._close(p, rpc)


class ModelRouter:
    def __init__(self, codex, login):
        self.codex, self.login, self.stefania = codex, login, StefaniaModel()

    def readiness(self, settings):
        if settings['provider']=='stefania': return self.stefania.readiness(settings)
        state = self.codex.status(self.login)
        return {'ready': state['connected'], 'label': 'Codex подключён' if state['connected'] else 'Войдите в Codex / ChatGPT',
                'detail': settings.get('model') or 'Модель по умолчанию в Codex'}

    def generate(self, prompt, settings, cancel):
        if settings['provider']=='stefania': return self.stefania.generate(prompt, settings, cancel)
        return self.codex.generate(self.login, prompt, settings, cancel)
