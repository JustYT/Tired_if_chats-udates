"""Verify signed public Git releases before guarded local installation."""
import json
import io
import fcntl
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import tarfile
import threading
import time
import urllib.request
import zipfile
import uuid
from pathlib import Path

from . import __version__

TAG = re.compile(r'^refs/tags/v(\d+)\.(\d+)\.(\d+)$')
PUBLIC_REPO = 'https://github.com/JustYT/Tired_if_chats-udates.git'
LEGACY_REPO = 'git@github.com:JustYT/Tired_if_chats-udates.git'
MIN_SIGNED_VERSION = (2, 3, 0)
SIGNER = 'chat-summarizer'


class UpdateError(Exception):
    pass


def version(value):
    match = re.fullmatch(r'v?(\d+)\.(\d+)\.(\d+)', value)
    if not match:
        raise UpdateError('Некорректная версия обновления')
    return tuple(map(int, match.groups()))


class Updates:
    def __init__(self, data_dir, install_dir):
        self.data_dir = Path(data_dir)
        self.install_dir = Path(install_dir)
        config_path = self.data_dir / 'updates.json'
        try:
            config = json.loads(config_path.read_text(encoding='utf-8'))
        except (OSError, ValueError) as error:
            raise UpdateError('Источник обновлений не настроен') from error
        if config.get('repository') not in (PUBLIC_REPO, LEGACY_REPO):
            raise UpdateError('Некорректный адрес репозитория обновлений')
        self.migrate_legacy = config['repository'] == LEGACY_REPO
        self.config_path = config_path
        self.config = config
        self.repo = PUBLIC_REPO
        self.signing_key = Path(__file__).with_name('release_signing.pub')
        try:
            public_key = self.signing_key.read_text(encoding='ascii').strip()
        except (OSError, UnicodeError) as error:
            raise UpdateError('Ключ проверки подписей отсутствует') from error
        if not re.fullmatch(r'ssh-ed25519 [A-Za-z0-9+/=]+(?: [^\r\n]+)?', public_key):
            raise UpdateError('Некорректный ключ проверки подписей')
        self.public_key = public_key
        self.checkout = self.data_dir / 'update-source'
        self.state_path = self.data_dir / 'update-state.json'
        self._fetch_lock = threading.Lock()

    def git_env(self):
        env = os.environ.copy()
        env['GIT_TERMINAL_PROMPT'] = '0'
        env['GIT_ASKPASS'] = '/usr/bin/false'
        env.pop('GIT_SSH_COMMAND', None)
        return env

    def run(self, args, timeout=90):
        if args[0] != 'git':
            raise UpdateError('Недопустимая команда обновления')
        command = ['git', '-c', 'credential.helper=', '-c', 'http.extraHeader='] + args[1:]
        result = subprocess.run(command, env=self.git_env(), capture_output=True,
                                text=True, timeout=timeout)
        if result.returncode:
            raise UpdateError('Не удалось получить обновления из GitHub')
        return result.stdout

    def latest(self):
        refs = self.run(['git', 'ls-remote', '--refs', '--tags', self.repo])
        candidates = []
        for line in refs.splitlines():
            parts = line.split('\t')
            match = TAG.fullmatch(parts[-1]) if len(parts) == 2 else None
            if match and tuple(map(int, match.groups())) >= MIN_SIGNED_VERSION:
                candidates.append(tuple(map(int, match.groups())))
        newest = max(candidates, default=None)
        if newest is None:
            raise UpdateError('Подписанные релизы не найдены')
        tag = 'v' + '.'.join(map(str, newest))
        self.fetch(tag)
        return tag

    def check(self):
        newest = self.latest()
        marker = self.data_dir / 'update-maintenance'
        if not marker.exists() or time.time() - marker.stat().st_mtime >= 900:
            if self.migrate_legacy:
                self._finish_migration()
            else:
                (self.data_dir / 'update_deploy_key').unlink(missing_ok=True)
        return {'current': __version__, 'latest': newest,
                'available': newest is not None and version(newest) > version(__version__)}

    def _finish_migration(self):
        updated = dict(self.config, repository=PUBLIC_REPO)
        temporary = self.data_dir / '.updates.json.public.tmp'
        try:
            temporary.write_text(json.dumps(updated), encoding='utf-8')
            temporary.chmod(0o600)
            temporary.replace(self.config_path)
            (self.data_dir / 'update_deploy_key').unlink(missing_ok=True)
            self.config = updated
            self.migrate_legacy = False
        finally:
            temporary.unlink(missing_ok=True)

    def state(self):
        try:
            return json.loads(self.state_path.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            return None

    def write_state(self, status, tag=None, error=None):
        payload = {'status': status, 'version': tag, 'error': error,
                   'updated_at': time.time()}
        temporary = self.state_path.with_suffix('.tmp')
        temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')
        temporary.chmod(0o600)
        temporary.replace(self.state_path)

    def launch_apply(self, tag):
        if not TAG.fullmatch('refs/tags/' + tag):
            raise UpdateError('Некорректный тег обновления')
        unit = 'chat-summarizer-update-' + uuid.uuid4().hex[:12]
        command = ['systemd-run', '--user', '--collect', '--quiet', f'--unit={unit}',
                   f'--setenv=CHAT_STUDIO_DATA={self.data_dir}',
                   f'--setenv=CHAT_STUDIO_INSTALL_DIR={self.install_dir}',
                   f'--setenv=PYTHONPATH={self.install_dir}:{self.install_dir / "site"}',
                   '/usr/bin/python3', '-m', 'app.update', 'apply', tag]
        result = subprocess.run(command, capture_output=True, text=True, timeout=15)
        if result.returncode:
            raise UpdateError('Не удалось запустить установку обновления')
        return {'status': 'installing', 'version': tag}

    def fetch(self, tag):
        if not TAG.fullmatch('refs/tags/' + tag) or version(tag) < MIN_SIGNED_VERSION:
            raise UpdateError('Некорректный тег обновления')
        with self._fetch_lock:
            if not self.checkout.exists():
                self.run(['git', 'clone', '--no-checkout', self.repo, str(self.checkout)], timeout=180)
            else:
                origin = self.run(['git', '-C', str(self.checkout), 'remote', 'get-url', 'origin']).strip()
                if origin == LEGACY_REPO and self.migrate_legacy:
                    self.run(['git', '-C', str(self.checkout), 'remote', 'set-url', 'origin', self.repo])
                    origin = self.repo
                if origin != self.repo:
                    raise UpdateError('Источник локального клона не совпадает с настроенным')
                self.run(['git', '-C', str(self.checkout), 'fetch', '--tags', '--prune', 'origin'], timeout=180)
            self.run(['git', '-C', str(self.checkout), 'rev-parse', '--verify', f'refs/tags/{tag}^{{commit}}'])
            self.verify_tag(tag)

    def verify_tag(self, tag):
        allowed = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', prefix='.release-signers-',
                                             dir=self.data_dir, delete=False) as file:
                allowed = Path(file.name)
                file.write(f'{SIGNER} {self.public_key}\n')
            result = subprocess.run([
                'git', '-c', 'credential.helper=', '-c', 'gpg.format=ssh',
                '-c', f'gpg.ssh.allowedSignersFile={allowed}', '-C', str(self.checkout),
                'verify-tag', tag,
            ], env=self.git_env(), capture_output=True, text=True, timeout=30)
            if result.returncode:
                raise UpdateError('Подпись релиза не прошла проверку')
        finally:
            if allowed is not None:
                allowed.unlink(missing_ok=True)

    def stage(self, tag):
        parent = self.install_dir.parent
        stage = Path(tempfile.mkdtemp(prefix='chat-summarizer-next-', dir=parent))
        try:
            result = subprocess.run(['git', '-C', str(self.checkout), 'archive',
                                     '--format=tar', tag, 'app', 'wheels', 'update.sh'],
                                    capture_output=True, timeout=60)
            if result.returncode:
                raise UpdateError('Не удалось подготовить файлы обновления')
            with tarfile.open(fileobj=io.BytesIO(result.stdout)) as archive:
                for member in archive.getmembers():
                    name = Path(member.name)
                    if (name.is_absolute() or '..' in name.parts or
                            name.parts[0] not in {'app', 'wheels', 'update.sh'} or
                            not (member.isdir() or member.isfile())):
                        raise UpdateError('Недопустимый файл в релизе')
                    target = stage / name
                    if member.isdir():
                        target.mkdir(parents=True, exist_ok=True)
                    else:
                        target.parent.mkdir(parents=True, exist_ok=True)
                        with archive.extractfile(member) as source, target.open('wb') as output:
                            shutil.copyfileobj(source, output)
            wheel = stage / 'wheels' / 'websockets-15.0.1-py3-none-any.whl'
            if not (stage / 'app' / 'server.py').is_file() or not wheel.is_file() or not (stage / 'update.sh').is_file():
                raise UpdateError('В релизе отсутствуют необходимые файлы')
            (stage / 'update.sh').chmod(0o700)
            with zipfile.ZipFile(wheel) as package:
                package.extractall(stage / 'site')
            shutil.rmtree(stage / 'wheels')
            check_env = os.environ.copy()
            check_env['PYTHONPYCACHEPREFIX'] = str(stage / '.pycache-validation')
            check = subprocess.run([sys.executable, '-m', 'compileall', '-q', str(stage / 'app')],
                                   env=check_env, capture_output=True, timeout=30)
            if check.returncode:
                raise UpdateError('Новые файлы не проходят проверку Python')
            shutil.rmtree(stage / '.pycache-validation', ignore_errors=True)
            return stage
        except Exception:
            shutil.rmtree(stage, ignore_errors=True)
            raise

    def apply(self, tag):
        lock_path = self.data_dir / 'update.lock'
        with lock_path.open('a+') as lock:
            lock_path.chmod(0o600)
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise UpdateError('Другое обновление уже выполняется') from None
            return self._apply_locked(tag)

    def ensure_idle(self):
        database = self.data_dir / 'studio.sqlite3'
        try:
            with sqlite3.connect(f'file:{database}?mode=ro', uri=True) as db:
                active = db.execute(
                    "SELECT COUNT(*) FROM jobs WHERE status IN ('queued','collecting','summarizing','sending')"
                ).fetchone()[0]
        except sqlite3.Error as error:
            raise UpdateError('Не удалось проверить активные задания') from error
        if active:
            raise UpdateError('Дождитесь окончания текущей саммаризации')

    def snapshot_database(self):
        fd, filename = tempfile.mkstemp(prefix='.preupdate-db-', dir=self.data_dir)
        os.close(fd)
        snapshot = Path(filename)
        try:
            with sqlite3.connect(f'file:{self.data_dir / "studio.sqlite3"}?mode=ro', uri=True) as source, \
                 sqlite3.connect(snapshot) as destination:
                source.backup(destination)
            snapshot.chmod(0o600)
            return snapshot
        except Exception:
            snapshot.unlink(missing_ok=True)
            raise

    def _apply_locked(self, tag):
        if not TAG.fullmatch('refs/tags/' + tag) or version(tag) <= version(__version__):
            raise UpdateError('Нужна новая версия в формате vX.Y.Z')
        if tag != self.latest():
            raise UpdateError('Выбранная версия уже не является последней')
        self.ensure_idle()
        backup = self.install_dir.parent / f'chat-summarizer-previous-{int(time.time())}'
        if backup.exists():
            raise UpdateError('Каталог резервной версии уже существует')
        stage = self.stage(tag)
        old_moved = False
        stopped = False
        database_backup = None
        marker = self.data_dir / 'update-maintenance'
        marker_created = False
        try:
            if marker.exists():
                if time.time() - marker.stat().st_mtime < 900:
                    raise UpdateError('Другое обновление уже выполняется')
                marker.unlink()
            with marker.open('x'):
                pass
            marker_created = True
            marker.chmod(0o600)
            self.ensure_idle()
            subprocess.run(['systemctl', '--user', 'stop', 'chat-studio.service'], check=True)
            stopped = True
            database_backup = self.snapshot_database()
            self.install_dir.rename(backup)
            old_moved = True
            stage.rename(self.install_dir)
            subprocess.run(['systemctl', '--user', 'start', 'chat-studio.service'], check=True)
            port = json.loads((self.data_dir / 'updates.json').read_text())['port']
            for _ in range(40):
                try:
                    with urllib.request.urlopen(f'http://127.0.0.1:{port}/api/health', timeout=1) as response:
                        health = json.load(response)
                    if health.get('version') == tag[1:]:
                        return {'installed': tag}
                except Exception:
                    pass
                time.sleep(.25)
            raise UpdateError('Новая версия не ответила на проверку здоровья')
        except Exception:
            if old_moved:
                subprocess.run(['systemctl', '--user', 'stop', 'chat-studio.service'],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                if self.install_dir.exists():
                    shutil.rmtree(self.install_dir)
                backup.rename(self.install_dir)
                if database_backup is not None:
                    database = self.data_dir / 'studio.sqlite3'
                    Path(str(database) + '-wal').unlink(missing_ok=True)
                    Path(str(database) + '-shm').unlink(missing_ok=True)
                    shutil.copy2(database_backup, database)
                    database.chmod(0o600)
            if stopped:
                subprocess.run(['systemctl', '--user', 'start', 'chat-studio.service'],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            raise
        finally:
            if marker_created:
                marker.unlink(missing_ok=True)
            if database_backup is not None:
                database_backup.unlink(missing_ok=True)
            if stage.exists():
                shutil.rmtree(stage, ignore_errors=True)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('check', 'apply'))
    parser.add_argument('tag', nargs='?')
    args = parser.parse_args()
    data = Path(os.environ.get('CHAT_STUDIO_DATA', str(Path.home() / '.local/share/chat-studio')))
    install = Path(os.environ.get('CHAT_STUDIO_INSTALL_DIR', str(Path.home() / '.local/opt/chat-studio')))
    updates = Updates(data, install)
    try:
        if args.action == 'check':
            result = updates.check()
        else:
            updates.write_state('installing', args.tag)
            result = updates.apply(args.tag or '')
            updates.write_state('installed', args.tag)
    except UpdateError as error:
        if args.action == 'apply':
            updates.write_state('failed', args.tag, str(error))
        raise SystemExit(str(error)) from None
    except Exception:
        if args.action == 'apply':
            updates.write_state('failed', args.tag, 'Ошибка установки; проверьте состояние службы')
        raise SystemExit('Ошибка установки обновления') from None
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
