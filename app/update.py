"""Read-only GitHub release fetch and guarded local installation.

The deploy key belongs to the update repository only. Chat data and runtime
credentials live outside the application directory and are never copied here.
"""
import json
import io
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import tarfile
import time
import urllib.request
import zipfile
import uuid
from pathlib import Path

from . import __version__

TAG = re.compile(r'^refs/tags/v(\d+)\.(\d+)\.(\d+)$')
REPO = re.compile(r'^git@github\.com:JustYT/[A-Za-z0-9_.-]+\.git$')


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
        self.repo = config.get('repository')
        self.key = self.data_dir / 'update_deploy_key'
        self.known_hosts = self.data_dir / 'github_known_hosts'
        self.checkout = self.data_dir / 'update-source'
        self.state_path = self.data_dir / 'update-state.json'
        if not isinstance(self.repo, str) or not REPO.fullmatch(self.repo):
            raise UpdateError('Некорректный адрес репозитория обновлений')
        if not self.key.is_file() or self.key.stat().st_mode & 0o077:
            raise UpdateError('Ключ обновлений отсутствует или открыт другим пользователям')

    def git_env(self):
        env = os.environ.copy()
        env['GIT_TERMINAL_PROMPT'] = '0'
        env['GIT_SSH_COMMAND'] = (
            f'ssh -i {shlex.quote(str(self.key))} -o IdentitiesOnly=yes '
            f'-o StrictHostKeyChecking=accept-new '
            f'-o UserKnownHostsFile={shlex.quote(str(self.known_hosts))}'
        )
        return env

    def run(self, args, timeout=90):
        result = subprocess.run(args, env=self.git_env(), capture_output=True,
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
            if match:
                candidates.append(tuple(map(int, match.groups())))
        newest = max(candidates, default=None)
        return None if newest is None else 'v' + '.'.join(map(str, newest))

    def check(self):
        newest = self.latest()
        return {'current': __version__, 'latest': newest,
                'available': newest is not None and version(newest) > version(__version__)}

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
        if not self.checkout.exists():
            self.run(['git', 'clone', '--no-checkout', self.repo, str(self.checkout)], timeout=180)
        else:
            origin = self.run(['git', '-C', str(self.checkout), 'remote', 'get-url', 'origin']).strip()
            if origin != self.repo:
                raise UpdateError('Источник локального клона не совпадает с настроенным')
            self.run(['git', '-C', str(self.checkout), 'fetch', '--tags', '--prune', 'origin'], timeout=180)
        self.run(['git', '-C', str(self.checkout), 'rev-parse', '--verify', f'refs/tags/{tag}^{{commit}}'])

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
        if not TAG.fullmatch('refs/tags/' + tag) or version(tag) <= version(__version__):
            raise UpdateError('Нужна новая версия в формате vX.Y.Z')
        if tag != self.latest():
            raise UpdateError('Выбранная версия уже не является последней')
        backup = self.install_dir.parent / f'chat-summarizer-previous-{int(time.time())}'
        if backup.exists():
            raise UpdateError('Каталог резервной версии уже существует')
        self.fetch(tag)
        stage = self.stage(tag)
        old_moved = False
        stopped = False
        try:
            subprocess.run(['systemctl', '--user', 'stop', 'chat-studio.service'], check=True)
            stopped = True
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
            if stopped:
                subprocess.run(['systemctl', '--user', 'start', 'chat-studio.service'],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            raise
        finally:
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
