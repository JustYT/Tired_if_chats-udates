"""Check the Splitty network path used by the service's Linux account."""
import os
import re
import shutil
import subprocess
import threading
import time


CHECK_URL = 'https://proxy-check.sec.yandex.net'
SUCCESS = 'Splitty is properly configured and ready to go!'
UNAVAILABLE = 'Splitty не подключён. Проверьте службу Splitty и корпоративную сеть.'


class SplittyProbe:
    def __init__(self, ttl=15):
        self.ttl = ttl
        self.lock = threading.Lock()
        self.checked_at = 0
        self.result = None

    def check(self, fresh=False):
        with self.lock:
            if not fresh and self.result is not None and time.monotonic() - self.checked_at < self.ttl:
                return self.result.copy()
            self.result = self._probe()
            self.checked_at = time.monotonic()
            return self.result.copy()

    def _probe(self):
        failed = {'ready': False, 'state': 'error', 'label': 'Не подключен'}
        search_path = os.pathsep.join((os.environ.get('PATH', ''),
                                       '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/bin'))
        splitty = shutil.which('splitty', path=search_path)
        curl = shutil.which('curl', path=search_path)
        if not splitty or not curl:
            return failed
        # These child processes need the current OS identity, but no service tokens.
        env = {'PATH': search_path,
               'HOME': os.path.expanduser('~'), 'LANG': 'C.UTF-8'}
        try:
            status = subprocess.run([splitty, 'status'], capture_output=True,
                                    text=True, timeout=3, env=env)
            if status.returncode or not re.search(r'^status:[ \t]*OK[ \t]*$', status.stdout, re.MULTILINE):
                return failed
            response = subprocess.run([curl, '--noproxy', '*', '--silent', '--show-error',
                                       '--fail', '--max-time', '8', CHECK_URL],
                                      capture_output=True, text=True, timeout=10, env=env)
            if response.returncode or SUCCESS not in response.stdout:
                return failed
        except (OSError, subprocess.TimeoutExpired):
            return failed
        return {'ready': True, 'state': 'found', 'label': 'Подключен'}
