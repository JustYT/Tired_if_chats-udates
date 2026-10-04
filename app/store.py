"""SQLite storage. Every row and operation is scoped to the authenticated principal."""
import copy
import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path


def now():
    return datetime.now(timezone.utc).isoformat()


def defaults(login, bot_login=''):
    return {
        'summary': {'provider': 'stefania', 'model': '', 'mark_read': False,
                    'daily': {'compression': 50},
                    'weekly': {'compression': 50, 'group_by': 'days'}},
        'destinations': {'bot': False, 'wiki': False, 'bot_login': bot_login,
                         'wiki_slug': f'users/{login}/chat-summaries'},
        'schedule': {'timezone': 'Europe/Moscow',
                     'daily': {'enabled': False, 'days': [0, 1, 2, 3, 4, 5, 6],
                               'times': ['19:00'] * 7, 'bot': False, 'wiki': False},
                     'weekly': {'enabled': False, 'day': 0, 'time': '09:00',
                                'bot': False, 'wiki': False}},
    }


def normalize_summary(summary):
    """Keep compression while upgrading old UI preferences to the current format."""
    result = copy.deepcopy(summary)
    result.setdefault('mark_read', False)
    if 'daily' not in result and 'weekly' not in result:
        strength = result.pop('compression', 50)
        result.pop('group_topics', None)
        result.pop('split', None)
        result['daily'] = {'compression': strength}
        result['weekly'] = {'compression': strength, 'group_by': 'days'}
    else:
        result['daily'] = {'compression': result['daily']['compression']}
        result['weekly'] = {'compression': result['weekly']['compression'],
                            'group_by': result['weekly'].get('group_by', 'days')}
    return result


def normalize_schedule(schedule, destinations):
    """Seed per-period delivery choices from the former shared switches."""
    result = copy.deepcopy(schedule)
    for kind in ('daily', 'weekly'):
        for target in ('bot', 'wiki'):
            result[kind].setdefault(target, destinations.get(target, False))
    return result


def targets_for_kind(settings, kind):
    """Old queued jobs retain their shared destination snapshot."""
    mode = settings['schedule']['weekly' if kind == 'weekly' else 'daily']
    return {target: mode.get(target, settings['destinations'].get(target, False))
            for target in ('bot', 'wiki')}


def summary_for_kind(summary, kind):
    normalized = normalize_summary(summary)
    mode = 'weekly' if kind=='weekly' else 'daily'
    return {key: normalized[key] for key in ('provider','model','mark_read')} | normalized[mode]


class Conflict(Exception):
    pass


class Store:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.lock = threading.RLock()
        with self.connect() as db:
            db.executescript('''
              PRAGMA journal_mode=WAL;
              CREATE TABLE IF NOT EXISTS profiles (
                login TEXT PRIMARY KEY, settings TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 0,
                stopped INTEGER NOT NULL DEFAULT 1, updated TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS chats (
                login TEXT NOT NULL, id TEXT NOT NULL, kind TEXT NOT NULL,
                title TEXT NOT NULL, nickname TEXT NOT NULL, members INTEGER NOT NULL,
                available INTEGER NOT NULL DEFAULT 1, is_telemost INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(login,id));
              CREATE TABLE IF NOT EXISTS selections (
                login TEXT NOT NULL, chat_id TEXT NOT NULL, PRIMARY KEY(login,chat_id));
              CREATE TABLE IF NOT EXISTS syncs (login TEXT PRIMARY KEY, updated TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, login TEXT NOT NULL,
                kind TEXT NOT NULL, text TEXT NOT NULL, created TEXT NOT NULL);
            ''')
            if 'is_telemost' not in {row[1] for row in db.execute('PRAGMA table_info(chats)')}:
                db.execute('ALTER TABLE chats ADD COLUMN is_telemost INTEGER NOT NULL DEFAULT 0')
        self.path.chmod(0o600)

    def connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        db.row_factory = sqlite3.Row
        return db

    def ensure(self, login, bot_login=''):
        with self.lock, self.connect() as db:
            db.execute('INSERT OR IGNORE INTO profiles(login,settings,updated) VALUES(?,?,?)',
                       (login, json.dumps(defaults(login, bot_login)), now()))

    def snapshot(self, login):
        with self.connect() as db:
            row = db.execute('SELECT * FROM profiles WHERE login=?', (login,)).fetchone()
            if not row:
                raise KeyError('unknown profile')
            chats = [dict(r) for r in db.execute(
                "SELECT id,kind,title,nickname,members,available,is_telemost FROM chats WHERE login=? ORDER BY CASE WHEN kind='external' THEN 0 ELSE 1 END,title COLLATE NOCASE", (login,))]
            selected = [r[0] for r in db.execute('SELECT chat_id FROM selections WHERE login=?', (login,))]
            synced = db.execute('SELECT updated FROM syncs WHERE login=?', (login,)).fetchone()
            events = [dict(r) for r in db.execute(
                'SELECT id,kind,text,created FROM events WHERE login=? ORDER BY id DESC LIMIT 25', (login,))]
        settings=json.loads(row['settings'])
        # Legacy values seed both independent modes; saved jobs remain untouched.
        settings['summary'] = normalize_summary(settings['summary'])
        settings['schedule'] = normalize_schedule(settings['schedule'], settings['destinations'])
        return {'settings': settings, 'revision': row['revision'],
                'stopped': bool(row['stopped']), 'updated': row['updated'], 'chats': chats,
                'selected': selected, 'synced_at': synced[0] if synced else None, 'events': events}

    def save(self, login, settings, selected, revision):
        with self.lock, self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            known = {r[0] for r in db.execute('SELECT id FROM chats WHERE login=?', (login,))}
            if not set(selected).issubset(known):
                raise ValueError('В выборе есть неизвестный чат. Обновите список.')
            changed = db.execute('UPDATE profiles SET settings=?,revision=revision+1,updated=? WHERE login=? AND revision=?',
                                 (json.dumps(settings, ensure_ascii=False), now(), login, revision))
            if changed.rowcount != 1:
                raise Conflict('Настройки изменились в другой вкладке. Обновите страницу.')
            db.execute('DELETE FROM selections WHERE login=?', (login,))
            db.executemany('INSERT INTO selections(login,chat_id) VALUES(?,?)', ((login, c) for c in selected))
            self._event(db, login, 'saved', 'Настройки и выбор чатов сохранены')
        return self.snapshot(login)

    def replace_chats(self, login, chats):
        # Only a complete paginated response replaces the previous catalog.
        with self.lock, self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('UPDATE chats SET available=0 WHERE login=?', (login,))
            for c in chats:
                db.execute('''INSERT INTO chats(login,id,kind,title,nickname,members,available,is_telemost) VALUES(?,?,?,?,?,?,1,?)
                    ON CONFLICT(login,id) DO UPDATE SET kind=excluded.kind,title=excluded.title,
                    nickname=excluded.nickname,members=excluded.members,available=1,
                    is_telemost=excluded.is_telemost''',
                           (login, c['id'], c['kind'], c['title'], c.get('nickname', ''),
                            c.get('members', 0), int(c.get('is_telemost', False))))
            db.execute('DELETE FROM chats WHERE login=? AND available=0 AND id NOT IN (SELECT chat_id FROM selections WHERE login=?)', (login,login))
            db.execute('INSERT OR REPLACE INTO syncs VALUES(?,?)', (login, now()))
            self._event(db, login, 'sync', f'Список обновлён: {len(chats)} чатов')

    def stop(self, login):
        with self.lock, self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT settings FROM profiles WHERE login=?', (login,)).fetchone()
            config = json.loads(row[0])
            config['schedule']['daily']['enabled'] = False
            config['schedule']['weekly']['enabled'] = False
            db.execute('UPDATE profiles SET settings=?,stopped=1,revision=revision+1,updated=? WHERE login=?',
                       (json.dumps(config), now(), login))
            self._event(db, login, 'stopped', 'Глобальная остановка: оба расписания выключены')
        return self.snapshot(login)

    @staticmethod
    def _event(db, login, kind, text):
        db.execute('INSERT INTO events(login,kind,text,created) VALUES(?,?,?,?)', (login, kind, text, now()))
        db.execute('DELETE FROM events WHERE login=? AND id NOT IN (SELECT id FROM events WHERE login=? ORDER BY id DESC LIMIT 200)', (login,login))
