"""Persistent, principal-scoped queue. No automatic retries of uncertain writes."""
import json
import uuid
from datetime import datetime, timedelta, timezone
from .store import now, Conflict, summary_for_kind
from .timing import estimate, TIMING_VERSION

ACTIVE = ('queued', 'collecting', 'summarizing', 'sending')

class Jobs:
    def __init__(self, store):
        self.store = store
        with store.connect() as db:
            db.executescript('''
            CREATE TABLE IF NOT EXISTS jobs(
              id TEXT PRIMARY KEY, login TEXT NOT NULL, kind TEXT NOT NULL,
              dedupe TEXT NOT NULL, status TEXT NOT NULL, progress TEXT NOT NULL,
              snapshot TEXT NOT NULL, start TEXT NOT NULL, end TEXT NOT NULL,
              created TEXT NOT NULL, updated TEXT NOT NULL, result TEXT NOT NULL DEFAULT '',
              stats TEXT NOT NULL DEFAULT '{}', error TEXT NOT NULL DEFAULT '',
              UNIQUE(login,dedupe));
            CREATE TABLE IF NOT EXISTS deliveries(
              job_id TEXT NOT NULL, login TEXT NOT NULL, target TEXT NOT NULL,
              part INTEGER NOT NULL, status TEXT NOT NULL, receipt TEXT NOT NULL DEFAULT '',
              PRIMARY KEY(job_id,login,target,part));
            CREATE INDEX IF NOT EXISTS jobs_by_owner_date ON jobs(login,created);
            ''')

    def prune(self, login):
        """Keep at most three weeks of completed run history and its receipts."""
        cutoff=(datetime.now(timezone.utc)-timedelta(days=21)).isoformat()
        terminal=('completed','failed','cancelled','interrupted')
        with self.store.lock, self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('''DELETE FROM deliveries WHERE login=? AND job_id IN
                (SELECT id FROM jobs WHERE login=? AND created<? AND status IN (?,?,?,?))''',
                (login,login,cutoff,*terminal))
            db.execute('''DELETE FROM jobs WHERE login=? AND created<? AND status IN (?,?,?,?)''',
                (login,cutoff,*terminal))

    def recover(self, login):
        with self.store.lock, self.store.connect() as db:
            db.execute("UPDATE deliveries SET status='unknown' WHERE login=? AND status='sending'", (login,))
            db.execute("UPDATE jobs SET status='interrupted',error='Служба перезапущена. Отправка могла состояться; автоматического повтора нет.',updated=? WHERE login=? AND status IN ('queued','collecting','summarizing','sending')", (now(),login))

    def list(self, login):
        self.prune(login)
        with self.store.connect() as db:
            rows=[dict(r) for r in db.execute('SELECT id,kind,status,progress,start,end,created,updated,result,stats,error FROM jobs WHERE login=? ORDER BY created DESC',(login,))]
            for r in rows:
                r['stats']=json.loads(r['stats'])
                r['deliveries']=[dict(d) for d in db.execute('SELECT target,part,status,receipt FROM deliveries WHERE login=? AND job_id=? ORDER BY target,part',(login,r['id']))]
                r['timing'] = estimate(r)
        return rows

    def timing_history(self, login, settings, kind='today'):
        with self.store.connect() as db:
            rows = db.execute("SELECT kind,snapshot,stats FROM jobs WHERE login=? AND status='completed' ORDER BY created DESC LIMIT 20", (login,)).fetchall()
        result = {}
        for row in rows:
            if (row['kind']=='weekly') != (kind=='weekly'): continue
            summary = summary_for_kind(json.loads(row['snapshot'])['settings']['summary'],row['kind'])
            if any(summary.get(k)!=settings.get(k) for k in ('provider','model','reasoning_effort','compression','unread_only')): continue
            timing=json.loads(row['stats']).get('timing', {})
            if timing.get('version')!=TIMING_VERSION: continue
            for phase, values in timing.get('durations', {}).items():
                result.setdefault(phase, []).extend(v for v in values if isinstance(v, (int,float)) and 0<v<3600)
        return {k: v[:60] for k,v in result.items()}

    def get(self, login, job_id):
        with self.store.connect() as db:
            r=db.execute('SELECT * FROM jobs WHERE login=? AND id=?',(login,job_id)).fetchone()
        if not r: raise KeyError('unknown job')
        r=dict(r); r['snapshot']=json.loads(r['snapshot']); r['stats']=json.loads(r['stats'])
        return r

    def enqueue(self, login, kind, snapshot, start, end, dedupe):
        with self.store.lock, self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute('SELECT id FROM jobs WHERE login=? AND dedupe=?',(login,dedupe)).fetchone()
            if old: return old[0],False
            if db.execute("SELECT 1 FROM jobs WHERE login=? AND status IN ('queued','collecting','summarizing','sending')",(login,)).fetchone():
                raise Conflict('Уже есть активное задание. Дождитесь завершения или остановите его.')
            job_id=uuid.uuid4().hex
            db.execute('INSERT INTO jobs(id,login,kind,dedupe,status,progress,snapshot,start,end,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                (job_id,login,kind,dedupe,'queued','Ожидает обработки',json.dumps(snapshot,ensure_ascii=False),start.isoformat(),end.isoformat(),now(),now()))
            self.store._event(db,login,'job','Задание поставлено в очередь')
        return job_id,True

    def update(self, login, job_id, **values):
        if not set(values)<={'status','progress','result','stats','error'}: raise ValueError('fields')
        if 'stats' in values: values['stats']=json.dumps(values['stats'])
        values['updated']=now()
        with self.store.lock,self.store.connect() as db:
            db.execute('UPDATE jobs SET '+','.join(k+'=?' for k in values)+' WHERE login=? AND id=?',(*values.values(),login,job_id))

    def delivery(self, login, job_id, target, part, status, receipt=''):
        with self.store.lock,self.store.connect() as db:
            db.execute('INSERT INTO deliveries VALUES(?,?,?,?,?,?) ON CONFLICT(job_id,login,target,part) DO UPDATE SET status=excluded.status,receipt=excluded.receipt',(job_id,login,target,part,status,receipt))
