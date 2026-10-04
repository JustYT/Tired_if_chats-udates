"""Worker, Moscow calendar scheduling and stop boundary."""
import threading
import uuid
from datetime import datetime,timedelta
from .jobs import Jobs, ACTIVE
from .history import TZ, Cancelled, RunError, checkpoint, collect
from .summarizer import StefaniaModel,summarize,render
from .delivery import Destinations,split_text,DeliveryUnknown
from .store import Conflict,now,summary_for_kind,targets_for_kind
from .read_state import read_targets
from .timing import RunTiming
from .splitty import SplittyProbe, UNAVAILABLE

def period(kind,at):
    at=at.astimezone(TZ)
    day=at.replace(hour=0,minute=0,second=0,microsecond=0)
    if kind=='weekly':
        end=day-timedelta(days=day.weekday());return end-timedelta(days=7),end
    return day,at

def due_slots(schedule,armed,at):
    at=at.astimezone(TZ);armed=armed.astimezone(TZ);day=at.replace(hour=0,minute=0,second=0,microsecond=0)
    due=[]
    for kind in ('daily','weekly'):
        s=schedule[kind]
        if not s['enabled']: continue
        if kind=='daily':
            if at.weekday() not in s['days']:continue
            t=s['times'][at.weekday()]
        else:
            if at.weekday()!=s['day']: continue
            t=s['time']
        h,m=map(int,t.split(':'));slot=day.replace(hour=h,minute=m)
        if armed<=slot<=at: due.append((kind,slot))
    return due

class Engine:
    def __init__(self,store,integrations,principal,model=None,destinations=None,splitty=None):
        self.store=store;self.integrations=integrations;self.principal=principal
        self.jobs=Jobs(store);self.model=model or StefaniaModel();self.destinations=destinations or Destinations(integrations)
        self.splitty=splitty if splitty is not None else SplittyProbe()
        self.gate=threading.RLock();self.cancel=threading.Event();self.worker=None;self.shutdown=threading.Event()
        with store.connect() as db:
            db.execute('CREATE TABLE IF NOT EXISTS controls(login TEXT PRIMARY KEY,armed_at TEXT NOT NULL)')

    def readiness(self,snap,kind=None,splitty_state=None):
        model=self.model.readiness(snap['settings']['summary']);dest=snap['settings']['destinations'];reasons=[]
        if not (splitty_state if splitty_state is not None else self.splitty.check())['ready']:
            reasons.append(UNAVAILABLE)
        targets=(targets_for_kind(snap['settings'],kind) if kind else
                 {target:any(targets_for_kind(snap['settings'],mode)[target] for mode in ('daily','weekly'))
                  for target in ('bot','wiki')})
        if not snap['selected']:reasons.append('Выберите хотя бы один чат.')
        available={c['id'] for c in snap['chats'] if c['available']}
        if not set(snap['selected'])<=available:reasons.append('В выборе есть недоступный чат.')
        if not model['ready']:reasons.append(model['label'])
        if not targets['bot'] and not targets['wiki']:reasons.append('Выберите направление отправки.')
        if targets['bot'] and not self.destinations.bot_ready(dest['bot_login']):reasons.append('Отправка от выбранного бота не подключена.')
        return {'ready':not reasons,'reasons':reasons,'model':model}

    def launch(self,kind='today',request_id=None,slot=None,arm=False):
        with self.gate:
            login=self.principal.login;snap=self.store.snapshot(login)
            ready=self.readiness(snap,kind,self.splitty.check(fresh=True))
            if not ready['ready']:raise Conflict(' '.join(ready['reasons']))
            at=slot or datetime.now(TZ);start,end=period(kind,at)
            dedupe=kind+':'+(slot.isoformat() if slot else (request_id or uuid.uuid4().hex))
            job_id,created=self.jobs.enqueue(login,kind,snap,start,end,dedupe)
            if not created:return job_id
            if arm:
                with self.store.lock,self.store.connect() as db:
                    db.execute('UPDATE profiles SET stopped=0,revision=revision+1,updated=? WHERE login=?',(now(),login))
                    db.execute('INSERT OR REPLACE INTO controls VALUES(?,?)',(login,at.isoformat()))
            self.cancel=threading.Event()
            self.worker=threading.Thread(target=self.run,args=(job_id,self.cancel),daemon=True,name='summary-worker')
            self.worker.start()
            return job_id

    def stop(self):
        # Held across each external write. After this call returns no new write can start.
        self.cancel.set()
        with self.gate:
            self.cancel.set()
            self.store.stop(self.principal.login)
            for job in self.jobs.list(self.principal.login):
                if job['status'] in ACTIVE:self.jobs.update(self.principal.login,job['id'],progress='Остановка запрошена. Ожидаем завершения текущего запроса.')

    def run(self,job_id,cancel):
        p=self.principal;job=self.jobs.get(p.login,job_id);snap=job['snapshot'];settings=snap['settings'];dest=settings['destinations']
        targets_for_run=targets_for_kind(settings,job['kind'])
        summary=summary_for_kind(settings['summary'],job['kind'])
        timing=RunTiming(self.jobs.timing_history(p.login,summary,job['kind']))
        counts={}
        def update(**kw):
            counts.update(kw.pop('stats',{}))
            if kw.get('status') in ('completed','failed','cancelled'): timing.finish()
            self.jobs.update(p.login,job_id,stats={**counts,'timing':timing.snapshot()},**kw)
        timing.changed=lambda:update()
        delivered=False
        try:
            checkpoint(cancel)
            update(status='collecting',progress='Читаем сообщения и проверяем активность старых веток')
            chats=[c for c in snap['chats'] if c['id'] in snap['selected']]
            timing.stage('collecting',len(chats))
            start=datetime.fromisoformat(job['start']);end=datetime.fromisoformat(job['end'])
            def collected(text, stats):
                update(progress=text,stats=stats)
                timing.advance()
            messages,stats=collect(self.integrations,p,chats,start,end,cancel,collected)
            update(status='summarizing',progress='Готовим саммари',stats=stats)
            if not self.splitty.check(fresh=True)['ready']: raise RunError(UNAVAILABLE)
            items=summarize(self.model,messages,summary,cancel,lambda text:update(progress=text),timing=timing)
            checkpoint(cancel)
            text=render(items,messages,summary,start,end,stats,kind=job['kind'])
            update(result=text,status='sending',progress='Отправляем в выбранные направления')
            targets=[]
            if targets_for_run['bot']:
                parts=split_text(text)
                targets.extend(('bot',n,part,len(parts)) for n,part in enumerate(parts,1))
            if targets_for_run['wiki']:targets.append(('wiki',1,text,1))
            marks=read_targets(messages,set(snap['selected']),start,end) if settings['summary'].get('mark_read',False) else []
            timing.stage('sending',len(targets)+len(marks))
            for target,n,part,total in targets:
                with self.gate:
                    checkpoint(cancel)
                    self.jobs.delivery(p.login,job_id,target,n,'sending')
                    try:
                        if target=='bot':receipt=self.destinations.send_bot(p,dest['bot_login'],part,job_id+'-'+str(n))
                        else:receipt=self.destinations.send_wiki(p,dest['wiki_slug'],part,job_id,cancel,
                                                               kind=job['kind'],start=start,end=end)
                    except Cancelled:
                        self.jobs.delivery(p.login,job_id,target,n,'cancelled')
                        raise
                    except Exception as e:
                        self.jobs.delivery(p.login,job_id,target,n,'unknown' if isinstance(e,DeliveryUnknown) else 'failed')
                        raise
                    self.jobs.delivery(p.login,job_id,target,n,'confirmed',receipt)
                    update(progress=f'Отправлено: {target}, часть {n}/{total}')
                    timing.advance()
            delivered=True
            checkpoint(cancel)
            progress='Саммари отправлено'
            if settings['summary'].get('mark_read',False):
                stats['read_marks']=marks
                update(stats=stats,progress='Саммари отправлено. Отмечаем обработанные сообщения прочитанными')
                # Ordered writes keep the global stop boundary and durable status
                # around each mutation. Later arrivals never change these targets.
                for mark in marks:
                    with self.gate:
                        checkpoint(cancel)
                        if mark['status']=='skipped':
                            timing.advance();continue
                        mark['status']='applying';update(stats=stats)
                        try: mark['status']=self.integrations.mark_read(p,mark,cancel)
                        except Cancelled:
                            mark['status']='cancelled';update(stats=stats);raise
                        except Exception: mark['status']='unknown'
                        update(stats=stats)
                        timing.advance()
                confirmed=sum(m['status']=='confirmed' for m in marks)
                if not marks: progress+='; нет сообщений для отметки прочтения'
                elif confirmed==len(marks): progress+='; обработанные сообщения отмечены прочитанными'
                else: progress+=f'; отметка прочтения не подтверждена для {len(marks)-confirmed} чатов или веток'
            update(status='completed',progress=progress)
        except Cancelled:
            if delivered: update(status='completed',progress='Саммари отправлено. Отметка прочтения остановлена.')
            else: update(status='cancelled',progress='Остановлено',error='Обработка остановлена. Уже подтверждённые отправки остаются у получателя.')
        except RunError as e:
            update(status='failed',progress='Не завершено',error=str(e))
        except Exception:
            update(status='failed',progress='Не завершено',error='Не удалось обработать данные. Проверьте подключения. Отправка не повторяется автоматически.')

    def tick(self,at=None):
        snap=self.store.snapshot(self.principal.login)
        if snap['stopped']:return
        with self.store.connect() as db:
            row=db.execute('SELECT armed_at FROM controls WHERE login=?',(self.principal.login,)).fetchone()
        if not row:return
        for kind,slot in due_slots(snap['settings']['schedule'],datetime.fromisoformat(row[0]),at or datetime.now(TZ)):
            try:self.launch(kind,slot=slot)
            except Conflict:pass

    def serve(self):
        self.jobs.recover(self.principal.login)
        def loop():
            while not self.shutdown.wait(15):
                try:self.tick()
                except Exception:pass # no credentials, content or raw API failures in logs
        threading.Thread(target=loop,daemon=True,name='summary-scheduler').start()
