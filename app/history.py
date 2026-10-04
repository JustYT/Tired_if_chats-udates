"""Read-only, complete history extraction including newly active old threads."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo
from .vendor.messenger.client import render_message
from .summary_links import thread_room

TZ=ZoneInfo('Europe/Moscow')
class Cancelled(Exception): pass
class RunError(Exception): pass

def checkpoint(cancel):
    if cancel.is_set(): raise Cancelled()

def pages(client, chat_id, before, cancel, floor=None):
    # Scan all roots for ThreadState.LastTsMcs; an old root may have new replies.
    for _ in range(1000):
        checkpoint(cancel)
        room=client._chat_history(chat_id,limit=500,before_ts=before)
        rows=room.get('Messages') or []
        if not rows: return
        stamps=[(r.get('ServerMessage') or r).get('ServerMessageInfo',{}).get('Timestamp') for r in rows]
        if any(type(t) is not int or t<=0 or t>=before for t in stamps):
            raise RunError('История вернула неверную временную границу; отправка отменена.')
        yield rows
        oldest=min(stamps)
        if floor is not None and oldest<=floor: return
        # Fanout MaxTimestamp is exclusive: subtracting 1 skips adjacent stamps.
        before=oldest
        if before<=0: return
    raise RunError('Превышен предел 500 000 сообщений в одном чате. Неполное саммари не отправлено.')

def extract_chat(client, chat, start, end, cancel):
    lo,hi=int(start.timestamp()*1_000_000),int(end.timestamp()*1_000_000)
    messages={}; root_count=0; thread_count=0
    def add(raw, parent=None, context=False):
        m=render_message(raw)
        if m.get('deleted'): return
        ts=m.get('ts')
        key=(parent or 0,ts)
        if key in messages: return
        media=[{'kind':x.get('kind'),'name':x.get('Name') or x.get('FileName')} for x in m.get('media',[])]
        messages[key]={'chat':chat['title'],'chat_id':chat['id'],'chat_kind':chat.get('kind','group'),'ts':ts,'seq':m.get('seq'),
            'account_uid':chat.get('account_uid'),
            'date':datetime.fromtimestamp(ts/1e6,TZ).isoformat(), 'author':m.get('author') or 'Участник',
            'author_guid':m.get('author_guid'),
            'text':m.get('text') or '', 'media':media,'reactions':m.get('reactions',[]),
            'thread':parent, 'context_only':context}
    for rows in pages(client,chat['id'],hi,cancel):
        for raw in rows:
            checkpoint(cancel); root_count+=1
            info=(raw.get('ServerMessage') or raw).get('ServerMessageInfo',{})
            ts=info['Timestamp']; state=info.get('ThreadState') or {}
            if lo<=ts<hi: add(raw)
            last=state.get('LastTsMcs')
            if state and (not isinstance(last,int) or last>=lo):
                found=False
                for replies in pages(client,thread_room(chat['id'],ts),hi,cancel,floor=lo):
                    for reply in replies:
                        rts=(reply.get('ServerMessage') or reply).get('ServerMessageInfo',{}).get('Timestamp')
                        if lo<=rts<hi:
                            add(reply,parent=ts); found=True
                if found:
                    if ts<lo: add(raw,context=True)
                    thread_count+=1
    ordered=sorted(messages.values(),key=lambda m:(m['ts'],m['thread'] or 0))
    return ordered,{'roots_scanned':root_count,'threads':thread_count,'messages':sum(not m['context_only'] for m in ordered)}

def collect(integrations, principal, chats, start, end, cancel, progress):
    result=[]; stats={'chats':len(chats),'roots_scanned':0,'threads':0,'messages':0}
    def extract_owned(chat):
        checkpoint(cancel)
        client=integrations.messenger(principal)
        actor=client.identity()
        return extract_chat(client,{**chat,'account_uid':actor['uid']},start,end,cancel)
    with ThreadPoolExecutor(max_workers=min(4,len(chats))) as pool:
        futures={pool.submit(extract_owned,c):c for c in chats}
        try:
            for i,f in enumerate(as_completed(futures),1):
                checkpoint(cancel)
                rows,counts=f.result(); result.extend(rows)
                for key,n in counts.items(): stats[key]+=n
                progress(f'Прочитано чатов: {i}/{len(chats)}',stats)
        except Exception:
            cancel.set()
            raise
    result.sort(key=lambda m:(m['ts'],m['chat_id']))
    for i,m in enumerate(result,1): m['source']=f's{i}'
    return result,stats
