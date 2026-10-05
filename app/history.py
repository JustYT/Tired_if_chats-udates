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

def is_deleted(raw):
    server=raw.get('ServerMessage') or raw
    info=server.get('ServerMessageInfo') or {}
    plain=(server.get('ClientMessage') or {}).get('Plain') or {}
    return bool(info.get('Deleted') or plain.get('Deleted'))

def reply_reference(raw, room, parent=None):
    """Same-room single quote is reply, matching Messenger's web adapter.

    A forwarded quote from another room is not a link into this chat. Scope the
    timestamp by the room, because a thread has its own message namespace.
    """
    server=raw.get('ServerMessage') or raw
    forwarded=server.get('ForwardedMessages') or []
    if len(forwarded)!=1: return None, None
    quote=forwarded[0];payload=quote.get('Payload') or {}
    info=quote.get('ServerMessageInfo') or {};ts=info.get('Timestamp')
    current_ts=(server.get('ServerMessageInfo') or {}).get('Timestamp')
    if (payload.get('ChatId')!=room or type(ts) is not int or ts<=0 or
            type(current_ts) is not int or ts>=current_ts): return None, None
    snapshot={'ServerMessage':{'ClientMessage':{'Plain':payload},'ServerMessageInfo':info}}
    return {'ts':ts,'thread':parent}, snapshot

def pages(client, chat_id, before, cancel, floor=None, state=None):
    # Scan all roots for ThreadState.LastTsMcs; an old root may have new replies.
    for _ in range(1000):
        checkpoint(cancel)
        room=client._chat_history(chat_id,limit=500,before_ts=before)
        if state is not None and 'last_seen' not in state:
            state['last_seen']=room.get('LastSeenByMeTsMcs')
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

def unread_day_messages(messages, last_seen, start, end):
    """Keep whole Moscow days from the first unread main-chat message."""
    if isinstance(last_seen,bool) or not str(last_seen).isdigit():
        raise RunError('Мессенджер не вернул границу прочтения чата; саммари не запущено.')
    marker=int(last_seen)
    lo,hi=int(start.timestamp()*1_000_000),int(end.timestamp()*1_000_000)
    first=min((m['ts'] for m in messages if not m['context_only'] and not m.get('thread')
               and lo<=m['ts']<hi and m['ts']>marker),default=None)
    if first is None:return []
    cutoff=int(datetime.fromtimestamp(first/1e6,TZ).replace(hour=0,minute=0,second=0,microsecond=0).timestamp()*1e6)
    primary=[m for m in messages if not m['context_only'] and m['ts']>=cutoff]
    indexed={(m.get('thread') or 0,m['ts']):m for m in messages}
    needed=[]
    for message in primary:
        if message.get('thread'):needed.append((0,message['thread']))
        reply=message.get('reply_to')
        if reply:needed.append((reply.get('thread') or 0,reply['ts']))
    context={}
    while needed:
        key=needed.pop()
        if key in context:continue
        parent=indexed.get(key)
        if parent is None or (parent['ts']>=cutoff and not parent['context_only']):continue
        context[key]={**parent,'context_only':True}
        if parent.get('thread'):needed.append((0,parent['thread']))
        reply=parent.get('reply_to')
        if reply:needed.append((reply.get('thread') or 0,reply['ts']))
    return sorted(primary+list(context.values()),key=lambda m:(m['ts'],m.get('thread') or 0))

def extract_chat(client, chat, start, end, cancel, unread_only=False):
    lo,hi=int(start.timestamp()*1_000_000),int(end.timestamp()*1_000_000)
    filter_unread=unread_only and not chat.get('is_channel',False)
    messages={}; deleted=set(); quoted_context={}; root_count=0; thread_count=0; room_state={}
    def add(raw, parent=None, context=False):
        m=render_message(raw)
        ts=m.get('ts')
        key=(parent or 0,ts)
        if m.get('deleted'):
            deleted.add(key);return
        if key in messages: return
        media=[{'kind':x.get('kind'),'name':(x.get('FileInfo') or x).get('Name') or x.get('FileName')} for x in m.get('media',[])]
        plain=((raw.get('ServerMessage') or raw).get('ClientMessage') or {}).get('Plain') or {}
        if plain.get('MiscFile'):
            media.append({'kind':'file','name':(plain['MiscFile'].get('FileInfo') or {}).get('Name')})
        room=thread_room(chat['id'],parent) if parent else chat['id']
        reply,snapshot=reply_reference(raw,room,parent)
        if reply:
            quoted_context[(parent or 0,reply['ts'])]=(snapshot,parent)
        messages[key]={'chat':chat['title'],'chat_id':chat['id'],'chat_kind':chat.get('kind','group'),
            'is_channel':bool(chat.get('is_channel',False)),'ts':ts,'seq':m.get('seq'),
            'account_uid':chat.get('account_uid'),
            'date':datetime.fromtimestamp(ts/1e6,TZ).isoformat(), 'author':m.get('author') or 'Участник',
            'author_guid':m.get('author_guid'),
            'text':m.get('text') or '', 'media':media,'reactions':m.get('reactions',[]),
            'thread':parent, 'reply_to':reply, 'context_only':context}
    for rows in pages(client,chat['id'],hi,cancel,state=room_state if filter_unread else None):
        for raw in rows:
            checkpoint(cancel); root_count+=1
            info=(raw.get('ServerMessage') or raw).get('ServerMessageInfo',{})
            ts=info['Timestamp']; state=info.get('ThreadState') or {}
            if is_deleted(raw): deleted.add((0,ts))
            if lo<=ts<hi: add(raw)
            last=state.get('LastTsMcs')
            if state and (not isinstance(last,int) or last>=lo):
                found=False
                for replies in pages(client,thread_room(chat['id'],ts),hi,cancel,floor=lo):
                    for reply in replies:
                        rts=(reply.get('ServerMessage') or reply).get('ServerMessageInfo',{}).get('Timestamp')
                        if is_deleted(reply): deleted.add((ts,rts))
                        if lo<=rts<hi:
                            add(reply,parent=ts); found=True
                if found:
                    if ts<lo: add(raw,context=True)
                    thread_count+=1
    # A quote can supply an old/unavailable reply target. Keep it as context only,
    # never as a newly published event or a target for marking messages as read.
    for key,(snapshot,parent) in list(quoted_context.items()):
        if key not in messages and key not in deleted:
            add(snapshot,parent=parent,context=True)
    ordered=sorted(messages.values(),key=lambda m:(m['ts'],m['thread'] or 0))
    if filter_unread and any(not m['context_only'] for m in ordered):
        ordered=unread_day_messages(ordered,room_state.get('last_seen'),start,end)
        thread_count=len({m['thread'] for m in ordered if m['thread'] and not m['context_only']})
    return ordered,{'roots_scanned':root_count,'threads':thread_count,'messages':sum(not m['context_only'] for m in ordered)}

def collect(integrations, principal, chats, start, end, cancel, progress, unread_only=False):
    result=[]; stats={'chats':len(chats),'roots_scanned':0,'threads':0,'messages':0}
    if unread_only:stats.update(unread_chats=0,channels_full_period=0)
    def extract_owned(chat):
        checkpoint(cancel)
        client=integrations.messenger(principal)
        actor=client.identity()
        return extract_chat(client,{**chat,'account_uid':actor['uid']},start,end,cancel,unread_only=unread_only)
    with ThreadPoolExecutor(max_workers=min(4,len(chats))) as pool:
        futures={pool.submit(extract_owned,c):c for c in chats}
        try:
            for i,f in enumerate(as_completed(futures),1):
                checkpoint(cancel)
                rows,counts=f.result(); result.extend(rows)
                for key,n in counts.items(): stats[key]+=n
                if unread_only and counts['messages']:
                    stats['channels_full_period' if futures[f].get('is_channel') else 'unread_chats']+=1
                progress(f'Прочитано чатов: {i}/{len(chats)}',stats)
        except Exception:
            cancel.set()
            raise
    result.sort(key=lambda m:(m['ts'],m['chat_id']))
    for i,m in enumerate(result,1): m['source']=f's{i}'
    return result,stats
