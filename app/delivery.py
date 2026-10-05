"""Destination adapters. A timeout is an unknown outcome, never a retry signal."""
import json
import os
import re
import html
import math
import time
import urllib.request
from datetime import datetime, timedelta
from .summarizer import NoRedirect
from .history import RunError, checkpoint, Cancelled, TZ
from .vendor.wiki.wiki_client import WikiAPIError
from .vendor.messenger.client import private_chat_id
from .summary_links import message_link

class DeliveryUnknown(RunError): pass

LINK_TOKEN=re.compile(r'\[(?:\\.|[^\]\\\n])+\]\(https?://[^\s)]+\)')
WIKI_LINK=re.compile(r'\[((?:\\.|[^\]\\\n])+)\]\((https?://[^\s)]+)\)')
INLINE_TOKEN=re.compile(LINK_TOKEN.pattern+r'|\*\*(?:\\.|[^\n])*?\*\*|\+\+(?:\\.|[^\n])*?\+\+')
WIKI_DATE=re.compile(r'\*\*(\d{2}\.\d{2}\.\d{4})\*\*')
WIKI_CHAT=re.compile(r'(➡️|🌈) \*\*\+\+(.+)\+\+\*\*')

def wiki_inline(text):
    """Render the verified summary's link subset inside a Wiki HTML block."""
    parts=[]; offset=0
    for match in WIKI_LINK.finditer(text):
        parts.append(html.escape(re.sub(r'\\([\\`*_{}\[\]()<>#!|+])',r'\1',text[offset:match.start()])))
        label,url=match.group(1),match.group(2)
        parts.append('<a href="'+html.escape(url,quote=True)+'" target="_blank" rel="noopener noreferrer">'+
                     html.escape(re.sub(r'\\([\\`*_{}\[\]()<>#!|+])',r'\1',label))+'</a>')
        offset=match.end()
    parts.append(html.escape(re.sub(r'\\([\\`*_{}\[\]()<>#!|+])',r'\1',text[offset:])))
    return ''.join(parts)

def wiki_color_block(index, heading, lines):
    color,border=('#FFF8EF','#E8D7BF') if index%2==0 else ('#F2F5DD','#D8E2B8')
    out=[f'<div style="background-color:{color};border:1px solid {border};border-radius:12px;padding:20px 24px;margin:0 0 20px;">',
         f'<h2 style="margin:0 0 14px;font-size:20px;">{heading}</h2>']
    for line in lines:
        chat=WIKI_CHAT.fullmatch(line)
        if chat:
            out.append('<p style="margin:17px 0 8px;line-height:1.6;">'+chat.group(1)+
                       ' <strong><u>'+wiki_inline(chat.group(2))+'</u></strong></p>')
        else:
            out.append('<p style="margin:7px 0;line-height:1.68;">'+wiki_inline(line)+'</p>')
    out.append('</div>')
    return '::: html\n'+'\n'.join(out)+'\n:::'

def wiki_day_blocks(text):
    """Keep a weekly date-grouped report on one page, alternating chosen colors."""
    groups=[]
    for line in text.splitlines():
        match=WIKI_DATE.fullmatch(line.strip())
        if match:
            groups.append((match.group(1),[]))
        elif groups:
            if line.strip(): groups[-1][1].append(line.strip())
        elif line.strip():
            return None  # By-chat weekly reports have no date blocks.
    if not groups: return None
    return '\n\n'.join(wiki_color_block(index,
                      f'<strong style="font-size:20px;">{html.escape(day)}</strong>',lines)
                      for index,(day,lines) in enumerate(groups))

def wiki_chat_blocks(text):
    """Give each chat its own alternating block when weekly dates are hidden."""
    groups=[]
    for line in text.splitlines():
        stripped=line.strip()
        chat=WIKI_CHAT.fullmatch(stripped)
        if chat:
            groups.append((chat,[]))
        elif groups:
            if stripped: groups[-1][1].append(stripped)
        elif stripped:
            return None
    if not groups: return None
    return '\n\n'.join(wiki_color_block(index,
                      chat.group(1)+' <strong><u>'+wiki_inline(chat.group(2))+'</u></strong>',lines)
                      for index,(chat,lines) in enumerate(groups))

def wiki_document(text, *, kind='today', start=None, end=None, elapsed_seconds=None):
    """Only user-facing text goes into Wiki; job identity is in the unique slug."""
    names=list(dict.fromkeys(re.findall(r'^(?:➡️|🌈) \*\*\+\+(.+)\+\+\*\*$',text,re.M)))
    unescape=lambda value:re.sub(r'\\([\\`*_{}\[\]()<>#!|+])',r'\1',value)
    title='Саммари — '+unescape(names[0]) if len(names)==1 else 'Саммари чатов'
    if kind=='weekly':
        if not all(isinstance(t,datetime) and t.utcoffset() is not None for t in (start,end)) or end<=start:
            raise RunError('Не указан корректный период недельного отчёта для Wiki.')
        first=start.astimezone(TZ)
        last=(end.astimezone(TZ)-timedelta(microseconds=1)).date()
        title=f'Неделя {first.isocalendar().week:02d} ({first:%d.%m.%Y} - {last:%d.%m.%Y})'
    if kind=='weekly':
        blocks=wiki_day_blocks(text)
        if blocks is None: blocks=wiki_chat_blocks(text)
        if blocks is not None:content=blocks
        else:content='  \n'.join(text.split('\n'))
    else:
        # YFM needs explicit hard breaks to preserve date/chat/topic lines.
        content='  \n'.join(text.split('\n'))
    if elapsed_seconds is not None:
        if not isinstance(elapsed_seconds,(int,float)) or not math.isfinite(elapsed_seconds) or elapsed_seconds<0:
            raise ValueError('Некорректная длительность подготовки Wiki')
        minutes,seconds=divmod(math.ceil(elapsed_seconds),60)
        duration=(f'{minutes} мин ' if minutes else '')+f'{seconds} сек'
        content+='\n\n_Затраченное время: '+duration+'_'
    return title,content

def wiki_matches(page,content,job_id):
    existing=page.get('content')
    if not isinstance(existing,str): return False
    # Reconcile an old delivery only if its COMPLETE body matches too.
    # A marker alone is never proof of delivery. No new markers are written.
    marker='<!-- chat-studio:'+job_id+' -->'
    for prefix in (marker+'\n','\\'+marker+'\n'):
        if existing.startswith(prefix):
            existing=existing[len(prefix):];break
    return existing.replace('\r\n','\n').strip('\n')==content.replace('\r\n','\n').strip('\n')

def split_text(text,limit=3500):
    if limit<1: raise ValueError('limit must be positive')
    parts=[]
    while text:
        cut=min(len(text),limit)
        if cut<len(text):
            boundary=text.rfind('\n',0,cut)
            if boundary>limit//2: cut=boundary+1
            else:
                space=text.rfind(' ',0,cut)
                if space>limit//2: cut=space+1
            for match in INLINE_TOKEN.finditer(text):
                if match.start()>=cut: break
                if match.start()<cut<match.end():
                    if match.end()-match.start()>limit: raise RunError('Фрагмент с оформлением слишком длинный для отправки в бот.')
                    cut=match.start() or match.end()
                    break
            # A literal escape also belongs to the character that follows it.
            slashes=len(text[:cut])-len(text[:cut].rstrip('\\'))
            if slashes%2: cut-=1
            if not cut: raise RunError('Не удалось разбить саммари без повреждения форматирования.')
        parts.append(text[:cut]);text=text[cut:]
    return parts

class Destinations:
    def __init__(self, integrations): self.integrations=integrations
    def bot_ready(self,login):
        return bool(os.environ.get('CHAT_STUDIO_BOT_TOKEN')) and login==os.environ.get('CHAT_STUDIO_BOT_LOGIN')
    def self_ready(self,principal):
        try:return bool(self.integrations.credentials.get(principal,'messenger'))
        except (AttributeError,PermissionError):return False
    def send_self(self,principal,text,payload_id):
        """Send only to the authenticated owner's self chat, then verify history."""
        client=self.integrations.messenger(principal)
        actor=client.identity()
        if actor.get('login')!=principal.login:
            raise RunError('Мессенджер подключён под другим пользователем.')
        guid=actor['guid']
        chat_id=private_chat_id(guid,guid)
        preview=client.send_message(text,to_guid=guid,payload_id=payload_id)
        if preview.get('status')!='preview_not_applied' or not preview.get('fingerprint'):
            raise RunError('Не удалось проверить отправку от вашего имени.')
        try:
            result=client.send_message(text,to_guid=guid,payload_id=payload_id,
                                       confirm_fingerprint=preview['fingerprint'])
        except Exception:
            raise DeliveryUnknown('Нет подтверждения отправки от вашего имени. Повтора нет.') from None
        if result.get('outcome')=='rejected':
            raise RunError('Мессенджер отклонил отправку от вашего имени.')
        for attempt in range(5):
            try:
                history=client.read_history(chat_id=chat_id,limit=50)
                match=next((m for m in history['messages'] if m.get('payload_id')==payload_id
                            and m.get('text')==text and m.get('author_guid')==guid),None)
                if match:
                    return message_link({'chat_id':chat_id,'ts':match['ts']}) or str(match['ts'])
            except Exception:pass
            if attempt<4:time.sleep(.4)
        raise DeliveryUnknown('Отправка от вашего имени не подтверждена в истории. Повтора нет.')
    def send_bot(self,principal,bot_login,text,payload_id):
        # Recipient comes exclusively from the authenticated principal, never the model.
        if not self.bot_ready(bot_login): raise RunError('Токен выбранного бота не подключён.')
        data={'login':principal.login,'text':text,'payload_id':payload_id,'disable_web_page_preview':True}
        req=urllib.request.Request('https://bp.mssngr.yandex.net/public/bot/v1/messages/sendText',
            data=json.dumps(data,ensure_ascii=False).encode(),
            headers={'Authorization':'OAuthTeam '+os.environ['CHAT_STUDIO_BOT_TOKEN'],'Content-Type':'application/json'},method='POST')
        try:
            with urllib.request.build_opener(NoRedirect()).open(req,timeout=15) as r:
                result=json.load(r)
            if result.get('ok') is True and result.get('message_id'): return str(result['message_id'])
        except Exception: pass
        raise DeliveryUnknown('Нет подтверждения отправки в бот. Сообщение могло дойти; автоматического повтора нет.')

    def send_wiki(self,principal,slug,text,job_id,cancel=None,*,kind='today',start=None,end=None,elapsed_seconds=None):
        def check():
            if cancel is not None: checkpoint(cancel)
        check()
        if not slug.startswith('users/'+principal.login+'/'): raise RunError('Wiki: чужой раздел.')
        title,content=wiki_document(text,kind=kind,start=start,end=end,elapsed_seconds=elapsed_seconds)
        def matches(page):
            return wiki_matches(page,content,job_id) and (kind!='weekly' or page.get('title')==title)
        client=self.integrations.wiki(principal)
        if client.me().get('username')!=principal.login: raise RunError('Wiki подключена под другим пользователем.')
        # Parents inherit their existing ACL. Never change sharing or overwrite pages.
        parts=slug.split('/')
        for n in range(3,len(parts)+1):
            check()
            parent='/'.join(parts[:n])
            try: client.get_page(slug=parent)
            except WikiAPIError as e:
                if e.status!=404: raise RunError('Не удалось проверить папку Wiki.') from None
                check()
                try: client.create_page('Саммари чатов',parent,page_type='wysiwyg',content='Сводки выбранных рабочих чатов.')
                except Exception:
                    try: client.get_page(slug=parent)
                    except Exception: raise DeliveryUnknown('Создание папки Wiki не подтверждено. Повтора нет.') from None
        target=slug+'/summary-'+job_id
        try:
            existing=client.get_page(slug=target,fields=['content'])
        except WikiAPIError as e:
            if e.status!=404: raise RunError('Не удалось проверить страницу Wiki.') from None
        else:
            if matches(existing): return 'https://wiki.yandex-team.ru/'+target+'/'
            raise RunError('Адрес Wiki занят другой страницей.')
        check()
        try:
            client.create_page(title,target,page_type='wysiwyg',content=content)
            existing=client.get_page(slug=target,fields=['content'])
            if not matches(existing):
                raise DeliveryUnknown('Wiki не подтвердила название или содержимое страницы.')
        except Exception:
            try:
                existing=client.get_page(slug=target,fields=['content'])
                if matches(existing): return 'https://wiki.yandex-team.ru/'+target+'/'
            except Exception: pass
            raise DeliveryUnknown('Запись в Wiki не подтверждена. Проверьте страницу; автоматического повтора нет.') from None
        return 'https://wiki.yandex-team.ru/'+target+'/'
