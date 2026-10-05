"""Local, single-chat request planning. No semantic guesses or model calls."""
from collections import OrderedDict
from dataclasses import dataclass
import json

from .history import RunError, checkpoint

MAX_INPUT_CHARS = 55000
MAX_BATCHES = 100


def message_key(message):
    return message['chat_id'], message.get('thread') or 0, message['ts']


def parent_keys(message):
    chat = message['chat_id']
    if message.get('thread'):
        yield chat, 0, message['thread']
    reply = message.get('reply_to')
    if reply:
        yield chat, reply.get('thread') or 0, reply['ts']


def is_parent(parent, child):
    return message_key(parent) in set(parent_keys(child))


def encode(rows):
    return json.dumps(rows, ensure_ascii=False, separators=(',', ':'))


@dataclass
class PreparedBatch:
    chat_id: str
    messages: list
    rows: list

    @property
    def sources(self):
        return {m['source']: m for m in self.messages}

    @property
    def primary_sources(self):
        return {m['source'] for m in self.messages if not m.get('context_only')}


def prepare_batches(messages, max_chars=MAX_INPUT_CHARS, cancel=None):
    """Keep linked spans intact when they fit; split with explicit parent context.

    Spans include intervening ordinary messages, preserving context for replies
    without metadata. Every event belongs to one primary batch. Repeated boundary
    messages are context only and cannot independently produce a new event.
    """
    chats = OrderedDict()
    seen_sources = set()
    seen_keys = set()
    for message in sorted(messages, key=lambda m: (m['chat_id'], m['ts'], m.get('thread') or 0)):
        source = message['source']
        key = message_key(message)
        if source in seen_sources or key in seen_keys:
            raise RunError('Повторный идентификатор сообщения; подготовка саммари остановлена.')
        seen_sources.add(source)
        seen_keys.add(key)
        chats.setdefault(message['chat_id'], []).append(message)

    result = []
    for chat_id, history in chats.items():
        if cancel is not None: checkpoint(cancel)
        if not any(not m.get('context_only') for m in history):
            continue
        indexed = {message_key(m): m for m in history}
        positions = {m['source']: n for n, m in enumerate(history)}

        def wire(message, available):
            # Routing/account identifiers stay local. The model needs source IDs,
            # authors and explicit relations, not GUIDs, read cursors or URLs.
            row = {key: message[key] for key in ('source', 'chat_id', 'date', 'author', 'text')}
            row['context_only'] = bool(message.get('context_only'))
            for key in ('media', 'reactions'):
                if message.get(key):
                    row[key] = message[key]
            for field, target in (('thread_source', (chat_id, 0, message.get('thread'))),
                                  ('reply_source', (chat_id, (message.get('reply_to') or {}).get('thread') or 0,
                                                    (message.get('reply_to') or {}).get('ts')))):
                if target[-1] is not None:
                    parent = indexed.get(target)
                    if parent and parent['source'] in available:
                        row[field] = parent['source']
                    else:
                        row[field + '_unavailable'] = True
            return row

        def packet(primary, with_neighbours=False):
            rows = {m['source']: m for m in primary}
            # Required, exact reply/thread parents from this chat only.
            for message in primary:
                for key in parent_keys(message):
                    parent = indexed.get(key)
                    if parent and parent['source'] not in rows:
                        rows[parent['source']] = {**parent, 'context_only': True}
            ordered = sorted(rows.values(), key=lambda m: (m['ts'], m.get('thread') or 0))
            projection = [wire(m, rows) for m in ordered]
            if with_neighbours:
                before = min(positions[m['source']] for m in primary)
                # A small chronological overlap helps unresolved ordinary replies.
                # It is optional; explicit parents above are never silently dropped.
                for neighbour in reversed(history[max(0, before-2):before]):
                    if neighbour['source'] in rows:
                        continue
                    candidate = sorted(ordered + [{**neighbour, 'context_only': True}],
                                       key=lambda m: (m['ts'], m.get('thread') or 0))
                    candidate_sources = {m['source'] for m in candidate}
                    candidate_wire = [wire(m, candidate_sources) for m in candidate]
                    if len(encode(candidate_wire)) <= max_chars:
                        rows[neighbour['source']] = neighbour
                        ordered, projection = candidate, candidate_wire
            return PreparedBatch(chat_id, ordered, projection)

        def fits(primary):
            return len(encode(packet(primary).rows)) <= max_chars

        def append(primary):
            if any(not m.get('context_only') for m in primary):
                result.append(packet(primary, with_neighbours=True))
                if len(result) > MAX_BATCHES:
                    raise RunError('Период слишком большой: более 100 блоков модели. Выберите меньше чатов.')

        # A reply/thread edge links an interval in chronological order. Overlapping
        # intervals form an atomic span; unrelated chats never enter this index.
        reach = list(range(len(history)))
        for n, message in enumerate(history):
            for key in parent_keys(message):
                parent = indexed.get(key)
                if parent:
                    p = positions[parent['source']]
                    left, right = min(p, n), max(p, n)
                    reach[left] = max(reach[left], right)
        spans = []
        start = 0
        while start < len(history):
            end, cursor = reach[start], start
            while cursor <= end:
                end = max(end, reach[cursor])
                cursor += 1
            spans.append(history[start:end+1])
            start = end+1
        pending = []
        for span in spans:
            if cancel is not None: checkpoint(cancel)
            if fits(pending + span):
                pending += span
                continue
            if pending:
                append(pending)
                pending = []
            if fits(span):
                pending = list(span)
                continue
            for message in span:
                if not fits(pending + [message]):
                    if pending:
                        append(pending)
                        pending = []
                    if not fits([message]):
                        raise RunError('Сообщение с контекстом reply/треда превышает предел запроса; текст не обрезан.')
                pending.append(message)
        if pending:
            append(pending)
    return result
