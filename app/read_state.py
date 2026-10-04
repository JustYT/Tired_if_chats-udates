"""Pinned read boundaries from collected messages, never from model output."""
from .history import checkpoint, Cancelled
from .summary_links import thread_room


def read_targets(messages, selected, start, end):
    lo,hi=int(start.timestamp()*1_000_000),int(end.timestamp()*1_000_000)
    latest={}
    for message in messages:
        ts=message.get('ts')
        if (message['chat_id'] not in selected or message.get('context_only')
                or type(ts) is not int or not lo<=ts<hi):
            continue
        parent=message.get('thread')
        room=thread_room(message['chat_id'],parent) if parent else message['chat_id']
        if room not in latest or latest[room]['timestamp']<ts:
            seq=message.get('seq')
            latest[room]={'room_id':room,'timestamp':ts,'seq':seq,
                          'status':'pending' if type(seq) is int and seq>0 else 'skipped'}
    return list(latest.values())


def mark_target(client,target,cancel):
    """Preview and confirm exactly the collected boundary; never resend a push."""
    checkpoint(cancel)
    args={'chat_id':target['room_id'],'target_timestamp':target['timestamp'],
          'target_seq_no':target['seq']}
    committing=False
    try:
        client.identity()  # expected_login/account are enforced by the client.
        preview=client.mark_chat_read(**args)
        if preview.get('verification',{}).get('target_read') is True:
            return 'confirmed'
        fingerprint=preview.get('fingerprint')
        if not isinstance(fingerprint,str) or not fingerprint:
            return 'failed'
        checkpoint(cancel)
        committing=True
        result=client.mark_chat_read(**args,confirm_fingerprint=fingerprint)
        if result.get('verification',{}).get('target_read') is True:
            return 'confirmed'
        return 'failed' if result.get('outcome')=='rejected' else 'unknown'
    except Cancelled:
        raise
    except Exception:
        # No raw transport exception or secret enters the job log.
        return 'unknown' if committing else 'failed'
