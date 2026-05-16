import json
import uuid as _uuid
import threading

_util = None

def _u():
    global _util
    if _util is None:
        import util
        _util = util.util()
    return _util

def ensure_uuid():
    if not _u().getkeyval('ntfyuuid'):
        new_id = str(_uuid.uuid4())
        _u().setkeyval('ntfyuuid', new_id)
        print(f"[NTFY] Generated topic UUID: {new_id}")

def get_uuid():
    return _u().getkeyval('ntfyuuid') or ''

def get_prefs():
    def _get(key, default):
        v = _u().getkeyval(f'ntfy_notify_{key}')
        return v if v is not None else default
    return {
        'notify_fill':   _get('fill',   'true'),
        'notify_cancel': _get('cancel', 'true'),
        'notify_create': _get('create', 'true'),
        'notify_user':   _get('user',   'false'),
        'notify_error':  _get('error',  'true'),
    }

def set_prefs(prefs):
    for key in ('notify_fill', 'notify_cancel', 'notify_create', 'notify_user', 'notify_error'):
        if key in prefs:
            _u().setkeyval(f'ntfy_{key}', prefs[key])

def _is_enabled(prefix):
    val = _u().getkeyval(f'ntfy_notify_{prefix}')
    if val is None:
        return prefix in ('fill', 'cancel', 'create', 'error')
    return val == 'true'

def _do_send(ntfy_uuid, title, body):
    import urllib.request
    try:
        req = urllib.request.Request(
            f'https://ntfy.sh/{ntfy_uuid}',
            data=body.encode('utf-8'),
            headers={
                'Title': title,
                'Priority': 'default',
                'Content-Type': 'text/plain',
            },
            method='POST'
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        print(f"[NTFY] Send failed: {e}")

def send_notification(event_type, data):
    ntfy_uuid = get_uuid()
    if not ntfy_uuid:
        return
    prefix = event_type.split(':')[0]
    if not _is_enabled(prefix):
        return
    title = event_type
    body = json.dumps(data) if isinstance(data, dict) else str(data)
    threading.Thread(target=_do_send, args=(ntfy_uuid, title, body), daemon=True).start()

def send_test():
    ntfy_uuid = get_uuid()
    if not ntfy_uuid:
        return False, 'No UUID configured'
    threading.Thread(
        target=_do_send,
        args=(ntfy_uuid, 'Test Notification', 'This is a test from your Coinbase trading bot.'),
        daemon=True
    ).start()
    return True, 'Test sent'
