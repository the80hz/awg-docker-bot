import base64
import json


def resolve_owner_id(username: str, server_id: str, expirations: dict) -> object:
    if not expirations:
        return None

    server_entries = expirations.get(username)
    if not isinstance(server_entries, dict):
        return None

    if server_id in server_entries:
        entry = server_entries[server_id]
    else:
        entry = server_entries.get(str(server_id))

    if isinstance(entry, dict):
        return entry.get('owner_id')
    return None


def format_owner_label(owner_id: object) -> str:
    if owner_id is None:
        return "📁 Unknown"
    if isinstance(owner_id, int):
        return f"ID {owner_id}"

    owner_str = str(owner_id)
    if owner_str.startswith('@'):
        return owner_str
    if owner_str.isdigit():
        return f"ID {owner_str}"
    return owner_str


def owner_sort_key(owner_id: object) -> tuple[int, str]:
    if isinstance(owner_id, int):
        return (0, str(owner_id))
    return (1, str(owner_id or '').lower())


def encode_owner_token(owner_id: object) -> str:
    if owner_id is None:
        return "unknown"

    payload = {'t': 'i' if isinstance(owner_id, int) else 's', 'v': owner_id}
    raw = json.dumps(payload, ensure_ascii=True)
    encoded = base64.urlsafe_b64encode(raw.encode()).decode().rstrip('=')
    return encoded


def decode_owner_token(token: str) -> object:
    if token == "unknown":
        return None

    padding = '=' * (-len(token) % 4)
    try:
        raw = base64.urlsafe_b64decode(token + padding).decode()
        payload = json.loads(raw)
        owner_type = payload.get('t')
        owner_value = payload.get('v')
        if owner_type == 'i':
            try:
                return int(owner_value)
            except (TypeError, ValueError):
                return None
        return owner_value
    except Exception:
        return None
