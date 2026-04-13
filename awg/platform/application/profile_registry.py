import json
import os
import threading
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

REGISTRY_PATH = os.path.join('data', 'profile_registry.json')
_registry_lock = threading.Lock()


def _load_registry() -> dict[str, dict[str, Any]]:
    if not os.path.exists(REGISTRY_PATH):
        return {}
    with open(REGISTRY_PATH, 'r', encoding='utf-8') as file:
        try:
            data = json.load(file)
        except json.JSONDecodeError:
            return {}
    if isinstance(data, dict):
        return data
    return {}


def _save_registry(registry: dict[str, dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(REGISTRY_PATH), exist_ok=True)
    with open(REGISTRY_PATH, 'w', encoding='utf-8') as file:
        json.dump(registry, file)


def get_profile(profile_id: str) -> dict[str, Any] | None:
    with _registry_lock:
        return _load_registry().get(profile_id)


def find_profile_id(server_id: str, username: str) -> str | None:
    with _registry_lock:
        registry = _load_registry()
        for profile_id, entry in registry.items():
            if (
                entry.get('server_id') == server_id
                and entry.get('username') == username
            ):
                return profile_id
    return None


def upsert_profile(
    server_id: str,
    username: str,
    owner_id: str | int | None,
) -> tuple[str, dict[str, Any]]:
    with _registry_lock:
        registry = _load_registry()
        for profile_id, entry in registry.items():
            if (
                entry.get('server_id') == server_id
                and entry.get('username') == username
            ):
                if owner_id is not None and entry.get('owner_id') != owner_id:
                    entry['owner_id'] = owner_id
                    registry[profile_id] = entry
                    _save_registry(registry)
                return profile_id, entry

        profile_id = str(uuid4())
        entry = {
            'profile_id': profile_id,
            'server_id': server_id,
            'username': username,
            'owner_id': owner_id,
            'created_at': datetime.now(timezone.utc).isoformat(),
        }
        registry[profile_id] = entry
        _save_registry(registry)
        return profile_id, entry


def delete_profile(profile_id: str) -> bool:
    with _registry_lock:
        registry = _load_registry()
        if profile_id not in registry:
            return False
        del registry[profile_id]
        _save_registry(registry)
        return True


def list_profiles_by_owner(owner_id: str | int) -> list[dict[str, Any]]:
    owner_str = str(owner_id)
    with _registry_lock:
        registry = _load_registry()
        return [
            entry
            for entry in registry.values()
            if str(entry.get('owner_id')) == owner_str
        ]


def list_all_profiles() -> list[dict[str, Any]]:
    with _registry_lock:
        return list(_load_registry().values())
