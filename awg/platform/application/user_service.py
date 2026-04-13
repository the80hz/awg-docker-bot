from dataclasses import dataclass
from typing import Any

from awg import db
from awg.platform.application import profile_registry


@dataclass(frozen=True)
class UserProfile:
    username: str
    owner_id: str | int | None


class UserService:
    """Переиспользуемый сервис для бота и будущего HTTP API."""

    def list_profiles(self, server_id: str) -> list[UserProfile]:
        clients = db.get_client_list(server_id=server_id) or []
        expirations = db.load_expirations()

        result: list[UserProfile] = []
        for client in clients:
            username = client[0]
            owner_id = self._resolve_owner_id(username, server_id, expirations)
            result.append(UserProfile(username=username, owner_id=owner_id))
        return result

    def list_users(self) -> list[dict[str, str | int]]:
        expirations = db.load_expirations()
        counters: dict[str, dict[str, str | int]] = {}

        for _, servers in expirations.items():
            if not isinstance(servers, dict):
                continue
            for info in servers.values():
                if not isinstance(info, dict):
                    continue
                owner_id = info.get('owner_id')
                if owner_id is None:
                    continue
                owner_key = str(owner_id)
                entry = counters.get(owner_key)
                if entry:
                    current_count = int(entry['profiles_count'])
                    entry['profiles_count'] = current_count + 1
                else:
                    counters[owner_key] = {
                        'user_id': owner_id,
                        'profiles_count': 1,
                    }

        # Подмешиваем пользователей, которые есть только в profile_registry.
        for entry in profile_registry.list_all_profiles():
            owner_id = entry.get('owner_id')
            if owner_id is None:
                continue
            owner_key = str(owner_id)
            if owner_key not in counters:
                counters[owner_key] = {
                    'user_id': owner_id,
                    'profiles_count': 0,
                }

        return list(counters.values())

    @staticmethod
    def _resolve_owner_id(
        username: str,
        server_id: str,
        expirations: dict[str, Any],
    ) -> str | int | None:
        user_servers = expirations.get(username)
        if not isinstance(user_servers, dict):
            return None

        info = user_servers.get(server_id) or user_servers.get(str(server_id))
        if isinstance(info, dict):
            return info.get('owner_id')
        return None
