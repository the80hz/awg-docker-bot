from dataclasses import dataclass
from typing import Any

from awg import db


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

    @staticmethod
    def _resolve_owner_id(username: str, server_id: str, expirations: dict[str, Any]) -> str | int | None:
        user_servers = expirations.get(username)
        if not isinstance(user_servers, dict):
            return None

        info = user_servers.get(server_id) or user_servers.get(str(server_id))
        if isinstance(info, dict):
            return info.get('owner_id')
        return None
