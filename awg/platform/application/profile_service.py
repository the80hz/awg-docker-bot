import re
import struct
import zlib
import base64
from dataclasses import dataclass

from awg import db
from awg.platform.application import profile_registry


_VALID_PROFILE_RE = re.compile(r'^[A-Za-z0-9_.-]{1,64}$')


@dataclass(frozen=True)
class CreatedProfile:
    profile_id: str
    server_id: str
    user_id: str | int
    profile_name: str
    conf_text: str
    vpn_uri: str


@dataclass(frozen=True)
class DeletedProfile:
    profile_id: str
    status: str


class ProfileService:
    def list_profiles_by_server(
        self,
        server_id: str,
    ) -> list[dict[str, str | int | None]]:
        expirations = db.load_expirations()
        clients = db.get_client_list(server_id=server_id) or []
        result: list[dict[str, str | int | None]] = []

        for client in clients:
            profile_name = client[0]
            owner_id = self._resolve_owner_id(
                profile_name,
                server_id,
                expirations,
            )
            profile_id, _ = profile_registry.upsert_profile(
                server_id,
                profile_name,
                owner_id,
            )
            result.append(
                {
                    'profile_id': profile_id,
                    'server_id': server_id,
                    'user_id': owner_id,
                    'profile_name': profile_name,
                }
            )
        return result

    def create_profile(
        self,
        server_id: str,
        user_id: str | int,
        profile_name: str,
    ) -> CreatedProfile:
        normalized_name = self._normalize_profile_name(profile_name)

        existing_profiles = db.get_client_list(server_id=server_id) or []
        if any(item[0] == normalized_name for item in existing_profiles):
            raise ValueError(
                'Профиль с таким именем уже существует на этом сервере.'
            )

        owner_slug = self._owner_slug_from_user(user_id)
        created = db.root_add(
            normalized_name,
            server_id=server_id,
            owner_slug=owner_slug,
        )
        if not created:
            raise RuntimeError(
                'Не удалось создать профиль в WireGuard/Amnezia.'
            )

        db.set_user_expiration(
            normalized_name,
            expiration=None,
            traffic_limit='Неограниченно',
            owner_id=user_id,
            server_id=server_id,
            owner_slug=owner_slug,
        )

        conf_path = db.profile_file_path(
            server_id,
            normalized_name,
            f'{normalized_name}.conf',
            ensure=False,
        )
        try:
            with open(conf_path, 'r', encoding='utf-8') as file:
                conf_text = file.read()
        except FileNotFoundError as exc:
            raise RuntimeError(
                'Профиль создан, но .conf файл не найден '
                'в локальном хранилище.'
            ) from exc

        vpn_uri = self._encode_vpn_uri(conf_text)
        profile_id, _ = profile_registry.upsert_profile(
            server_id,
            normalized_name,
            user_id,
        )

        return CreatedProfile(
            profile_id=profile_id,
            server_id=server_id,
            user_id=user_id,
            profile_name=normalized_name,
            conf_text=conf_text,
            vpn_uri=vpn_uri,
        )

    def delete_profile(self, profile_id: str) -> DeletedProfile:
        entry = profile_registry.get_profile(profile_id)
        if not entry:
            raise KeyError('Профиль не найден по profile_id.')

        server_id = entry['server_id']
        username = entry['username']
        deleted = db.deactive_user_db(username, server_id=server_id)
        if not deleted:
            raise RuntimeError(
                'Не удалось удалить профиль в WireGuard/Amnezia.'
            )

        db.remove_user_expiration(username, server_id=server_id)
        profile_registry.delete_profile(profile_id)
        return DeletedProfile(profile_id=profile_id, status='deleted')

    def list_profiles_by_owner(
        self,
        user_id: str | int,
        server_id: str | None = None,
    ) -> list[dict[str, str | int | None]]:
        if server_id:
            servers = [server_id]
        else:
            servers = db.get_server_list()

        result: list[dict[str, str | int | None]] = []
        for srv_id in servers:
            scoped_clients = db.get_clients_by_owner(
                owner_id=user_id,
                server_id=srv_id,
            )
            for client in scoped_clients:
                profile_name = client[0]
                profile_id, _ = profile_registry.upsert_profile(
                    srv_id,
                    profile_name,
                    user_id,
                )
                result.append(
                    {
                        'profile_id': profile_id,
                        'server_id': srv_id,
                        'user_id': user_id,
                        'profile_name': profile_name,
                    }
                )
        return result

    @staticmethod
    def _normalize_profile_name(profile_name: str) -> str:
        value = (profile_name or '').strip()
        if not value:
            raise ValueError('Название профиля не может быть пустым.')
        if not _VALID_PROFILE_RE.fullmatch(value):
            raise ValueError(
                'Название профиля должно содержать только A-Z, a-z, '
                '0-9, _, -, . и быть не длиннее 64 символов.'
            )
        return value

    @staticmethod
    def _owner_slug_from_user(user_id: str | int) -> str:
        owner = str(user_id).strip().lower()
        owner = re.sub(r'[^a-z0-9._-]', '-', owner)
        owner = re.sub(r'-{2,}', '-', owner).strip('-')
        return owner or 'user'

    @staticmethod
    def _encode_vpn_uri(conf_text: str) -> str:
        conf_bytes = conf_text.encode('utf-8')
        compressed = zlib.compress(conf_bytes, 8)
        payload = struct.pack('>I', len(conf_bytes)) + compressed
        encoded = base64.urlsafe_b64encode(payload).rstrip(b'=')
        encoded = encoded.decode('ascii')
        return f'vpn://{encoded}'

    @staticmethod
    def _resolve_owner_id(
        username: str,
        server_id: str,
        expirations: dict,
    ) -> str | int | None:
        user_servers = expirations.get(username)
        if not isinstance(user_servers, dict):
            return None

        entry = user_servers.get(server_id) or user_servers.get(str(server_id))
        if isinstance(entry, dict):
            return entry.get('owner_id')
        return None
