from dataclasses import dataclass

from awg import db


@dataclass(frozen=True)
class ServerSummary:
    server_id: int | str
    server_name: str | None
    host: str | None
    port: int | str | None
    username: str | None
    auth_type: str | None
    endpoint: str | None


def _server_key(server_id: int | str) -> str:
    return str(server_id)


def _external_server_id(server_key: str) -> int | str:
    return int(server_key) if server_key.isdigit() else server_key


class ServerService:
    def list_servers(self) -> list[ServerSummary]:
        servers = db.load_servers()
        result: list[ServerSummary] = []
        for server_key, info in servers.items():
            result.append(
                ServerSummary(
                    server_id=_external_server_id(str(server_key)),
                    server_name=info.get('name') or str(server_key),
                    host=info.get('host'),
                    port=info.get('port'),
                    username=info.get('username'),
                    auth_type=info.get('auth_type'),
                    endpoint=info.get('endpoint'),
                )
            )
        return result

    def create_server(
        self,
        server_id: int | str,
        server_name: str,
        host: str,
        port: int,
        username: str,
        auth_type: str,
        password: str | None,
        key_path: str | None,
        endpoint: str | None,
    ) -> dict:
        return db.add_server(
            server_id=_server_key(server_id),
            host=host,
            port=port,
            username=username,
            auth_type=auth_type,
            password=password,
            key_path=key_path,
            endpoint=endpoint,
            server_name=server_name,
        )

    def delete_server(self, server_id: int | str) -> bool:
        return db.remove_server(_server_key(server_id))

    def update_server(
        self,
        server_id: int | str,
        server_name: str | None = None,
        host: str | None = None,
        port: int | None = None,
        username: str | None = None,
        endpoint: str | None = None,
        auth_type: str | None = None,
        password: str | None = None,
        key_path: str | None = None,
    ) -> dict:
        server_key = _server_key(server_id)
        servers = db.load_servers()
        if server_key not in servers:
            raise KeyError('Сервер не найден.')

        current = servers[server_key]
        if server_name is not None:
            current['name'] = server_name
        if host is not None:
            current['host'] = host
        if port is not None:
            current['port'] = port
        if username is not None:
            current['username'] = username
        if endpoint is not None:
            current['endpoint'] = endpoint

        if auth_type == 'password':
            if not password:
                raise ValueError(
                    'Для auth_type=password нужно передать password.'
                )
            current['auth_type'] = 'password'
            current['key_path'] = None
            db.save_servers(servers)
            db.update_server_password(server_key, password)
            servers = db.load_servers()
            current = servers[server_key]
        elif auth_type == 'key':
            if not key_path:
                raise ValueError('Для auth_type=key нужно передать key_path.')
            db.save_servers(servers)
            db.update_server_key(server_key, key_path)
            servers = db.load_servers()
            current = servers[server_key]

        db.save_servers(servers)
        return current

    def test_connection(self, server_id: int | str) -> dict[str, str]:
        server_key = _server_key(server_id)
        servers = db.load_servers()
        if server_key not in servers:
            raise KeyError('Сервер не найден.')

        info = servers[server_key]
        ssh = db.SSHManager(
            server_id=server_key,
            host=info.get('host'),
            port=int(info.get('port', 22)),
            username=info.get('username'),
            auth_type=info.get('auth_type'),
            password=info.get('_original_password'),
            key_path=info.get('key_path'),
        )
        if not ssh.connect():
            return {'status': 'error', 'message': 'SSH connection failed'}
        return {'status': 'ok', 'message': 'SSH connection established'}
