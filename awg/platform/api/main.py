"""ASGI entrypoint для админского API.

Запуск после установки FastAPI/uvicorn:
    uvicorn awg.platform.api.main:app --host 0.0.0.0 --port 8080
"""

import os
import sys

if __package__ in {None, ""}:
    project_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..")
    )
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

try:
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel, Field
except ImportError as exc:  # pragma: no cover
    raise RuntimeError(
        "FastAPI не установлен. Добавьте fastapi и uvicorn в зависимости, "
        "когда начнете перенос API слоя."
    ) from exc

from awg.platform.application.profile_service import ProfileService
from awg.platform.application.server_service import ServerService
from awg.platform.application.user_service import UserService


class CreateProfileRequest(BaseModel):
    server_id: str = Field(min_length=1)
    user_id: str | int
    profile_name: str = Field(min_length=1, max_length=128)


class CreateProfileResponse(BaseModel):
    profile_id: str
    server_id: str
    user_id: str | int
    profile_name: str
    conf_text: str
    vpn_uri: str


class DeleteProfileResponse(BaseModel):
    profile_id: str
    status: str


class CreateServerRequest(BaseModel):
    server_id: str
    host: str
    port: int = 22
    username: str
    auth_type: str
    password: str | None = None
    key_path: str | None = None
    endpoint: str | None = None


class UpdateServerRequest(BaseModel):
    host: str | None = None
    port: int | None = None
    username: str | None = None
    endpoint: str | None = None
    auth_type: str | None = None
    password: str | None = None
    key_path: str | None = None


app = FastAPI(title="AWG Admin API", version="0.2.0")
user_service = UserService()
profile_service = ProfileService()
server_service = ServerService()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/v1/servers/{server_id}/profiles")
def profiles_by_server(server_id: str) -> list[dict[str, str | int | None]]:
    try:
        return profile_service.list_profiles_by_server(server_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/v1/profiles", response_model=CreateProfileResponse)
def create_profile(payload: CreateProfileRequest) -> CreateProfileResponse:
    try:
        created = profile_service.create_profile(
            server_id=payload.server_id,
            user_id=payload.user_id,
            profile_name=payload.profile_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return CreateProfileResponse(
        profile_id=created.profile_id,
        server_id=created.server_id,
        user_id=created.user_id,
        profile_name=created.profile_name,
        conf_text=created.conf_text,
        vpn_uri=created.vpn_uri,
    )


@app.delete(
    "/api/v1/profiles/{profile_id}",
    response_model=DeleteProfileResponse,
)
def delete_profile(profile_id: str) -> DeleteProfileResponse:
    try:
        result = profile_service.delete_profile(profile_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return DeleteProfileResponse(
        profile_id=result.profile_id,
        status=result.status,
    )


@app.get("/api/v1/users/{user_id}/profiles")
def profiles_by_user(
    user_id: str,
    server_id: str | None = None,
) -> list[dict[str, str | int | None]]:
    try:
        return profile_service.list_profiles_by_owner(
            user_id=user_id,
            server_id=server_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/v1/users")
def list_users() -> list[dict[str, str | int]]:
    try:
        return user_service.list_users()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/v1/servers")
def list_servers() -> list[dict[str, str | int | None]]:
    try:
        servers = server_service.list_servers()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return [
        {
            'server_id': server.server_id,
            'host': server.host,
            'port': server.port,
            'username': server.username,
            'auth_type': server.auth_type,
            'endpoint': server.endpoint,
        }
        for server in servers
    ]


@app.post("/api/v1/servers")
def create_server(payload: CreateServerRequest) -> dict:
    try:
        return server_service.create_server(
            server_id=payload.server_id,
            host=payload.host,
            port=payload.port,
            username=payload.username,
            auth_type=payload.auth_type,
            password=payload.password,
            key_path=payload.key_path,
            endpoint=payload.endpoint,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.patch("/api/v1/servers/{server_id}")
def update_server(server_id: str, payload: UpdateServerRequest) -> dict:
    try:
        return server_service.update_server(
            server_id=server_id,
            host=payload.host,
            port=payload.port,
            username=payload.username,
            endpoint=payload.endpoint,
            auth_type=payload.auth_type,
            password=payload.password,
            key_path=payload.key_path,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.delete("/api/v1/servers/{server_id}")
def delete_server(server_id: str) -> dict[str, str]:
    try:
        deleted = server_service.delete_server(server_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not deleted:
        raise HTTPException(
            status_code=404,
            detail='Сервер не найден или не удален.',
        )
    return {'status': 'deleted'}


@app.post("/api/v1/servers/{server_id}/test-connection")
def test_server_connection(server_id: str) -> dict[str, str]:
    try:
        return server_service.test_connection(server_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "awg.platform.api.main:app",
        host="0.0.0.0",
        port=8080,
        reload=False,
    )
