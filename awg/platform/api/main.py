"""ASGI entrypoint для админского API.

Запуск после установки FastAPI/uvicorn:
    uvicorn awg.platform.api.main:app --host 0.0.0.0 --port 8080
"""

import os
import sys
from typing import Literal, NoReturn

if __package__ in {None, ""}:
    project_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..")
    )
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

try:
    from fastapi import FastAPI, HTTPException, Path, Query, Request
    from fastapi.responses import JSONResponse
    from pydantic import BaseModel, Field
except ImportError as exc:  # pragma: no cover
    raise RuntimeError(
        "FastAPI не установлен. Добавьте fastapi и uvicorn в зависимости, "
        "когда начнете перенос API слоя."
    ) from exc

from awg.platform.application.profile_service import ProfileService
from awg.platform.application.server_service import ServerService
from awg.platform.application.user_service import UserService


class ApiError(BaseModel):
    code: str = Field(description="Код ошибки")
    message: str = Field(description="Читаемое описание ошибки")


class ErrorResponse(BaseModel):
    ok: Literal[False] = Field(default=False)
    error: ApiError


class HealthData(BaseModel):
    status: Literal["ok"] = "ok"


class HealthResponse(BaseModel):
    ok: Literal[True] = Field(default=True)
    data: HealthData


class ProfileData(BaseModel):
    profile_id: str | None = Field(default=None)
    server_id: str
    user_id: str | int | None = Field(default=None)
    profile_name: str


class ProfileListResponse(BaseModel):
    ok: Literal[True] = Field(default=True)
    data: list[ProfileData]


class CreatedProfileData(BaseModel):
    profile_id: str
    server_id: str
    user_id: str | int
    profile_name: str
    conf_text: str = Field(description="Содержимое .conf")
    vpn_uri: str = Field(description="Ссылка формата vpn://...")


class CreateProfileResponse(BaseModel):
    ok: Literal[True] = Field(default=True)
    data: CreatedProfileData


class DeleteProfileData(BaseModel):
    profile_id: str
    status: str = Field(examples=["deleted"])


class DeleteProfileResponse(BaseModel):
    ok: Literal[True] = Field(default=True)
    data: DeleteProfileData


class UserData(BaseModel):
    user_id: str | int
    profiles_count: int


class UserListResponse(BaseModel):
    ok: Literal[True] = Field(default=True)
    data: list[UserData]


class ServerData(BaseModel):
    server_id: str
    host: str | None = None
    port: int | str | None = None
    username: str | None = None
    auth_type: str | None = None
    endpoint: str | None = None
    key_path: str | None = None
    docker_container: str | None = None
    wg_config_file: str | None = None
    is_remote: str | None = None


class ServerListResponse(BaseModel):
    ok: Literal[True] = Field(default=True)
    data: list[ServerData]


class ServerResponse(BaseModel):
    ok: Literal[True] = Field(default=True)
    data: ServerData


class DeleteServerData(BaseModel):
    status: Literal["deleted"]


class DeleteServerResponse(BaseModel):
    ok: Literal[True] = Field(default=True)
    data: DeleteServerData


class ServerTestData(BaseModel):
    status: str
    message: str


class ServerTestResponse(BaseModel):
    ok: Literal[True] = Field(default=True)
    data: ServerTestData


class CreateProfileRequest(BaseModel):
    server_id: str = Field(min_length=1, description="ID сервера")
    user_id: str | int = Field(description="ID пользователя владельца")
    profile_name: str = Field(
        min_length=1,
        max_length=128,
        description="Имя профиля (латиница, цифры, _, -, .)",
    )


class CreateServerRequest(BaseModel):
    server_id: str = Field(min_length=1, description="ID сервера")
    host: str = Field(description="Host/IP сервера")
    port: int = Field(default=22, ge=1, le=65535)
    username: str = Field(description="SSH пользователь")
    auth_type: Literal["password", "key"]
    password: str | None = Field(default=None, description="SSH пароль")
    key_path: str | None = Field(
        default=None,
        description="Путь к SSH приватному ключу",
    )
    endpoint: str | None = Field(default=None)


class UpdateServerRequest(BaseModel):
    host: str | None = None
    port: int | None = Field(default=None, ge=1, le=65535)
    username: str | None = None
    endpoint: str | None = None
    auth_type: Literal["password", "key"] | None = None
    password: str | None = None
    key_path: str | None = None


app = FastAPI(
    title="AWG Admin API",
    version="0.3.0",
    description="HTTP API для управления профилями AWG и серверами.",
)
user_service = UserService()
profile_service = ProfileService()
server_service = ServerService()


def _error(status_code: int, code: str, message: str) -> NoReturn:
    raise HTTPException(
        status_code=status_code,
        detail={"code": code, "message": message},
    )


def _server_data(server_dict: dict) -> ServerData:
    return ServerData(
        server_id=str(server_dict.get("id") or ""),
        host=server_dict.get("host"),
        port=server_dict.get("port"),
        username=server_dict.get("username"),
        auth_type=server_dict.get("auth_type"),
        endpoint=server_dict.get("endpoint"),
        key_path=server_dict.get("key_path"),
        docker_container=server_dict.get("docker_container"),
        wg_config_file=server_dict.get("wg_config_file"),
        is_remote=server_dict.get("is_remote"),
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(
    _: Request,
    exc: HTTPException,
) -> JSONResponse:
    detail = exc.detail
    if isinstance(detail, dict):
        code = str(detail.get("code") or "http_error")
        message = str(detail.get("message") or "HTTP error")
    else:
        code = "http_error"
        message = str(detail)
    payload = ErrorResponse(error=ApiError(code=code, message=message))
    return JSONResponse(
        status_code=exc.status_code,
        content=payload.model_dump(),
    )


def _profile_data(item: dict[str, str | int | None]) -> ProfileData:
    return ProfileData(
        profile_id=(
            str(item.get("profile_id")) if item.get("profile_id") is not None
            else None
        ),
        server_id=str(item.get("server_id") or ""),
        user_id=item.get("user_id"),
        profile_name=str(item.get("profile_name") or ""),
    )


def _user_data(item: dict[str, str | int]) -> UserData:
    return UserData(
        user_id=item.get("user_id") or "",
        profiles_count=int(item.get("profiles_count") or 0),
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(
    _: Request,
    exc: Exception,
) -> JSONResponse:
    payload = ErrorResponse(
        error=ApiError(code="internal_error", message=str(exc)),
    )
    return JSONResponse(status_code=500, content=payload.model_dump())


@app.get("/health")
def health() -> HealthResponse:
    return HealthResponse(data=HealthData())


@app.get(
    "/api/v1/servers/{server_id}/profiles",
    response_model=ProfileListResponse,
    responses={400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
    summary="Список профилей сервера",
)
def profiles_by_server(
    server_id: str = Path(description="ID сервера"),
) -> ProfileListResponse:
    try:
        profiles = profile_service.list_profiles_by_server(server_id)
    except Exception as exc:
        _error(500, "profiles_by_server_failed", str(exc))
    return ProfileListResponse(data=[_profile_data(item) for item in profiles])


@app.post(
    "/api/v1/profiles",
    response_model=CreateProfileResponse,
    responses={400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
    summary="Создать профиль",
)
def create_profile(payload: CreateProfileRequest) -> CreateProfileResponse:
    try:
        created = profile_service.create_profile(
            server_id=payload.server_id,
            user_id=payload.user_id,
            profile_name=payload.profile_name,
        )
    except ValueError as exc:
        _error(400, "invalid_profile_payload", str(exc))
    except RuntimeError as exc:
        _error(500, "profile_create_failed", str(exc))

    return CreateProfileResponse(
        data=CreatedProfileData(
            profile_id=created.profile_id,
            server_id=created.server_id,
            user_id=created.user_id,
            profile_name=created.profile_name,
            conf_text=created.conf_text,
            vpn_uri=created.vpn_uri,
        )
    )


@app.delete(
    "/api/v1/profiles/{profile_id}",
    response_model=DeleteProfileResponse,
    responses={404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
    summary="Удалить профиль",
)
def delete_profile(
    profile_id: str = Path(description="Идентификатор профиля"),
) -> DeleteProfileResponse:
    try:
        result = profile_service.delete_profile(profile_id)
    except KeyError as exc:
        _error(404, "profile_not_found", str(exc))
    except RuntimeError as exc:
        _error(500, "profile_delete_failed", str(exc))

    return DeleteProfileResponse(
        data=DeleteProfileData(
            profile_id=result.profile_id,
            status=result.status,
        )
    )


@app.get(
    "/api/v1/users/{user_id}/profiles",
    response_model=ProfileListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="Список профилей пользователя",
)
def profiles_by_user(
    user_id: str = Path(description="ID пользователя"),
    server_id: str | None = Query(
        default=None,
        description="Опциональный фильтр по серверу",
    ),
) -> ProfileListResponse:
    try:
        profiles = profile_service.list_profiles_by_owner(
            user_id=user_id,
            server_id=server_id,
        )
    except Exception as exc:
        _error(500, "profiles_by_user_failed", str(exc))
    return ProfileListResponse(data=[_profile_data(item) for item in profiles])


@app.get(
    "/api/v1/users",
    response_model=UserListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="Список пользователей",
)
def list_users() -> UserListResponse:
    try:
        users = user_service.list_users()
    except Exception as exc:
        _error(500, "users_list_failed", str(exc))
    return UserListResponse(data=[_user_data(item) for item in users])


@app.get(
    "/api/v1/servers",
    response_model=ServerListResponse,
    responses={500: {"model": ErrorResponse}},
    summary="Список серверов",
)
def list_servers() -> ServerListResponse:
    try:
        servers = server_service.list_servers()
    except Exception as exc:
        _error(500, "servers_list_failed", str(exc))

    return ServerListResponse(
        data=[
            ServerData(
                server_id=server.server_id,
                host=server.host,
                port=server.port,
                username=server.username,
                auth_type=server.auth_type,
                endpoint=server.endpoint,
            )
            for server in servers
        ]
    )


@app.post(
    "/api/v1/servers",
    response_model=ServerResponse,
    responses={400: {"model": ErrorResponse}},
    summary="Создать сервер",
)
def create_server(payload: CreateServerRequest) -> ServerResponse:
    try:
        server = server_service.create_server(
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
        _error(400, "server_create_failed", str(exc))

    server["id"] = payload.server_id
    return ServerResponse(data=_server_data(server))


@app.patch(
    "/api/v1/servers/{server_id}",
    response_model=ServerResponse,
    responses={
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="Обновить сервер",
)
def update_server(
    payload: UpdateServerRequest,
    server_id: str = Path(description="ID сервера"),
) -> ServerResponse:
    try:
        server = server_service.update_server(
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
        _error(404, "server_not_found", str(exc))
    except ValueError as exc:
        _error(400, "invalid_server_payload", str(exc))
    except Exception as exc:
        _error(500, "server_update_failed", str(exc))

    server["id"] = server_id
    return ServerResponse(data=_server_data(server))


@app.delete(
    "/api/v1/servers/{server_id}",
    response_model=DeleteServerResponse,
    responses={404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
    summary="Удалить сервер",
)
def delete_server(
    server_id: str = Path(description="ID сервера"),
) -> DeleteServerResponse:
    try:
        deleted = server_service.delete_server(server_id)
    except Exception as exc:
        _error(500, "server_delete_failed", str(exc))
    if not deleted:
        _error(
            404,
            "server_not_found",
            "Сервер не найден или не удален.",
        )
    return DeleteServerResponse(data=DeleteServerData(status="deleted"))


@app.post(
    "/api/v1/servers/{server_id}/test-connection",
    response_model=ServerTestResponse,
    responses={404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
    summary="Проверить SSH подключение",
)
def test_server_connection(
    server_id: str = Path(description="ID сервера"),
) -> ServerTestResponse:
    try:
        result = server_service.test_connection(server_id)
    except KeyError as exc:
        _error(404, "server_not_found", str(exc))
    except Exception as exc:
        _error(500, "server_test_failed", str(exc))
    return ServerTestResponse(data=ServerTestData(**result))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "awg.platform.api.main:app",
        host="0.0.0.0",
        port=8080,
        reload=False,
    )
