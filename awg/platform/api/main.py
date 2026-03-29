"""ASGI entrypoint для будущей админ-панели и публичного API.

Запуск после установки FastAPI/uvicorn:
    uvicorn awg.platform.api.main:app --host 0.0.0.0 --port 8080
"""

try:
    from fastapi import FastAPI, HTTPException
except ImportError as exc:  # pragma: no cover
    raise RuntimeError(
        "FastAPI не установлен. Добавьте fastapi и uvicorn в зависимости, "
        "когда начнете перенос API слоя."
    ) from exc

from awg.platform.application.user_service import UserService


app = FastAPI(title="AWG Admin API", version="0.1.0")
user_service = UserService()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/v1/servers/{server_id}/profiles")
def profiles_by_server(server_id: str) -> list[dict[str, str | int | None]]:
    try:
        profiles = user_service.list_profiles(server_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return [
        {
            "username": profile.username,
            "owner_id": profile.owner_id,
        }
        for profile in profiles
    ]
