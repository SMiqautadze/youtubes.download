from __future__ import annotations

import asyncio
import json
import subprocess
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.request import urlopen

import socketio
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from app.config import settings
from app.db import init_db
from app.services.coordinator import CoordinatorError, TaskCoordinator
from app.services.validators import resolve_target_container, validate_youtube_url


class ValidateUrlRequest(BaseModel):
    url: str


class VideoDownloadRequest(BaseModel):
    url: str
    quality: str | None = None
    target_container: str | None = None


class PlaylistDownloadRequest(BaseModel):
    url: str
    format: str = Field(default="mp4")
    quality: str | None = None
    target_container: str | None = None


class CleanupSettingsRequest(BaseModel):
    auto_cleanup_enabled: bool
    cleanup_window: str


sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")


def normalize_ytdlp_version(version: str) -> str:
    parts = [part for part in (version or "").strip().split(".") if part]
    normalized: list[str] = []
    for part in parts:
        if part.isdigit():
            normalized.append(str(int(part)))
        else:
            normalized.append(part.lstrip("0") or "0")
    return ".".join(normalized)


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    coordinator._ensure_default_settings()
    coordinator.attach_loop(asyncio.get_running_loop())
    coordinator.start_cleanup_scheduler()
    yield
    coordinator.shutdown()


api_app = FastAPI(title=settings.app_name, lifespan=lifespan)
api_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
coordinator = TaskCoordinator(sio=sio)


@api_app.exception_handler(CoordinatorError)
async def handle_coordinator_error(_, exc: CoordinatorError):
    return JSONResponse(
        status_code=exc.status_code,
        content={"status": "error", "message": exc.message, "error_code": exc.error_code},
    )


@api_app.get("/health")
async def health() -> dict:
    return {"status": "healthy", "service": settings.app_name}


@api_app.get("/api/v1/system/version")
async def system_version() -> dict:
    try:
        installed = subprocess.run(
            ["yt-dlp", "--version"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        ).stdout.strip() or "unknown"
    except Exception:
        installed = "unknown"

    latest = "unknown"
    try:
        with urlopen("https://pypi.org/pypi/yt-dlp/json", timeout=2) as response:
            payload = json.loads(response.read().decode("utf-8"))
            latest = payload.get("info", {}).get("version", "unknown")
    except Exception:
        latest = "unknown"

    return {
        "status": "success",
        "yt_dlp_version": installed,
        "latest_version": latest,
        "latest_version_known": latest != "unknown",
        "is_latest": normalize_ytdlp_version(installed) == normalize_ytdlp_version(latest)
        if "unknown" not in {installed, latest}
        else False,
    }


@api_app.post("/api/v1/system/cleanup")
async def system_cleanup() -> dict:
    return coordinator.cleanup_all()


@api_app.get("/api/v1/settings/cleanup")
async def get_cleanup_settings() -> dict:
    return coordinator.get_settings()


@api_app.post("/api/v1/settings/cleanup")
async def update_cleanup_settings(payload: CleanupSettingsRequest) -> dict:
    return coordinator.update_settings(payload.auto_cleanup_enabled, payload.cleanup_window)


@api_app.post("/api/v1/validate-url")
async def validate_url(payload: ValidateUrlRequest) -> dict:
    result = validate_youtube_url(payload.url)
    return {
        "status": "success" if result.is_valid else "error",
        "is_valid": result.is_valid,
        "message": result.message,
        "normalized_url": result.normalized_url,
    }


@api_app.get("/api/v1/history")
async def history(
    limit: int = Query(default=10, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    q: str | None = Query(default=None),
) -> dict:
    return coordinator.get_history(limit=limit, offset=offset, query=q)


@api_app.get("/api/v1/progress/{task_id}")
async def progress(task_id: str) -> dict:
    return coordinator.get_progress(task_id)


@api_app.post("/api/v1/download/stop/{task_id}")
async def stop_download(task_id: str) -> dict:
    result = coordinator.stop_task(task_id)
    if result["status"] == "error":
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@api_app.post("/api/v1/video/mp3")
async def download_mp3(payload: VideoDownloadRequest) -> JSONResponse:
    quality = payload.quality or "320"
    result = coordinator.start_single_download(payload.url, "mp3", quality, target_container="mp3")
    return JSONResponse(status_code=202, content=result)


@api_app.post("/api/v1/video/mp4/{quality}")
async def download_video(quality: str, payload: VideoDownloadRequest) -> JSONResponse:
    target = payload.target_container or resolve_target_container("mp4", quality)
    result = coordinator.start_single_download(payload.url, "mp4", quality, target_container=target)
    return JSONResponse(status_code=202, content=result)


@api_app.post("/api/v1/playlist/download")
async def download_playlist(payload: PlaylistDownloadRequest) -> JSONResponse:
    format_type = payload.format
    quality = payload.quality or ("320" if format_type == "mp3" else "720")
    target = payload.target_container or resolve_target_container(format_type, quality)
    result = coordinator.start_playlist_download(payload.url, format_type, quality, target_container=target)
    return JSONResponse(
        status_code=202,
        content={
            "status": "success",
            "message": "Playlist download initiated",
            "task_id": result["batch_id"],
            "playlist_task_id": result["batch_id"],
            "playlist_info": result["playlist_info"],
        },
    )


@api_app.get("/api/v1/playlist/status/{batch_id}")
async def playlist_status(batch_id: str) -> dict:
    return coordinator.get_playlist_status(batch_id)


@api_app.post("/api/v1/playlist/stop/{batch_id}")
async def stop_playlist(batch_id: str) -> dict:
    result = coordinator.stop_playlist(batch_id)
    if result["status"] == "error":
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@api_app.get("/api/v1/files/{task_id}")
async def list_files(task_id: str) -> dict:
    return coordinator.list_task_files(task_id)


@api_app.get("/api/v1/files/{task_id}/{filename:path}")
async def download_file(task_id: str, filename: str):
    listing = coordinator.list_task_files(task_id)
    file_row = next((item for item in listing["files"] if item["filename"] == filename), None)
    if not file_row:
        raise HTTPException(status_code=404, detail="File not found")
    file_path = settings.downloads_dir / task_id / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(file_path, filename=filename)


@api_app.get("/", include_in_schema=False)
async def index():
    return FileResponse(settings.static_dir / "index.html")


@api_app.get("/{path:path}", include_in_schema=False)
async def static_files(path: str):
    candidate = settings.static_dir / path
    if candidate.is_file():
        return FileResponse(candidate)
    return FileResponse(settings.static_dir / "index.html")


app = socketio.ASGIApp(sio, other_asgi_app=api_app, socketio_path="socket.io")
