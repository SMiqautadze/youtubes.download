from __future__ import annotations

import asyncio
import json
import shutil
import threading
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import socketio
from sqlalchemy import or_, select

from app.config import settings
from app.db import SessionLocal, session_scope
from app.models import AppSetting, PlaylistItem, PlaylistTask, TaskRegistry
from app.services.downloader import DownloadCancelled, DownloadResult, DownloadSpec, LocalDownloader
from app.services.validators import (
    classify_error_message,
    cleanup_window_to_seconds,
    resolve_target_container,
    sanitize_filename,
    validate_youtube_url,
)


ACTIVE_STATUSES = {"PENDING", "ANALYZING", "PROCESSING", "DOWNLOADING", "UPLOADING"}
FINAL_STATUSES = {"SUCCESS", "FAILED", "CANCELLED", "COMPLETED"}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def format_playlist_display_title(playlist_title: str, owner: str | None = None) -> str:
    normalized_title = (playlist_title or "").strip() or "Untitled Playlist"
    normalized_owner = (owner or "").strip()
    if normalized_owner:
        return f"{normalized_owner} : {normalized_title}"
    return f"Playlist: {normalized_title}"


@dataclass
class TaskControl:
    task_id: str
    cancel_event: threading.Event = field(default_factory=threading.Event)
    current_process: Any | None = None


class CoordinatorError(Exception):
    def __init__(self, message: str, status_code: int = 400, error_code: str = "UNKNOWN_ERROR"):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.error_code = error_code


class TaskCoordinator:
    def __init__(self, sio: socketio.AsyncServer) -> None:
        self.sio = sio
        self.downloader = LocalDownloader()
        self.executor = ThreadPoolExecutor(max_workers=settings.max_workers, thread_name_prefix="ytdl")
        self.loop: asyncio.AbstractEventLoop | None = None
        self._lock = threading.RLock()
        self._controls: dict[str, TaskControl] = {}
        self._futures: dict[str, Future] = {}
        self._progress_store: dict[str, dict[str, Any]] = {}
        self._cleanup_stop = threading.Event()
        self._cleanup_thread: threading.Thread | None = None

    def attach_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop

    def shutdown(self) -> None:
        self._cleanup_stop.set()
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=2)
        self.executor.shutdown(wait=False, cancel_futures=True)

    def start_cleanup_scheduler(self) -> None:
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            return

        def runner() -> None:
            while not self._cleanup_stop.wait(settings.cleanup_interval_seconds):
                try:
                    self.cleanup_expired()
                except Exception:
                    continue

        self._cleanup_thread = threading.Thread(target=runner, name="cleanup-scheduler", daemon=True)
        self._cleanup_thread.start()

    def get_settings(self) -> dict[str, Any]:
        with session_scope() as session:
            settings_row = session.get(AppSetting, 1)
            if not settings_row:
                settings_row = AppSetting(id=1)
                session.add(settings_row)
                session.flush()
            return {
                "status": "success",
                "auto_cleanup_enabled": settings_row.auto_cleanup_enabled,
                "cleanup_window": settings_row.cleanup_window,
            }

    def update_settings(self, auto_cleanup_enabled: bool, cleanup_window: str) -> dict[str, Any]:
        cleanup_window_to_seconds(cleanup_window)
        with session_scope() as session:
            settings_row = session.get(AppSetting, 1)
            if not settings_row:
                settings_row = AppSetting(id=1)
                session.add(settings_row)
            settings_row.auto_cleanup_enabled = auto_cleanup_enabled
            settings_row.cleanup_window = cleanup_window
            session.flush()
            return {
                "status": "success",
                "auto_cleanup_enabled": settings_row.auto_cleanup_enabled,
                "cleanup_window": settings_row.cleanup_window,
            }

    def start_single_download(
        self,
        url: str,
        format_type: str,
        quality: str,
        target_container: str | None = None,
    ) -> dict[str, Any]:
        validation = validate_youtube_url(url)
        if not validation.is_valid:
            raise CoordinatorError(validation.message, status_code=400, error_code="INVALID_LINK")

        target = target_container or resolve_target_container(format_type, quality)
        task_id = uuid.uuid4().hex[:12]
        info = self._analyze_video_or_raise(validation.normalized_url or url)

        with session_scope() as session:
            task = TaskRegistry(
                task_id=task_id,
                video_url=validation.normalized_url or url,
                video_title=info["title"],
                video_id=info["video_id"],
                format=format_type,
                quality=str(quality),
                target_container=target,
                task_type="single_audio" if format_type == "mp3" else "single_video",
                status="PENDING",
                current_operation="Queued",
                metadata_json=json.dumps({"webpage_url": info["webpage_url"]}),
            )
            session.add(task)

        with self._lock:
            self._controls[task_id] = TaskControl(task_id=task_id)
            self._progress_store[task_id] = self._progress_payload(task_id, "PENDING", 0, "Queued")
            self._futures[task_id] = self.executor.submit(
                self._run_single_download,
                task_id,
                validation.normalized_url or url,
                format_type,
                str(quality),
                target,
            )

        return {"status": "success", "task_id": task_id, "video_title": info["title"]}

    def start_playlist_download(
        self,
        url: str,
        format_type: str,
        quality: str,
        target_container: str | None = None,
    ) -> dict[str, Any]:
        validation = validate_youtube_url(url)
        if not validation.is_valid:
            raise CoordinatorError(validation.message, status_code=400, error_code="INVALID_LINK")

        batch_id = f"playlist_task_{uuid.uuid4().hex[:12]}"
        target = target_container or resolve_target_container(format_type, quality)
        analysis = self._analyze_playlist_or_raise(validation.normalized_url or url)
        entries = analysis["entries"]

        with session_scope() as session:
            playlist = PlaylistTask(
                batch_id=batch_id,
                playlist_url=validation.normalized_url or url,
                playlist_title=analysis["title"],
                total_videos=len(entries),
                status="PENDING",
                format=format_type,
                quality=str(quality),
                target_container=target,
                current_operation="Queued",
                metadata_json=json.dumps(
                    {
                        "playlist_id": analysis.get("playlist_id"),
                        "owner": analysis.get("owner"),
                    }
                ),
            )
            session.add(playlist)

            for entry in entries:
                task_id = uuid.uuid4().hex[:12]
                task = TaskRegistry(
                    task_id=task_id,
                    playlist_batch_id=batch_id,
                    video_url=entry["video_url"],
                    video_title=entry["video_title"],
                    video_id=entry["video_id"],
                    format=format_type,
                    quality=str(quality),
                    target_container=target,
                    task_type="playlist_audio" if format_type == "mp3" else "playlist_video",
                    status="PENDING",
                    current_operation="Queued",
                )
                item = PlaylistItem(
                    item_id=uuid.uuid4().hex,
                    batch_id=batch_id,
                    task_id=task_id,
                    video_url=entry["video_url"],
                    video_id=entry["video_id"],
                    video_title=entry["video_title"],
                    position_in_playlist=entry["position_in_playlist"],
                    status="PENDING",
                    progress_percentage=0,
                    current_operation="Queued",
                )
                session.add(task)
                session.add(item)

        with self._lock:
            self._controls[batch_id] = TaskControl(task_id=batch_id)
            self._progress_store[batch_id] = self._progress_payload(batch_id, "PENDING", 0, "Queued")
            self._futures[batch_id] = self.executor.submit(
                self._run_playlist_download,
                batch_id,
                format_type,
                str(quality),
                target,
            )

        return {
            "status": "success",
            "batch_id": batch_id,
            "playlist_info": {
                "title": analysis["title"],
                "owner": analysis.get("owner"),
                "display_title": format_playlist_display_title(analysis["title"], analysis.get("owner")),
                "total_videos": len(entries),
                "format": format_type,
                "quality": str(quality),
                "target_container": target,
            },
        }

    def stop_task(self, task_id: str) -> dict[str, Any]:
        with self._lock:
            control = self._controls.get(task_id)
        if not control:
            with session_scope() as session:
                task = session.scalar(select(TaskRegistry).where(TaskRegistry.task_id == task_id))
                if task and task.playlist_batch_id:
                    return self.stop_playlist(task.playlist_batch_id)
            return {"status": "error", "message": "Task not found"}

        control.cancel_event.set()
        self.downloader.terminate_process(control.current_process)
        return {"status": "success", "message": "Stop command sent"}

    def stop_playlist(self, batch_id: str) -> dict[str, Any]:
        with self._lock:
            control = self._controls.get(batch_id)
        if not control:
            return {"status": "error", "message": "Playlist task not found"}
        control.cancel_event.set()
        self.downloader.terminate_process(control.current_process)
        return {"status": "success", "message": "Playlist stop command sent"}

    def get_progress(self, task_id: str) -> dict[str, Any]:
        if task_id.startswith("playlist_task_"):
            return self._get_playlist_progress(task_id)

        with self._lock:
            in_memory = self._progress_store.get(task_id)
        if in_memory:
            return {"status": "success", "progress": in_memory}

        with session_scope() as session:
            task = session.scalar(select(TaskRegistry).where(TaskRegistry.task_id == task_id))
            if not task:
                raise CoordinatorError("Task not found", status_code=404)
            return {
                "status": "success",
                "progress": self._serialize_task_progress(task),
            }

    def get_history(self, limit: int, offset: int, query: str | None = None) -> dict[str, Any]:
        with session_scope() as session:
            task_stmt = select(TaskRegistry).where(TaskRegistry.playlist_batch_id.is_(None))
            playlist_stmt = select(PlaylistTask)

            if query:
                pattern = f"%{query.lower()}%"
                task_stmt = task_stmt.where(
                    or_(
                        TaskRegistry.video_title.ilike(pattern),
                        TaskRegistry.status.ilike(pattern),
                    )
                )
                playlist_stmt = playlist_stmt.where(
                    or_(
                        PlaylistTask.playlist_title.ilike(pattern),
                        PlaylistTask.status.ilike(pattern),
                    )
                )

            tasks = session.scalars(task_stmt.order_by(TaskRegistry.created_at.desc())).all()
            playlists = session.scalars(playlist_stmt.order_by(PlaylistTask.created_at.desc())).all()

            rows = [self._serialize_history_task(task) for task in tasks] + [
                self._serialize_history_playlist(playlist) for playlist in playlists
            ]
            rows.sort(key=lambda item: item.get("created_at") or "", reverse=True)
            paginated = rows[offset : offset + limit]
            return {"status": "success", "history": paginated, "count": len(rows)}

    def get_playlist_status(self, batch_id: str) -> dict[str, Any]:
        with session_scope() as session:
            playlist = session.scalar(select(PlaylistTask).where(PlaylistTask.batch_id == batch_id))
            if not playlist:
                raise CoordinatorError("Playlist task not found", status_code=404)
            items = session.scalars(
                select(PlaylistItem).where(PlaylistItem.batch_id == batch_id).order_by(PlaylistItem.position_in_playlist.asc())
            ).all()
            return {
                "status": "success",
                "data": {
                    "batch_id": playlist.batch_id,
                    "playlist_title": playlist.playlist_title,
                    "playlist_owner": playlist.metadata_payload.get("owner"),
                    "display_title": format_playlist_display_title(
                        playlist.playlist_title,
                        playlist.metadata_payload.get("owner"),
                    ),
                    "status": playlist.status,
                    "progress_percentage": playlist.progress_percentage,
                    "current_operation": playlist.current_operation,
                    "total_videos": playlist.total_videos,
                    "completed_videos": playlist.completed_videos,
                    "failed_videos": playlist.failed_videos,
                    "format": playlist.format,
                    "quality": playlist.quality,
                    "target_container": playlist.target_container,
                    "items": [self._serialize_playlist_item(item) for item in items],
                },
            }

    def list_task_files(self, task_id: str) -> dict[str, Any]:
        with session_scope() as session:
            task = session.scalar(select(TaskRegistry).where(TaskRegistry.task_id == task_id))
            if not task or not task.download_path:
                raise CoordinatorError("No files found", status_code=404)
            path = Path(task.download_path)
            if not path.exists():
                raise CoordinatorError("No files found", status_code=404)
            return {
                "status": "success",
                "task_id": task_id,
                "files": [
                    {
                        "filename": path.name,
                        "size": path.stat().st_size,
                        "download_url": task.download_url,
                    }
                ],
                "count": 1,
            }

    def cleanup_all(self) -> dict[str, Any]:
        with session_scope() as session:
            tasks = session.scalars(select(TaskRegistry).where(TaskRegistry.status.in_(FINAL_STATUSES))).all()
            deleted_files = self._delete_task_files(session, tasks)
        return {
            "status": "success",
            "deleted_files": deleted_files,
            "message": f"Deleted files for {deleted_files} completed tasks",
        }

    def cleanup_expired(self) -> dict[str, Any]:
        settings_state = self.get_settings()
        if not settings_state["auto_cleanup_enabled"]:
            return {"status": "success", "removed": 0}

        cutoff = utcnow() - timedelta(seconds=cleanup_window_to_seconds(settings_state["cleanup_window"]))
        with session_scope() as session:
            tasks = session.scalars(
                select(TaskRegistry).where(
                    TaskRegistry.status.in_(FINAL_STATUSES),
                    TaskRegistry.completed_at.is_not(None),
                    TaskRegistry.completed_at < cutoff,
                )
            ).all()
            deleted_files = self._delete_task_files(session, tasks)
        return {"status": "success", "deleted_files": deleted_files}

    def _run_single_download(
        self,
        task_id: str,
        url: str,
        format_type: str,
        quality: str,
        target_container: str,
    ) -> None:
        control = self._controls[task_id]
        task_dir = settings.downloads_dir / task_id
        self._update_task(task_id, status="PROCESSING", progress=0, operation="Preparing download")

        try:
            result = self.downloader.run_download(
                DownloadSpec(
                    url=url,
                    format_type=format_type,
                    quality=quality,
                    target_container=target_container,
                    task_dir=task_dir,
                ),
                progress_callback=lambda status, progress, operation, extra: self._update_task(
                    task_id,
                    status=status,
                    progress=progress,
                    operation=operation,
                    extra=extra,
                ),
                process_callback=lambda process: self._set_current_process(task_id, process),
                cancel_requested=control.cancel_event.is_set,
            )
            self._mark_task_success(task_id, result)
        except DownloadCancelled:
            self._mark_task_cancelled(task_id)
        except Exception as exc:
            self._mark_task_failure(task_id, str(exc))
        finally:
            self._finalize_control(task_id)

    def _run_playlist_download(
        self,
        batch_id: str,
        format_type: str,
        quality: str,
        target_container: str,
    ) -> None:
        control = self._controls[batch_id]
        with session_scope() as session:
            playlist = session.scalar(select(PlaylistTask).where(PlaylistTask.batch_id == batch_id))
            items = session.scalars(
                select(PlaylistItem).where(PlaylistItem.batch_id == batch_id).order_by(PlaylistItem.position_in_playlist.asc())
            ).all()

        self._update_playlist(batch_id, status="PROCESSING", progress=0, operation="Starting playlist")

        for index, item in enumerate(items, start=1):
            if control.cancel_event.is_set():
                self._cancel_remaining_playlist_items(batch_id, items[index - 1 :])
                self._update_playlist(batch_id, status="CANCELLED", operation="Cancelled")
                self._finalize_control(batch_id)
                return

            self._update_playlist(
                batch_id,
                status="PROCESSING",
                operation=f"Downloading {index}/{len(items)}: {item.video_title}",
            )
            self._update_task(item.task_id, status="PROCESSING", progress=0, operation="Preparing download")
            self._update_playlist_item(
                item.task_id,
                status="PROCESSING",
                progress=0,
                operation="Preparing download",
            )

            try:
                result = self.downloader.run_download(
                    DownloadSpec(
                        url=item.video_url,
                        format_type=format_type,
                        quality=quality,
                        target_container=target_container,
                        task_dir=settings.downloads_dir / item.task_id,
                        is_playlist_item=True,
                    ),
                    progress_callback=lambda status, progress, operation, extra, child_task_id=item.task_id, completed=index - 1, total=len(items): self._handle_playlist_child_progress(
                        batch_id=batch_id,
                        child_task_id=child_task_id,
                        status=status,
                        progress=progress,
                        operation=operation,
                        extra=extra,
                        completed_items=completed,
                        total_items=total,
                    ),
                    process_callback=lambda process: self._set_current_process(batch_id, process),
                    cancel_requested=control.cancel_event.is_set,
                )
                self._mark_task_success(item.task_id, result, emit=False)
                self._mark_playlist_item_success(item.task_id, result)
            except DownloadCancelled:
                self._mark_task_cancelled(item.task_id, emit=False)
                self._update_playlist_item(item.task_id, status="CANCELLED", progress=0, operation="Cancelled")
                self._cancel_remaining_playlist_items(batch_id, items[index:])
                self._update_playlist(batch_id, status="CANCELLED", operation="Cancelled")
                self._emit_progress(
                    batch_id,
                    task_status="CANCELLED",
                    percentage=self._playlist_percentage(batch_id),
                    current_operation="Cancelled",
                )
                self._finalize_control(batch_id)
                return
            except Exception as exc:
                self._mark_task_failure(item.task_id, str(exc), emit=False)
                self._update_playlist_item(
                    item.task_id,
                    status="FAILED",
                    progress=0,
                    operation="Failed",
                    error_message=str(exc),
                )

            self._recalculate_playlist(batch_id)

        with session_scope() as session:
            playlist = session.scalar(select(PlaylistTask).where(PlaylistTask.batch_id == batch_id))
            if not playlist:
                self._finalize_control(batch_id)
                return
            final_status = "FAILED" if playlist.completed_videos == 0 and playlist.failed_videos else "COMPLETED"
            playlist.status = final_status
            playlist.progress_percentage = 100
            playlist.current_operation = "Completed"
            playlist.completed_at = utcnow()
        self._emit_progress(
            batch_id,
            task_status=final_status,
            percentage=100,
            current_operation="Completed",
            completed_videos=playlist.completed_videos,
            total_videos=playlist.total_videos,
        )
        self._finalize_control(batch_id)

    def _ensure_default_settings(self) -> None:
        with session_scope() as session:
            existing = session.get(AppSetting, 1)
            if not existing:
                session.add(AppSetting(id=1, auto_cleanup_enabled=False, cleanup_window="1w"))

    def _analyze_video_or_raise(self, url: str) -> dict[str, Any]:
        try:
            return self.downloader.analyze_video(url)
        except Exception as exc:
            code, message = classify_error_message(str(exc))
            raise CoordinatorError(message, status_code=400, error_code=code) from exc

    def _analyze_playlist_or_raise(self, url: str) -> dict[str, Any]:
        try:
            return self.downloader.analyze_playlist(url)
        except Exception as exc:
            code, message = classify_error_message(str(exc))
            raise CoordinatorError(message, status_code=400, error_code=code) from exc

    def _serialize_task_progress(self, task: TaskRegistry) -> dict[str, Any]:
        return {
            "task_id": task.task_id,
            "task_status": task.status,
            "percentage": float(task.progress_percentage or 0),
            "current_operation": task.current_operation or task.status.title(),
        }

    def _serialize_history_task(self, task: TaskRegistry) -> dict[str, Any]:
        file_exists = bool(task.download_path and Path(task.download_path).exists())
        return {
            "task_id": task.task_id,
            "video_title": task.video_title or "Untitled",
            "video_url": task.video_url,
            "status": task.status,
            "progress_percentage": task.progress_percentage,
            "current_operation": task.current_operation,
            "format": task.format,
            "quality": task.quality,
            "target_container": task.target_container,
            "task_type": task.task_type,
            "download_url": task.download_url,
            "output_file_name": task.output_file_name,
            "file_exists": file_exists,
            "error_message": task.error_message,
            "error_code": task.error_code,
            "created_at": task.created_at.isoformat() if task.created_at else None,
            "completed_at": task.completed_at.isoformat() if task.completed_at else None,
        }

    def _serialize_history_playlist(self, playlist: PlaylistTask) -> dict[str, Any]:
        owner = playlist.metadata_payload.get("owner")
        return {
            "task_id": playlist.batch_id,
            "video_title": format_playlist_display_title(playlist.playlist_title, owner),
            "video_url": playlist.playlist_url,
            "status": playlist.status,
            "progress_percentage": playlist.progress_percentage,
            "current_operation": playlist.current_operation,
            "format": playlist.format,
            "quality": playlist.quality,
            "target_container": playlist.target_container,
            "task_type": "playlist",
            "file_exists": False,
            "error_message": playlist.error_message,
            "error_code": playlist.error_code,
            "created_at": playlist.created_at.isoformat() if playlist.created_at else None,
            "completed_at": playlist.completed_at.isoformat() if playlist.completed_at else None,
        }

    def _serialize_playlist_item(self, item: PlaylistItem) -> dict[str, Any]:
        task = item.task
        return {
            "item_id": item.item_id,
            "task_id": item.task_id,
            "video_title": item.video_title or "Untitled",
            "video_url": item.video_url,
            "video_id": item.video_id,
            "position_in_playlist": item.position_in_playlist,
            "status": item.status,
            "progress_percentage": item.progress_percentage,
            "current_operation": item.current_operation,
            "download_url": item.download_url,
            "output_file_name": item.output_file_name,
            "format": task.format if task else None,
            "quality": task.quality if task else None,
            "target_container": task.target_container if task else None,
            "file_exists": bool(task and task.download_path and Path(task.download_path).exists()),
            "error_message": item.error_message,
            "error_code": item.error_code,
            "created_at": item.created_at.isoformat() if item.created_at else None,
            "completed_at": item.completed_at.isoformat() if item.completed_at else None,
        }

    def _progress_payload(self, task_id: str, status: str, percentage: float, current_operation: str) -> dict[str, Any]:
        return {
            "task_id": task_id,
            "task_status": status,
            "percentage": float(percentage),
            "current_operation": current_operation,
        }

    def _emit_progress(self, task_id: str, **payload: Any) -> None:
        data = {"task_id": task_id, **payload}
        with self._lock:
            self._progress_store[task_id] = {
                "task_id": task_id,
                "task_status": data.get("task_status", "PROCESSING"),
                "percentage": float(data.get("percentage", 0) or 0),
                "current_operation": data.get("current_operation") or "",
                "completed_videos": data.get("completed_videos"),
                "total_videos": data.get("total_videos"),
                "error_message": data.get("error_message"),
            }
        if self.loop:
            asyncio.run_coroutine_threadsafe(self.sio.emit("download_progress", data), self.loop)

    def _set_current_process(self, key: str, process: Any | None) -> None:
        with self._lock:
            control = self._controls.get(key)
            if control:
                control.current_process = process

    def _update_task(
        self,
        task_id: str,
        *,
        status: str,
        progress: float,
        operation: str,
        extra: dict[str, Any] | None = None,
    ) -> None:
        with session_scope() as session:
            task = session.scalar(select(TaskRegistry).where(TaskRegistry.task_id == task_id))
            if not task:
                return
            task.status = status
            task.progress_percentage = int(progress)
            task.current_operation = operation
            if extra and extra.get("title") and not task.video_title:
                task.video_title = extra["title"]
            if status == "PROCESSING" and not task.started_at:
                task.started_at = utcnow()
        self._emit_progress(task_id, task_status=status, percentage=progress, current_operation=operation)

    def _mark_task_success(self, task_id: str, result: DownloadResult, emit: bool = True) -> None:
        with session_scope() as session:
            task = session.scalar(select(TaskRegistry).where(TaskRegistry.task_id == task_id))
            if not task:
                return
            task.status = "SUCCESS"
            task.progress_percentage = 100
            task.current_operation = "Completed"
            task.video_title = result.title or task.video_title
            task.video_id = result.video_id or task.video_id
            task.download_path = str(result.file_path)
            task.output_file_name = result.file_path.name
            task.download_url = f"/api/v1/files/{task.task_id}/{result.file_path.name}"
            task.file_size_bytes = result.file_size_bytes
            task.completed_at = utcnow()
        if emit:
            self._emit_progress(task_id, task_status="SUCCESS", percentage=100, current_operation="Completed")

    def _mark_task_failure(self, task_id: str, raw_message: str, emit: bool = True) -> None:
        error_code, message = classify_error_message(raw_message)
        with session_scope() as session:
            task = session.scalar(select(TaskRegistry).where(TaskRegistry.task_id == task_id))
            if not task:
                return
            task.status = "FAILED"
            task.current_operation = "Failed"
            task.error_code = error_code
            task.error_message = message
            task.completed_at = utcnow()
        if emit:
            self._emit_progress(
                task_id,
                task_status="FAILED",
                percentage=0,
                current_operation="Failed",
                error_message=message,
            )

    def _mark_task_cancelled(self, task_id: str, emit: bool = True) -> None:
        with session_scope() as session:
            task = session.scalar(select(TaskRegistry).where(TaskRegistry.task_id == task_id))
            if not task:
                return
            task.status = "CANCELLED"
            task.current_operation = "Cancelled"
            task.completed_at = utcnow()
        if emit:
            self._emit_progress(task_id, task_status="CANCELLED", percentage=0, current_operation="Cancelled")

    def _update_playlist(
        self,
        batch_id: str,
        *,
        status: str,
        progress: int | None = None,
        operation: str | None = None,
    ) -> None:
        with session_scope() as session:
            playlist = session.scalar(select(PlaylistTask).where(PlaylistTask.batch_id == batch_id))
            if not playlist:
                return
            playlist.status = status
            if progress is not None:
                playlist.progress_percentage = progress
            if operation is not None:
                playlist.current_operation = operation
            if status == "PROCESSING" and not playlist.started_at:
                playlist.started_at = utcnow()
            if status in {"COMPLETED", "FAILED", "CANCELLED"}:
                playlist.completed_at = utcnow()
            completed_videos = playlist.completed_videos
            total_videos = playlist.total_videos
            current_operation = playlist.current_operation or operation or ""
            percentage = playlist.progress_percentage
        self._emit_progress(
            batch_id,
            task_status=status,
            percentage=percentage,
            current_operation=current_operation,
            completed_videos=completed_videos,
            total_videos=total_videos,
        )

    def _update_playlist_item(
        self,
        task_id: str,
        *,
        status: str,
        progress: float,
        operation: str,
        error_message: str | None = None,
    ) -> None:
        with session_scope() as session:
            item = session.scalar(select(PlaylistItem).where(PlaylistItem.task_id == task_id))
            if not item:
                return
            item.status = status
            item.progress_percentage = int(progress)
            item.current_operation = operation
            if error_message:
                item.error_message = error_message
            if status in FINAL_STATUSES:
                item.completed_at = utcnow()

    def _mark_playlist_item_success(self, task_id: str, result: DownloadResult) -> None:
        with session_scope() as session:
            item = session.scalar(select(PlaylistItem).where(PlaylistItem.task_id == task_id))
            task = session.scalar(select(TaskRegistry).where(TaskRegistry.task_id == task_id))
            if not item or not task:
                return
            item.status = "SUCCESS"
            item.progress_percentage = 100
            item.current_operation = "Completed"
            item.download_url = task.download_url
            item.output_file_name = task.output_file_name
            item.completed_at = utcnow()

    def _recalculate_playlist(self, batch_id: str) -> None:
        with session_scope() as session:
            playlist = session.scalar(select(PlaylistTask).where(PlaylistTask.batch_id == batch_id))
            if not playlist:
                return
            items = session.scalars(select(PlaylistItem).where(PlaylistItem.batch_id == batch_id)).all()
            playlist.completed_videos = sum(1 for item in items if item.status == "SUCCESS")
            playlist.failed_videos = sum(1 for item in items if item.status == "FAILED")
            done = sum(1 for item in items if item.status in {"SUCCESS", "FAILED", "CANCELLED"})
            playlist.progress_percentage = int((done / max(len(items), 1)) * 100)
            if done < len(items):
                playlist.status = "PROCESSING"
            completed_videos = playlist.completed_videos
            total_videos = playlist.total_videos
            percentage = playlist.progress_percentage
            current_operation = playlist.current_operation or "Processing playlist"
            status = playlist.status
        self._emit_progress(
            batch_id,
            task_status=status,
            percentage=percentage,
            current_operation=current_operation,
            completed_videos=completed_videos,
            total_videos=total_videos,
        )

    def _handle_playlist_child_progress(
        self,
        *,
        batch_id: str,
        child_task_id: str,
        status: str,
        progress: float,
        operation: str,
        extra: dict[str, Any] | None,
        completed_items: int,
        total_items: int,
    ) -> None:
        self._update_task(
            child_task_id,
            status=status,
            progress=progress,
            operation=operation,
            extra=extra,
        )
        self._update_playlist_item(
            child_task_id,
            status=status,
            progress=progress,
            operation=operation,
        )
        batch_progress = ((completed_items + (max(progress, 0) / 100.0)) / max(total_items, 1)) * 100
        with session_scope() as session:
            playlist = session.scalar(select(PlaylistTask).where(PlaylistTask.batch_id == batch_id))
            if not playlist:
                return
            playlist.status = "PROCESSING"
            playlist.progress_percentage = int(batch_progress)
            playlist.current_operation = operation
            completed_videos = playlist.completed_videos
            total_videos = playlist.total_videos
        self._emit_progress(
            batch_id,
            task_status="PROCESSING",
            percentage=batch_progress,
            current_operation=operation,
            completed_videos=completed_videos,
            total_videos=total_videos,
        )

    def _cancel_remaining_playlist_items(self, batch_id: str, items: list[PlaylistItem]) -> None:
        for item in items:
            self._mark_task_cancelled(item.task_id, emit=False)
            self._update_playlist_item(item.task_id, status="CANCELLED", progress=0, operation="Cancelled")
        self._recalculate_playlist(batch_id)

    def _playlist_percentage(self, batch_id: str) -> int:
        with session_scope() as session:
            playlist = session.scalar(select(PlaylistTask).where(PlaylistTask.batch_id == batch_id))
            return playlist.progress_percentage if playlist else 0

    def _get_playlist_progress(self, batch_id: str) -> dict[str, Any]:
        with self._lock:
            in_memory = self._progress_store.get(batch_id)
        if in_memory:
            return {"status": "success", "progress": in_memory}
        with session_scope() as session:
            playlist = session.scalar(select(PlaylistTask).where(PlaylistTask.batch_id == batch_id))
            if not playlist:
                raise CoordinatorError("Task not found", status_code=404)
            return {
                "status": "success",
                "progress": {
                    "task_id": batch_id,
                    "task_status": playlist.status,
                    "percentage": float(playlist.progress_percentage),
                    "current_operation": playlist.current_operation or playlist.status.title(),
                    "completed_videos": playlist.completed_videos,
                    "total_videos": playlist.total_videos,
                },
            }

    def _finalize_control(self, key: str) -> None:
        with self._lock:
            self._futures.pop(key, None)
            control = self._controls.get(key)
            if control:
                control.current_process = None
            self._controls.pop(key, None)

    def _delete_task_files(self, session: SessionLocal, tasks: list[TaskRegistry]) -> int:
        deleted_files = 0
        for task in tasks:
            if task.task_id in self._controls:
                continue
            if self._delete_task_file(task):
                deleted_files += 1
            self._clear_task_file_metadata(session, task)
            with self._lock:
                self._progress_store.pop(task.task_id, None)
        return deleted_files

    def _clear_task_file_metadata(self, session: SessionLocal, task: TaskRegistry) -> None:
        task.download_path = None
        task.download_url = None
        task.output_file_name = None
        task.file_size_bytes = None

        playlist_item = session.scalar(select(PlaylistItem).where(PlaylistItem.task_id == task.task_id))
        if playlist_item:
            playlist_item.download_url = None
            playlist_item.output_file_name = None

    def _delete_task_file(self, task: TaskRegistry) -> bool:
        if not task.download_path:
            return False

        path = Path(task.download_path).parent
        existed = path.exists()
        if existed:
            shutil.rmtree(path, ignore_errors=True)
        return existed
