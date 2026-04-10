from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class TaskRegistry(Base):
    __tablename__ = "task_registry"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    task_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    playlist_batch_id: Mapped[str | None] = mapped_column(String(64), index=True, nullable=True)
    video_url: Mapped[str] = mapped_column(Text)
    video_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    video_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    format: Mapped[str] = mapped_column(String(16), default="mp3")
    quality: Mapped[str] = mapped_column(String(16), default="320")
    target_container: Mapped[str] = mapped_column(String(16), default="mp3")
    task_type: Mapped[str] = mapped_column(String(32), default="single_audio")
    status: Mapped[str] = mapped_column(String(32), default="PENDING")
    progress_percentage: Mapped[int] = mapped_column(Integer, default=0)
    current_operation: Mapped[str | None] = mapped_column(Text, nullable=True)
    download_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    download_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    output_file_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    file_size_bytes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    metadata_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    playlist_items: Mapped[list["PlaylistItem"]] = relationship(
        "PlaylistItem",
        back_populates="task",
        primaryjoin="TaskRegistry.task_id==foreign(PlaylistItem.task_id)",
        lazy="selectin",
    )

    @property
    def metadata_payload(self) -> dict[str, Any]:
        if not self.metadata_json:
            return {}
        try:
            return json.loads(self.metadata_json)
        except json.JSONDecodeError:
            return {}


class PlaylistTask(Base):
    __tablename__ = "playlist_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    batch_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    playlist_url: Mapped[str] = mapped_column(Text)
    playlist_title: Mapped[str] = mapped_column(Text)
    total_videos: Mapped[int] = mapped_column(Integer, default=0)
    completed_videos: Mapped[int] = mapped_column(Integer, default=0)
    failed_videos: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(32), default="PENDING")
    progress_percentage: Mapped[int] = mapped_column(Integer, default=0)
    format: Mapped[str] = mapped_column(String(16), default="mp4")
    quality: Mapped[str] = mapped_column(String(16), default="720")
    target_container: Mapped[str] = mapped_column(String(16), default="mp4")
    current_operation: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    metadata_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    items: Mapped[list["PlaylistItem"]] = relationship(
        "PlaylistItem",
        back_populates="playlist_task",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    @property
    def metadata_payload(self) -> dict[str, Any]:
        if not self.metadata_json:
            return {}
        try:
            return json.loads(self.metadata_json)
        except json.JSONDecodeError:
            return {}


class PlaylistItem(Base):
    __tablename__ = "playlist_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    item_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    batch_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("playlist_tasks.batch_id", ondelete="CASCADE"), index=True
    )
    task_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("task_registry.task_id", ondelete="CASCADE"), unique=True
    )
    video_url: Mapped[str] = mapped_column(Text)
    video_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    video_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    position_in_playlist: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(32), default="PENDING")
    progress_percentage: Mapped[int] = mapped_column(Integer, default=0)
    current_operation: Mapped[str | None] = mapped_column(Text, nullable=True)
    download_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    output_file_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    playlist_task: Mapped[PlaylistTask] = relationship("PlaylistTask", back_populates="items")
    task: Mapped[TaskRegistry] = relationship("TaskRegistry", back_populates="playlist_items")


class AppSetting(Base):
    __tablename__ = "app_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    auto_cleanup_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    cleanup_window: Mapped[str] = mapped_column(String(8), default="1w")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )
