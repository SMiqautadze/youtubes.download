from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

from yt_dlp import DownloadError, YoutubeDL

from app.config import settings
from app.services.validators import quality_to_height, sanitize_filename


ProgressCallback = Callable[[str, float, str, dict | None], None]
ProcessCallback = Callable[[subprocess.Popen[str] | None], None]


class DownloadCancelled(Exception):
    """Raised when a user stops a download."""


@dataclass
class DownloadSpec:
    url: str
    format_type: str
    quality: str
    target_container: str
    task_dir: Path
    is_playlist_item: bool = False


@dataclass
class DownloadResult:
    title: str
    video_id: str | None
    file_path: Path
    file_size_bytes: int | None


class LocalDownloader:
    def __init__(self) -> None:
        self.temp_dir = settings.temp_dir

    def analyze_video(self, url: str) -> dict:
        options = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "noplaylist": True,
        }
        with YoutubeDL(options) as ydl:
            info = ydl.extract_info(url, download=False)
        return {
            "title": info.get("title") or "Untitled",
            "video_id": info.get("id"),
            "webpage_url": info.get("webpage_url") or url,
        }

    def analyze_playlist(self, url: str) -> dict:
        options = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "extract_flat": True,
        }
        with YoutubeDL(options) as ydl:
            info = ydl.extract_info(url, download=False)

        owner = (
            info.get("playlist_uploader")
            or info.get("uploader")
            or info.get("channel")
            or info.get("creator")
            or info.get("channel_handle")
            or info.get("uploader_id")
            or info.get("channel_id")
        )

        entries = []
        for position, entry in enumerate(info.get("entries") or [], start=1):
            if not entry:
                continue
            video_id = entry.get("id")
            webpage_url = entry.get("url") or entry.get("webpage_url") or ""
            if webpage_url and not webpage_url.startswith("http"):
                webpage_url = f"https://www.youtube.com/watch?v={webpage_url}"
            entries.append(
                {
                    "video_id": video_id,
                    "video_title": entry.get("title") or f"Item {position}",
                    "video_url": webpage_url,
                    "position_in_playlist": position,
                }
            )

        return {
            "title": info.get("title") or "Untitled Playlist",
            "playlist_id": info.get("id"),
            "owner": owner or None,
            "entries": entries,
        }

    def run_download(
        self,
        spec: DownloadSpec,
        progress_callback: ProgressCallback,
        process_callback: ProcessCallback,
        cancel_requested: Callable[[], bool],
    ) -> DownloadResult:
        spec.task_dir.mkdir(parents=True, exist_ok=True)
        command = self._build_command(spec)

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        process_callback(process)

        title = "Resolving..."
        video_id = None
        final_path: Path | None = None
        recent_lines: list[str] = []

        try:
            assert process.stdout is not None
            for raw_line in process.stdout:
                line = raw_line.strip()
                if not line:
                    continue
                recent_lines.append(line)
                recent_lines = recent_lines[-20:]

                if cancel_requested():
                    self.terminate_process(process)
                    raise DownloadCancelled("Cancelled by user")

                if line.startswith("__TITLE__="):
                    title = line.split("=", 1)[1].strip() or title
                    progress_callback("PROCESSING", 0, "Resolving metadata", {"title": title})
                    continue
                if line.startswith("__VIDEO_ID__="):
                    video_id = line.split("=", 1)[1].strip() or None
                    continue
                if line.startswith("__FINAL_PATH__="):
                    final_path = Path(line.split("=", 1)[1].strip())
                    continue

                percent = self._extract_percent(line)
                if percent is not None:
                    progress_callback("DOWNLOADING", percent, "Downloading media", None)
                    continue

                lowered = line.lower()
                if "[merger]" in lowered or "merging formats" in lowered:
                    progress_callback("PROCESSING", 96, "Merging streams", None)
                elif "[extractaudio]" in lowered:
                    progress_callback("PROCESSING", 96, "Extracting audio", None)
                elif "[embedthumbnail]" in lowered or "thumbnail" in lowered:
                    progress_callback("PROCESSING", 97, "Embedding cover art", None)
                elif "[metadata]" in lowered:
                    progress_callback("PROCESSING", 98, "Writing metadata", None)

            return_code = process.wait()
        finally:
            process_callback(None)

        if cancel_requested():
            raise DownloadCancelled("Cancelled by user")

        if return_code != 0:
            message = "\n".join(recent_lines) or "yt-dlp exited with a non-zero status"
            raise RuntimeError(message)

        if final_path is None or not final_path.exists():
            discovered = self._discover_output_file(spec.task_dir)
            if discovered is None:
                raise RuntimeError("Downloaded file could not be located")
            final_path = discovered

        if spec.format_type != "mp3":
            info = self._load_info_json(spec.task_dir)
            if info:
                progress_callback("PROCESSING", 99, "Embedding video metadata", None)
                final_path = self._embed_video_metadata(final_path, info)

        progress_callback("SUCCESS", 100, "Completed", None)
        return DownloadResult(
            title=title,
            video_id=video_id,
            file_path=final_path,
            file_size_bytes=final_path.stat().st_size if final_path.exists() else None,
        )

    def terminate_process(self, process: subprocess.Popen[str] | None) -> None:
        if process is None or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)

    def _build_command(self, spec: DownloadSpec) -> list[str]:
        template = str(spec.task_dir / "%(title).180B [%(id)s].%(ext)s")
        command = [
            "yt-dlp",
            "--newline",
            "--no-warnings",
            "--progress",
            "--restrict-filenames",
            "--windows-filenames",
            "--output",
            template,
            "--write-info-json",
            "--print",
            "before_dl:__TITLE__=%(title)s",
            "--print",
            "before_dl:__VIDEO_ID__=%(id)s",
            "--print",
            "after_move:__FINAL_PATH__=%(filepath)s",
            "--no-playlist",
        ]

        if spec.format_type == "mp3":
            command.extend(
                [
                    "-f",
                    "bestaudio/best",
                    "--extract-audio",
                    "--audio-format",
                    "mp3",
                    "--audio-quality",
                    str(spec.quality),
                    "--embed-thumbnail",
                    "--convert-thumbnails",
                    "jpg",
                    "--add-metadata",
                ]
            )
        else:
            height = quality_to_height(spec.quality)
            selector = f"bestvideo[height<={height}]+bestaudio/best[height<={height}]"
            command.extend(
                [
                    "-f",
                    selector,
                    "--merge-output-format",
                    spec.target_container,
                    "--add-metadata",
                ]
            )
            if spec.target_container == "mkv":
                # yt-dlp passes bare ffmpeg postprocessor args near the output path.
                # For merger jobs, input-only ffmpeg options must target an input slot.
                command.extend(["--postprocessor-args", "Merger+ffmpeg_i1:-hwaccel auto"])

        command.append(spec.url)
        return command

    def _discover_output_file(self, task_dir: Path) -> Path | None:
        candidates = [
            path
            for path in task_dir.iterdir()
            if path.is_file() and path.suffix.lower() not in {".json", ".part", ".ytdl"}
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
        return candidates[0]

    def _load_info_json(self, task_dir: Path) -> dict:
        candidates = sorted(task_dir.glob("*.info.json"), key=lambda path: path.stat().st_mtime, reverse=True)
        if not candidates:
            return {}
        try:
            return json.loads(candidates[0].read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

    def _embed_video_metadata(self, file_path: Path, info: dict) -> Path:
        metadata = self._build_video_metadata(info)
        if not metadata:
            return file_path

        temp_path = file_path.with_name(f"{file_path.stem}.metadata{file_path.suffix}")
        command = [
            "ffmpeg",
            "-y",
            "-i",
            str(file_path),
            "-map",
            "0",
            "-c",
            "copy",
        ]
        if file_path.suffix.lower() == ".mp4":
            command.extend(["-movflags", "+use_metadata_tags"])
        for key, value in metadata.items():
            command.extend(["-metadata", f"{key}={value}"])
        command.append(str(temp_path))

        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        if result.returncode != 0 or not temp_path.exists():
            temp_path.unlink(missing_ok=True)
            return file_path

        temp_path.replace(file_path)
        return file_path

    def _build_video_metadata(self, info: dict) -> dict[str, str]:
        owner = self._clean_metadata_value(
            info.get("uploader")
            or info.get("channel")
            or info.get("creator")
            or info.get("channel_handle")
            or info.get("uploader_id")
            or info.get("channel_id"),
            255,
        )
        webpage_url = self._clean_metadata_value(info.get("webpage_url") or info.get("original_url"), 1024)
        description = self._clean_metadata_value(info.get("description"), 4000)
        category = self._clean_metadata_value((info.get("categories") or [None])[0] or info.get("genre"), 255)
        date_value = self._format_upload_date(info.get("upload_date"))

        metadata: dict[str, str] = {}
        if title := self._clean_metadata_value(info.get("title"), 255):
            metadata["title"] = title
        if owner:
            metadata["artist"] = owner
            metadata["album_artist"] = owner
            metadata["publisher"] = owner
        if album := self._clean_metadata_value(info.get("playlist_title"), 255):
            metadata["album"] = album
        if date_value:
            metadata["date"] = date_value
            metadata["creation_time"] = f"{date_value}T00:00:00Z"
        if category:
            metadata["genre"] = category
        if description:
            metadata["description"] = description
            metadata["synopsis"] = description
        if webpage_url:
            metadata["comment"] = webpage_url
        return metadata

    def _clean_metadata_value(self, value: object, limit: int = 255) -> str | None:
        if value is None:
            return None
        text = re.sub(r"\s+", " ", str(value)).replace("\x00", "").strip()
        if not text:
            return None
        return text[:limit]

    def _format_upload_date(self, upload_date: object) -> str | None:
        if not upload_date:
            return None
        raw = str(upload_date).strip()
        if not re.fullmatch(r"\d{8}", raw):
            return None
        try:
            return datetime.strptime(raw, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            return None

    def _extract_percent(self, line: str) -> float | None:
        match = re.search(r"(\d+(?:\.\d+)?)%", line)
        if match:
            try:
                return max(0.0, min(100.0, float(match.group(1))))
            except ValueError:
                return None
        return None
