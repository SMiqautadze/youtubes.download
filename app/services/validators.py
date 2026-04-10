from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urlparse


ALLOWED_YOUTUBE_HOSTS = {
    "youtube.com",
    "www.youtube.com",
    "m.youtube.com",
    "music.youtube.com",
    "youtu.be",
}

QUALITY_TO_HEIGHT = {
    "720": 720,
    "1080": 1080,
    "2k": 1440,
    "4k": 2160,
    "8k": 4320,
}


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    message: str
    normalized_url: str | None = None


def normalize_url(url: str) -> str:
    return (url or "").strip()


def validate_youtube_url(url: str) -> ValidationResult:
    candidate = normalize_url(url)
    if not candidate:
        return ValidationResult(False, "URL is required")

    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"}:
        return ValidationResult(False, "Invalid Link")

    hostname = (parsed.hostname or "").lower()
    if hostname not in ALLOWED_YOUTUBE_HOSTS:
        return ValidationResult(False, "Invalid Link")

    return ValidationResult(True, "OK", candidate)


def resolve_target_container(format_type: str, quality: str) -> str:
    quality_value = (quality or "").lower()
    if format_type == "mp3":
        return "mp3"
    if quality_value in {"1080", "2k", "4k", "8k"}:
        return "mkv"
    return "mp4"


def quality_to_height(quality: str) -> int:
    quality_value = (quality or "").lower().replace("p", "")
    return QUALITY_TO_HEIGHT.get(quality_value, 720)


def cleanup_window_to_seconds(window: str) -> int:
    mapping = {
        "1h": 60 * 60,
        "1d": 60 * 60 * 24,
        "1w": 60 * 60 * 24 * 7,
        "1m": 60 * 60 * 24 * 30,
    }
    if window not in mapping:
        raise ValueError("cleanup_window must be one of 1h, 1d, 1w, 1m")
    return mapping[window]


def sanitize_filename(filename: str) -> str:
    return "".join(c for c in (filename or "") if c.isalnum() or c in (" ", ".", "-", "_")).rstrip()


def classify_error_message(message: str) -> tuple[str, str]:
    normalized = (message or "").lower()

    checks: Iterable[tuple[tuple[str, ...], tuple[str, str]]] = (
        (("invalid url", "unsupported url", "invalid link"), ("INVALID_LINK", "Invalid Link")),
        (
            ("private video", "video unavailable", "deleted video", "not available"),
            ("PRIVATE_OR_DELETED", "Video Deleted/Private"),
        ),
        (
            ("unavailable in your country", "geoblocked", "geo-restricted", "region"),
            ("REGION_BLOCKED", "Region Blocked"),
        ),
        (("sign in to confirm your age", "age-restricted", "age restricted"), ("AGE_RESTRICTED", "Age Restricted")),
        (("timed out", "network", "connection reset", "temporary failure"), ("NETWORK_TIMEOUT", "Network Timeout")),
        (("confirm you're not a bot", "confirm you are not a bot", "bot"), ("YOUTUBE_BOT_CHECK", "YouTube requested additional verification")),
        (("ffmpeg", "postprocess", "post-processing", "merg"), ("TRANSCODE_FAILED", "Post-processing failed")),
        (("encoder", "nvenc", "vaapi", "qsv", "libcuda"), ("ENCODER_UNAVAILABLE", "Required FFmpeg encoder is unavailable")),
        (("killed", "oom", "cannot allocate memory", "no space left"), ("RESOURCE_EXHAUSTED", "Worker resources exhausted")),
    )

    for patterns, response in checks:
        if any(pattern in normalized for pattern in patterns):
            return response

    return ("UNKNOWN_ERROR", "An unexpected error occurred")

