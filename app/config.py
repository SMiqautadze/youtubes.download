from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    app_name: str
    host: str
    port: int
    base_dir: Path
    static_dir: Path
    runtime_dir: Path
    data_dir: Path
    downloads_dir: Path
    temp_dir: Path
    database_url: str
    max_workers: int
    cleanup_interval_seconds: int

def get_settings() -> Settings:
    base_dir = Path(__file__).resolve().parent.parent
    runtime_dir = base_dir / "runtime"
    data_dir = runtime_dir / "data"
    downloads_dir = runtime_dir / "downloads"
    temp_dir = runtime_dir / "temp"
    db_path = data_dir / "app.db"

    return Settings(
        app_name="youtubes.download",
        host=os.getenv("APP_HOST", "0.0.0.0"),
        port=int(os.getenv("APP_PORT", "80")),
        base_dir=base_dir,
        static_dir=base_dir / "static",
        runtime_dir=runtime_dir,
        data_dir=data_dir,
        downloads_dir=downloads_dir,
        temp_dir=temp_dir,
        database_url=os.getenv("DATABASE_URL", f"sqlite:///{db_path}"),
        max_workers=int(os.getenv("MAX_WORKERS", "3")),
        cleanup_interval_seconds=int(os.getenv("CLEANUP_INTERVAL_SECONDS", "900")),
    )


settings = get_settings()
