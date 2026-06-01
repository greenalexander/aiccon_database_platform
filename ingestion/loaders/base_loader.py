"""
ingestion/loaders/base_loader.py

Shared base class and utilities for all data ingestion scripts.
Every API source (eurostat.py, istat.py, etc.) inherits from BaseLoader.

Responsibilities:
- Load config (settings.yaml + .env)
- Set up logging
- Provide retry-wrapped HTTP requests
- Save raw data as parquet with a standard schema
- Track what was fetched in a simple manifest
"""

from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import yaml
from dotenv import load_dotenv
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)


# ── Logging setup ─────────────────────────────────────────────────────────────

def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Return a logger that writes to stdout with a consistent format."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    return logger


# ── Config loader ─────────────────────────────────────────────────────────────

def load_config(
    settings_path: str | Path | None = None,
    env_path: str | Path | None = None,
) -> dict:
    """
    Load settings.yaml and .env, returning a merged config dict.

    The function searches for settings.yaml by walking up from the current
    file's location, so it works regardless of which directory the script
    is run from.
    """
    # Locate project root (directory containing settings.yaml)
    if settings_path is None:
        here = Path(__file__).resolve()
        for parent in [here, *here.parents]:
            candidate = parent / "config" / "settings.yaml"
            if candidate.exists():
                settings_path = candidate
                break
        else:
            raise FileNotFoundError(
                "Could not find config/settings.yaml. "
                "Run the pipeline from the project root."
            )

    with open(settings_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Load .env (silently skip if not found — CI/CD may inject env vars directly)
    env_path = env_path or Path(settings_path).parent.parent / "config" / ".env"
    load_dotenv(env_path, override=False)

    # Resolve SharePoint root from environment
    sharepoint_root = os.getenv("SHAREPOINT_ROOT", "")
    if not sharepoint_root:
        raise EnvironmentError(
            "SHAREPOINT_ROOT is not set. "
            "Copy config/.env.example to config/.env and fill in the path."
        )
    config["sharepoint_root"] = Path(sharepoint_root).expanduser()

    return config


# ── Path helpers ──────────────────────────────────────────────────────────────

def resolve_sharepoint_path(config: dict, subfolder: str) -> Path:
    """
    Build an absolute path inside the SharePoint synced folder.

    Example:
        resolve_sharepoint_path(config, "raw/social_economy")
        → /Users/name/OneDrive - AICCON/aiccon-data/raw/social_economy
    """
    root = config["sharepoint_root"]
    base = config["sharepoint"]["raw_dir"]  # e.g. "aiccon-data/raw"
    path = root / base / subfolder
    path.mkdir(parents=True, exist_ok=True)
    return path


def raw_path(config: dict, domain: str) -> Path:
    return resolve_sharepoint_path(config, domain)


def processed_path(config: dict, domain: str) -> Path:
    root = config["sharepoint_root"]
    base = config["sharepoint"]["processed_dir"]
    path = root / base / domain
    path.mkdir(parents=True, exist_ok=True)
    return path


def database_path(config: dict) -> Path:
    root = config["sharepoint_root"]
    base = config["sharepoint"]["database_dir"]
    path = root / base
    path.mkdir(parents=True, exist_ok=True)
    return path


# ── HTTP with retry ───────────────────────────────────────────────────────────

_logger = get_logger("base_loader")


def make_retry_session(
    total_attempts: int = 5,
    backoff_factor: float = 2.0,
    logger: logging.Logger | None = None,
) -> requests.Session:
    """
    Return a requests.Session with exponential backoff retry logic via tenacity.

    Usage:
        session = make_retry_session()
        response = session.get(url, params=params)
    """
    lg = logger or _logger
    session = requests.Session()
    session.headers.update({"User-Agent": "aiccon-data/1.0 (research; contact: aiccon)"})

    # Wrap session.get with retry logic
    @retry(
        retry=retry_if_exception_type(
            (requests.ConnectionError, requests.Timeout, requests.HTTPError)
        ),
        stop=stop_after_attempt(total_attempts),
        wait=wait_exponential(multiplier=backoff_factor, min=2, max=60),
        before_sleep=before_sleep_log(lg, logging.WARNING),
        reraise=True,
    )
    def _get(url: str, **kwargs) -> requests.Response:
        response = session.get(url, timeout=60, **kwargs)
        response.raise_for_status()
        return response

    # Attach the retry-wrapped method directly to the session instance
    session.get_with_retry = _get
    return session


# ── Parquet I/O ───────────────────────────────────────────────────────────────

# Columns that every raw parquet file must have regardless of source
REQUIRED_RAW_COLUMNS = {
    "source_id",       # matches domain_sources.csv source_id
    "dataset_code",    # API dataset identifier (e.g. "DCCV_INSTNONPROFIT")
    "extracted_at",    # UTC timestamp of this fetch
}


def add_metadata_columns(df: pd.DataFrame, source_id: str, dataset_code: str) -> pd.DataFrame:
    """
    Add standard metadata columns to a raw dataframe before saving.
    Called by every loader before writing parquet.
    """
    df = df.copy()
    df["source_id"] = source_id
    df["dataset_code"] = dataset_code
    df["extracted_at"] = datetime.now(timezone.utc).isoformat()
    return df


def save_raw_parquet(
    df: pd.DataFrame,
    output_dir: Path,
    filename: str,
    logger: logging.Logger | None = None,
) -> Path:
    """
    Save a dataframe as parquet in the raw layer.

    Files are named with a date suffix so monthly runs don't overwrite each other:
        istat_DCCV_INSTNONPROFIT_2024-11.parquet

    Returns the path of the written file.
    """
    lg = logger or _logger

    missing = REQUIRED_RAW_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            f"DataFrame is missing required metadata columns: {missing}. "
            "Call add_metadata_columns() before saving."
        )

    # Date-stamped filename so monthly runs accumulate rather than overwrite
    date_suffix = datetime.now(timezone.utc).strftime("%Y-%m")
    stem = Path(filename).stem
    out_path = output_dir / f"{stem}_{date_suffix}.parquet"

    df.to_parquet(out_path, index=False, engine="pyarrow")
    lg.info(f"Saved {len(df):,} rows → {out_path}")
    return out_path


def load_parquet(path: Path, logger: logging.Logger | None = None) -> pd.DataFrame:
    """Load a parquet file with basic logging."""
    lg = logger or _logger
    df = pd.read_parquet(path, engine="pyarrow")
    lg.info(f"Loaded {len(df):,} rows ← {path}")
    return df


# ── Manifest ──────────────────────────────────────────────────────────────────

def write_manifest(
    output_dir: Path,
    dataset_code: str,
    row_count: int,
    parquet_path: Path,
    extra: dict[str, Any] | None = None,
) -> None:
    """
    Write a small JSON manifest alongside each parquet file.

    Useful for debugging and for the pipeline log. Contains the row count,
    extraction timestamp, and any source-specific metadata.
    """
    manifest = {
        "dataset_code": dataset_code,
        "parquet_file": parquet_path.name,
        "row_count": row_count,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        **(extra or {}),
    }
    manifest_path = output_dir / f"{parquet_path.stem}_manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)


# ── Base loader class ─────────────────────────────────────────────────────────

class BaseLoader(ABC):
    """
    Abstract base class for all domain data loaders.

    Subclass this in each api_sources/{domain}/ script and implement fetch().

    Example subclass skeleton:

        class EurostatSocialEconomyLoader(BaseLoader):
            SOURCE_ID = "eurostat_nama_10r_3empers"
            DOMAIN = "social_economy"

            def fetch(self) -> list[pd.DataFrame]:
                ...
                return [df1, df2]

        if __name__ == "__main__":
            loader = EurostatSocialEconomyLoader()
            loader.run()
    """

    # Subclasses must set these
    SOURCE_ID: str = ""
    DOMAIN: str = ""

    def __init__(self, config: dict | None = None):
        if not self.SOURCE_ID or not self.DOMAIN:
            raise NotImplementedError(
                "Subclasses must define SOURCE_ID and DOMAIN class attributes."
            )

        self.config = config or load_config()
        log_level = self.config.get("pipeline", {}).get("log_level", "INFO")
        self.logger = get_logger(self.__class__.__name__, level=log_level)
        self.session = make_retry_session(logger=self.logger)
        self.output_dir = raw_path(self.config, self.DOMAIN)

        self.logger.info(
            f"Initialised {self.__class__.__name__} "
            f"[domain={self.DOMAIN}, output={self.output_dir}]"
        )

    @abstractmethod
    def fetch(self) -> list[pd.DataFrame]:
        """
        Fetch data from the source and return a list of raw DataFrames.

        Each DataFrame should correspond to one dataset/endpoint.
        Do not add metadata columns here — run() does that automatically.
        """
        ...

    def run(self) -> list[Path]:
        """
        Execute the full fetch → enrich → save cycle.

        Returns a list of paths to the written parquet files.
        """
        self.logger.info(f"Starting fetch for {self.SOURCE_ID}")
        dataframes = self.fetch()

        written = []
        for df in dataframes:
            if df.empty:
                self.logger.warning(
                    f"{self.SOURCE_ID}: fetch returned an empty DataFrame — skipping."
                )
                continue

            dataset_code = df.attrs.get("dataset_code", self.SOURCE_ID)
            df = add_metadata_columns(df, self.SOURCE_ID, dataset_code)
            path = save_raw_parquet(
                df,
                self.output_dir,
                filename=dataset_code,
                logger=self.logger,
            )
            write_manifest(
                self.output_dir,
                dataset_code=dataset_code,
                row_count=len(df),
                parquet_path=path,
                extra=df.attrs,
            )
            written.append(path)

        self.logger.info(
            f"Finished {self.SOURCE_ID}: {len(written)} file(s) written."
        )
        return written