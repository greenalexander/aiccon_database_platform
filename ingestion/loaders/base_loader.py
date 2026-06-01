"""
ingestion/loaders/base_loader.py

Shared base class and utilities for all data ingestion scripts.
Every API source (eurostat.py, istat.py, etc.) inherits from BaseLoader.

Responsibilities:
- Load config (settings.yaml + .env)
- Set up logging
- Provide retry-wrapped HTTP requests
- Save raw data as parquet with a standard schema (written locally then uploaded to GCS)
- Track what was fetched in a simple manifest (written to GCS)
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import yaml
from dotenv import load_dotenv
from google.cloud import storage
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

    # Load .env (silently skip if not found — CI/CD injects env vars directly)
    env_path = env_path or Path(settings_path).parent.parent / "config" / ".env"
    load_dotenv(env_path, override=False)

    # Resolve GCP config from environment and merge into config dict
    gcs_bucket = os.getenv("GCS_BUCKET", "")
    bq_project = os.getenv("BQ_PROJECT", "")
    bq_dataset = os.getenv("BQ_DATASET", "")

    missing = [k for k, v in {
        "GCS_BUCKET": gcs_bucket,
        "BQ_PROJECT": bq_project,
        "BQ_DATASET": bq_dataset,
    }.items() if not v]

    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}. "
            "Copy config/.env.example to config/.env and fill in the values."
        )

    config["gcs"]["bucket"] = gcs_bucket
    config["bigquery"]["project"] = bq_project
    config["bigquery"]["dataset"] = bq_dataset

    return config


# ── GCS URI helpers ───────────────────────────────────────────────────────────

def gcs_uri(config: dict, *parts: str) -> str:
    """
    Build a GCS URI from the configured bucket and prefix plus any path parts.

    Example:
        gcs_uri(config, "raw", "social_economy", "file.parquet")
        → "gs://my-bucket/aiccon-data/raw/social_economy/file.parquet"
    """
    bucket = config["gcs"]["bucket"]
    prefix = config["gcs"]["prefix"].rstrip("/")
    path = "/".join([prefix, *parts])
    return f"gs://{bucket}/{path}"


def raw_gcs_prefix(config: dict, domain: str) -> str:
    """GCS URI prefix for raw files of a given domain."""
    return gcs_uri(config, config["gcs"]["raw_dir"], domain)


def processed_gcs_prefix(config: dict, domain: str) -> str:
    """GCS URI prefix for processed files of a given domain."""
    return gcs_uri(config, config["gcs"]["processed_dir"], domain)


# ── GCS client ────────────────────────────────────────────────────────────────

def get_gcs_client() -> storage.Client:
    """Return a GCS client. Credentials resolved from GOOGLE_APPLICATION_CREDENTIALS."""
    return storage.Client()


def _upload_to_gcs(
    local_path: Path,
    uri: str,
    logger: logging.Logger | None = None,
) -> None:
    """Upload a local file to a GCS URI."""
    lg = logger or get_logger("base_loader")
    # Parse bucket and blob name from gs://bucket/blob
    without_scheme = uri[len("gs://"):]
    bucket_name, _, blob_name = without_scheme.partition("/")
    client = get_gcs_client()
    blob = client.bucket(bucket_name).blob(blob_name)
    blob.upload_from_filename(str(local_path))
    lg.info(f"Uploaded → {uri}")


def _upload_string_to_gcs(
    content: str,
    uri: str,
    content_type: str = "application/octet-stream",
    logger: logging.Logger | None = None,
) -> None:
    """Upload a string directly to a GCS URI without a local temp file."""
    lg = logger or get_logger("base_loader")
    without_scheme = uri[len("gs://"):]
    bucket_name, _, blob_name = without_scheme.partition("/")
    client = get_gcs_client()
    blob = client.bucket(bucket_name).blob(blob_name)
    blob.upload_from_string(content, content_type=content_type)
    lg.info(f"Uploaded → {uri}")


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
        response = session.get_with_retry(url, params=params)
    """
    lg = logger or _logger
    session = requests.Session()
    session.headers.update({"User-Agent": "aiccon-data/1.0 (research; contact: aiccon)"})

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

    session.get_with_retry = _get
    return session


# ── Parquet I/O ───────────────────────────────────────────────────────────────

# Columns that every raw parquet file must have regardless of source
REQUIRED_RAW_COLUMNS = {
    "source_id",       # matches domain_sources.csv source_id
    "dataset_code",    # API dataset identifier (e.g. "DCCV_INSTNONPROFIT")
    "extracted_at",    # UTC timestamp of this fetch
}


def add_metadata_columns(
    df: pd.DataFrame,
    source_id: str,
    dataset_code: str,
) -> pd.DataFrame:
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
    gcs_prefix: str,
    filename: str,
    config: dict,
    logger: logging.Logger | None = None,
) -> str:
    """
    Save a dataframe as parquet in the GCS raw layer.

    Writes to a local temp file first, then uploads to GCS. Files are named
    with a date suffix so monthly runs accumulate rather than overwrite:
        istat_DCCV_INSTNONPROFIT_2024-11.parquet

    Returns the GCS URI of the written file.
    """
    lg = logger or _logger

    missing = REQUIRED_RAW_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            f"DataFrame is missing required metadata columns: {missing}. "
            "Call add_metadata_columns() before saving."
        )

    date_suffix = datetime.now(timezone.utc).strftime("%Y-%m")
    stem = Path(filename).stem
    parquet_filename = f"{stem}_{date_suffix}.parquet"
    uri = f"{gcs_prefix.rstrip('/')}/{parquet_filename}"

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        df.to_parquet(tmp_path, index=False, engine="pyarrow")
        _upload_to_gcs(tmp_path, uri, logger=lg)
    finally:
        tmp_path.unlink(missing_ok=True)

    lg.info(f"Saved {len(df):,} rows → {uri}")
    return uri


def load_parquet(uri: str, logger: logging.Logger | None = None) -> pd.DataFrame:
    """
    Load a parquet file from a GCS URI.

    Requires gcsfs to be installed (pip install gcsfs).
    """
    lg = logger or _logger
    df = pd.read_parquet(uri, engine="pyarrow")
    lg.info(f"Loaded {len(df):,} rows ← {uri}")
    return df


# ── Manifest ──────────────────────────────────────────────────────────────────

def write_manifest(
    gcs_prefix: str,
    dataset_code: str,
    row_count: int,
    parquet_uri: str,
    extra: dict[str, Any] | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """
    Write a small JSON manifest to GCS alongside each parquet file.

    Useful for debugging and for the pipeline log. Contains the row count,
    extraction timestamp, and any source-specific metadata.
    """
    parquet_filename = parquet_uri.rsplit("/", 1)[-1]
    manifest = {
        "dataset_code": dataset_code,
        "parquet_file": parquet_filename,
        "parquet_uri": parquet_uri,
        "row_count": row_count,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        **(extra or {}),
    }
    manifest_filename = parquet_filename.replace(".parquet", "_manifest.json")
    manifest_uri = f"{gcs_prefix.rstrip('/')}/{manifest_filename}"

    _upload_string_to_gcs(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        manifest_uri,
        content_type="application/json",
        logger=logger,
    )


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

        # GCS prefix for raw output — replaces the local output_dir Path
        self.gcs_prefix = raw_gcs_prefix(self.config, self.DOMAIN)

        self.logger.info(
            f"Initialised {self.__class__.__name__} "
            f"[domain={self.DOMAIN}, gcs_prefix={self.gcs_prefix}]"
        )

    @abstractmethod
    def fetch(self) -> list[pd.DataFrame]:
        """
        Fetch data from the source and return a list of raw DataFrames.

        Each DataFrame should correspond to one dataset/endpoint.
        Do not add metadata columns here — run() does that automatically.
        """
        ...

    def run(self) -> list[str]:
        """
        Execute the full fetch → enrich → save cycle.

        Returns a list of GCS URIs of the written parquet files.
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
            uri = save_raw_parquet(
                df,
                gcs_prefix=self.gcs_prefix,
                filename=dataset_code,
                config=self.config,
                logger=self.logger,
            )
            write_manifest(
                gcs_prefix=self.gcs_prefix,
                dataset_code=dataset_code,
                row_count=len(df),
                parquet_uri=uri,
                extra=df.attrs,
                logger=self.logger,
            )
            written.append(uri)

        self.logger.info(
            f"Finished {self.SOURCE_ID}: {len(written)} file(s) written."
        )
        return written