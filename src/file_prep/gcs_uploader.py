"""Uploads prepared CSVs to the AMP'd GCS bucket.

Dry run is the default. Existing objects are protected by ``gcs.overwrite_policy``
(``fail`` | ``skip`` | ``overwrite``) and every action is recorded for the audit
report.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..models.pipeline_spec import PipelineSpec, TableSpec
from ..utils.config import AppConfig
from ..utils.logging_setup import get_logger

logger = get_logger(__name__)


class GcsOverwriteError(RuntimeError):
    """Raised when an object exists and the overwrite policy is ``fail``."""


@dataclass
class UploadResult:
    local_path: Path
    gcs_uri: str
    uploaded: bool
    size_bytes: int = 0
    skipped_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "local_path": str(self.local_path),
            "gcs_uri": self.gcs_uri,
            "uploaded": self.uploaded,
            "size_bytes": self.size_bytes,
            "skipped_reason": self.skipped_reason,
        }


@dataclass
class GcsUploader:
    """Thin, dry-run-aware wrapper over ``google-cloud-storage``."""

    config: AppConfig
    dry_run: bool = True
    audit: list[dict[str, Any]] = field(default_factory=list)
    _client: Any = None
    _bucket: Any = None

    # -- client -------------------------------------------------------------
    def client(self):
        if self._client is None:
            from google.cloud import storage  # imported lazily so dry runs need no creds

            credentials = _impersonated_credentials(self.config)
            self._client = storage.Client(
                project=self.config.gcp.project_id, credentials=credentials
            )
            logger.info("Connected to GCS as project %s", self.config.gcp.project_id)
        return self._client

    def bucket(self, name: str):
        if self._bucket is None or self._bucket.name != name:
            self._bucket = self.client().bucket(name)
        return self._bucket

    # -- upload -------------------------------------------------------------
    def upload_file(self, local_path: str | Path, bucket_name: str, object_name: str) -> UploadResult:
        path = Path(local_path)
        uri = f"gs://{bucket_name}/{object_name}"

        if self.dry_run:
            size = path.stat().st_size if path.exists() else 0
            logger.info("[DRY RUN] would upload %s -> %s (%s bytes)", path.name, uri, size)
            return self._record(UploadResult(path, uri, uploaded=False, size_bytes=size,
                                             skipped_reason="dry run"))

        if not path.exists():
            raise FileNotFoundError(f"Local file to upload not found: {path}")

        blob = self.bucket(bucket_name).blob(object_name)
        if blob.exists():
            policy = self.config.gcs.overwrite_policy
            if policy == "fail":
                raise GcsOverwriteError(
                    f"{uri} already exists and gcs.overwrite_policy='fail'. "
                    "Set it to 'overwrite' (rerun replacing the run) or 'skip'."
                )
            if policy == "skip":
                logger.warning("Skipping existing object %s (overwrite_policy='skip')", uri)
                return self._record(UploadResult(path, uri, uploaded=False,
                                                 size_bytes=path.stat().st_size,
                                                 skipped_reason="object exists, policy=skip"))
            logger.warning("Overwriting existing object %s", uri)

        blob.upload_from_filename(str(path), content_type="text/csv")
        logger.info("Uploaded %s -> %s", path.name, uri)
        return self._record(UploadResult(path, uri, uploaded=True, size_bytes=path.stat().st_size))

    def upload_table_files(
        self, spec: PipelineSpec, table: TableSpec, local_files: list[Path]
    ) -> list[UploadResult]:
        """Upload one table's CSVs under ``gs://<bucket>/<gcs_folder>/``."""
        results: list[UploadResult] = []
        for local_file in local_files:
            object_name = f"{table.gcs_folder}/{Path(local_file).name}"
            results.append(self.upload_file(local_file, spec.gcs_bucket, object_name))
        return results

    # -- inspection ---------------------------------------------------------
    def list_objects(self, bucket_name: str, prefix: str) -> list[str]:
        if self.dry_run and self._client is None:
            logger.info("[DRY RUN] would list gs://%s/%s", bucket_name, prefix)
            return []
        return [blob.name for blob in self.client().list_blobs(bucket_name, prefix=prefix)]

    def _record(self, result: UploadResult) -> UploadResult:
        self.audit.append(result.to_dict())
        return result


def _impersonated_credentials(config: AppConfig):
    """Service-account impersonation, when configured. No keys are ever read from code."""
    target = config.gcp.impersonate_service_account
    if not target:
        return None
    import google.auth
    from google.auth import impersonated_credentials

    source, _ = google.auth.default()
    logger.info("Impersonating service account %s", target)
    return impersonated_credentials.Credentials(
        source_credentials=source,
        target_principal=target,
        target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )


__all__ = ["GcsUploader", "UploadResult", "GcsOverwriteError"]
