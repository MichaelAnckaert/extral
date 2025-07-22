"""Utility functions for file connectors."""

import logging
import tempfile
import urllib.request
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional, Union
from urllib.error import HTTPError, URLError

logger = logging.getLogger(__name__)


@contextmanager
def get_file_handle(
    path: str, mode: str = "r"
) -> Generator[Union[Path, tempfile._TemporaryFileWrapper], None, None]:
    """
    Get a file handle for either local file or HTTP/HTTPS URL.

    For HTTP/HTTPS URLs, downloads the file to a temporary location.
    For local files, returns the path directly.

    Args:
        path: Local file path or HTTP/HTTPS URL
        mode: File open mode (only used for local files)

    Yields:
        Path object for local files or temporary file for URLs

    Raises:
        HTTPError: If HTTP download fails
        URLError: If URL is invalid or network error
        FileNotFoundError: If local file doesn't exist
    """
    if path.startswith("http://") or path.startswith("https://"):
        logger.info(f"Downloading file from {path}")
        try:
            with tempfile.NamedTemporaryFile(mode="wb", delete=False) as tmp_file:
                with urllib.request.urlopen(path) as response:
                    # Stream the download to handle large files
                    chunk_size = 8192
                    total_size = 0
                    while True:
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break
                        tmp_file.write(chunk)
                        total_size += len(chunk)
                        if total_size % (chunk_size * 1000) == 0:
                            logger.debug(
                                f"Downloaded {total_size / 1024 / 1024:.1f} MB"
                            )

                tmp_file.flush()
                tmp_path = Path(tmp_file.name)
                logger.info(
                    f"Downloaded {total_size / 1024 / 1024:.1f} MB to {tmp_path}"
                )

            try:
                yield tmp_path
            finally:
                # Clean up temporary file
                tmp_path.unlink()
                logger.debug(f"Cleaned up temporary file {tmp_path}")

        except HTTPError as e:
            logger.error(f"HTTP error downloading {path}: {e}")
            raise
        except URLError as e:
            logger.error(f"URL error downloading {path}: {e}")
            raise
    else:
        # Local file path
        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        yield file_path


def estimate_file_size(path: str) -> Optional[int]:
    """
    Estimate the size of a file from local path or HTTP headers.

    Args:
        path: Local file path or HTTP/HTTPS URL

    Returns:
        File size in bytes, or None if cannot be determined
    """
    if path.startswith("http://") or path.startswith("https://"):
        try:
            with urllib.request.urlopen(path) as response:
                content_length = response.headers.get("Content-Length")
                if content_length:
                    return int(content_length)
        except (HTTPError, URLError, ValueError):
            logger.warning(f"Could not determine size for {path}")
        return None
    else:
        try:
            return Path(path).stat().st_size
        except OSError:
            logger.warning(f"Could not determine size for {path}")
            return None
