"""Robust file reading for OneDrive-backed and locked Windows files.

Two things break plain ``open()`` in this workspace:

* OneDrive "files on demand" placeholders need hydrating.
* Excel keeps workbooks open with a restrictive share mode, so ``open()`` fails
  with ``PermissionError`` even for read-only access.

:func:`local_readable_copy` works around both by falling back to a Win32
``CreateFileW`` handle opened with full sharing and copying the bytes to a temp
file.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path

from .logging_setup import get_logger

logger = get_logger(__name__)

_GENERIC_READ = 0x80000000
_FILE_SHARE_ALL = 0x00000007  # READ | WRITE | DELETE
_OPEN_EXISTING = 3
_FILE_ATTRIBUTE_NORMAL = 0x80
_INVALID_HANDLE = -1


def _open_shared(path: Path):
    """Open a file for reading even when another process holds it open."""
    import ctypes
    import msvcrt
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateFileW.restype = wintypes.HANDLE
    kernel32.CreateFileW.argtypes = [
        wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
        ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE,
    ]
    handle = kernel32.CreateFileW(
        str(path), _GENERIC_READ, _FILE_SHARE_ALL, None,
        _OPEN_EXISTING, _FILE_ATTRIBUTE_NORMAL, None,
    )
    if handle in (_INVALID_HANDLE, ctypes.c_void_p(-1).value, None):
        raise ctypes.WinError(ctypes.get_last_error())
    fd = msvcrt.open_osfhandle(handle, os.O_RDONLY | os.O_BINARY)
    return os.fdopen(fd, "rb")


def local_readable_copy(path: Path) -> Path:
    """Return a path that ``open()`` can definitely read.

    The original path is returned when it is already readable; otherwise a temp
    copy is made. Callers should pass the result to :func:`cleanup_copy`.
    """
    try:
        with open(path, "rb") as handle:
            handle.read(1)
        return path
    except PermissionError:
        pass

    temp_dir = Path(tempfile.mkdtemp(prefix="brd_src_"))
    target = temp_dir / path.name
    try:
        shutil.copyfile(path, target)
    except PermissionError:
        if os.name != "nt":
            raise
        logger.info("%s is locked or not hydrated; copying via a shared Win32 handle.", path.name)
        with _open_shared(path) as source, open(target, "wb") as destination:
            shutil.copyfileobj(source, destination)
    return target


def cleanup_copy(original: Path, copy: Path) -> None:
    """Remove the temp copy created by :func:`local_readable_copy`, if any."""
    if copy == original:
        return
    shutil.rmtree(copy.parent, ignore_errors=True)


@contextmanager
def readable(path: str | Path):
    """Context manager yielding a readable path and cleaning up afterwards."""
    original = Path(path)
    copy = local_readable_copy(original)
    try:
        yield copy
    finally:
        cleanup_copy(original, copy)


__all__ = ["local_readable_copy", "cleanup_copy", "readable"]
