from pathlib import Path
import shutil
import os
import time
from typing import Literal, Iterable
import warnings


def remove_path(
    path: str | Path | list[str | Path] | list[Path] | list[str] | tuple[str | Path] | tuple[Path] | tuple[str] | None,
) -> None:
    """
    Remove a file or directory. If a list or tuple of paths is provided, all will be removed.

    param path: A file or directory path, or a list/tuple of paths to remove.
    """
    if remove_path is None:
        return
    assert isinstance(path, (str, Path, list, tuple)), "path must be a string, Path, list or tuple"
    if isinstance(path, (str, Path)):
        path = Path(path)
        if path.exists():
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path)
    else:
        for p in path:
            remove_path(p)


def clean_folder(
    root: str | Path,
    age: float,
    unit: Literal["seconds", "minutes", "hours", "days", "weeks"] = "hours",
    time_type: Literal["mtime", "atime", "ctime"] = "mtime",
    recursive: bool = True,
    remove_empty_dirs: bool = True,
    n_jobs: int | None = -1,
    batch_size: int = 5000,
) -> int:
    if n_jobs is None or n_jobs in (0, 1):
        return _clean_folder(
            root=root, age=age, unit=unit, time_type=time_type, recursive=recursive, remove_empty_dirs=remove_empty_dirs
        )
    else:
        try:
            import joblib  # pyright: ignore[reportMissingTypeStubs, reportMissingImports, reportUnusedImport]
        except ImportError:
            warnings.warn(
                "joblib is not installed, falling back to single-threaded cleanup. Install joblib for faster performance.",
                UserWarning,
            )
            return _clean_folder(
                root=root,
                age=age,
                unit=unit,
                time_type=time_type,
                recursive=recursive,
                remove_empty_dirs=remove_empty_dirs,
            )
        return _clean_folder_parallel(
            root=root,
            age=age,
            unit=unit,
            time_type=time_type,
            recursive=recursive,
            remove_empty_dirs=remove_empty_dirs,
            n_jobs=n_jobs,
            batch_size=batch_size,
        )


def _clean_folder(
    root: str | Path,
    age: float,
    unit: Literal["seconds", "minutes", "hours", "days", "weeks"] = "hours",
    time_type: Literal["mtime", "atime", "ctime"] = "mtime",
    recursive: bool = True,
    remove_empty_dirs: bool = True,
) -> int:
    """
    Remove files older than a given age and optionally remove empty directories.
    Errors are ignored (locked files, permission errors, etc.).

    Returns number of deleted files.
    """
    root = Path(root)
    if not root.is_dir():
        raise ValueError(f"Path '{root}' is not a directory")
    unit_seconds = {
        "seconds": 1,
        "minutes": 60,
        "hours": 3600,
        "days": 86400,
        "weeks": 604800,
    }

    threshold = time.time() - age * unit_seconds[unit]
    deleted = 0

    def scan(dir_path: str | Path):
        nonlocal deleted

        try:
            with os.scandir(dir_path) as it:
                for entry in it:
                    try:
                        if entry.is_file(follow_symlinks=False):
                            stat = entry.stat()

                            t = {
                                "mtime": stat.st_mtime,
                                "atime": stat.st_atime,
                                "ctime": stat.st_birthtime if hasattr(stat, "st_birthtime") else stat.st_ctime,  # pyright: ignore[reportDeprecated]
                            }[time_type]

                            if t < threshold:
                                try:
                                    os.unlink(entry.path)
                                    deleted += 1
                                except Exception:
                                    pass

                        elif recursive and entry.is_dir(follow_symlinks=False):
                            scan(entry.path)

                    except Exception:
                        pass

        except Exception:
            return

        if remove_empty_dirs:
            try:
                if not os.listdir(dir_path):
                    os.rmdir(dir_path)
            except Exception:
                pass

    scan(root)
    return deleted


def _delete_batch(files: list[str]) -> int:
    deleted = 0
    for f in files:
        try:
            os.unlink(f)
            deleted += 1
        except Exception:
            pass
    return deleted


def _clean_folder_parallel(
    root: str | Path,
    age: float,
    unit: Literal["seconds", "minutes", "hours", "days", "weeks"] = "hours",
    time_type: Literal["mtime", "atime", "ctime"] = "mtime",
    n_jobs: int = -1,
    batch_size: int = 5000,
    recursive: bool = True,
    remove_empty_dirs: bool = True,
) -> int:
    """
    Ultra-fast cleanup of old files using joblib parallelism.
    Supports optional recursive scanning and removal of empty directories.
    """
    try:
        from joblib import Parallel, delayed  # pyright: ignore[reportMissingTypeStubs, reportMissingImports, reportUnknownVariableType]
    except ImportError:
        raise ImportError("joblib is required for parallel cleanup. Install it with 'pip install joblib'.")
    unit_seconds = {
        "seconds": 1,
        "minutes": 60,
        "hours": 3600,
        "days": 86400,
        "weeks": 604800,
    }

    threshold = time.time() - age * unit_seconds[unit]

    files_to_delete: list[str] = []
    directories: list[str] = []

    # -------------------------
    # SCANSIONE ITERATIVA
    # -------------------------
    stack = [root]

    while stack:
        path = stack.pop()

        try:
            with os.scandir(path) as it:
                for entry in it:
                    try:
                        if entry.is_file(follow_symlinks=False):
                            stat = entry.stat()

                            t = {
                                "mtime": stat.st_mtime,
                                "atime": stat.st_atime,
                                "ctime": stat.st_birthtime if hasattr(stat, "st_birthtime") else stat.st_ctime,  # pyright: ignore[reportDeprecated]
                            }[time_type]

                            if t < threshold:
                                files_to_delete.append(entry.path)

                        elif entry.is_dir(follow_symlinks=False):
                            directories.append(entry.path)
                            if recursive:
                                stack.append(entry.path)

                    except Exception:
                        pass

        except Exception:
            pass

    # -------------------------
    # CANCELLAZIONE PARALLELA
    # -------------------------
    deleted = 0

    if files_to_delete:
        batches = [files_to_delete[i : i + batch_size] for i in range(0, len(files_to_delete), batch_size)]

        results: Iterable[int] = Parallel(n_jobs=n_jobs, prefer="threads")(  # pyright: ignore[reportUnknownVariableType, reportAssignmentType]
            delayed(_delete_batch)(b) for b in batches
        )

        if results is not None:  # pyright: ignore[reportUnnecessaryComparison]
            deleted = sum(results)  # pyright: ignore[reportUnknownArgumentType]
        else:
            deleted = 0

    # -------------------------
    # RIMOZIONE DIRECTORY VUOTE
    # -------------------------
    if remove_empty_dirs and directories:
        directories.sort(key=len, reverse=True)

        for d in directories:
            try:
                if not os.listdir(d):
                    os.rmdir(d)
            except Exception:
                pass

    return deleted
