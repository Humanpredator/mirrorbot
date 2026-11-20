from asyncio import CancelledError
from errno import EXDEV
from logging import getLogger
from os import makedirs, path as ospath, remove, rename, walk
from shutil import copystat, rmtree
from time import time

from ...ext_utils.bot_utils import sync_to_async
from ...ext_utils.files_utils import get_mime_type

LOGGER = getLogger(__name__)

_COPY_CHUNK = 4 * 1024 * 1024


class LocalUploader:
    """Moves (or copies, when seeding) the finished task output into a local
    directory on the host instead of uploading it to a remote service."""

    def __init__(self, listener, path):
        self._listener = listener
        self._path = path
        self._processed_bytes = 0
        self._start_time = time()

    @property
    def processed_bytes(self):
        return self._processed_bytes

    @property
    def speed(self):
        try:
            return self._processed_bytes / (time() - self._start_time)
        except Exception:
            return 0

    @staticmethod
    def _unique_target(dest_dir, name):
        target = ospath.join(dest_dir, name)
        if not ospath.exists(target):
            return target
        base, ext = ospath.splitext(name)
        if ospath.isdir(target):
            base, ext = name, ""
        index = 1
        while True:
            candidate = ospath.join(dest_dir, f"{base} ({index}){ext}")
            if not ospath.exists(candidate):
                return candidate
            index += 1

    def _copy_file(self, src, dst):
        with open(src, "rb") as fsrc, open(dst, "wb") as fdst:
            while True:
                if self._listener.is_cancelled:
                    raise CancelledError()
                chunk = fsrc.read(_COPY_CHUNK)
                if not chunk:
                    break
                fdst.write(chunk)
                self._processed_bytes += len(chunk)
        copystat(src, dst)

    def _copy_tree(self, src, dst):
        if ospath.isfile(src):
            self._copy_file(src, dst)
            return
        for root, _, files in walk(src):
            rel = ospath.relpath(root, src)
            target_root = dst if rel == "." else ospath.join(dst, rel)
            makedirs(target_root, exist_ok=True)
            for name in files:
                self._copy_file(ospath.join(root, name), ospath.join(target_root, name))

    def _transfer(self, dest_dir, keep_source):
        makedirs(dest_dir, exist_ok=True)
        target = self._unique_target(dest_dir, ospath.basename(self._path))
        if not keep_source:
            try:
                rename(self._path, target)
                self._processed_bytes = self._listener.size
                return target
            except OSError as e:
                if e.errno != EXDEV:
                    raise
        # Different filesystem (or seeding): copy with progress.
        try:
            self._copy_tree(self._path, target)
        except BaseException:
            if ospath.isdir(target):
                rmtree(target, ignore_errors=True)
            elif ospath.exists(target):
                remove(target)
            raise
        if not keep_source:
            if ospath.isdir(self._path):
                rmtree(self._path, ignore_errors=True)
            else:
                remove(self._path)
        return target

    @staticmethod
    def _count(target):
        if ospath.isfile(target):
            return 1, 0
        files = folders = 0
        for _, dirs, fnames in walk(target):
            folders += len(dirs)
            files += len(fnames)
        return files, folders

    async def upload(self):
        dest_dir = self._listener.up_dest.removeprefix("lc:")
        LOGGER.info(f"Moving to local path: {self._path} -> {dest_dir}")
        try:
            target = await sync_to_async(
                self._transfer, dest_dir, bool(self._listener.seed)
            )
        except CancelledError:
            return
        except Exception as e:
            LOGGER.error(f"Local Move Error: {e} - Path: {self._path}")
            await self._listener.on_upload_error(f"Local: {e}")
            return
        if self._listener.is_cancelled:
            return
        if ospath.isdir(target):
            mime_type = "Folder"
        else:
            mime_type = await sync_to_async(get_mime_type, target)
        files, folders = await sync_to_async(self._count, target)
        LOGGER.info(f"Moved To Local: {target}")
        await self._listener.on_upload_complete(
            "", files, folders, mime_type, rclone_path=target
        )

    async def cancel_task(self):
        self._listener.is_cancelled = True
        LOGGER.info(f"Cancelling Local Move: {self._listener.name}")
        await self._listener.on_upload_error("your upload has been stopped!")
