from time import time
from secrets import token_hex
from aiofiles.os import makedirs
from asyncio import create_subprocess_exec, subprocess, wait_for, Event, gather as async_gather
from re import search as re_search
from contextlib import suppress

from ... import LOGGER, task_dict, task_dict_lock
from ...core.config_manager import Config
from ..ext_utils.status_utils import MirrorStatus
from ..ext_utils.bot_utils import cmd_exec, mega_selection_buttons
from ..ext_utils.task_manager import (
    check_running_tasks,
    stop_duplicate_check,
    limit_checker,
)
from ..mirror_leech_utils.status_utils.mega_status import MegaDownloadStatus
from ..mirror_leech_utils.status_utils.queue_status import QueueStatus
from ..telegram_helper.message_utils import send_status_message, send_message, delete_message

mega_tasks = {}

# ------------------------------------------------------------------
# Mega file-selection sessions: mid -> MegaSelectSession
# ------------------------------------------------------------------
_mega_select_sessions: dict[int, "MegaSelectSession"] = {}


def get_mega_select_session(mid: int):
    return _mega_select_sessions.get(mid)


def register_mega_select_session(session):
    _mega_select_sessions[session.mid] = session


def unregister_mega_select_session(mid: int):
    _mega_select_sessions.pop(mid, None)


class MegaSelectSession:
    """Holds the asyncio Event and result for the web-based file selector."""
    def __init__(self, mid: int, listener):
        self.mid = mid
        self.listener = listener
        self.event = Event()
        self.selected_paths: list[str] = []   # full mega paths chosen by user
        self.cancelled = False


async def mega_cleanup():
    if not mega_tasks:
        return
    LOGGER.info("Running Mega Cleanup...")
    for path in list(mega_tasks.values()):
        try:
            await cmd_exec(["mega-rm", "-r", "-f", path])
        except Exception as e:
            LOGGER.error(f"Mega Restart Cleanup Failed for {path}: {e}")
    mega_tasks.clear()


# ------------------------------------------------------------------
# MegaCMD recursive listing helper
# ------------------------------------------------------------------

async def megals_recursive(mega_path: str) -> list[dict]:
    """
    Two-step approach using only standard MegaCMD commands:

    Step 1 — mega-find <path>
      Lists every node recursively, one full virtual path per line.
      Works on virtual paths (/wzml_xxx/FolderName) after mega-import.
      Example output:
        /wzml_abc/Anime/Episode01.mkv
        /wzml_abc/Anime/Episode02.mkv
        /wzml_abc/Subs/English.ass

    Step 2 — mega-ls -l <dir> once per unique parent directory
      Parses size for each item (folders show "-", files show bytes).

    Returns list of {path, name, size} for files only.
    """
    # ── Step 1: get all paths recursively via mega-find ─────────────
    stdout, stderr, ret = await cmd_exec(["mega-find", mega_path])
    if ret != 0 or not stdout.strip():
        LOGGER.error(f"mega-find failed for {mega_path}: {stderr!r}")
        return []

    all_paths = [
        line.strip()
        for line in stdout.strip().splitlines()
        if line.strip() and line.strip() != mega_path
    ]

    if not all_paths:
        LOGGER.error(f"mega-find returned no results for {mega_path}")
        return []

    LOGGER.info(f"mega-find found {len(all_paths)} nodes under {mega_path}")

    # ── Step 2: get sizes — one mega-ls -l per unique parent dir ────
    parent_dirs: set[str] = set()
    for p in all_paths:
        parent = p.rsplit("/", 1)[0] if "/" in p else mega_path
        parent_dirs.add(parent)

    size_map: dict[str, int] = {}

    for parent_dir in parent_dirs:
        ls_out, ls_err, ls_ret = await cmd_exec(["mega-ls", "-l", parent_dir])
        if ls_ret != 0 or not ls_out.strip():
            LOGGER.warning(f"mega-ls -l failed for {parent_dir}: {ls_err!r}")
            continue
        for line in ls_out.strip().splitlines():
            if not line.strip():
                continue
            match = re_search(
                r"^[d\-][rwx\-]{9}\s+(\d+|-)\s+\S+\s+\d{2}:\d{2}:\d{2}\s+(.+)$",
                line,
            )
            if not match:
                continue
            size_str = match.group(1)
            name = match.group(2).strip()
            if size_str == "-":
                continue
            size_map[f"{parent_dir}/{name}"] = int(size_str)

    # ── Step 3: return only confirmed file paths ─────────────────────
    files: list[dict] = []
    for p in all_paths:
        if p in size_map:
            files.append({
                "path": p,
                "name": p.rsplit("/", 1)[-1],
                "size": size_map[p],
            })

    LOGGER.info(f"megals_recursive: resolved {len(files)} files with sizes")
    return files


def build_file_tree(files: list[dict], root_prefix: str) -> list[dict]:
    """
    Convert a flat list of {path, name, size} into the nested tree
    structure that page.html / mega_selector page expects:
      [{id, name, type, size, selected, children?}]
    id is the mega full path (used as identifier when user submits).
    """
    root: dict = {"children": {}}   # intermediate build tree

    for f in files:
        # Strip root prefix to get relative path
        rel = f["path"]
        if rel.startswith(root_prefix):
            rel = rel[len(root_prefix):]
        rel = rel.lstrip("/")

        parts = rel.split("/")
        node = root
        for part in parts[:-1]:
            node = node["children"].setdefault(part, {"children": {}, "_is_dir": True})
        # leaf file
        node["children"][parts[-1]] = {
            "_is_file": True,
            "full_path": f["path"],
            "size": f["size"],
        }

    def _convert(node_dict: dict, node_id_counter: list) -> list:
        result = []
        for name, child in sorted(node_dict.items()):
            if child.get("_is_file"):
                result.append({
                    "id": child["full_path"],
                    "name": name,
                    "type": "file",
                    "size": child["size"],
                    "selected": True,
                })
            else:
                children = _convert(child["children"], node_id_counter)
                node_id_counter[0] += 1
                result.append({
                    "id": f"megaFolder_{node_id_counter[0]}",
                    "name": name,
                    "type": "folder",
                    "children": children,
                })
        return result

    return _convert(root["children"], [0])


class MegaAppListener:
    def __init__(self, listener):
        self.listener = listener
        self.process = None
        self.gid = token_hex(5)
        self.mega_status = None
        self.name = ""
        self.size = 0
        self.temp_path = f"/wzml_{self.gid}"
        self.mega_tags = set()
        self._is_cleaned = False
        self._last_time = time()
        self._val_last = 0
        mega_tasks[self.gid] = self.temp_path

    async def login(self):
        if (MEGA_EMAIL := Config.MEGA_EMAIL) and (
            MEGA_PASSWORD := Config.MEGA_PASSWORD
        ):
            try:
                await cmd_exec(["mega-login", MEGA_EMAIL, MEGA_PASSWORD])
            except Exception as e:
                raise Exception(f"Mega Login Failed: {e}")
        else:
            raise Exception("MegaCMD: Credentials Missing! Login required")

    async def create_temp_path(self):
        await cmd_exec(["mega-mkdir", self.temp_path])

    async def import_link(self):
        stdout, stderr, ret = await cmd_exec(
            ["mega-import", self.listener.link, self.temp_path]
        )
        if ret != 0:
            raise Exception(f"Mega Import Failed: {stderr}")

    async def get_metadata_and_target(self):
        stdout, _, ret = await cmd_exec(["mega-ls", "-l", self.temp_path])
        if ret != 0 or not stdout:
            raise Exception("Mega Metadata Failed")

        lines = [line for line in stdout.strip().split("\n") if line.strip()]
        if not lines:
            raise Exception("Mega Import: No items found")

        for line in lines:
            match = re_search(r"\s(\d+|-)\s+\S+\s+\d{2}:\d{2}:\d{2}\s+(.*)$", line)
            if match:
                size_str = match.group(1)
                self.name = match.group(2).strip()
                self.size = int(size_str) if size_str.isdigit() else 0
                break

        if not self.name:
            s_stdout, _, _ = await cmd_exec(["mega-ls", self.temp_path])
            if s_stdout:
                self.name = s_stdout.strip().split("\n")[0].strip()

        if not self.name:
            self.name = self.listener.name or f"MEGA_Download_{self.gid}"

        self.listener.name = self.name
        self.listener.size = self.size

        return f"{self.temp_path}/{self.name}"

    async def cleanup(self):
        if self._is_cleaned:
            return
        self._is_cleaned = True
        try:
            LOGGER.info(f"Cleaning up Mega Task: {self.name}")
            await cmd_exec(["mega-rm", "-r", "-f", self.temp_path])
            if self.gid in mega_tasks:
                del mega_tasks[self.gid]
        except Exception as e:
            LOGGER.error(f"Mega Cleanup Failed: {e}")

    # ------------------------------------------------------------------
    # Main download entry — branches on listener.mega_select
    # ------------------------------------------------------------------

    async def download(self, path):
        try:
            await self.login()
            await self.create_temp_path()
            await self.import_link()
            target_node = await self.get_metadata_and_target()

            msg, button = await stop_duplicate_check(self.listener)
            if msg:
                await self.listener.on_download_error(msg, button)
                return

            if limit_exceeded := await limit_checker(self.listener):
                await self.listener.on_download_error(limit_exceeded, is_limit=True)
                return

            # ── Mega file selection (-ms / -megaselect) ──────────────
            if getattr(self.listener, "mega_select", False):
                selected_paths = await self._run_file_selector(target_node)
                if self.listener.is_cancelled:
                    return
                if selected_paths is None:
                    # cancelled — on_download_start already called in _run_file_selector
                    # so call on_upload_error to cleanly end the task
                    await self.listener.on_upload_error("Mega selection cancelled.")
                    return
                await self._download_selected(path, selected_paths)
                return

            # ── Normal single mega-get ───────────────────────────────
            await self._download_single(path, target_node)

        except Exception as e:
            if self.listener.is_cancelled:
                return
            LOGGER.error(f"Mega Download Logic Error: {e}")
            await self.listener.on_download_error(str(e))
        finally:
            await self.cleanup()

    # ------------------------------------------------------------------
    # File selector: list files via megals, send web UI link, wait
    # ------------------------------------------------------------------

    async def _run_file_selector(self, target_node: str) -> list[str] | None:
        """
        Lists all files under target_node using mega-ls -lr,
        stores them in a MegaSelectSession keyed by listener.mid, sends the
        web URL to the user, and waits for the web POST + Done button.
        Returns list of selected mega paths, or None if cancelled.
        """
        LOGGER.info(f"MegaSelect: listing files under {target_node} via mega-ls -lr")
        files = await megals_recursive(target_node)
        LOGGER.info(f"MegaSelect: found {len(files)} files")

        if not files:
            LOGGER.error(f"MegaSelect: no files found under {target_node}")
            # on_download_start not called yet — use on_download_error directly
            await self.listener.on_download_error(
                "No files found in this Mega folder. Check that the link is a folder."
            )
            return None

        # Show task in status bar while waiting for user selection
        from ...helper.ext_utils.status_utils import MirrorStatus
        from ...helper.mirror_leech_utils.status_utils.mega_status import MegaDownloadStatus
        self.mega_status = MegaDownloadStatus(
            self.listener, self, self.gid, MirrorStatus.STATUS_PAUSED
        )
        async with task_dict_lock:
            task_dict[self.listener.mid] = self.mega_status
        await self.listener.on_download_start()
        if self.listener.multi <= 1:
            await send_status_message(self.listener.message)

        # Store the file list so the web server can serve it
        session = MegaSelectSession(self.listener.mid, self.listener)
        session.files = files
        session.target_node = target_node
        register_mega_select_session(session)

        # Build selection URL and send UI message
        pin = "".join([c for c in self.gid if c.isdigit()][:4]) or "0000"
        buttons = mega_selection_buttons(self.listener.mid, self.gid)

        ui_msg = await send_message(
            self.listener.message,
            f"<b>Mega File Selector</b>\n\n"
            f"<b>Name:</b> <code>{self.name}</code>\n"
            f"<b>Files found:</b> {len(files)}\n\n"
            f"Open the web page, select the files you want to download, "
            f"submit your selection, then press <b>Done Selecting</b> in this chat.",
            buttons,
        )

        # Wait for user to press Done Selecting or Cancel in Telegram
        await session.event.wait()
        unregister_mega_select_session(self.listener.mid)
        await delete_message(ui_msg)

        if session.cancelled or self.listener.is_cancelled:
            return None

        return session.selected_paths

    # ------------------------------------------------------------------
    # Download selected files one by one with mega-get
    # ------------------------------------------------------------------

    async def _download_selected(self, local_path: str, mega_paths: list[str]):
        """
        Download each selected mega path into local_path.
        NOTE: on_download_start() was already called by _run_file_selector,
        so we only call it again if we were queued and are now resuming.
        """
        added_to_queue, event = await check_running_tasks(self.listener)
        if added_to_queue:
            LOGGER.info(f"Added to Queue/Download: {self.name}")
            async with task_dict_lock:
                task_dict[self.listener.mid] = QueueStatus(
                    self.listener, self.gid, "Dl"
                )
            # on_download_start already called — just wait for queue slot
            await event.wait()
            if self.listener.is_cancelled:
                return

        # Switch status from PAUSED → DOWNLOAD
        self.mega_status = MegaDownloadStatus(
            self.listener, self, self.gid, MirrorStatus.STATUS_DOWNLOAD
        )
        async with task_dict_lock:
            task_dict[self.listener.mid] = self.mega_status

        LOGGER.info(f"Download from Mega (selected {len(mega_paths)} files): {self.name}")

        await makedirs(local_path, exist_ok=True)

        total = len(mega_paths)
        for idx, mega_path in enumerate(mega_paths, start=1):
            if self.listener.is_cancelled:
                break

            LOGGER.info(f"MegaSelect get [{idx}/{total}]: {mega_path}")
            self.listener.subname = mega_path.rsplit("/", 1)[-1]
            self.listener.proceed_count = idx - 1

            command = ["mega-get", mega_path, local_path]
            self.process = await create_subprocess_exec(
                *command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )

            while True:
                if self.listener.is_cancelled:
                    break
                try:
                    line_bytes = await wait_for(
                        self.process.stdout.readuntil(b"\r"), timeout=5
                    )
                    line = line_bytes.decode().strip()
                    if not line:
                        if self.process.returncode is not None:
                            break
                        continue
                    self._parse_progress(line)
                except TimeoutError:
                    await self.update_daemon_status()
                    if self.process.returncode is not None:
                        break
                    continue
                except Exception:
                    break
                if self.process.returncode is not None:
                    break

            await self.process.wait()

            if self.process.returncode != 0 and not self.listener.is_cancelled:
                LOGGER.error(f"mega-get failed for {mega_path} (code {self.process.returncode})")

            self.listener.proceed_count = idx

        if self.listener.is_cancelled:
            return

        await self.cleanup()
        await self.listener.on_download_complete()

    # ------------------------------------------------------------------
    # Normal single mega-get (unchanged behaviour)
    # ------------------------------------------------------------------

    async def _download_single(self, path, target_node):
        added_to_queue, event = await check_running_tasks(self.listener)
        if added_to_queue:
            LOGGER.info(f"Added to Queue/Download: {self.name}")
            async with task_dict_lock:
                task_dict[self.listener.mid] = QueueStatus(
                    self.listener, self.gid, "Dl"
                )
            await self.listener.on_download_start()
            if self.listener.multi <= 1:
                await send_status_message(self.listener.message)
            await event.wait()
            if self.listener.is_cancelled:
                return

        self.mega_status = MegaDownloadStatus(
            self.listener, self, self.gid, MirrorStatus.STATUS_DOWNLOAD
        )
        async with task_dict_lock:
            task_dict[self.listener.mid] = self.mega_status

        if added_to_queue:
            LOGGER.info(f"Start Queued Download from Mega: {self.name}")
        else:
            LOGGER.info(f"Download from Mega: {self.name}")
            await self.listener.on_download_start()
            if self.listener.multi <= 1:
                await send_status_message(self.listener.message)

        await makedirs(path, exist_ok=True)

        command = ["mega-get", target_node, path]

        self.process = await create_subprocess_exec(
            *command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        while True:
            if self.listener.is_cancelled:
                break

            try:
                line_bytes = await wait_for(
                    self.process.stdout.readuntil(b"\r"), timeout=5
                )
                line = line_bytes.decode().strip()
                if not line:
                    if self.process.returncode is not None:
                        break
                    continue
                self._parse_progress(line)
            except TimeoutError:
                await self.update_daemon_status()
                if self.process.returncode is not None:
                    break
                continue
            except Exception:
                break

            if self.process.returncode is not None:
                break

        await self.process.wait()

        if self.process.returncode == 0:
            await self.cleanup()
            await self.listener.on_download_complete()
        else:
            if self.listener.is_cancelled:
                return
            if self.process.returncode != -9:
                await self.listener.on_download_error(
                    f"MegaCMD exited with {self.process.returncode}"
                )

    def _parse_progress(self, line):
        multipliers = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "B": 1}
        match = re_search(r"\(([\d\.]+)/([\d\.]+)\s([KMGT]?B)", line)
        if match:
            dl_val = float(match.group(1))
            unit_char = (match.group(3))[0].upper()
            mult = multipliers.get(unit_char, 1)
            self.mega_status._downloaded_bytes = int(dl_val * mult)

            if not self.listener.size or self.listener.size == 0:
                total_val = float(match.group(2))
                self.mega_status._size = int(total_val * mult)
                self.listener.size = self.mega_status._size

            cur_time = time()
            if cur_time - self._last_time >= 2:
                self.mega_status._speed = int(
                    (self.mega_status._downloaded_bytes - self._val_last)
                    / (cur_time - self._last_time)
                )
                self._last_time = cur_time
                self._val_last = self.mega_status._downloaded_bytes

    async def update_daemon_status(self):
        try:
            stdout, _, _ = await cmd_exec(["mega-transfers", "--col-separator=|"])
            for line in stdout.splitlines():
                if self.gid in line:
                    parts = line.split("|")
                    if len(parts) > 1:
                        self.mega_tags.add(parts[1].strip())
                        if len(parts) > 4:
                            status = parts[5].strip().capitalize()
                            if self.mega_status._status != "Downloading":
                                self.mega_status._status = status
        except Exception:
            pass

    async def cancel_task(self):
        LOGGER.info(f"Cancelling {self.mega_status._status}: {self.name}")
        self.listener.is_cancelled = True

        await self.update_daemon_status()

        for tag in self.mega_tags:
            try:
                LOGGER.info(f"Cancelling Transfer Tag: {tag}")
                await cmd_exec(["mega-transfers", "-c", tag])
            except Exception as e:
                LOGGER.error(f"Mega Transfer Cancel Failed for {tag}: {e}")

        try:
            stdout, _, _ = await cmd_exec(["mega-transfers"])
            for line in stdout.splitlines():
                if self.gid in line:
                    parts = line.split()
                    if (
                        len(parts) > 1
                        and (tag := parts[1])
                        and tag not in self.mega_tags
                    ):
                        LOGGER.info(f"Cancelling Straggler Tag: {tag}")
                        await cmd_exec(["mega-transfers", "-c", tag])
        except Exception as e:
            LOGGER.error(f"Mega Final Cancel Check Failed: {e}")

        if self.process is not None:
            with suppress(Exception):
                self.process.kill()

        # Also cancel any pending MegaSelectSession
        session = get_mega_select_session(self.listener.mid)
        if session:
            session.cancelled = True
            session.event.set()



async def mega_cleanup():
    if not mega_tasks:
        return
    LOGGER.info("Running Mega Cleanup...")
    for path in list(mega_tasks.values()):
        try:
            await cmd_exec(["mega-rm", "-r", "-f", path])
        except Exception as e:
            LOGGER.error(f"Mega Restart Cleanup Failed for {path}: {e}")
    mega_tasks.clear()


class MegaAppListener:
    def __init__(self, listener):
        self.listener = listener
        self.process = None
        self.gid = token_hex(5)
        self.mega_status = None
        self.name = ""
        self.size = 0
        self.temp_path = f"/wzml_{self.gid}"
        self.mega_tags = set()
        self._is_cleaned = False
        self._last_time = time()
        self._val_last = 0
        mega_tasks[self.gid] = self.temp_path

    async def login(self):
        if (MEGA_EMAIL := Config.MEGA_EMAIL) and (
            MEGA_PASSWORD := Config.MEGA_PASSWORD
        ):
            try:
                await cmd_exec(["mega-login", MEGA_EMAIL, MEGA_PASSWORD])
            except Exception as e:
                raise Exception(f"Mega Login Failed: {e}")
        else:
            raise Exception("MegaCMD: Credentials Missing! Login required")

    async def create_temp_path(self):
        await cmd_exec(["mega-mkdir", self.temp_path])

    async def import_link(self):
        stdout, stderr, ret = await cmd_exec(
            ["mega-import", self.listener.link, self.temp_path]
        )
        if ret != 0:
            raise Exception(f"Mega Import Failed: {stderr}")

    async def get_metadata_and_target(self):
        stdout, _, ret = await cmd_exec(["mega-ls", "-l", self.temp_path])
        if ret != 0 or not stdout:
            raise Exception("Mega Metadata Failed")

        lines = [line for line in stdout.strip().split("\n") if line.strip()]
        if not lines:
            raise Exception("Mega Import: No items found")

        for line in lines:
            match = re_search(r"\s(\d+|-)\s+\S+\s+\d{2}:\d{2}:\d{2}\s+(.*)$", line)
            if match:
                size_str = match.group(1)
                self.name = match.group(2).strip()
                self.size = int(size_str) if size_str.isdigit() else 0
                break

        if not self.name:
            s_stdout, _, _ = await cmd_exec(["mega-ls", self.temp_path])
            if s_stdout:
                self.name = s_stdout.strip().split("\n")[0].strip()

        if not self.name:
            self.name = self.listener.name or f"MEGA_Download_{self.gid}"

        self.listener.name = self.name
        self.listener.size = self.size

        return f"{self.temp_path}/{self.name}"

    async def cleanup(self):
        if self._is_cleaned:
            return
        self._is_cleaned = True
        try:
            LOGGER.info(f"Cleaning up Mega Task: {self.name}")
            await cmd_exec(["mega-rm", "-r", "-f", self.temp_path])
            if self.gid in mega_tasks:
                del mega_tasks[self.gid]
        except Exception as e:
            LOGGER.error(f"Mega Cleanup Failed: {e}")

    async def download(self, path):
        try:
            await self.login()
            await self.create_temp_path()
            await self.import_link()
            target_node = await self.get_metadata_and_target()

            msg, button = await stop_duplicate_check(self.listener)
            if msg:
                await self.listener.on_download_error(msg, button)
                return

            if limit_exceeded := await limit_checker(self.listener):
                await self.listener.on_download_error(limit_exceeded, is_limit=True)
                return

            added_to_queue, event = await check_running_tasks(self.listener)
            if added_to_queue:
                LOGGER.info(f"Added to Queue/Download: {self.name}")
                async with task_dict_lock:
                    task_dict[self.listener.mid] = QueueStatus(
                        self.listener, self.gid, "Dl"
                    )
                await self.listener.on_download_start()
                if self.listener.multi <= 1:
                    await send_status_message(self.listener.message)
                await event.wait()
                if self.listener.is_cancelled:
                    return

            self.mega_status = MegaDownloadStatus(
                self.listener, self, self.gid, MirrorStatus.STATUS_DOWNLOAD
            )
            async with task_dict_lock:
                task_dict[self.listener.mid] = self.mega_status

            if added_to_queue:
                LOGGER.info(f"Start Queued Download from Mega: {self.name}")
            else:
                LOGGER.info(f"Download from Mega: {self.name}")
                await self.listener.on_download_start()
                if self.listener.multi <= 1:
                    await send_status_message(self.listener.message)

            await makedirs(path, exist_ok=True)

            command = ["mega-get", target_node, path]

            self.process = await create_subprocess_exec(
                *command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )

            while True:
                if self.listener.is_cancelled:
                    break

                try:
                    line_bytes = await wait_for(
                        self.process.stdout.readuntil(b"\r"), timeout=5
                    )
                    line = line_bytes.decode().strip()
                    if not line:
                        if self.process.returncode is not None:
                            break
                        continue
                    self._parse_progress(line)
                except TimeoutError:
                    await self.update_daemon_status()
                    if self.process.returncode is not None:
                        break
                    continue
                except Exception:
                    break

                if self.process.returncode is not None:
                    break

            await self.process.wait()

            if self.process.returncode == 0:
                await self.cleanup()
                await self.listener.on_download_complete()
            else:
                if self.listener.is_cancelled:
                    return
                if self.process.returncode != -9:
                    await self.listener.on_download_error(
                        f"MegaCMD exited with {self.process.returncode}"
                    )
        except Exception as e:
            if self.listener.is_cancelled:
                return
            LOGGER.error(f"Mega Download Logic Error: {e}")
            await self.listener.on_download_error(str(e))
        finally:
            await self.cleanup()

    def _parse_progress(self, line):
        multipliers = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "B": 1}
        match = re_search(r"\(([\d\.]+)/([\d\.]+)\s([KMGT]?B)", line)
        if match:
            dl_val = float(match.group(1))
            unit_char = (match.group(3))[0].upper()
            mult = multipliers.get(unit_char, 1)
            self.mega_status._downloaded_bytes = int(dl_val * mult)

            if not self.listener.size or self.listener.size == 0:
                total_val = float(match.group(2))
                self.mega_status._size = int(total_val * mult)
                self.listener.size = self.mega_status._size

            cur_time = time()
            if cur_time - self._last_time >= 2:
                self.mega_status._speed = int(
                    (self.mega_status._downloaded_bytes - self._val_last)
                    / (cur_time - self._last_time)
                )
                self._last_time = cur_time
                self._val_last = self.mega_status._downloaded_bytes

    async def update_daemon_status(self):
        try:
            stdout, _, _ = await cmd_exec(["mega-transfers", "--col-separator=|"])
            for line in stdout.splitlines():
                if self.gid in line:
                    parts = line.split("|")
                    if len(parts) > 1:
                        self.mega_tags.add(parts[1].strip())
                        if len(parts) > 4:
                            status = parts[5].strip().capitalize()
                            if self.mega_status._status != "Downloading":
                                self.mega_status._status = status
        except Exception:
            pass

    async def cancel_task(self):
        LOGGER.info(f"Cancelling {self.mega_status._status}: {self.name}")
        self.listener.is_cancelled = True

        await self.update_daemon_status()

        for tag in self.mega_tags:
            try:
                LOGGER.info(f"Cancelling Transfer Tag: {tag}")
                await cmd_exec(["mega-transfers", "-c", tag])
            except Exception as e:
                LOGGER.error(f"Mega Transfer Cancel Failed for {tag}: {e}")

        try:
            stdout, _, _ = await cmd_exec(["mega-transfers"])
            for line in stdout.splitlines():
                if self.gid in line:
                    parts = line.split()
                    if (
                        len(parts) > 1
                        and (tag := parts[1])
                        and tag not in self.mega_tags
                    ):
                        LOGGER.info(f"Cancelling Straggler Tag: {tag}")
                        await cmd_exec(["mega-transfers", "-c", tag])
        except Exception as e:
            LOGGER.error(f"Mega Final Cancel Check Failed: {e}")

        if self.process is not None:
            with suppress(Exception):
                self.process.kill()
