"""
Stream Remove module for WZML-X.
Handles the -sr / -streamremove argument: after download, shows inline buttons
for audio/subtitle tracks so the user can choose which to remove before upload.
"""

from asyncio import Event, create_subprocess_exec, gather as async_gather
from asyncio.subprocess import PIPE
from os import path as ospath, walk

from aiofiles.os import path as aiopath, remove as aio_remove, rename as aio_rename

from .. import LOGGER, task_dict, task_dict_lock, cpu_eater_lock
from ..helper.ext_utils.bot_utils import new_task
from ..helper.ext_utils.media_utils import FFMpeg, get_document_type, get_media_info, get_streams
from ..helper.ext_utils.status_utils import MirrorStatus, get_readable_file_size
from ..helper.mirror_leech_utils.status_utils.ffmpeg_status import FFmpegStatus
from ..helper.telegram_helper.button_build import ButtonMaker
from ..helper.telegram_helper.message_utils import (
    delete_message,
    edit_message,
    send_message,
)

# ------------------------------------------------------------------
# In-memory store: mid -> StreamRemoveSession
# ------------------------------------------------------------------
_sessions: dict[int, "StreamRemoveSession"] = {}


def get_stream_remove_session(mid: int) -> "StreamRemoveSession | None":
    return _sessions.get(mid)


def register_stream_remove_session(session: "StreamRemoveSession"):
    _sessions[session.mid] = session


def unregister_stream_remove_session(mid: int):
    _sessions.pop(mid, None)


# ------------------------------------------------------------------
# "Waiting for user" status — shows in the progress bar as "StreamWait"
# ------------------------------------------------------------------

class StreamWaitStatus:
    """
    Shown in the task list while the bot is waiting for the user to
    select which streams to remove. Progress bar stays hidden and the
    status line reads "StreamWait".
    """

    def __init__(self, listener, gid: str):
        self.listener = listener
        self._gid = gid

    def gid(self):
        return self._gid

    def name(self):
        return self.listener.name

    def size(self):
        return get_readable_file_size(self.listener.size)

    def status(self):
        return MirrorStatus.STATUS_STREAMWAIT

    def processed_bytes(self):
        return "0B"

    def speed(self):
        return "0B/s"

    def progress(self):
        return "0%"

    def eta(self):
        return "-"

    def task(self):
        return self

    async def cancel_task(self):
        LOGGER.info(f"Cancelling StreamWait: {self.listener.name}")
        self.listener.is_cancelled = True
        await self.listener.on_upload_error("Stream Remove cancelled by user!")


# ------------------------------------------------------------------
# Language helpers
# ------------------------------------------------------------------

_LANG_MAP = {
    "jpn": "JPN", "ja": "JPN", "jap": "JPN",
    "eng": "ENG", "en": "ENG",
    "por": "POR", "pt": "POR",
    "spa": "SPA", "es": "SPA",
    "fre": "FRE", "fr": "FRE",
    "ger": "GER", "de": "GER",
    "ita": "ITA", "it": "ITA",
    "chi": "CHI", "zh": "CHI",
    "kor": "KOR", "ko": "KOR",
    "ara": "ARA", "ar": "ARA",
    "rus": "RUS", "ru": "RUS",
    "hin": "HIN", "hi": "HIN",
    "tha": "THA", "th": "THA",
    "vie": "VIE", "vi": "VIE",
    "ind": "IND", "id": "IND",
    "may": "MAY", "ms": "MAY",
    "tur": "TUR", "tr": "TUR",
    "pol": "POL", "pl": "POL",
    "dut": "DUT", "nl": "DUT",
    "swe": "SWE", "sv": "SWE",
    "nor": "NOR", "no": "NOR",
    "dan": "DAN", "da": "DAN",
    "fin": "FIN", "fi": "FIN",
    "heb": "HEB", "he": "HEB",
    "ces": "CZE", "cs": "CZE",
    "hun": "HUN", "hu": "HUN",
    "rum": "ROM", "ro": "ROM",
    "bul": "BUL", "bg": "BUL",
    "hrv": "CRO", "hr": "CRO",
    "ukr": "UKR", "uk": "UKR",
    "cat": "CAT", "ca": "CAT",
    "und": "UND",
}


def _fmt_lang(lang: str | None) -> str:
    if not lang:
        return "UND"
    return _LANG_MAP.get(lang.lower(), lang.upper()[:3])


# ------------------------------------------------------------------
# Build stream track list from a single representative file
# ------------------------------------------------------------------

async def _collect_tracks(file_path: str) -> list[dict]:
    """
    Returns a list of dicts, one per audio/subtitle stream:
      {index, type, lang, label}
    index = ffprobe stream index (used later with -map -0:index)
    """
    streams = await get_streams(file_path)
    if not streams:
        return []
    tracks = []
    for s in streams:
        codec_type = s.get("codec_type", "")
        if codec_type not in ("audio", "subtitle"):
            continue
        lang = s.get("tags", {}).get("language", None)
        label_lang = _fmt_lang(lang)
        type_label = "Audio" if codec_type == "audio" else "Subtitle"
        tracks.append({
            "index": s["index"],
            "type": type_label,
            "lang": label_lang,
            "label": f"{type_label} ~ {label_lang}",
        })
    return tracks


# ------------------------------------------------------------------
# Session object — holds state while user is choosing streams
# ------------------------------------------------------------------

class StreamRemoveSession:
    def __init__(self, listener, tracks: list[dict], ui_message):
        self.mid = listener.mid
        self.listener = listener
        self.tracks = tracks
        self.selected: set[int] = set()   # set of track list indices (not ffprobe idx)
        self.event = Event()
        self.cancelled = False
        self.ui_message = ui_message

    def build_markup(self):
        bm = ButtonMaker()
        for i, track in enumerate(self.tracks):
            tick = " ✓" if i in self.selected else ""
            bm.data_button(
                f"{track['label']}{tick}",
                f"sr {self.mid} toggle {i}",
            )
        bm.data_button("✅ Done", f"sr {self.mid} done", position="footer")
        bm.data_button("❌ Cancel", f"sr {self.mid} cancel", position="footer")
        return bm.build_menu(b_cols=2, f_cols=2)

    def selected_stream_indices(self) -> list[int]:
        return [self.tracks[i]["index"] for i in sorted(self.selected)]


# ------------------------------------------------------------------
# Callback query handler (registered in handlers.py)
# ------------------------------------------------------------------

@new_task
async def stream_remove_callback(_, query):
    data = query.data.split()
    # data: ["sr", mid, action, opt_index]
    if len(data) < 3:
        await query.answer()
        return

    mid = int(data[1])
    action = data[2]
    user_id = query.from_user.id

    session = get_stream_remove_session(mid)
    if session is None:
        await query.answer("Session expired or already processed.", show_alert=True)
        return

    if user_id != session.listener.user_id:
        await query.answer("This selection is not for you!", show_alert=True)
        return

    if action == "toggle":
        idx = int(data[3])
        if idx in session.selected:
            session.selected.discard(idx)
        else:
            session.selected.add(idx)
        await query.answer()
        await edit_message(
            session.ui_message,
            session.ui_message.text,
            session.build_markup(),
        )

    elif action == "done":
        await query.answer("Processing…")
        session.cancelled = False
        session.event.set()
        await delete_message(session.ui_message)

    elif action == "cancel":
        await query.answer("Cancelled.")
        session.cancelled = True
        session.event.set()
        await delete_message(session.ui_message)


# ------------------------------------------------------------------
# Core function called from task_listener.py
# ------------------------------------------------------------------

async def prompt_stream_remove(listener, dl_path: str, gid: str) -> str:
    """
    Called after download (and after extract if -e was used), before
    metadata/compress/upload.  Shows stream selection UI, waits for the
    user, then removes streams via ffmpeg.  Returns dl_path (files are
    modified in-place so the path itself doesn't change).
    """
    # ── 1. Collect tracks from the first media file ───────────────────
    tracks = await _get_tracks_for_path(dl_path)
    if not tracks:
        LOGGER.info(f"StreamRemove: no audio/subtitle tracks found in {dl_path}, skipping.")
        return dl_path

    # ── 2. Switch status to StreamWait so the task bar shows "Waiting" ─
    #       instead of being stuck on "Downloading 100%"                 ─
    async with task_dict_lock:
        task_dict[listener.mid] = StreamWaitStatus(listener, gid)
    listener.progress = False   # suppress default progress updater

    # ── 3. Build mention / filename for the selection message ─────────
    user = listener.user
    if hasattr(user, "mention"):
        mention = user.mention
    elif hasattr(user, "first_name"):
        mention = f"<a href='tg://user?id={user.id}'>{user.first_name}</a>"
    else:
        mention = str(user.id)

    filename = ospath.basename(dl_path)
    msg_text = (
        f"<b>Stream Remove:</b> {mention}\n\n"
        f"<b>Filename:</b> <code>{filename}</code>\n\n"
        f"<b>Select stream(s) to remove:</b>"
    )

    # ── 4. Send placeholder → build session → edit with real buttons ──
    bm_placeholder = ButtonMaker()
    bm_placeholder.data_button("Loading…", f"sr {listener.mid} noop")
    ui_msg = await send_message(
        listener.message,
        msg_text,
        bm_placeholder.build_menu(b_cols=1),
    )

    session = StreamRemoveSession(listener, tracks, ui_msg)
    register_stream_remove_session(session)
    await edit_message(ui_msg, msg_text, session.build_markup())

    # ── 5. Wait for Done / Cancel ─────────────────────────────────────
    await session.event.wait()
    unregister_stream_remove_session(listener.mid)

    # Restore progress flag regardless of outcome
    listener.progress = True

    if listener.is_cancelled or session.cancelled:
        return dl_path

    selected_indices = session.selected_stream_indices()
    if not selected_indices:
        LOGGER.info("StreamRemove: no streams selected, skipping ffmpeg step.")
        return dl_path

    # ── 6. Remove streams with full progress tracking ─────────────────
    LOGGER.info(f"StreamRemove: removing stream indices {selected_indices} from {dl_path}")
    dl_path = await _remove_streams(listener, dl_path, selected_indices, gid)
    return dl_path


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

async def _get_tracks_for_path(dl_path: str) -> list[dict]:
    """Pick the first media file in dl_path (file or dir) and return its tracks."""
    if await aiopath.isfile(dl_path):
        is_video, is_audio, _ = await get_document_type(dl_path)
        if is_video or is_audio:
            return await _collect_tracks(dl_path)
        return []

    for dirpath, _, files in walk(dl_path):
        for fname in sorted(files):
            fpath = ospath.join(dirpath, fname)
            is_video, is_audio, _ = await get_document_type(fpath)
            if is_video or is_audio:
                return await _collect_tracks(fpath)
    return []


async def _collect_media_files(dl_path: str) -> list[str]:
    """Return sorted list of all media file paths under dl_path."""
    if await aiopath.isfile(dl_path):
        is_video, is_audio, _ = await get_document_type(dl_path)
        return [dl_path] if (is_video or is_audio) else []

    result = []
    for dirpath, _, files in walk(dl_path):
        for fname in sorted(files):
            fpath = ospath.join(dirpath, fname)
            is_video, is_audio, _ = await get_document_type(fpath)
            if is_video or is_audio:
                result.append(fpath)
    return result


async def _remove_streams(listener, dl_path: str, stream_indices: list[int], gid: str) -> str:
    """
    Strip selected stream indices from every media file using ffmpeg -c copy
    (no re-encode, very fast).

    Progress display:
      - Status label : "Stream Rm"  (via FFmpegStatus "StreamRemove")
      - Progress %   : per-file time-based progress from FFMpeg._ffmpeg_progress()
      - Count        : (files_done / total_files) shown in "Count:" row
      - Sub Name     : current filename being processed
    """
    media_files = await _collect_media_files(dl_path)
    total_files = len(media_files)
    if total_files == 0:
        return dl_path

    # Populate files_to_proceed so the "Count:" field shows the denominator
    listener.files_to_proceed = list(media_files)

    # Build ffmpeg -map args: keep all, then exclude each selected index
    map_args: list[str] = ["-map", "0"]
    for idx in stream_indices:
        map_args += ["-map", f"-0:{idx}"]

    ffmpeg = FFMpeg(listener)

    # Switch task status to FFmpegStatus "StreamRemove" → displays "Stream Rm"
    async with task_dict_lock:
        task_dict[listener.mid] = FFmpegStatus(listener, ffmpeg, gid, "StreamRemove")

    listener.progress = False
    await cpu_eater_lock.acquire()
    listener.progress = True

    try:
        for file_index, f_path in enumerate(media_files, start=1):
            if listener.is_cancelled:
                break

            # Update progress metadata for the status bar
            listener.subname = ospath.basename(f_path)
            listener.subsize = ospath.getsize(f_path) if ospath.exists(f_path) else 0
            listener.proceed_count = file_index - 1  # will be set to file_index on success

            base, ext = ospath.splitext(f_path)
            out_path = f"{base}_sr_out{ext}"

            # Use -progress pipe:1 so FFMpeg._ffmpeg_progress() can read it
            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "error",
                "-progress", "pipe:1",
                "-i", f_path,
            ] + map_args + [
                "-c", "copy",
                "-y",
                out_path,
            ]

            LOGGER.info(f"StreamRemove [{file_index}/{total_files}]: {listener.subname}")

            # Prime FFMpeg for this file
            ffmpeg.clear()
            ffmpeg._total_time = (await get_media_info(f_path))[0]

            listener.subproc = await create_subprocess_exec(
                *cmd, stdout=PIPE, stderr=PIPE
            )

            # Run progress reader alongside the process; communicate() drains stderr
            await async_gather(
                ffmpeg._ffmpeg_progress(),
                listener.subproc.communicate(),
            )
            returncode = listener.subproc.returncode

            if returncode == 0:
                await aio_remove(f_path)
                await aio_rename(out_path, f_path)
                listener.proceed_count = file_index  # mark done
            else:
                LOGGER.error(
                    f"StreamRemove ffmpeg failed (code {returncode}) for: {f_path}"
                )
                try:
                    if await aiopath.exists(out_path):
                        await aio_remove(out_path)
                except Exception:
                    pass

    finally:
        cpu_eater_lock.release()
        listener.subproc = None
        listener.subname = ""
        listener.subsize = 0
        listener.files_to_proceed = []

    return dl_path
