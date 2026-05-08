"""
Stream Remove module for WZML-X.
Handles the -sr / -streamremove argument: after download, shows inline buttons
for audio/subtitle tracks so the user can choose which to remove before upload.
"""

from asyncio import Event
from os import path as ospath, walk

from aiofiles.os import path as aiopath

from .. import LOGGER, task_dict, task_dict_lock
from ..helper.ext_utils.bot_utils import new_task
from ..helper.ext_utils.media_utils import get_document_type, get_streams
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
        self.tracks = tracks          # list of track dicts
        self.selected: set[int] = set()   # set of track *list* indices (not ffprobe idx)
        self.event = Event()
        self.cancelled = False
        self.ui_message = ui_message  # the Telegram message with inline buttons

    # Build InlineKeyboardMarkup
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

    # Indices of selected ffprobe stream indices
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
# Core function called from TaskListener / common.py
# ------------------------------------------------------------------

async def prompt_stream_remove(listener, dl_path: str, gid: str) -> str:
    """
    Called after download (and after extract if -e was used), before metadata/compress/upload.
    Shows stream selection UI, waits for user choice, then removes streams via ffmpeg.
    Returns the (possibly modified) dl_path.
    """
    # Collect tracks from a representative media file
    tracks = await _get_tracks_for_path(dl_path)

    if not tracks:
        LOGGER.info(f"StreamRemove: no audio/subtitle tracks found in {dl_path}, skipping.")
        return dl_path

    # Build mention text
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

    # Send placeholder first, then build session (we need the message object)
    bm_placeholder = ButtonMaker()
    bm_placeholder.data_button("Loading…", f"sr {listener.mid} noop")
    ui_msg = await send_message(
        listener.message,
        msg_text,
        bm_placeholder.build_menu(b_cols=1),
    )

    session = StreamRemoveSession(listener, tracks, ui_msg)
    register_stream_remove_session(listener.mid)

    # Now re-edit with real buttons
    _sessions[listener.mid] = session
    await edit_message(ui_msg, msg_text, session.build_markup())

    # Wait for user to press Done or Cancel
    await session.event.wait()
    unregister_stream_remove_session(listener.mid)

    if listener.is_cancelled or session.cancelled:
        return dl_path  # let the normal cancel flow handle it

    selected_indices = session.selected_stream_indices()
    if not selected_indices:
        LOGGER.info("StreamRemove: no streams selected, skipping ffmpeg step.")
        return dl_path

    LOGGER.info(f"StreamRemove: removing stream indices {selected_indices} from {dl_path}")
    dl_path = await _remove_streams(listener, dl_path, selected_indices, gid)
    return dl_path


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

async def _get_tracks_for_path(dl_path: str) -> list[dict]:
    """Pick the first media file in dl_path (file or dir) and get its tracks."""
    if await aiopath.isfile(dl_path):
        is_video, is_audio, _ = await get_document_type(dl_path)
        if is_video or is_audio:
            return await _collect_tracks(dl_path)
        return []

    # Directory: find first media file
    for dirpath, _, files in walk(dl_path):
        for fname in sorted(files):
            fpath = ospath.join(dirpath, fname)
            is_video, is_audio, _ = await get_document_type(fpath)
            if is_video or is_audio:
                return await _collect_tracks(fpath)
    return []


async def _remove_streams(listener, dl_path: str, stream_indices: list[int], gid: str) -> str:
    """
    Use ffmpeg to strip the selected streams from every media file in dl_path.
    The same stream indices are removed from every file (multi-file support).
    Files are processed in-place: temp output replaces original.
    """
    from asyncio.subprocess import PIPE
    from asyncio import create_subprocess_exec

    from ..helper.mirror_leech_utils.status_utils.ffmpeg_status import FFmpegStatus
    from ..helper.ext_utils.media_utils import FFMpeg
    from .. import cpu_eater_lock

    ffmpeg = FFMpeg(listener)

    # Build -map args: keep everything EXCEPT selected indices
    # Strategy: -map 0  then  -map -0:<idx> for each to remove
    def _build_map_args(indices: list[int]) -> list[str]:
        args = ["-map", "0"]
        for idx in indices:
            args += ["-map", f"-0:{idx}"]
        return args

    map_args = _build_map_args(stream_indices)

    checked = False

    async def _process_file(f_path: str):
        nonlocal checked
        is_video, is_audio, _ = await get_document_type(f_path)
        if not is_video and not is_audio:
            return f_path

        base, ext = ospath.splitext(f_path)
        out_path = f"{base}_sr_out{ext}"

        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "error",
            "-i", f_path,
        ] + map_args + [
            "-c", "copy",
            "-y",
            out_path,
        ]

        if not checked:
            checked = True
            async with task_dict_lock:
                task_dict[listener.mid] = FFmpegStatus(listener, ffmpeg, gid, "StreamRemove")
            listener.progress = False
            await cpu_eater_lock.acquire()
            listener.progress = True

        LOGGER.info(f"StreamRemove ffmpeg: {' '.join(cmd)}")
        proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        _, stderr = await proc.communicate()

        if proc.returncode == 0:
            # Replace original with stripped version
            from aiofiles.os import remove, rename
            await remove(f_path)
            await rename(out_path, f_path)
            return f_path
        else:
            err = stderr.decode().strip()
            LOGGER.error(f"StreamRemove ffmpeg error for {f_path}: {err}")
            # Clean up temp if exists
            try:
                from aiofiles.os import remove as aio_remove
                if await aiopath.exists(out_path):
                    await aio_remove(out_path)
            except Exception:
                pass
            return f_path

    try:
        if await aiopath.isfile(dl_path):
            await _process_file(dl_path)
        else:
            for dirpath, _, files in walk(dl_path):
                for fname in sorted(files):
                    if listener.is_cancelled:
                        break
                    fpath = ospath.join(dirpath, fname)
                    listener.subname = fname
                    listener.subsize = ospath.getsize(fpath) if ospath.exists(fpath) else 0
                    await _process_file(fpath)
    finally:
        if checked:
            cpu_eater_lock.release()

    return dl_path
