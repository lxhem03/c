from asyncio import Lock, sleep
from time import time
from secrets import token_hex
from pyrogram import Client
from pyrogram.errors import FloodWait, PeerIdInvalid, ChannelInvalid

from bot.helper.ext_utils.hyperdl_utils import HyperTGDownload

try:
    from pyrogram.errors import FloodPremiumWait
except ImportError:
    FloodPremiumWait = FloodWait

from .... import (
    LOGGER,
    task_dict,
    task_dict_lock,
    user_data,
)
from ....core.tg_client import TgClient
from ....core.config_manager import Config
from ...ext_utils.task_manager import check_running_tasks, stop_duplicate_check
from ...mirror_leech_utils.status_utils.queue_status import QueueStatus
from ...mirror_leech_utils.status_utils.telegram_status import TelegramStatus
from ...telegram_helper.message_utils import send_status_message

global_lock = Lock()
GLOBAL_GID = dict()

# Cache of started per-user Pyrogram clients: {user_id: Client}
_user_clients: dict = {}
_user_clients_lock = Lock()


async def get_personal_client(user_id: int) -> Client | None:
    """
    Return a started Pyrogram client for the given user_id using their
    USER_SESSION_STRING, or None if they haven't set one.
    Clients are cached in _user_clients so we don't reconnect every download.
    """
    udata = user_data.get(user_id, {})
    session_string = udata.get("USER_SESSION_STRING", "")
    if not session_string:
        return None

    async with _user_clients_lock:
        if user_id in _user_clients:
            return _user_clients[user_id]
        try:
            client = Client(
                f"WZ-UserSession-{user_id}",
                api_id=Config.TELEGRAM_API,
                api_hash=Config.TELEGRAM_HASH,
                session_string=session_string,
                in_memory=True,
                no_updates=True,
                sleep_threshold=60,
            )
            await client.start()
            _user_clients[user_id] = client
            uname = client.me.username or client.me.first_name
            LOGGER.info(f"Per-user session started for [{uname}] (uid={user_id})")
            return client
        except Exception as e:
            LOGGER.error(f"Failed to start per-user session for uid={user_id}: {e}")
            return None


async def stop_personal_client(user_id: int):
    """Stop and remove a cached per-user client (e.g. when session is removed)."""
    async with _user_clients_lock:
        client = _user_clients.pop(user_id, None)
        if client:
            try:
                await client.stop()
            except Exception:
                pass


class TelegramDownloadHelper:
    def __init__(self, listener):
        self._processed_bytes = 0
        self._start_time = 1
        self._listener = listener
        self._id = ""
        self.session = ""
        self._personal_client: Client | None = None
        self._hyper_dl = len(TgClient.helper_bots) != 0 and Config.LEECH_DUMP_CHAT

    @property
    def speed(self):
        return self._processed_bytes / (time() - self._start_time)

    @property
    def processed_bytes(self):
        return self._processed_bytes

    async def _on_download_start(self, file_id, gid, from_queue):
        async with global_lock:
            GLOBAL_GID[file_id] = gid
        self._id = file_id
        async with task_dict_lock:
            task_dict[self._listener.mid] = TelegramStatus(
                self._listener, self, gid, "dl", self._hyper_dl
            )
        if not from_queue:
            await self._listener.on_download_start()
            if self._listener.multi <= 1:
                await send_status_message(self._listener.message)
            LOGGER.info(f"Download from Telegram: {self._listener.name}")
        else:
            LOGGER.info(f"Start Queued Download from Telegram: {self._listener.name}")

    async def _on_download_progress(self, current, _):
        if self._listener.is_cancelled:
            if self.session == "personal":
                if self._personal_client:
                    self._personal_client.stop_transmission()
            elif self.session == "user":
                TgClient.user.stop_transmission()
            elif self.session == "hbots":
                for hbot in TgClient.helper_bots.values():
                    hbot.stop_transmission()
            else:
                TgClient.bot.stop_transmission()
        self._processed_bytes = current

    async def _on_download_error(self, error):
        async with global_lock:
            if self._id in GLOBAL_GID:
                GLOBAL_GID.pop(self._id)
        await self._listener.on_download_error(error)

    async def _on_download_complete(self):
        await self._listener.on_download_complete()
        async with global_lock:
            GLOBAL_GID.pop(self._id)
        return

    async def _get_message_via_best_client(self, chat_id, message_id):
        """
        Try to fetch a message using the best available client in priority order:
        1. Personal user session (per-user) — can access any chat the user is in
        2. Owner user session (TgClient.user) — owner's account
        3. Bot session fallback
        Returns (message, session_label) or raises on total failure.
        """
        # 1. Per-user personal session
        if self._personal_client:
            try:
                msg = await self._personal_client.get_messages(
                    chat_id=chat_id, message_ids=message_id
                )
                return msg, "personal"
            except (PeerIdInvalid, ChannelInvalid):
                LOGGER.warning(
                    f"Personal session not in chat {chat_id}, trying owner/bot session"
                )
            except Exception as e:
                LOGGER.warning(f"Personal session error: {e}")

        # 2. Owner user session
        if TgClient.user:
            try:
                msg = await TgClient.user.get_messages(
                    chat_id=chat_id, message_ids=message_id
                )
                return msg, "user"
            except (PeerIdInvalid, ChannelInvalid):
                LOGGER.warning(f"Owner user session not in chat {chat_id}, using bot")
            except Exception as e:
                LOGGER.warning(f"Owner user session error: {e}")

        # 3. Bot session
        msg = await self._listener.client.get_messages(
            chat_id=chat_id, message_ids=message_id
        )
        return msg, "bot"

    async def _download(self, message, path):
        try:
            if self._hyper_dl:
                try:
                    download = await HyperTGDownload().download_media(
                        message,
                        file_name=path,
                        progress=self._on_download_progress,
                        dump_chat=Config.LEECH_DUMP_CHAT,
                    )
                except Exception:
                    # hyper dl failed — fall back to best available session
                    try:
                        fetched_msg, session_label = await self._get_message_via_best_client(
                            message.chat.id, message.id
                        )
                        download = await fetched_msg.download(
                            file_name=path, progress=self._on_download_progress
                        )
                        if session_label != self.session:
                            self.session = session_label
                    except Exception:
                        download = await message.download(
                            file_name=path, progress=self._on_download_progress
                        )
            elif self.session == "personal" and self._personal_client:
                try:
                    fetched_msg = await self._personal_client.get_messages(
                        chat_id=message.chat.id, message_ids=message.id
                    )
                    download = await fetched_msg.download(
                        file_name=path, progress=self._on_download_progress
                    )
                except (PeerIdInvalid, ChannelInvalid):
                    LOGGER.warning(
                        "Personal session cannot access this chat; falling back to bot"
                    )
                    self.session = "bot"
                    download = await message.download(
                        file_name=path, progress=self._on_download_progress
                    )
            else:
                download = await message.download(
                    file_name=path, progress=self._on_download_progress
                )
            if self._listener.is_cancelled:
                return
        except (FloodWait, FloodPremiumWait) as f:
            LOGGER.warning(str(f))
            await sleep(f.value)
            await self._download(message, path)
            return
        except Exception as e:
            LOGGER.error(str(e), exc_info=True)
            await self._on_download_error(str(e))
            return
        if download is not None:
            await self._on_download_complete()
        elif not self._listener.is_cancelled:
            await self._on_download_error("Internal error occurred")
        return

    async def add_download(self, message, path, session):
        self.session = session

        # Load the personal client for this user if they have a session string set
        self._personal_client = await get_personal_client(self._listener.user_id)

        if not self.session:
            if self._hyper_dl:
                self.session = "hbots"
            elif self._personal_client:
                # Per-user session takes highest priority
                self.session = "personal"
                try:
                    message = await self._personal_client.get_messages(
                        chat_id=message.chat.id, message_ids=message.id
                    )
                except (PeerIdInvalid, ChannelInvalid):
                    LOGGER.warning(
                        "Personal user session is not in this chat! Trying owner/bot session."
                    )
                    # Fall through to owner session or bot
                    try:
                        message, self.session = await self._get_message_via_best_client(
                            message.chat.id, message.id
                        )
                    except Exception:
                        self.session = "bot"
            elif self._listener.user_transmission and self._listener.is_super_chat:
                self.session = "user"
                try:
                    message = await TgClient.user.get_messages(
                        chat_id=message.chat.id, message_ids=message.id
                    )
                except (PeerIdInvalid, ChannelInvalid):
                    LOGGER.warning(
                        "Owner user session is not in this chat! Downloading with bot session"
                    )
                    self.session = "bot"
            else:
                self.session = "bot"

        media = getattr(message, message.media.value) if message.media else None

        if media is not None:
            async with global_lock:
                download = media.file_unique_id not in GLOBAL_GID

            if download:
                if not self._listener.name:
                    if hasattr(media, "file_name") and media.file_name:
                        if "/" in media.file_name:
                            self._listener.name = media.file_name.rsplit("/", 1)[-1]
                            path = path + self._listener.name
                        else:
                            self._listener.name = media.file_name
                    else:
                        self._listener.name = "None"
                else:
                    path = path + self._listener.name
                self._listener.size = media.file_size
                gid = token_hex(5)

                msg, button = await stop_duplicate_check(self._listener)
                if msg:
                    await self._listener.on_download_error(msg, button)
                    return

                add_to_queue, event = await check_running_tasks(self._listener)
                if add_to_queue:
                    LOGGER.info(f"Added to Queue/Download: {self._listener.name}")
                    async with task_dict_lock:
                        task_dict[self._listener.mid] = QueueStatus(
                            self._listener, gid, "dl"
                        )
                    await self._listener.on_download_start()
                    if self._listener.multi <= 1:
                        await send_status_message(self._listener.message)
                    await event.wait()
                    # Re-fetch message after queue wait using best available session
                    if self.session == "bot":
                        message = await self._listener.client.get_messages(
                            chat_id=message.chat.id, message_ids=message.id
                        )
                    elif self.session == "personal" and self._personal_client:
                        try:
                            message = await self._personal_client.get_messages(
                                chat_id=message.chat.id, message_ids=message.id
                            )
                        except (PeerIdInvalid, ChannelInvalid):
                            message = await self._listener.client.get_messages(
                                chat_id=message.chat.id, message_ids=message.id
                            )
                    else:
                        try:
                            message = await TgClient.user.get_messages(
                                chat_id=message.chat.id, message_ids=message.id
                            )
                        except (PeerIdInvalid, ChannelInvalid):
                            message = await self._listener.client.get_messages(
                                chat_id=message.chat.id, message_ids=message.id
                            )
                    if self._listener.is_cancelled:
                        async with global_lock:
                            if self._id in GLOBAL_GID:
                                GLOBAL_GID.pop(self._id)
                        return
                self._start_time = time()
                await self._on_download_start(media.file_unique_id, gid, add_to_queue)
                await self._download(message, path)
            else:
                await self._on_download_error("File already being downloaded!")
        else:
            await self._on_download_error(
                "No document in the replied message! Use SuperGroup incase you are trying to download with User session!"
            )

    async def cancel_task(self):
        self._listener.is_cancelled = True
        LOGGER.info(
            f"Cancelling download on user request: name: {self._listener.name} id: {self._id}"
        )
        await self._on_download_error("Stopped by user!")
