"""
Mega file selector callback handler for WZML-X.
Handles the "Done Selecting" and "Cancel" inline buttons sent alongside
the web-based Mega file selector URL (-ms / -megaselect).
"""

from ..helper.ext_utils.bot_utils import new_task
from ..helper.listeners.mega_listener import (
    get_mega_select_session,
    unregister_mega_select_session,
)


@new_task
async def mega_select_callback(_, query):
    """
    Callback data format:
      megasel done  <mid>    — user pressed "Done Selecting"
      megasel cancel <mid>   — user pressed "Cancel"
    """
    data = query.data.split()
    if len(data) < 3:
        await query.answer()
        return

    action = data[1]
    mid = int(data[2])
    user_id = query.from_user.id

    session = get_mega_select_session(mid)
    if session is None:
        await query.answer("Session expired or already processed.", show_alert=True)
        return

    if user_id != session.listener.user_id:
        await query.answer("This task is not for you!", show_alert=True)
        return

    if action == "done":
        if not session.selected_paths:
            await query.answer(
                "No files selected yet! Select files in the web page first.",
                show_alert=True,
            )
            return
        await query.answer(f"Starting download of {len(session.selected_paths)} file(s)…")
        session.cancelled = False
        session.event.set()

    elif action == "cancel":
        await query.answer("Mega selection cancelled.")
        session.cancelled = True
        session.event.set()
        unregister_mega_select_session(mid)
