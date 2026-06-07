from __future__ import annotations

import asyncio
import logging
import queue
import threading
from dataclasses import dataclass, field
from typing import Any

from gateway.stream_text_cleaner import StreamDisplayTextFilter, clean_stream_display_text


@dataclass
class FeishuCardToolBlock:
    token: str
    tool_name: str
    preview: str = ""
    args: dict[str, Any] = field(default_factory=dict)
    status: str = "running"


@dataclass
class FeishuCardRunState:
    text_blocks: list[str] = field(default_factory=list)
    tools: list[FeishuCardToolBlock] = field(default_factory=list)
    terminal: str = "running"
    _next_tool_index: int = 0

    def append_text(self, text: str) -> None:
        if not text:
            return
        if self.text_blocks:
            self.text_blocks[-1] += text
        else:
            self.text_blocks.append(text)

    def append_commentary(self, text: str) -> None:
        if text:
            self.text_blocks.append(text)

    def start_tool(self, *, tool_name: str, preview: str = "", args: dict[str, Any] | None = None, token: str | None = None) -> str:
        if token is None:
            token = f"tool-{self._next_tool_index}"
            self._next_tool_index += 1
        self.tools.append(FeishuCardToolBlock(token=token, tool_name=tool_name, preview=preview or "", args=args or {}))
        return token

    def finish_tool(self, token: str, *, ok: bool = True, output: str | None = None) -> None:
        for tool in self.tools:
            if tool.token == token:
                tool.status = "done" if ok else "error"
                return

    def finish_oldest_running_tool(self, *, tool_name: str, ok: bool = True) -> str | None:
        for tool in self.tools:
            if tool.tool_name == tool_name and tool.status == "running":
                tool.status = "done" if ok else "error"
                return tool.token
        return None

    def finalize(self) -> None:
        self.terminal = "done"


class FeishuCardRunRenderer:
    def render(self, state: FeishuCardRunState) -> dict[str, Any]:
        elements: list[dict[str, Any]] = []
        for block in state.text_blocks:
            if block.strip():
                elements.append({"tag": "markdown", "content": block})
        for tool in state.tools:
            preview = f" — `{tool.preview}`" if tool.preview else ""
            elements.append({
                "tag": "markdown",
                "content": f"**{tool.tool_name}**{preview}\n\nStatus: {tool.status}",
            })
        if state.terminal == "running":
            elements.append({"tag": "markdown", "content": "_calling tools_" if state.tools else "_outputting_"})
        return {
            "schema": "2.0",
            "config": {
                "streaming_mode": state.terminal == "running",
                "summary": {"content": "running" if state.terminal == "running" else "done"},
            },
            "body": {"elements": elements or [{"tag": "markdown", "content": "_running_"}]},
        }


logger = logging.getLogger("gateway.feishu_card_stream")


class FeishuCardRunSink:
    def __init__(
        self,
        *,
        adapter: Any,
        chat_id: str,
        metadata: dict[str, Any] | None = None,
        reply_to: str | None = None,
        update_interval_sec: float = 0.25,
        max_update_failures: int = 3,
        loop: asyncio.AbstractEventLoop | None = None,
    ):
        self.adapter = adapter
        self.chat_id = chat_id
        self.metadata = metadata
        self.reply_to = reply_to
        self.update_interval_sec = update_interval_sec
        self.max_update_failures = max_update_failures
        self._loop = loop
        # The sink must be constructed inside an async context so drain
        # scheduling works.  When constructed outside async (tests),
        # drain_pending_updates() is called synchronously by the test.
        if self._loop is None:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                self._loop = None  # ok: test context, drain called manually
        self.state = FeishuCardRunState()
        self.renderer = FeishuCardRunRenderer()
        self.text_filter = StreamDisplayTextFilter()
        self.message_id: str | None = None
        self.update_handle: str | None = None
        self.final_response_sent = False
        self.final_content_delivered = False
        self.fallback_sent = False
        self.card_updates_disabled = False
        self._closed = False
        self._event_queue: queue.Queue[tuple[str, tuple[Any, ...], dict[str, Any]]] = queue.Queue()
        self._drain_task: asyncio.Task[None] | None = None
        self._schedule_lock = threading.Lock()
        self._flush_lock = asyncio.Lock()
        self._sequence = 0
        self._update_failures = 0

    def on_delta(self, text: str) -> None:
        self._enqueue("delta", text)

    def on_commentary(self, text: str) -> None:
        self._enqueue("commentary", text)

    def on_tool_progress(
        self,
        event_type: str,
        tool_name: str | None = None,
        preview: str | None = None,
        args: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self._enqueue("tool", event_type, tool_name, preview, args, **kwargs)

    def _enqueue(self, kind: str, *args: Any, **kwargs: Any) -> None:
        if self._closed:
            return
        self._event_queue.put((kind, args, kwargs))
        self._schedule_drain_threadsafe()

    def _schedule_drain_threadsafe(self) -> None:
        if self._loop is None:
            return
        with self._schedule_lock:
            self._loop.call_soon_threadsafe(self._ensure_drain_task)

    def _ensure_drain_task(self) -> None:
        if self._closed:
            return
        if self._drain_task is None or self._drain_task.done():
            self._drain_task = asyncio.create_task(self._drain_and_flush())

    async def _drain_and_flush(self) -> None:
        if self.update_interval_sec > 0:
            await asyncio.sleep(self.update_interval_sec)
        changed = self._drain_events()
        if changed and not self.card_updates_disabled:
            ok = await self.flush()
            if not ok:
                self._record_update_failure()

    def _drain_events(self) -> bool:
        changed = False
        while True:
            try:
                kind, args, kwargs = self._event_queue.get_nowait()
            except queue.Empty:
                return changed
            changed = True
            if kind == "delta":
                cleaned = self.text_filter.feed(str(args[0]))
                if cleaned:
                    cleaned = clean_stream_display_text(cleaned)
                    self.state.append_text(cleaned)
            elif kind == "commentary":
                cleaned = self.text_filter.feed(str(args[0]))
                if cleaned:
                    cleaned = clean_stream_display_text(cleaned)
                    self.state.append_commentary(cleaned)
            elif kind == "tool":
                self._apply_tool_progress(*args, **kwargs)

    def _apply_tool_progress(
        self,
        event_type: str,
        tool_name: str | None = None,
        preview: str | None = None,
        args: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        if not tool_name:
            return
        token = kwargs.get("tool_call_id") or kwargs.get("call_id") or kwargs.get("id")
        if event_type == "tool.started":
            self.state.start_tool(tool_name=tool_name, preview=preview or "", args=args, token=token)
        elif event_type == "tool.completed":
            ok = not bool(kwargs.get("error") or kwargs.get("failed"))
            if token:
                self.state.finish_tool(str(token), ok=ok)
            else:
                self.state.finish_oldest_running_tool(tool_name=tool_name, ok=ok)

    def _record_update_failure(self) -> None:
        self._update_failures += 1
        logger.warning("feishu_card_update_failed")
        if self._update_failures >= self.max_update_failures:
            self.card_updates_disabled = True
            logger.warning("feishu_card_updates_disabled")

    async def drain_pending_updates(self) -> None:
        task = self._drain_task
        if task is not None and not task.done():
            await task
        if not self._event_queue.empty():
            if self.card_updates_disabled:
                self._drain_events()
            else:
                await self._drain_and_flush()

    async def _send_fallback_text(self, text: str) -> bool:
        result = await self.adapter.send(self.chat_id, text, metadata=self.metadata, reply_to=self.reply_to)
        if getattr(result, "success", False):
            self.message_id = getattr(result, "message_id", None)
            self.fallback_sent = True
            self.final_response_sent = True
            self.final_content_delivered = True
            logger.info("feishu_card_fallback_sent")
            return True
        return False

    async def _ensure_card(self) -> tuple[bool, bool]:
        """Ensure a card exists. Returns (success, created_now).

        ``created_now`` is True when the card was just created (first flush).
        Callers should skip the immediate update in that case because the
        create call already rendered the current state.
        """
        if self.update_handle:
            return True, False
        card = self.renderer.render(self.state)
        result = await self.adapter.create_card_stream_message(
            self.chat_id, card, metadata=self.metadata, reply_to=self.reply_to
        )
        if getattr(result, "success", False):
            self.message_id = getattr(result, "message_id", None)
            self.update_handle = (
                getattr(result, "update_handle", None)
                or getattr(result, "card_id", None)
                or getattr(result, "message_id", None)
            )
            return bool(self.update_handle), True
        logger.warning("feishu_card_create_failed")
        return False, False

    async def flush(self) -> bool:
        if self.card_updates_disabled:
            return False
        async with self._flush_lock:
            seq = self._sequence = self._sequence + 1
            ok, created_now = await self._ensure_card()
            if not ok:
                return False
            # Card was just created with the current state — no update needed.
            if created_now:
                return True
            result = await self.adapter.update_card_stream_message(
                self.update_handle, self.renderer.render(self.state), sequence=seq
            )
            return bool(getattr(result, "success", False))

    def _flush_text_filter_pending(self) -> None:
        """Flush any partial-tag text held by the think-block filter into state."""
        flushed = self.text_filter.feed("")
        if flushed:
            cleaned = clean_stream_display_text(flushed)
            if cleaned:
                self.state.append_text(cleaned)

    async def finalize(self, final_text: str) -> bool:
        self._closed = True
        await self.drain_pending_updates()
        self._flush_text_filter_pending()
        # Append final_text as a new block rather than replacing accumulated
        # streaming text, so any tool commentary / intermediate content
        # the user saw during streaming remains visible in the card.
        if final_text:
            cleaned = clean_stream_display_text(final_text)
            if cleaned:
                self.state.append_text(cleaned)
        self.state.finalize()
        if await self.flush():
            self.final_response_sent = True
            self.final_content_delivered = True
            return True
        # Fallback: send accumulated text as plain message.  Use final_text
        # first, then accumulated blocks, then a hard-coded sentinel.
        fallback = final_text or "".join(self.state.text_blocks) or "(empty response)"
        return await self._send_fallback_text(fallback)

    async def update_final_after_transform(self, final_text: str) -> bool:
        self._closed = True
        await self.drain_pending_updates()
        self._flush_text_filter_pending()
        if final_text:
            self.state.text_blocks = [final_text]
        self.state.finalize()
        if self.update_handle and await self.flush():
            self.final_response_sent = True
            self.final_content_delivered = True
            return True
        fallback = final_text or "".join(self.state.text_blocks) or "(empty response)"
        return await self._send_fallback_text(fallback)

    async def finish_failed(self, error_text: str) -> bool:
        self._closed = True
        await self.drain_pending_updates()
        self._flush_text_filter_pending()
        if error_text:
            self.state.append_commentary(clean_stream_display_text(error_text))
        self.state.terminal = "error"
        if self.update_handle and await self.flush():
            self.final_response_sent = True
            self.final_content_delivered = True
            return True
        fallback = error_text or "".join(self.state.text_blocks) or "Agent failed"
        return await self._send_fallback_text(fallback)
