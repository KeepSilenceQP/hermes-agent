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
    process_blocks: list[str] = field(default_factory=list)
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
            self.process_blocks.append(text)

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
    STREAM_ELEMENT_ID = "stream_md"

    @staticmethod
    def _tool_display_name(tool_name: str) -> str:
        if tool_name in {"terminal", "exec_command"}:
            return "command_execution"
        return tool_name

    @staticmethod
    def _inline_code(text: str) -> str:
        return text.replace("`", "'")

    def _tool_line(self, tool: FeishuCardToolBlock) -> str:
        icon = "✅" if tool.status == "done" else ("❌" if tool.status == "error" else "⏳")
        name = self._tool_display_name(tool.tool_name)
        preview = tool.preview
        if not preview:
            preview = str(tool.args.get("command") or tool.args.get("cmd") or "")
        suffix = f" — `{self._inline_code(preview)}`" if preview else ""
        return f"> {icon} **{name}**{suffix}"

    def content(self, state: FeishuCardRunState, *, include_running_status: bool = True) -> str:
        content_parts: list[str] = []
        for block in state.process_blocks:
            if block.strip():
                content_parts.append(block)
        for tool in state.tools:
            content_parts.append(self._tool_line(tool))
        for block in state.text_blocks:
            if block.strip():
                content_parts.append(block)
        if include_running_status and state.terminal == "running":
            content_parts.append("_calling tools_" if state.tools else "_outputting_")
        return "\n\n".join(content_parts)

    def render(self, state: FeishuCardRunState, *, include_running_status: bool = True) -> dict[str, Any]:
        content = self.content(state, include_running_status=include_running_status)
        return {
            "schema": "2.0",
            "config": {
                "enable_forward_interaction": False,
                "streaming_config": {
                    "print_frequency_ms": {"default": 70},
                    "print_step": {"default": 1},
                    "print_strategy": "fast",
                },
                "streaming_mode": state.terminal == "running",
                "summary": {"content": "running" if state.terminal == "running" else "done"},
            },
            "body": {
                "elements": [
                    {
                        "tag": "markdown",
                        "element_id": self.STREAM_ELEMENT_ID,
                        "content": content,
                    }
                ]
            },
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

    def _visible_text_chars(self) -> int:
        return sum(len(block) for block in self.state.process_blocks + self.state.text_blocks)

    async def start(self, initial_text: str = "正在处理...") -> bool:
        if initial_text:
            self.state.append_commentary(initial_text)
        if self.update_handle:
            return await self.flush()
        async with self._flush_lock:
            self._sequence += 1
            ok, _ = await self._ensure_card(include_running_status=False)
            return ok

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
        while not self._closed:
            if self.update_interval_sec > 0:
                await asyncio.sleep(self.update_interval_sec)
            changed = self._drain_events()
            if changed and not self.card_updates_disabled:
                ok = await self.flush()
                if not ok:
                    self._record_update_failure()
            if self._event_queue.empty():
                return

    def _drain_events(self) -> bool:
        changed = False
        while True:
            try:
                kind, args, kwargs = self._event_queue.get_nowait()
            except queue.Empty:
                return changed
            changed = True
            if kind == "delta":
                if args[0] is None:
                    continue
                cleaned = self.text_filter.feed(str(args[0]))
                if cleaned:
                    cleaned = clean_stream_display_text(cleaned)
                    self.state.append_text(cleaned)
            elif kind == "commentary":
                if args[0] is None:
                    continue
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
        if event_type in {"_thinking", "reasoning.available"}:
            text = preview or tool_name or ""
            if text:
                self.state.append_commentary(clean_stream_display_text(str(text)))
            return
        if not tool_name:
            return
        token = kwargs.get("tool_call_id") or kwargs.get("call_id") or kwargs.get("id")
        if event_type == "tool.started":
            self.state.start_tool(tool_name=tool_name, preview=preview or "", args=args, token=token)
            logger.info(
                "feishu_card_tool_event event=started tool=%s preview_chars=%s",
                tool_name,
                len(preview or ""),
            )
        elif event_type == "tool.completed":
            ok = not bool(kwargs.get("error") or kwargs.get("failed"))
            if token:
                self.state.finish_tool(str(token), ok=ok)
            else:
                self.state.finish_oldest_running_tool(tool_name=tool_name, ok=ok)
            logger.info(
                "feishu_card_tool_event event=completed tool=%s ok=%s",
                tool_name,
                ok,
            )

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

    async def _ensure_card(self, *, include_running_status: bool = True) -> tuple[bool, bool]:
        """Ensure a card exists. Returns (success, created_now).

        ``created_now`` is True when the card was just created (first flush).
        Callers should skip the immediate update in that case because the
        create call already rendered the current state.
        """
        if self.update_handle:
            return True, False
        card = self.renderer.render(self.state, include_running_status=include_running_status)
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
            logger.info(
                "feishu_card_create_success seq=%s text_chars=%s tools=%s terminal=%s",
                self._sequence,
                self._visible_text_chars(),
                len(self.state.tools),
                self.state.terminal,
            )
            return bool(self.update_handle), True
        logger.warning("feishu_card_create_failed")
        return False, False

    async def _update_stream_text(self, seq: int) -> bool:
        content = self.renderer.content(self.state)
        if hasattr(self.adapter, "update_card_stream_text"):
            result = await self.adapter.update_card_stream_text(
                self.update_handle,
                self.renderer.STREAM_ELEMENT_ID,
                content,
                sequence=seq,
            )
        else:
            result = await self.adapter.update_card_stream_message(
                self.update_handle, self.renderer.render(self.state), sequence=seq
            )
        ok = bool(getattr(result, "success", False))
        if not ok:
            logger.warning("feishu_card_update_failed: %s", getattr(result, "error", None) or "unknown error")
        else:
            logger.info(
                "feishu_card_update_success seq=%s text_chars=%s tools=%s terminal=%s",
                seq,
                self._visible_text_chars(),
                len(self.state.tools),
                self.state.terminal,
            )
        return ok

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
            return await self._update_stream_text(seq)

    def _flush_text_filter_pending(self) -> None:
        """Flush any partial-tag text held by the think-block filter into state."""
        flushed = self.text_filter.feed("")
        if flushed:
            cleaned = clean_stream_display_text(flushed)
            if cleaned:
                self.state.append_text(cleaned)

    async def finalize(self, final_text: str) -> bool:
        await self.drain_pending_updates()
        self._flush_text_filter_pending()
        if not self.update_handle and final_text and not self.card_updates_disabled:
            async with self._flush_lock:
                self._sequence += 1
                ok, _ = await self._ensure_card(include_running_status=False)
            if not ok:
                fallback = final_text or "".join(self.state.text_blocks) or "(empty response)"
                return await self._send_fallback_text(fallback)
        self._closed = True
        if final_text:
            cleaned = clean_stream_display_text(final_text)
            if cleaned:
                self.state.text_blocks = [cleaned]
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
