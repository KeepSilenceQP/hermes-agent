from __future__ import annotations

import asyncio
import json
import logging
import queue
import threading
from dataclasses import dataclass, field
from typing import Any

from gateway.stream_text_cleaner import StreamDisplayTextFilter, clean_stream_display_text


@dataclass
class FeishuCardAnswerBlock:
    block_id: str
    content: str = ""
    finalized: bool = False


@dataclass
class FeishuCardToolBlock:
    tool_key: str
    tool_name: str
    preview: str = ""
    args: dict[str, Any] = field(default_factory=dict)
    output: str = ""
    status: str = "running"


@dataclass
class FeishuCardRunBlock:
    kind: str
    block_id: str
    content: str = ""
    tool: FeishuCardToolBlock | None = None


@dataclass
class FeishuCardRunState:
    placeholder: str = ""
    blocks: list[FeishuCardRunBlock] = field(default_factory=list)
    tool_index: dict[str, int] = field(default_factory=dict)
    answer_block_id: str | None = None
    pending_model_segment: str = ""
    terminal: str = "running"
    _next_block_index: int = 0
    _next_tool_index: int = 0

    @property
    def tools(self) -> list[FeishuCardToolBlock]:
        return [block.tool for block in self.blocks if block.tool is not None]

    def start_placeholder(self, text: str) -> None:
        self.placeholder = text or ""

    def has_real_event_content(self) -> bool:
        return bool(self.pending_model_segment) or any(
            block.content or block.tool is not None for block in self.blocks
        )

    def _hide_placeholder(self) -> None:
        self.placeholder = ""

    def append_text(self, text: str) -> None:
        self.append_answer_text(text)

    def append_answer_text(self, text: str) -> None:
        if not text:
            return
        self._hide_placeholder()
        answer = self._ensure_answer_block()
        answer.content += text

    def append_model_text(self, text: str) -> None:
        if not text:
            return
        self._hide_placeholder()
        self.pending_model_segment += text

    def _commit_pending_model_segment_as_process(self) -> None:
        cleaned = clean_stream_display_text(self.pending_model_segment or "")
        self.pending_model_segment = ""
        if cleaned:
            self.append_commentary(cleaned)

    def _commit_pending_model_segment_as_answer(self) -> None:
        cleaned = clean_stream_display_text(self.pending_model_segment or "")
        self.pending_model_segment = ""
        if cleaned:
            self.append_answer_text(cleaned)

    def close_interim_segment(self, text: str = "", *, already_streamed: bool = False) -> None:
        if already_streamed:
            return
        cleaned = clean_stream_display_text(text or "")
        if cleaned:
            self.append_commentary(cleaned)

    def append_commentary(self, text: str) -> None:
        if not text:
            return
        self._hide_placeholder()
        self.blocks.append(
            FeishuCardRunBlock(
                kind="process",
                block_id=self._next_block_id("process"),
                content=text,
            )
        )

    def set_footer(self, text: str) -> None:
        if self.terminal != "running":
            return
        if not text:
            return
        for block in self.blocks:
            if block.kind == "footer":
                block.content = text
                return
        self.blocks.append(
            FeishuCardRunBlock(
                kind="footer",
                block_id=self._next_block_id("footer"),
                content=text,
            )
        )

    def _next_block_id(self, prefix: str) -> str:
        block_id = f"{prefix}-{self._next_block_index}"
        self._next_block_index += 1
        return block_id

    def _rebuild_tool_index(self) -> None:
        self.tool_index = {
            block.tool.tool_key: index
            for index, block in enumerate(self.blocks)
            if block.tool is not None
        }

    def _ensure_answer_block(self) -> FeishuCardRunBlock:
        if self.answer_block_id is not None:
            for block in self.blocks:
                if block.block_id == self.answer_block_id:
                    return block
        block = FeishuCardRunBlock(
            kind="answer",
            block_id=self._next_block_id("answer"),
            content="",
        )
        self.blocks.append(block)
        self.answer_block_id = block.block_id
        return block

    def start_tool(
        self,
        *,
        tool_name: str,
        preview: str = "",
        args: dict[str, Any] | None = None,
        tool_key: str | None = None,
        token: str | None = None,
    ) -> str:
        key = str(tool_key or token or f"tool-{self._next_tool_index}")
        if tool_key is None and token is None:
            self._next_tool_index += 1
        self._hide_placeholder()
        self._commit_pending_model_segment_as_process()
        tool = FeishuCardToolBlock(
            tool_key=key,
            tool_name=tool_name,
            preview=preview or "",
            args=args or {},
        )
        self.blocks.append(
            FeishuCardRunBlock(
                kind="tool",
                block_id=self._next_block_id("tool"),
                tool=tool,
            )
        )
        self.tool_index[key] = len(self.blocks) - 1
        return key

    def finish_tool(self, tool_key: str, *, ok: bool = True, output: str | None = None) -> None:
        index = self.tool_index.get(tool_key)
        if index is not None and 0 <= index < len(self.blocks):
            tool = self.blocks[index].tool
            if tool is not None:
                tool.status = "done" if ok else "error"
                if output:
                    tool.output = output
                return
        synthetic = FeishuCardToolBlock(
            tool_key=tool_key,
            tool_name="unknown",
            status="done" if ok else "error",
            output=output or "",
        )
        self.blocks.append(
            FeishuCardRunBlock(
                kind="tool",
                block_id=self._next_block_id("tool"),
                tool=synthetic,
            )
        )
        self.tool_index[tool_key] = len(self.blocks) - 1

    def finish_oldest_running_tool(
        self,
        *,
        tool_name: str,
        ok: bool = True,
        output: str | None = None,
    ) -> str | None:
        for block in self.blocks:
            tool = block.tool
            if tool and tool.tool_name == tool_name and tool.status == "running":
                tool.status = "done" if ok else "error"
                if output:
                    tool.output = output
                return tool.tool_key
        return None

    def finalize(self, final_text: str = "") -> None:
        self._hide_placeholder()
        cleaned_final = clean_stream_display_text(final_text or "")
        if cleaned_final:
            self.pending_model_segment = ""
            answer = self._ensure_answer_block()
            answer.content = cleaned_final
            self._drop_duplicate_commentary(cleaned_final)
            self._move_answer_block_to_terminal_position()
        else:
            self._commit_pending_model_segment_as_answer()
            self._move_answer_block_to_terminal_position()
        self.terminal = "done"

    def _move_answer_block_to_terminal_position(self) -> None:
        if self.answer_block_id is None:
            return
        answer: FeishuCardRunBlock | None = None
        remaining: list[FeishuCardRunBlock] = []
        for block in self.blocks:
            if block.block_id == self.answer_block_id:
                answer = block
            else:
                remaining.append(block)
        if answer is None:
            return
        insert_at = next(
            (index for index, block in enumerate(remaining) if block.kind == "footer"),
            len(remaining),
        )
        remaining.insert(insert_at, answer)
        self.blocks = remaining
        self._rebuild_tool_index()

    @staticmethod
    def _normalize_for_dedupe(text: str) -> str:
        normalized = " ".join(str(text or "").split())
        for suffix in ("_outputting_", "_calling tools_"):
            if normalized.endswith(suffix):
                normalized = normalized[: -len(suffix)].strip()
        return normalized

    @classmethod
    def _is_duplicate_text(cls, left: str, right: str) -> bool:
        a = cls._normalize_for_dedupe(left)
        b = cls._normalize_for_dedupe(right)
        if not a or not b:
            return False
        if a == b:
            return True
        shorter, longer = (a, b) if len(a) <= len(b) else (b, a)
        return shorter in longer and (len(shorter) / max(len(longer), 1)) >= 0.8

    def _drop_duplicate_commentary(self, final_text: str) -> None:
        self.blocks = [
            block
            for block in self.blocks
            if block.kind != "process" or not self._is_duplicate_text(block.content, final_text)
        ]
        self._rebuild_tool_index()


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

    @staticmethod
    def _tool_output(text: str) -> str:
        text = text.strip()
        if not text:
            return ""
        if len(text) > 1200:
            text = f"{text[:1200]}..."
        quoted_lines = "\n".join(f"> {line}" if line else ">" for line in text.splitlines())
        return f"\n> **Output**\n{quoted_lines}"

    def _tool_line(self, tool: FeishuCardToolBlock) -> str:
        icon = "✅" if tool.status == "done" else ("❌" if tool.status == "error" else "⏳")
        name = self._tool_display_name(tool.tool_name)
        preview = tool.preview
        if not preview:
            preview = str(tool.args.get("command") or tool.args.get("cmd") or "")
        suffix = f" — `{self._inline_code(preview)}`" if preview else ""
        output = self._tool_output(tool.output) if tool.status == "error" else ""
        return f"> {icon} **{name}**{suffix}{output}"

    def content(self, state: FeishuCardRunState, *, include_running_status: bool = True) -> str:
        content_parts: list[str] = []
        if state.placeholder and state.terminal == "running":
            content_parts.append(state.placeholder)
        pending_emitted = False
        for block in state.blocks:
            if block.kind == "process" and block.content.strip():
                content_parts.append(block.content)
            elif block.kind == "tool" and block.tool is not None:
                content_parts.append(self._tool_line(block.tool))
            elif block.kind == "answer" and block.content.strip():
                content_parts.append(block.content)
            elif block.kind == "footer" and block.content.strip():
                if state.pending_model_segment.strip() and not pending_emitted:
                    content_parts.append(state.pending_model_segment)
                    pending_emitted = True
                content_parts.append(block.content)
        if state.pending_model_segment.strip() and not pending_emitted:
            content_parts.append(state.pending_model_segment)
        if include_running_status and state.terminal == "running":
            has_running_tool = any(
                block.tool is not None and block.tool.status == "running"
                for block in state.blocks
            )
            content_parts.append("_calling tools_" if has_running_tool else "_outputting_")
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
        self._drain_lock: asyncio.Lock | None = None
        self._producer_task: asyncio.Task[None] | None = None
        self._producer_wakeup: asyncio.Event | None = None
        self._producer_started = False
        self._schedule_lock = threading.Lock()
        self._flush_lock = asyncio.Lock()
        self._sequence = 0
        self._update_failures = 0

    def _visible_text_chars(self) -> int:
        return (
            len(self.state.placeholder)
            + len(self.state.pending_model_segment)
            + sum(len(block.content) for block in self.state.blocks)
            + sum(len(block.tool.output) for block in self.state.blocks if block.tool is not None)
        )

    async def start(self, initial_text: str = "正在处理...") -> bool:
        if self._loop is None:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                pass
        if initial_text and not self.state.has_real_event_content() and self._event_queue.empty():
            self.state.start_placeholder(initial_text)
        if self.update_handle:
            return await self.flush()
        async with self._flush_lock:
            self._sequence += 1
            ok, _ = await self._ensure_card(include_running_status=False)
            if ok:
                self._ensure_producer()
            return ok

    def on_delta(self, text: str) -> None:
        self._enqueue("delta", text)

    def on_commentary(self, text: str, *, already_streamed: bool = False) -> None:
        self._enqueue("commentary", text, already_streamed=already_streamed)

    def on_footer(self, text: str) -> None:
        self._enqueue("footer", text)

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
            self._loop.call_soon_threadsafe(self._wake_producer)

    def _ensure_producer(self) -> None:
        if self._loop is None:
            return
        if self._producer_wakeup is None:
            self._producer_wakeup = asyncio.Event()
        if self._producer_task is None or self._producer_task.done():
            self._producer_task = asyncio.create_task(self._producer_loop())
            self._producer_started = True

    def _wake_producer(self) -> None:
        if self._closed:
            return
        self._ensure_producer()
        if self._producer_wakeup is not None:
            self._producer_wakeup.set()

    def _close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._producer_wakeup is not None:
            try:
                self._producer_wakeup.set()
            except RuntimeError:
                pass

    def _ensure_drain_task(self) -> None:
        if self._closed:
            return
        if self._drain_task is None or self._drain_task.done():
            self._drain_task = asyncio.create_task(self._drain_and_flush())

    async def _producer_loop(self) -> None:
        if self._producer_wakeup is None:
            self._producer_wakeup = asyncio.Event()
        while not self._closed:
            await self._producer_wakeup.wait()
            self._producer_wakeup.clear()
            if self.card_updates_disabled:
                self._drain_events()
                continue
            await self._drain_and_flush(delay=False)

    def flush_threadsafe(self, *, timeout_sec: float = 2.0) -> bool:
        if self._closed or self.card_updates_disabled or self._loop is None:
            return False
        try:
            if asyncio.get_running_loop() is self._loop:
                self._wake_producer()
                return True
        except RuntimeError:
            pass
        try:
            self._loop.call_soon_threadsafe(self._wake_producer)
            return True
        except Exception as exc:
            logger.warning("feishu_card_flush_threadsafe_schedule_failed: %s", exc)
            return False

    async def _drain_and_flush(self, *, delay: bool = True) -> None:
        if self._drain_lock is None:
            self._drain_lock = asyncio.Lock()
        while not self._closed:
            if delay and self.update_interval_sec > 0:
                await asyncio.sleep(self.update_interval_sec)
            async with self._drain_lock:
                changed = self._drain_events()
                if self.state.terminal != "running":
                    return
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
                    self.state.append_model_text(cleaned)
            elif kind == "commentary":
                if args[0] is None:
                    continue
                cleaned = self.text_filter.feed(str(args[0]))
                if cleaned:
                    cleaned = clean_stream_display_text(cleaned)
                    self.state.close_interim_segment(
                        cleaned,
                        already_streamed=bool(kwargs.get("already_streamed", False)),
                    )
            elif kind == "footer":
                if args[0] is None:
                    continue
                cleaned = clean_stream_display_text(str(args[0]))
                if cleaned:
                    self.state.set_footer(cleaned)
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
        tool_key = (
            kwargs.get("tool_key")
            or kwargs.get("tool_call_id")
            or kwargs.get("call_id")
            or kwargs.get("id")
        )
        if event_type == "tool.started":
            key = self.state.start_tool(
                tool_name=tool_name,
                preview=preview or "",
                args=args,
                tool_key=str(tool_key) if tool_key else None,
            )
            logger.info(
                "feishu_card_event_reduced event=tool_use tool=%s tool_key=%s preview_chars=%s",
                tool_name,
                key,
                len(preview or ""),
            )
        elif event_type == "tool.completed":
            ok = not bool(kwargs.get("error") or kwargs.get("failed") or kwargs.get("is_error"))
            output = self._stringify_tool_result(
                kwargs["result"] if "result" in kwargs else kwargs.get("output")
            )
            if tool_key:
                self.state.finish_tool(str(tool_key), ok=ok, output=output)
            else:
                logger.warning(
                    "feishu_card_tool_progress_missing_key event=tool_result tool=%s",
                    tool_name,
                )
                self.state.finish_oldest_running_tool(tool_name=tool_name, ok=ok, output=output)
            logger.info(
                "feishu_card_event_reduced event=tool_result tool=%s tool_key=%s ok=%s",
                tool_name,
                tool_key or "",
                ok,
            )

    @staticmethod
    def _stringify_tool_result(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        try:
            return json.dumps(value, ensure_ascii=False, default=str)
        except Exception:
            return str(value)

    def _record_update_failure(self) -> None:
        self._update_failures += 1
        logger.warning("feishu_card_update_failed")
        if self._update_failures >= self.max_update_failures:
            self.card_updates_disabled = True
            logger.warning("feishu_card_updates_disabled")

    async def drain_pending_updates(self) -> None:
        # Cancel any still-pending background drain so we can drain
        # immediately without waiting for the delay.
        task = self._drain_task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
            self._drain_task = None
        if not self._event_queue.empty():
            if self.card_updates_disabled:
                self._drain_events()
            else:
                await self._drain_and_flush(delay=False)

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

    async def _update_full_card(self, seq: int) -> bool:
        result = await self.adapter.update_card_stream_message(
            self.update_handle,
            self.renderer.render(self.state),
            sequence=seq,
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
            if self.state.terminal != "running" and hasattr(self.adapter, "update_card_stream_message"):
                return await self._update_full_card(seq)
            return await self._update_stream_text(seq)

    def _flush_text_filter_pending(self) -> None:
        """Flush any partial-tag text held by the think-block filter into state."""
        flushed = self.text_filter.feed("")
        if flushed:
            cleaned = clean_stream_display_text(flushed)
            if cleaned:
                self.state.append_text(cleaned)

    def _build_fallback_text(self) -> str:
        for block in self.state.blocks:
            if block.kind == "answer" and block.content.strip():
                return block.content
        return "(empty response)"

    async def finalize(self, final_text: str) -> bool:
        await self.drain_pending_updates()
        self._flush_text_filter_pending()
        if not self.update_handle and final_text and not self.card_updates_disabled:
            async with self._flush_lock:
                self._sequence += 1
                ok, _ = await self._ensure_card(include_running_status=False)
            if not ok:
                fallback = final_text or self._build_fallback_text()
                return await self._send_fallback_text(fallback)
        self.state.finalize(final_text)
        if await self.flush():
            self._close()
            self.final_response_sent = True
            self.final_content_delivered = True
            return True
        self._close()
        fallback = final_text or self._build_fallback_text()
        return await self._send_fallback_text(fallback)

    async def update_final_after_transform(self, final_text: str) -> bool:
        await self.drain_pending_updates()
        self._flush_text_filter_pending()
        self.state.finalize(final_text)
        if self.update_handle and await self.flush():
            self._close()
            self.final_response_sent = True
            self.final_content_delivered = True
            return True
        self._close()
        fallback = final_text or self._build_fallback_text()
        return await self._send_fallback_text(fallback)

    async def finish_failed(self, error_text: str) -> bool:
        await self.drain_pending_updates()
        self._flush_text_filter_pending()
        if error_text:
            self.state.append_commentary(clean_stream_display_text(error_text))
        self.state.terminal = "error"
        if self.update_handle and await self.flush():
            self._close()
            self.final_response_sent = True
            self.final_content_delivered = True
            return True
        self._close()
        fallback = error_text or self._build_fallback_text() or "Agent failed"
        return await self._send_fallback_text(fallback)
