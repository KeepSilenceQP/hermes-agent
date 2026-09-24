from __future__ import annotations

import asyncio
import json
import logging
import queue
import re
import threading
from dataclasses import dataclass, field
from typing import Any

from agent.redact import redact_sensitive_text
from gateway.stream_text_cleaner import StreamDisplayTextFilter, clean_stream_display_text

_NATIVE_AT_TAG_RE = re.compile(
    r"<at\s+user_id=([\"'])(ou_[A-Za-z0-9_-]+)\1\s*>(.*?)</at>",
    re.DOTALL,
)


@dataclass(frozen=True)
class NativeAtParagraph:
    text: str
    target_open_ids: tuple[str, ...]


def _strip_fenced_code_blocks(text: str) -> str:
    output_lines: list[str] = []
    in_fence = False
    for line in text.splitlines():
        if line.strip().startswith("```"):
            in_fence = not in_fence
            continue
        if not in_fence:
            output_lines.append(line)
    return "\n".join(output_lines)


def _split_paragraphs(text: str) -> list[str]:
    paragraphs: list[str] = []
    current: list[str] = []
    for line in text.splitlines():
        if line.strip():
            current.append(line)
            continue
        if current:
            paragraphs.append("\n".join(current).strip())
            current = []
    if current:
        paragraphs.append("\n".join(current).strip())
    return paragraphs


def _normalize_native_at_paragraph(text: str) -> str:
    return "\n".join(line.rstrip() for line in text.strip().splitlines())


def render_native_at_as_human_text(text: str) -> str:
    def _rewrite_one_paragraph(paragraph: str) -> str:
        matches = list(_NATIVE_AT_TAG_RE.finditer(paragraph))
        if not matches:
            return paragraph
        names = [m.group(3).strip() for m in matches]
        cleaned = _NATIVE_AT_TAG_RE.sub("", paragraph).strip()
        if not cleaned:
            return f"对 {'、'.join(names)} 说"
        return f"对 {'、'.join(names)} 说，{cleaned}"

    in_fence = False
    outside_lines: list[str] = []
    output_blocks: list[str] = []

    for line in (text or "").splitlines():
        stripped = line.strip()
        if stripped.startswith("```"):
            # Flush accumulated outside-fence lines as a paragraph block.
            if outside_lines:
                joined = "\n".join(outside_lines)
                if _NATIVE_AT_TAG_RE.search(joined):
                    output_blocks.append(_rewrite_one_paragraph(joined))
                else:
                    output_blocks.append(joined)
                outside_lines = []
            in_fence = not in_fence
            output_blocks.append(line)
            continue

        if in_fence:
            output_blocks.append(line)
        elif not line.strip():
            # Blank line — flush current paragraph.
            if outside_lines:
                joined = "\n".join(outside_lines)
                if _NATIVE_AT_TAG_RE.search(joined):
                    output_blocks.append(_rewrite_one_paragraph(joined))
                else:
                    output_blocks.append(joined)
                outside_lines = []
            output_blocks.append(line)
        else:
            outside_lines.append(line)

    # Flush any trailing outside-fence lines.
    if outside_lines:
        joined = "\n".join(outside_lines)
        if _NATIVE_AT_TAG_RE.search(joined):
            output_blocks.append(_rewrite_one_paragraph(joined))
        else:
            output_blocks.append(joined)

    return "\n".join(output_blocks)


def extract_native_at_paragraphs(text: str, *, max_messages: int = 5) -> list[NativeAtParagraph]:
    if max_messages <= 0:
        return []

    cleaned = _strip_fenced_code_blocks(text or "")
    selected: list[NativeAtParagraph] = []
    seen: set[str] = set()

    for paragraph in _split_paragraphs(cleaned):
        matches = list(_NATIVE_AT_TAG_RE.finditer(paragraph))
        if not matches:
            continue
        normalized = _normalize_native_at_paragraph(paragraph)
        if normalized in seen:
            continue
        seen.add(normalized)
        selected.append(
            NativeAtParagraph(
                text=paragraph,
                target_open_ids=tuple(match.group(2) for match in matches),
            )
        )
        if len(selected) >= max_messages:
            remaining = sum(
                1
                for p in _split_paragraphs(cleaned)
                if _NATIVE_AT_TAG_RE.search(p) and _normalize_native_at_paragraph(p) not in seen
            )
            if remaining > 0:
                logger.warning(
                    "feishu_native_bot_at_cap_reached max=%d skipped=%d",
                    max_messages,
                    remaining,
                )
            break

    return selected


class NativeBotAtForwarder:
    def __init__(
        self,
        *,
        adapter: Any,
        chat_id: str,
        metadata: dict[str, Any] | None = None,
        reply_to: str | None = None,
        enabled: bool = False,
        max_messages: int = 5,
    ):
        self.adapter = adapter
        self.chat_id = chat_id
        self.metadata = metadata
        self.reply_to = reply_to
        self.enabled = enabled
        self.max_messages = max(0, int(max_messages))

    async def forward_from_final_text(self, text: str) -> int:
        if not self.enabled or not text or not hasattr(self.adapter, "send_raw_text"):
            return 0

        paragraphs = extract_native_at_paragraphs(text, max_messages=self.max_messages)
        sent = 0
        for paragraph in paragraphs:
            try:
                result = await self.adapter.send_raw_text(
                    self.chat_id,
                    paragraph.text,
                    metadata=self.metadata,
                    reply_to=self.reply_to,
                )
            except Exception as exc:
                logger.warning(
                    "feishu_native_bot_at_forward_failed chat_id=%s targets=%s error=%s",
                    self.chat_id,
                    ",".join(paragraph.target_open_ids),
                    exc,
                )
                continue
            if getattr(result, "success", False):
                sent += 1
                logger.info(
                    "feishu_native_bot_at_forward_sent chat_id=%s targets=%s",
                    self.chat_id,
                    ",".join(paragraph.target_open_ids),
                )
            else:
                logger.warning(
                    "feishu_native_bot_at_forward_failed chat_id=%s targets=%s error=%s",
                    self.chat_id,
                    ",".join(paragraph.target_open_ids),
                    getattr(result, "error", None) or "unknown error",
                )
        return sent


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
    visible: bool = True
    event_order: int = 0


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
    _next_tool_event_order: int = 0

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

    @staticmethod
    def _merge_overlapping_segments(earlier: str, later: str) -> str | None:
        min_overlap = max(4, min(len(earlier), len(later)) // 4)
        for overlap in range(min(len(earlier), len(later)), min_overlap - 1, -1):
            if earlier.endswith(later[:overlap]):
                return f"{earlier}{later[overlap:]}"
        return None

    def close_interim_segment(
        self, text: str = "", *, already_streamed: bool = False
    ) -> None:
        if already_streamed:
            return
        cleaned = clean_stream_display_text(text or "")
        pending = clean_stream_display_text(self.pending_model_segment or "")
        merged = (
            self._merge_overlapping_segments(pending, cleaned)
            if pending and cleaned
            else None
        )
        if merged:
            self.pending_model_segment = ""
            self.append_commentary(merged)
            return
        if (
            cleaned
            and pending
            and (
                cleaned in pending
                or pending in cleaned
                or self._is_duplicate_text(cleaned, pending)
            )
        ):
            self.pending_model_segment = ""
            self.append_commentary(cleaned if len(cleaned) >= len(pending) else pending)
            return
        if pending:
            self._commit_pending_model_segment_as_process()
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

    def raw_answer_text(self) -> str:
        if self.answer_block_id is None:
            return ""
        for block in self.blocks:
            if block.block_id == self.answer_block_id and block.kind == "answer":
                return block.content
        return ""

    def start_tool(
        self,
        *,
        tool_name: str,
        preview: str = "",
        args: dict[str, Any] | None = None,
        tool_key: str | None = None,
        token: str | None = None,
        visible: bool = True,
    ) -> str:
        key = str(tool_key or token or f"tool-{self._next_tool_index}")
        if tool_key is None and token is None:
            self._next_tool_index += 1
        if visible:
            self._hide_placeholder()
        self._commit_pending_model_segment_as_process()
        tool = FeishuCardToolBlock(
            tool_key=key,
            tool_name=tool_name,
            preview=preview or "",
            args=args or {},
            visible=visible,
            event_order=self._advance_tool_event_order(),
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
                tool.event_order = self._advance_tool_event_order()
                if output:
                    tool.output = output
                return
        synthetic = FeishuCardToolBlock(
            tool_key=tool_key,
            tool_name="unknown",
            status="done" if ok else "error",
            output=output or "",
            event_order=self._advance_tool_event_order(),
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
                tool.event_order = self._advance_tool_event_order()
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
        self.mark_running_tools_incomplete()
        self.terminal = "done"

    def mark_running_tools_incomplete(self) -> None:
        for tool in self.tools:
            if tool.status == "running":
                tool.status = "incomplete"
                tool.output = "Tool status unavailable"
                tool.event_order = self._advance_tool_event_order()

    def _advance_tool_event_order(self) -> int:
        self._next_tool_event_order += 1
        return self._next_tool_event_order

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

    def __init__(
        self,
        *,
        tool_display: str = "compact",
        tool_preview_length: int = 40,
    ) -> None:
        self.tool_display = tool_display
        self.tool_preview_length = max(4, int(tool_preview_length))

    @staticmethod
    def _tool_display_name(tool_name: str) -> str:
        if tool_name in {"terminal", "exec_command"}:
            return "command_execution"
        return tool_name

    @staticmethod
    def _compact_tool_name(tool_name: str) -> str:
        names = {
            "terminal": "Command",
            "exec_command": "Command",
            "execute_code": "Code",
            "web_search": "Web search",
            "web_extract": "Read web",
            "browser_exec": "Browser",
            "read_file": "Read file",
            "search_files": "Search files",
            "write_file": "Write file",
            "patch": "Edit file",
            "delegate_task": "Sub-agent",
        }
        if tool_name in names:
            return names[tool_name]
        short_name = tool_name.rsplit("__", 1)[-1]
        return short_name.replace("_", " ").strip().title() or "Tool"

    @staticmethod
    def _compact_safe_text(text: str) -> str:
        safe = redact_sensitive_text(
            " ".join(str(text or "").split()),
            force=True,
            redact_url_credentials=True,
        )
        return safe.replace("<", "‹").replace(">", "›").replace("`", "'")

    @staticmethod
    def _compact_error_message(text: str) -> str:
        def _find_error(value: Any) -> str | None:
            if isinstance(value, dict):
                for key in ("error", "message", "detail"):
                    candidate = value.get(key)
                    if isinstance(candidate, str) and candidate.strip():
                        return candidate.strip()
                for candidate in value.values():
                    found = _find_error(candidate)
                    if found:
                        return found
            elif isinstance(value, list):
                for candidate in value:
                    found = _find_error(candidate)
                    if found:
                        return found
            return None

        stripped = text.strip()
        if not stripped:
            return "Tool failed"
        try:
            message = _find_error(json.loads(stripped))
        except RecursionError:
            message = "Tool failed"
        except (TypeError, ValueError):
            message = None
        if not message:
            message = next(
                (
                    line.strip()
                    for line in stripped.splitlines()
                    if line.strip() not in {"{", "}"}
                ),
                "Tool failed",
            )
        message = redact_sensitive_text(
            " ".join(message.split()),
            force=True,
            redact_url_credentials=True,
        )
        message = message.replace("<", "‹").replace(">", "›").replace("`", "'")
        return f"{message[:157].rstrip()}..." if len(message) > 160 else message

    def _compact_tool_labels(self, tools: list[FeishuCardToolBlock]) -> list[str]:
        counts: dict[str, int] = {}
        for tool in tools:
            name = self._compact_tool_name(tool.tool_name)
            counts[name] = counts.get(name, 0) + 1
        labels = [
            f"{name} ×{count}" if count > 1 else name for name, count in counts.items()
        ]
        if len(labels) > 4:
            hidden_count = len(labels) - 4
            labels = [*labels[:4], f"+{hidden_count} more"]
        return labels

    def _compact_tool_summary(
        self,
        tools: list[FeishuCardToolBlock],
        *,
        aggregate_tools: list[FeishuCardToolBlock] | None = None,
    ) -> str:
        aggregates = aggregate_tools if aggregate_tools is not None else tools
        running = [tool for tool in aggregates if tool.status == "running"]
        completed = [tool for tool in aggregates if tool.status == "done"]
        failed = [tool for tool in aggregates if tool.status == "error"]
        incomplete = [tool for tool in aggregates if tool.status == "incomplete"]
        active_count = len(running)
        latest = max(tools, key=lambda tool: tool.event_order)

        if latest.status == "running":
            name = self._compact_tool_name(latest.tool_name)
            preview = latest.preview or str(
                latest.args.get("command") or latest.args.get("cmd") or ""
            )
            preview = self._compact_safe_text(preview)
            if len(preview) > self.tool_preview_length:
                prefix_length = self.tool_preview_length - 3
                preview = f"{preview[:prefix_length].rstrip()}..."
            suffix = f" · `{self._inline_code(preview)}`" if preview else ""
            if active_count > 1:
                suffix += f" · +{active_count - 1} active"
            if completed:
                suffix += f" · ✓ {len(completed)} done"
            if failed:
                suffix += f" · ⚠ {len(failed)} failed"
            if incomplete:
                suffix += f" · ⚠ {len(incomplete)} unavailable"
            return f"> ⏳ **{name}**{suffix}"

        if latest.status == "incomplete":
            name = self._compact_tool_name(latest.tool_name)
            preview = self._compact_safe_text(latest.preview)
            suffix = f" · `{self._inline_code(preview)}`" if preview else ""
            if completed:
                suffix += f" · ✓ {len(completed)} completed"
            if failed:
                suffix += f" · ⚠ {len(failed)} failed"
            if len(incomplete) > 1:
                suffix += f" · +{len(incomplete) - 1} unavailable"
            if active_count:
                suffix += f" · +{active_count} active"
            return f"> ⚠ **{name}** · `Status unavailable`{suffix}"

        if latest.status == "done":
            labels = self._compact_tool_labels(completed)
            noun = "tool" if len(completed) == 1 else "tools"
            if failed or incomplete:
                status_suffix = ""
                if failed:
                    failed_when = (
                        "failed earlier"
                        if all(tool.event_order < latest.event_order for tool in failed)
                        else "failed"
                    )
                    status_suffix += f" · ⚠ {len(failed)} {failed_when}"
                if incomplete:
                    status_suffix += f" · ⚠ {len(incomplete)} unavailable"
                summary = (
                    f"> ✓ **{len(completed)} {noun} completed** · "
                    f"{' · '.join(labels)}{status_suffix}"
                )
            else:
                summary = f"> ✓ **{len(completed)} {noun}** · {' · '.join(labels)}"
            if active_count:
                summary += f" · +{active_count} active"
            return summary

        name = self._compact_tool_name(latest.tool_name)
        message = self._compact_error_message(latest.output)
        status_counts: list[str] = []
        if len(failed) > 1:
            status_counts.append(f"{len(failed)} failed")
        if completed:
            status_counts.append(f"✓ {len(completed)} completed")
        if active_count:
            status_counts.append(f"+{active_count} active")
        if incomplete:
            status_counts.append(f"⚠ {len(incomplete)} unavailable")
        suffix = f" · {' · '.join(status_counts)}" if status_counts else ""
        return f"> ⚠ **{name}** · `{message}`{suffix}"

    @staticmethod
    def _inline_code(text: str) -> str:
        return text.replace("`", "'")

    @staticmethod
    def _safe_detailed_text(text: str) -> str:
        safe = redact_sensitive_text(
            str(text or ""),
            force=True,
            redact_url_credentials=True,
        )
        return safe.replace("<", "‹").replace(">", "›")

    @classmethod
    def _tool_output(cls, text: str) -> str:
        text = cls._safe_detailed_text(text).strip()
        if not text:
            return ""
        if len(text) > 1200:
            text = f"{text[:1200]}..."
        quoted_lines = "\n".join(f"> {line}" if line else ">" for line in text.splitlines())
        return f"\n> **Output**\n{quoted_lines}"

    def _tool_line(self, tool: FeishuCardToolBlock) -> str:
        icon = {
            "done": "✅",
            "error": "❌",
            "incomplete": "⚠",
        }.get(tool.status, "⏳")
        name = self._tool_display_name(tool.tool_name)
        preview = tool.preview
        if not preview:
            preview = str(tool.args.get("command") or tool.args.get("cmd") or "")
        preview = self._safe_detailed_text(preview)
        suffix = f" — `{self._inline_code(preview)}`" if preview else ""
        output = self._tool_output(tool.output) if tool.status == "error" else ""
        return f"> {icon} **{name}**{suffix}{output}"

    def content(self, state: FeishuCardRunState, *, include_running_status: bool = True) -> str:
        content_parts: list[str] = []
        all_tools = [block.tool for block in state.blocks if block.tool is not None]
        tools = [tool for tool in all_tools if tool.visible]
        aggregate_tools = [tool for tool in all_tools if tool.tool_name != "clarify"]
        compact_tools_emitted = False
        if state.placeholder and state.terminal == "running":
            content_parts.append(state.placeholder)
        pending_emitted = False
        for block in state.blocks:
            if block.kind == "process" and block.content.strip():
                content_parts.append(block.content)
            elif block.kind == "tool" and block.tool is not None:
                if not block.tool.visible:
                    continue
                if self.tool_display == "detailed":
                    content_parts.append(self._tool_line(block.tool))
                elif self.tool_display == "compact" and not compact_tools_emitted:
                    content_parts.append(
                        self._compact_tool_summary(
                            tools,
                            aggregate_tools=aggregate_tools,
                        )
                    )
                    compact_tools_emitted = True
            elif block.kind == "answer" and block.content.strip():
                rendered_block_content = render_native_at_as_human_text(block.content)
                content_parts.append(rendered_block_content)
            elif block.kind == "footer" and block.content.strip():
                if state.pending_model_segment.strip() and not pending_emitted:
                    content_parts.append(state.pending_model_segment)
                    pending_emitted = True
                content_parts.append(block.content)
        if state.pending_model_segment.strip() and not pending_emitted:
            content_parts.append(state.pending_model_segment)
        if include_running_status and state.terminal == "running":
            has_running_tool = any(tool.status == "running" for tool in all_tools)
            if self.tool_display == "off" and has_running_tool:
                if not state.placeholder:
                    content_parts.append("_working_")
            elif self.tool_display != "compact" or not has_running_tool:
                content_parts.append(
                    "_calling tools_" if has_running_tool else "_outputting_"
                )
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
        tool_display: str = "compact",
        tool_progress_mode: str = "all",
        tool_preview_length: int = 40,
        native_bot_at_forward: bool = False,
        native_bot_at_forward_max_messages: int = 5,
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
        self.renderer = FeishuCardRunRenderer(
            tool_display=tool_display,
            tool_preview_length=tool_preview_length,
        )
        self.tool_progress_mode = tool_progress_mode
        self._last_visible_tool_name: str | None = None
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
        self.native_at_forwarder = NativeBotAtForwarder(
            adapter=self.adapter,
            chat_id=self.chat_id,
            metadata=self.metadata,
            reply_to=self.reply_to,
            enabled=native_bot_at_forward,
            max_messages=native_bot_at_forward_max_messages,
        )

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
        if not self._event_queue.empty():
            self._drain_events()
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
            visible = (
                self.tool_progress_mode not in {"off", "log"} and tool_name != "clarify"
            )
            if (
                self.tool_progress_mode == "new"
                and tool_name == self._last_visible_tool_name
            ):
                visible = False
            if visible:
                self._last_visible_tool_name = tool_name
            key = self.state.start_tool(
                tool_name=tool_name,
                preview=preview or "",
                args=args,
                tool_key=str(tool_key) if tool_key else None,
                visible=visible,
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

    async def _forward_native_bot_at_from_raw_answer(self) -> None:
        if self.final_response_sent:
            return
        raw_answer = self.state.raw_answer_text()
        if not raw_answer:
            return
        await self.native_at_forwarder.forward_from_final_text(raw_answer)

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
            await self._forward_native_bot_at_from_raw_answer()
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
            await self._forward_native_bot_at_from_raw_answer()
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
        self.state.mark_running_tools_incomplete()
        self.state.terminal = "error"
        if self.update_handle and await self.flush():
            self._close()
            self.final_response_sent = True
            self.final_content_delivered = True
            return True
        self._close()
        fallback = error_text or self._build_fallback_text() or "Agent failed"
        return await self._send_fallback_text(fallback)
