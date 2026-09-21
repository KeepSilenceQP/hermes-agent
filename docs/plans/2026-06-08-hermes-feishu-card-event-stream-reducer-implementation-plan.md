# Hermes Feishu Card Event-Stream Reducer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rework Hermes Feishu card streaming so 小A matches 小P's lark-channel bridge event-stream card behavior.

**Architecture:** Keep the existing Hermes Python gateway callbacks, but normalize them into Feishu-local `RunEvent` objects. A reducer owns `FeishuCardRunState`, the renderer projects that state into one CardKit-managed card, and a producer-style sink reduces events plus flushes card updates while the Hermes turn is still active. Feishu transport must match 小P: `cardkit.card.create -> send card_id reference -> card.update/card_element.content`.

**Tech Stack:** Python, Hermes gateway, Feishu CardKit 2.0 card JSON, pytest, `uv run --extra dev --extra feishu python -m pytest`.

---

## Command Convention

Run every implementation and verification command from:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
```

Use:

```bash
uv run --extra dev --extra feishu python -m pytest ...
```

Do not use bare `python` or bare `pytest` for verification.

Commit steps in this plan are checkpoint suggestions for subagent-driven
execution. For inline execution, ask Qin Peng before creating commits; if no
commit is requested, still complete the tests and report the dirty files.

## File Structure

- Modify `gateway/platforms/feishu_card_stream.py`
  - Owns `FeishuRunEvent`, `FeishuCardRunState`, pending model-text segment state, answer/tool block state, reducer methods, renderer, producer event queue, and flush sequencing.
- Modify `gateway/platforms/feishu.py`
  - Creates CardKit-managed cards with `cardkit.card.create`, sends visible Feishu messages by `card_id` reference, and updates by stored `card_id`.
- Modify `agent/tool_executor.py`
  - Passes stable tool ids into `agent.tool_progress_callback` for both single-tool and concurrent-tool paths.
- Modify `gateway/run.py`
  - Keeps card-streaming callbacks attached, routes tool progress into the sink, respects `already_streamed`, and finalizes through the sink after all prior events are drained.
- Modify `run_agent.py` only if the existing callback metadata is insufficient
  - Preserves or extends `already_streamed` / model-response phase metadata for Feishu card streaming without changing non-Feishu display behavior.
- Modify `gateway/config.py` or existing config migration/default path if needed
  - Ensures live Feishu card streaming validation can explicitly enable `display.streaming`, `streaming.enabled`, and `display.platforms.feishu.card_streaming`.
- Modify `tests/gateway/test_feishu_card_stream.py`
  - Reducer, renderer, sink, fake adapter, placeholder, dedupe, tool-key, pending model-text lane, producer wakeup, and flush-order tests.
- Modify `tests/gateway/test_feishu_card_stream_runner.py`
  - Gateway helper tests for tool progress routing and final-send suppression.
- Modify `tests/gateway/test_feishu_card_stream_runner.py`
  - Locks gateway helper behavior for forwarding tool progress kwargs.
- Modify or create `tests/gateway/test_feishu_card_transport.py`
  - Feishu managed-card transport tests.

## Task 1: Add Reducer-Focused Failing Tests

**Files:**
- Modify: `tests/gateway/test_feishu_card_stream.py`
- Test: `tests/gateway/test_feishu_card_stream.py`

- [ ] **Step 1: Add tests for placeholder lifecycle, answer authority, deterministic commentary dedupe, and tool keys**

Append these tests after the existing state tests near the top of `tests/gateway/test_feishu_card_stream.py`:

```python
def test_reducer_hides_placeholder_after_first_real_event():
    state = FeishuCardRunState()
    state.start_placeholder("正在处理...")

    assert FeishuCardRunRenderer().content(state, include_running_status=False) == "正在处理..."

    state.append_text("visible answer")

    content = FeishuCardRunRenderer().content(state, include_running_status=False)
    assert "正在处理..." not in content
    assert "visible answer" in content


def test_reducer_final_removes_placeholder_and_renders_answer_once():
    state = FeishuCardRunState()
    state.start_placeholder("正在处理...")
    state.append_text("draft answer")

    state.finalize("final answer")

    content = FeishuCardRunRenderer().content(state, include_running_status=False)
    assert "正在处理..." not in content
    assert "draft answer" not in content
    assert content.count("final answer") == 1


def test_reducer_suppresses_commentary_that_duplicates_final_answer():
    state = FeishuCardRunState()
    final = "结论：工具链应该使用事件流 reducer。"
    state.append_commentary(f"  {final}  ")

    state.finalize(final)

    content = FeishuCardRunRenderer().content(state, include_running_status=False)
    assert content.count(final) == 1


def test_reducer_updates_same_name_tools_by_tool_key():
    state = FeishuCardRunState()
    state.start_tool(tool_key="call-1", tool_name="terminal", preview="pwd")
    state.start_tool(tool_key="call-2", tool_name="terminal", preview="ls")

    state.finish_tool("call-2", ok=True)

    assert state.tools[0].status == "running"
    assert state.tools[1].status == "done"
    assert state.tools[0].tool_key == "call-1"
    assert state.tools[1].tool_key == "call-2"


def test_reducer_delta_tool_delta_final_has_one_answer_source():
    state = FeishuCardRunState()
    state.append_text("draft part one")
    state.start_tool(tool_key="call-1", tool_name="terminal", preview="pwd")
    state.append_text(" draft part two")

    state.finalize("final answer")

    content = FeishuCardRunRenderer().content(state, include_running_status=False)
    assert "draft part one" not in content
    assert "draft part two" not in content
    assert content.count("final answer") == 1
    assert "command_execution" in content
    assert "pwd" in content
```

- [ ] **Step 2: Run the reducer tests and verify they fail**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py::test_reducer_hides_placeholder_after_first_real_event \
  tests/gateway/test_feishu_card_stream.py::test_reducer_final_removes_placeholder_and_renders_answer_once \
  tests/gateway/test_feishu_card_stream.py::test_reducer_suppresses_commentary_that_duplicates_final_answer \
  tests/gateway/test_feishu_card_stream.py::test_reducer_updates_same_name_tools_by_tool_key \
  tests/gateway/test_feishu_card_stream.py::test_reducer_delta_tool_delta_final_has_one_answer_source -q
```

Expected:

- Fails because `start_placeholder` does not exist.
- Fails because `finalize()` currently takes no final text.
- Fails because `FeishuCardToolBlock` has `token`, not `tool_key`.

## Task 2: Implement `FeishuCardRunState` As The Reducer Contract

**Files:**
- Modify: `gateway/platforms/feishu_card_stream.py`
- Test: `tests/gateway/test_feishu_card_stream.py`

- [ ] **Step 1: Replace the split state with ordered run blocks**

In `gateway/platforms/feishu_card_stream.py`, update the block dataclasses and state methods to this shape:

```python
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
    terminal: str = "running"
    _next_block_index: int = 0
    _next_tool_index: int = 0

    @property
    def tools(self) -> list[FeishuCardToolBlock]:
        return [block.tool for block in self.blocks if block.tool is not None]

    def start_placeholder(self, text: str) -> None:
        self.placeholder = text or ""

    def _hide_placeholder(self) -> None:
        self.placeholder = ""

    def append_text(self, text: str) -> None:
        if not text:
            return
        self._hide_placeholder()
        answer = self._ensure_answer_block()
        answer.content += text

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

    def _next_block_id(self, prefix: str) -> str:
        block_id = f"{prefix}-{self._next_block_index}"
        self._next_block_index += 1
        return block_id

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
            answer = self._ensure_answer_block()
            answer.content = cleaned_final
            self._drop_duplicate_commentary(cleaned_final)
        self.terminal = "done"
```

- [ ] **Step 2: Add deterministic duplicate-commentary helpers**

Add these methods inside `FeishuCardRunState`:

```python
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
```

- [ ] **Step 3: Update renderer content to use the new state fields**

Update `FeishuCardRunRenderer.content()`:

```python
    def content(self, state: FeishuCardRunState, *, include_running_status: bool = True) -> str:
        content_parts: list[str] = []
        if state.placeholder and state.terminal == "running":
            content_parts.append(state.placeholder)
        for block in state.blocks:
            if block.kind == "process" and block.content.strip():
                content_parts.append(block.content)
            elif block.kind == "tool" and block.tool is not None:
                content_parts.append(self._tool_line(block.tool))
            elif block.kind == "answer" and block.content.strip():
                content_parts.append(block.content)
        if include_running_status and state.terminal == "running":
            has_running_tool = any(
                block.tool is not None and block.tool.status == "running"
                for block in state.blocks
            )
            content_parts.append("_calling tools_" if has_running_tool else "_outputting_")
        return "\n\n".join(content_parts)
```

Update `_visible_text_chars()`:

```python
    def _visible_text_chars(self) -> int:
        return (
            len(self.state.placeholder)
            + sum(len(block.content) for block in self.state.blocks)
            + sum(len(block.tool.output) for block in self.state.blocks if block.tool is not None)
        )
```

- [ ] **Step 4: Run reducer tests and update old expectations**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu_card_stream.py -q
```

Expected:

- New reducer tests pass.
- Existing tests that still expect `正在处理...\n\nfinal answer` fail.

Update old assertions:

```python
assert "正在处理..." in adapter.created[-1][1]["body"]["elements"][0]["content"]
assert "正在处理..." not in adapter.text_updated[-1][2]
assert adapter.text_updated[-1][2] == "final answer"
```

- [ ] **Step 5: Commit reducer contract**

Run:

```bash
git add gateway/platforms/feishu_card_stream.py tests/gateway/test_feishu_card_stream.py
git commit -m "refactor: model feishu card stream as run reducer"
```

Expected:

- Commit succeeds.

## Task 3: Normalize Tool Events With Stable `tool_key`

**Files:**
- Modify: `gateway/platforms/feishu_card_stream.py`
- Modify: `agent/tool_executor.py`
- Test: `tests/gateway/test_feishu_card_stream.py`

- [ ] **Step 1: Add sink tests for explicit `tool_call_id` and legacy fallback**

Append:

```python
def test_sink_tool_progress_matches_completion_by_tool_call_id():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        sink.on_tool_progress("tool.started", tool_name="terminal", preview="pwd", tool_call_id="call-1")
        sink.on_tool_progress("tool.started", tool_name="terminal", preview="ls", tool_call_id="call-2")
        sink.on_tool_progress("tool.completed", tool_name="terminal", tool_call_id="call-2")
        await sink.drain_pending_updates()

        assert sink.state.tools[0].status == "running"
        assert sink.state.tools[1].status == "done"

    asyncio.run(run())


def test_sink_tool_progress_legacy_completion_uses_oldest_running_tool():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        sink.on_tool_progress("tool.started", tool_name="terminal", preview="pwd")
        sink.on_tool_progress("tool.completed", tool_name="terminal")
        await sink.drain_pending_updates()

        assert sink.state.tools[0].status == "done"

    asyncio.run(run())
```

- [ ] **Step 2: Update `_apply_tool_progress()` to normalize `tool_key`**

In `FeishuCardRunSink._apply_tool_progress()`:

```python
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
```

- [ ] **Step 3: Pass ids from `agent/tool_executor.py` single-tool callbacks**

At the single-tool start callback, change:

```python
agent.tool_progress_callback("tool.started", function_name, preview, function_args)
```

to:

```python
agent.tool_progress_callback(
    "tool.started",
    function_name,
    preview,
    function_args,
    tool_call_id=getattr(tool_call, "id", "") or "",
)
```

At the single-tool completion callback, change:

```python
agent.tool_progress_callback(
    "tool.completed", function_name, None, None,
    duration=tool_duration, is_error=_is_error_result,
    result=function_result,
)
```

to:

```python
agent.tool_progress_callback(
    "tool.completed",
    function_name,
    None,
    None,
    duration=tool_duration,
    is_error=_is_error_result,
    result=function_result,
    tool_call_id=getattr(tool_call, "id", "") or "",
)
```

- [ ] **Step 4: Pass ids from concurrent tool callbacks**

At the concurrent start callback, change:

```python
agent.tool_progress_callback("tool.started", name, preview, args)
```

to:

```python
agent.tool_progress_callback(
    "tool.started",
    name,
    preview,
    args,
    tool_call_id=getattr(tc, "id", "") or "",
)
```

At the concurrent completion callback in the post-execution loop, change:

```python
agent.tool_progress_callback(
    "tool.completed", function_name, None, None,
    duration=tool_duration, is_error=is_error,
    result=function_result,
)
```

to:

```python
agent.tool_progress_callback(
    "tool.completed",
    function_name,
    None,
    None,
    duration=tool_duration,
    is_error=is_error,
    result=function_result,
    tool_call_id=getattr(tc, "id", "") or "",
)
```

Do not use `function_name` or `tool_name` as the id for this path. The post-execution loop still has `tc` from `parsed_calls`, and `getattr(tc, "id", "") or ""` is the same id used by the concurrent start callback.

- [ ] **Step 5: Emit completion for concurrent cancelled or missing-result tools**

In the post-execution loop, the `r is None` branch creates `function_result` for cancelled or missing-result tools. After that branch sets `tool_duration = 0.0`, add:

```python
            if agent.tool_progress_callback:
                try:
                    agent.tool_progress_callback(
                        "tool.completed",
                        name,
                        None,
                        None,
                        duration=tool_duration,
                        is_error=True,
                        result=function_result,
                        tool_call_id=getattr(tc, "id", "") or "",
                    )
                except Exception as cb_err:
                    logging.warning("Tool progress callback error: %s", cb_err, exc_info=True)
```

Keep this inside the `r is None` branch so normal completions still use the existing normal completion callback.

- [ ] **Step 6: Add a degraded fallback visibility test**

Append this test to `tests/gateway/test_feishu_card_stream.py`:

```python
def test_tool_progress_completion_without_key_is_logged_as_degraded(caplog):
    import logging

    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)
        sink.on_tool_progress("tool.started", tool_name="terminal", preview="pwd", tool_call_id="call-1")
        sink.on_tool_progress("tool.completed", tool_name="terminal")
        await sink.drain_pending_updates()

    with caplog.at_level(logging.WARNING):
        asyncio.run(run())

    assert "feishu_card_tool_progress_missing_key" in caplog.text
```

This test verifies fallback is visible as degraded behavior. The primary executor paths are covered by the exact `tool_executor.py` edits in Steps 3, 4, and 5.

- [ ] **Step 7: Run targeted tests**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py::test_sink_tool_progress_matches_completion_by_tool_call_id \
  tests/gateway/test_feishu_card_stream.py::test_sink_tool_progress_legacy_completion_uses_oldest_running_tool \
  tests/gateway/test_feishu_card_stream.py::test_tool_progress_completion_without_key_is_logged_as_degraded -q
```

Expected:

- PASS.

- [ ] **Step 8: Commit tool-key normalization**

Run:

```bash
git add gateway/platforms/feishu_card_stream.py agent/tool_executor.py tests/gateway/test_feishu_card_stream.py
git commit -m "fix: propagate feishu card tool progress ids"
```

Expected:

- Commit succeeds.

## Task 4: Serialize Event Reduction And Terminal Barrier Flushes

**Files:**
- Modify: `gateway/platforms/feishu_card_stream.py`
- Test: `tests/gateway/test_feishu_card_stream.py`

- [ ] **Step 1: Add failing tests for terminal barrier and stale update ordering**

Append:

```python
def test_sink_reduces_tool_event_queued_before_final():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=60)

        await sink.start("正在处理...")
        sink.on_tool_progress("tool.started", tool_name="terminal", preview="pwd", tool_call_id="call-1")

        delivered = await sink.finalize("final answer")

        assert delivered is True
        final_content = adapter.text_updated[-1][2]
        assert "command_execution" in final_content
        assert "pwd" in final_content
        assert "正在处理..." not in final_content
        assert final_content.count("final answer") == 1

    asyncio.run(run())


def test_sink_final_card_uses_full_card_update_to_disable_streaming():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        await sink.start("正在处理...")
        delivered = await sink.finalize("final answer")

        assert delivered is True
        assert adapter.updated
        final_card = adapter.updated[-1][1]
        assert final_card["config"]["streaming_mode"] is False
        assert "正在处理..." not in str(final_card)
        assert str(final_card).count("final answer") == 1

    asyncio.run(run())


def test_sink_terminal_update_uses_higher_sequence_than_prior_text_updates():
    class SequenceAdapter(_FakeFeishuCardAdapter):
        async def update_card_stream_text(self, update_handle, element_id, content, sequence=None):
            self.text_updated.append((update_handle, element_id, content, sequence))
            await asyncio.sleep(0.01)
            return SimpleNamespace(success=True)

        async def update_card_stream_message(self, update_handle, card, sequence=None):
            self.updated.append((update_handle, card, sequence))
            return SimpleNamespace(success=True)

    async def run():
        adapter = SequenceAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        sink.on_delta("draft")
        await sink.drain_pending_updates()
        sink.on_delta(" still draft")
        await sink.drain_pending_updates()

        delivered = await sink.finalize("final answer")

        assert delivered is True
        assert adapter.text_updated
        assert adapter.updated
        max_text_seq = max(item[3] for item in adapter.text_updated)
        final_seq = adapter.updated[-1][2]
        assert final_seq > max_text_seq
        final_card = adapter.updated[-1][1]
        assert final_card["config"]["streaming_mode"] is False
        assert str(final_card).count("final answer") == 1

    asyncio.run(run())
```

- [ ] **Step 2: Make finalization drain before closing**

In `finalize()`, `update_final_after_transform()`, and `finish_failed()`, do not set `_closed = True` before draining. Use this order:

```python
await self.drain_pending_updates()
self._flush_text_filter_pending()
self.state.finalize(final_text)
self._closed = True
```

For `finish_failed()`:

```python
await self.drain_pending_updates()
self._flush_text_filter_pending()
if error_text:
    self.state.append_commentary(clean_stream_display_text(error_text))
self.state.terminal = "error"
self._closed = True
```

- [ ] **Step 3: Make terminal flush update the full card, not only markdown text**

Update `flush()` so terminal updates call `update_card_stream_message` when available:

```python
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
```

Then:

```python
            if self.state.terminal != "running" and hasattr(self.adapter, "update_card_stream_message"):
                return await self._update_full_card(seq)
            return await self._update_stream_text(seq)
```

- [ ] **Step 4: Prevent stale text-only updates after terminal**

Keep `_flush_lock` as the single update lock and ensure `_drain_and_flush()` exits after terminal state:

```python
                if self.state.terminal != "running":
                    return
```

Do not add a second independent update path outside `_flush_lock`.

Feishu CardKit update APIs receive `sequence`; this plan relies on higher
terminal sequences winning over older in-flight text updates. Keep `_sequence`
monotonic and never reuse a sequence value. Do not schedule terminal full-card
updates outside the serialized `flush()` path.

- [ ] **Step 5: Run sink tests**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu_card_stream.py -q
```

Expected:

- PASS.

- [ ] **Step 6: Commit flush ordering**

Run:

```bash
git add gateway/platforms/feishu_card_stream.py tests/gateway/test_feishu_card_stream.py
git commit -m "fix: serialize feishu card terminal flushes"
```

Expected:

- Commit succeeds.

## Task 5: Update Gateway Routing Tests And Final-Send Contract

**Files:**
- Modify: `gateway/run.py`
- Modify: `tests/gateway/test_feishu_card_stream_runner.py`

- [ ] **Step 1: Add route test that preserves tool kwargs and flushes immediately**

In `tests/gateway/test_feishu_card_stream_runner.py`, update or add:

```python
def test_route_feishu_card_tool_progress_preserves_tool_key_kwargs():
    from gateway.run import _route_feishu_card_tool_progress

    class Sink:
        def __init__(self):
            self.calls = []
            self.flushes = 0

        def on_tool_progress(self, event_type, tool_name, preview, args, **kwargs):
            self.calls.append((event_type, tool_name, preview, args, kwargs))

        def flush_threadsafe(self):
            self.flushes += 1
            return True

    sink = Sink()

    assert _route_feishu_card_tool_progress(
        sink,
        "tool.started",
        "terminal",
        "pwd",
        {"command": "pwd"},
        tool_call_id="call-1",
    ) is True
    assert sink.calls == [
        ("tool.started", "terminal", "pwd", {"command": "pwd"}, {"tool_call_id": "call-1"})
    ]
    assert sink.flushes == 1
```

- [ ] **Step 2: Ensure `_route_feishu_card_tool_progress()` forwards all kwargs**

Verify the helper still calls:

```python
sink.on_tool_progress(event_type, tool_name, preview, args, **kwargs)
```

No code change is needed if this is already true. Keep the test to lock the contract.

- [ ] **Step 3: Add final-send suppression test for terminal full-card update**

In `tests/gateway/test_feishu_card_stream_runner.py`, add a helper-level test if one does not already exist:

```python
def test_card_sink_delivery_suppresses_final_send_after_terminal_card_update():
    from gateway.run import _card_sink_delivered_final

    sink = type(
        "Sink",
        (),
        {
            "final_response_sent": True,
            "final_content_delivered": True,
            "fallback_sent": False,
        },
    )()

    assert _card_sink_delivered_final(sink) is True
```

- [ ] **Step 4: Run gateway runner tests**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu_card_stream_runner.py -q
```

Expected:

- PASS.

- [ ] **Step 5: Commit gateway routing tests**

Run:

```bash
git add gateway/run.py tests/gateway/test_feishu_card_stream_runner.py
git commit -m "test: lock feishu card progress routing contract"
```

Expected:

- Commit succeeds.

## Task 6: Add End-To-End Fake Adapter Sequence Test

**Files:**
- Modify: `tests/gateway/test_feishu_card_stream.py`
- Test: `tests/gateway/test_feishu_card_stream.py`

- [ ] **Step 1: Add the fake adapter sequence test from the spec**

Append:

```python
def test_sink_sequence_placeholder_tool_final_matches_user_visible_contract():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        await sink.start("正在处理...")
        created_content = adapter.created[-1][1]["body"]["elements"][0]["content"]
        assert "正在处理..." in created_content

        sink.on_tool_progress("tool.started", tool_name="terminal", preview="pwd", tool_call_id="call-1")
        await sink.drain_pending_updates()
        tool_content = adapter.text_updated[-1][2]
        assert "正在处理..." not in tool_content
        assert "command_execution" in tool_content
        assert "pwd" in tool_content

        delivered = await sink.finalize("final answer")

        assert delivered is True
        final_card = adapter.updated[-1][1]
        assert final_card["config"]["streaming_mode"] is False
        final_rendered = str(final_card)
        assert "正在处理..." not in final_rendered
        assert final_rendered.count("final answer") == 1
        assert "command_execution" in final_rendered

    asyncio.run(run())
```

- [ ] **Step 2: Run this test**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py::test_sink_sequence_placeholder_tool_final_matches_user_visible_contract -q
```

Expected:

- PASS.

- [ ] **Step 3: Commit sequence test**

Run:

```bash
git add tests/gateway/test_feishu_card_stream.py
git commit -m "test: cover feishu card visible event sequence"
```

Expected:

- Commit succeeds.

## Task 7: Implement 小P-Aligned Managed CardKit Transport

**Files:**
- Modify: `gateway/platforms/feishu.py`
- Test: `tests/gateway/test_feishu_card_transport.py`

- [ ] **Step 1: Add failing tests for managed-card creation and updates**

Create `tests/gateway/test_feishu_card_transport.py` with a fake CardKit client that records API calls:

```python
import json
from types import SimpleNamespace

import pytest

from gateway.platforms.feishu import FeishuAdapter


class _FakeCardApi:
    def __init__(self):
        self.created = []
        self.updated = []

    async def acreate(self, request):
        self.created.append(request)
        return SimpleNamespace(
            success=lambda: True,
            code=0,
            data=SimpleNamespace(card_id="card_1"),
        )

    async def aupdate(self, request):
        self.updated.append(request)
        return SimpleNamespace(success=lambda: True, code=0)


class _FakeCardElementApi:
    def __init__(self):
        self.content_updates = []

    async def acontent(self, request):
        self.content_updates.append(request)
        return SimpleNamespace(success=lambda: True, code=0)


class _FakeCardKit:
    def __init__(self):
        self.v1 = SimpleNamespace(
            card=_FakeCardApi(),
            card_element=_FakeCardElementApi(),
        )


class _FakeClient:
    def __init__(self):
        self.cardkit = _FakeCardKit()


@pytest.mark.asyncio
async def test_card_stream_create_uses_cardkit_create_then_card_id_reference(monkeypatch):
    adapter = FeishuAdapter({})
    adapter._client = _FakeClient()
    sent = {}

    async def fake_send_with_retry(*, chat_id, msg_type, payload, reply_to=None, metadata=None):
        sent["chat_id"] = chat_id
        sent["msg_type"] = msg_type
        sent["payload"] = payload
        sent["reply_to"] = reply_to
        return SimpleNamespace(success=lambda: True, code=0, data=SimpleNamespace(message_id="om_1"))

    monkeypatch.setattr(adapter, "_feishu_send_with_retry", fake_send_with_retry)
    monkeypatch.setattr(adapter, "_response_succeeded", lambda response: True)
    monkeypatch.setattr(adapter, "_extract_response_field", lambda response, field: "om_1" if field == "message_id" else None)

    result = await adapter.create_card_stream_message("oc_1", {"schema": "2.0", "body": {"elements": []}})

    assert result.success is True
    assert result.message_id == "om_1"
    assert result.update_handle == "card_1"
    assert adapter._client.cardkit.v1.card.created
    assert sent["msg_type"] == "interactive"
    assert json.loads(sent["payload"]) == {"type": "card", "data": {"card_id": "card_1"}}


@pytest.mark.asyncio
async def test_card_stream_updates_use_card_id_not_message_id(monkeypatch):
    adapter = FeishuAdapter({})
    adapter._client = _FakeClient()

    ok = await adapter.update_card_stream_message("card_1", {"schema": "2.0"}, sequence=7)

    assert ok is True
    assert adapter._client.cardkit.v1.card.updated
```

- [ ] **Step 2: Run the transport tests and verify they fail**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu_card_transport.py -q
```

Expected:

- FAIL because `_create_card_stream_transport()` still sends raw card JSON and depends on `aid_convert`.

- [ ] **Step 3: Implement CardKit `card.create` as the primary create path**

In `gateway/platforms/feishu.py`, change `_create_card_stream_transport()` to this shape:

```python
    async def _create_card_stream_transport(
        self, chat_id: str, card: Dict[str, Any], metadata=None, reply_to=None
    ) -> SendResult:
        if not self._client:
            return SendResult(success=False, error="Not connected")
        try:
            card_id = await self._create_cardkit_card(card)
            if not card_id:
                return SendResult(success=False, error="cardkit.card.create returned no card_id")

            payload = json.dumps(
                {"type": "card", "data": {"card_id": card_id}},
                ensure_ascii=False,
            )
            response = await self._feishu_send_with_retry(
                chat_id=chat_id,
                msg_type="interactive",
                payload=payload,
                reply_to=reply_to,
                metadata=metadata,
            )
            if not self._response_succeeded(response):
                return self._response_error_result(response, default_message="card stream create failed")

            message_id = self._extract_response_field(response, "message_id")
            if not message_id:
                return SendResult(success=False, error="card stream create: no message_id in response")

            logger.info("[Feishu] card stream managed create: card_id=%s message_id=%s", card_id, message_id)
            return SendResult(
                success=True,
                message_id=message_id,
                card_id=card_id,
                raw_response=response,
            )
        except ImportError:
            logger.warning("[Feishu] cardkit SDK not available")
            return SendResult(success=False, error="cardkit SDK not available")
        except Exception as exc:
            logger.warning("[Feishu] card create transport error: %s", exc)
            return SendResult(success=False, error=str(exc))
```

- [ ] **Step 4: Add `_create_cardkit_card()` helper**

Add this helper next to `_create_card_stream_transport()`:

```python
    async def _create_cardkit_card(self, card: Dict[str, Any]) -> str | None:
        from lark_oapi.api.cardkit.v1 import (
            CreateCardRequest,
            CreateCardRequestBody,
        )
        from lark_oapi.api.cardkit.v1.model.card import Card as CardKitCard

        create_card = CardKitCard.builder()
        create_card.type("card_json")
        create_card.data(json.dumps(card, ensure_ascii=False))

        body = CreateCardRequestBody.builder()
        body.card(create_card.build())

        request = CreateCardRequest.builder()
        request.request_body(body.build())

        response = await self._client.cardkit.v1.card.acreate(request.build())
        ok = response.success() if hasattr(response, "success") else response.code == 0
        if not ok:
            code = getattr(response, "code", None)
            msg = getattr(response, "msg", None) or getattr(response, "message", None)
            logger.warning("[Feishu] card create response failed: code=%s msg=%s", code, msg)
            return None
        return getattr(response.data, "card_id", "") if response.data else ""
```

If the installed SDK exposes synchronous `create()` instead of `acreate()`, add a small compatibility branch:

```python
        card_api = self._client.cardkit.v1.card
        if hasattr(card_api, "acreate"):
            response = await card_api.acreate(request.build())
        else:
            response = card_api.create(request.build())
```

- [ ] **Step 5: Keep `aid_convert` only as a legacy helper**

Do not call `_convert_message_id_to_card_id()` in the primary create path. Leave it in the adapter only for future legacy migration code. Add a comment above it:

```python
    # Legacy fallback only. New card-stream messages are created as CardKit
    # managed cards and already have card_id from cardkit.card.create.
```

- [ ] **Step 6: Run transport tests**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu_card_transport.py -q
```

Expected:

- PASS.

- [ ] **Step 7: Commit managed-card transport**

Run:

```bash
git add gateway/platforms/feishu.py tests/gateway/test_feishu_card_transport.py
git commit -m "fix: create feishu stream cards as managed cardkit cards"
```

Expected:

- Commit succeeds.

## Task 8: Implement Producer-Style Real-Time Flush

**Files:**
- Modify: `gateway/platforms/feishu_card_stream.py`
- Modify: `tests/gateway/test_feishu_card_stream.py`

- [ ] **Step 1: Add a failing test for worker-thread process events updating before finalization**

Append this test to `tests/gateway/test_feishu_card_stream.py`:

```python
def test_sink_worker_thread_process_event_updates_before_finalize():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)
        await sink.start("正在处理...")

        ready = threading.Event()

        def emit_tool():
            sink.on_tool_progress(
                "tool.started",
                tool_name="terminal",
                preview="pwd",
                tool_call_id="call-1",
            )
            ready.set()

        worker = threading.Thread(target=emit_tool)
        worker.start()
        assert ready.wait(2)
        worker.join()

        for _ in range(20):
            if adapter.text_updated:
                break
            await asyncio.sleep(0.05)

        assert adapter.text_updated
        content = adapter.text_updated[-1][2]
        assert "command_execution" in content
        assert "pwd" in content
        assert "正在处理..." not in content

    asyncio.run(run())
```

- [ ] **Step 2: Run the producer test and verify it fails if updates wait for final**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py::test_sink_worker_thread_process_event_updates_before_finalize -q
```

Expected:

- FAIL when the process event only becomes visible during `finalize()`.

- [ ] **Step 3: Add producer lifecycle fields to `FeishuCardRunSink`**

In `FeishuCardRunSink.__init__()`, add:

```python
        self._producer_task: asyncio.Task[None] | None = None
        self._producer_wakeup: asyncio.Event | None = None
        self._producer_started = False
```

- [ ] **Step 4: Start the producer when the sink starts**

In `start()`, after the card is created, ensure a producer task exists:

```python
            if ok:
                self._ensure_producer()
            return ok
```

Add:

```python
    def _ensure_producer(self) -> None:
        if self._loop is None:
            return
        if self._producer_wakeup is None:
            self._producer_wakeup = asyncio.Event()
        if self._producer_task is None or self._producer_task.done():
            self._producer_task = asyncio.create_task(self._producer_loop())
            self._producer_started = True
```

- [ ] **Step 5: Replace delayed drain scheduling with producer wakeup**

Update `_schedule_drain_threadsafe()`:

```python
    def _schedule_drain_threadsafe(self) -> None:
        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._wake_producer)

    def _wake_producer(self) -> None:
        if self._closed:
            return
        self._ensure_producer()
        if self._producer_wakeup is not None:
            self._producer_wakeup.set()
```

- [ ] **Step 6: Add producer loop that reduces process events before finalization**

Add:

```python
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
```

Keep `drain_pending_updates()` as a barrier helper for finalization. It should cancel or bypass delayed work and synchronously drain remaining queued events.

- [ ] **Step 7: Run producer and existing sink tests**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu_card_stream.py -q
```

Expected:

- PASS.
- The new worker-thread test passes without calling `finalize()`.

- [ ] **Step 8: Commit producer flush behavior**

Run:

```bash
git add gateway/platforms/feishu_card_stream.py tests/gateway/test_feishu_card_stream.py
git commit -m "fix: flush feishu card process events during active runs"
```

Expected:

- Commit succeeds.

## Task 9: Set And Verify Streaming Configuration Baseline

**Files:**
- Modify: `gateway/config.py` or config migration/default code if defaults are owned there.
- Modify: `tests/gateway/test_feishu_card_stream_runner.py`
- Read/modify live only during rollout: `/Users/bytedance/Documents/Hermes/home/config.yaml`

- [ ] **Step 1: Add a config contract test for Feishu card streaming**

Add this test to `tests/gateway/test_feishu_card_stream_runner.py`:

```python
def test_feishu_card_streaming_config_requires_global_streaming_enabled():
    user_config = {
        "display": {
            "streaming": True,
            "platforms": {
                "feishu": {
                    "card_streaming": True,
                }
            },
        },
        "streaming": {
            "enabled": True,
            "transport": "auto",
        },
    }

    assert user_config["display"]["streaming"] is True
    assert user_config["streaming"]["enabled"] is True
    assert user_config["display"]["platforms"]["feishu"]["card_streaming"] is True
```

This test is intentionally a contract fixture. If Hermes has a config loader helper that materializes display/platform settings, replace the raw dict with that loader and assert the loaded fields.

- [ ] **Step 2: Add a no-duplicate final test when global streaming and card streaming are both enabled**

Add or update a gateway runner test to build a turn config with:

```python
user_config = {
    "display": {
        "streaming": True,
        "interim_assistant_messages": True,
        "platforms": {"feishu": {"card_streaming": True}},
    },
    "streaming": {"enabled": True, "transport": "auto"},
}
```

Assert the normal final send is suppressed when the card sink reports final delivery:

```python
from gateway.run import _card_sink_delivered_final

sink = type(
    "Sink",
    (),
    {
        "final_response_sent": True,
        "final_content_delivered": True,
        "fallback_sent": False,
    },
)()

assert _card_sink_delivered_final(sink) is True
```

- [ ] **Step 3: Run config/gateway tests**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu_card_stream_runner.py -q
```

Expected:

- PASS.

- [ ] **Step 4: Commit config contract tests or config defaults**

Run:

```bash
git add gateway/config.py tests/gateway/test_feishu_card_stream_runner.py
git commit -m "test: lock feishu card streaming config baseline"
```

Expected:

- Commit succeeds, or `gateway/config.py` is omitted from `git add` if no default code changed.

## Task 10: Classify Model Text Lanes And Terminal Answer Placement

**Files:**
- Modify: `gateway/platforms/feishu_card_stream.py`
- Modify: `gateway/run.py`
- Modify: `run_agent.py` only if `already_streamed` metadata cannot be preserved through existing callbacks
- Test: `tests/gateway/test_feishu_card_stream.py`
- Test: `tests/gateway/test_feishu_card_stream_runner.py`

- [ ] **Step 1: Add reducer tests for tool-call preamble classification and answer placement**

Append these tests near the reducer tests in `tests/gateway/test_feishu_card_stream.py`:

```python
def test_model_text_before_tool_commits_as_process_not_answer():
    state = FeishuCardRunState()

    state.append_model_text("我先查官方输出。")
    state.start_tool(tool_key="call-1", tool_name="web_search", preview="site:openai.com memory")

    content = FeishuCardRunRenderer().content(state, include_running_status=False)
    assert "我先查官方输出。" in content
    assert "web_search" in content
    assert any(block.kind == "process" and "我先查官方输出。" in block.content for block in state.blocks)
    assert not any(block.kind == "answer" for block in state.blocks)


def test_already_streamed_interim_segment_does_not_render_duplicate_commentary():
    state = FeishuCardRunState()

    state.append_model_text("我先查官方输出。")
    state.close_interim_segment("我先查官方输出。", already_streamed=True)

    content = FeishuCardRunRenderer().content(state, include_running_status=False)
    assert content.count("我先查官方输出。") == 1


def test_final_answer_renders_after_tools_even_when_model_text_arrived_first():
    state = FeishuCardRunState()

    state.append_model_text("我先查官方输出。")
    state.start_tool(tool_key="call-1", tool_name="web_search", preview="site:openai.com memory")
    state.finish_tool("call-1", ok=True)
    state.finalize("结论：OpenAI 和 Anthropic 都在强化 Agent Memory。")

    content = FeishuCardRunRenderer().content(state, include_running_status=False)
    assert content.count("结论：OpenAI 和 Anthropic 都在强化 Agent Memory。") == 1
    assert content.index("web_search") < content.index("结论：OpenAI 和 Anthropic 都在强化 Agent Memory。")
```

- [ ] **Step 2: Run the new reducer tests and verify they fail**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py::test_model_text_before_tool_commits_as_process_not_answer \
  tests/gateway/test_feishu_card_stream.py::test_already_streamed_interim_segment_does_not_render_duplicate_commentary \
  tests/gateway/test_feishu_card_stream.py::test_final_answer_renders_after_tools_even_when_model_text_arrived_first -q
```

Expected:

- FAIL because `append_model_text()` and `close_interim_segment()` do not exist.
- FAIL because `finalize()` currently keeps the existing answer block in its original position.

- [ ] **Step 3: Implement pending model-text segment state**

In `gateway/platforms/feishu_card_stream.py`, extend `FeishuCardRunState` with a pending segment and lane-aware methods:

```python
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

    def append_model_text(self, text: str) -> None:
        if not text:
            return
        self._hide_placeholder()
        self.pending_model_segment += text

    def _commit_pending_model_segment_as_process(self) -> None:
        text = clean_stream_display_text(self.pending_model_segment or "")
        self.pending_model_segment = ""
        if text:
            self.append_commentary(text)

    def _commit_pending_model_segment_as_answer(self) -> None:
        text = clean_stream_display_text(self.pending_model_segment or "")
        self.pending_model_segment = ""
        if text:
            self.append_answer_text(text)

    def append_answer_text(self, text: str) -> None:
        if not text:
            return
        self._hide_placeholder()
        answer = self._ensure_answer_block()
        answer.content += text

    def append_text(self, text: str) -> None:
        self.append_answer_text(text)

    def close_interim_segment(self, text: str = "", *, already_streamed: bool = False) -> None:
        if already_streamed:
            return
        cleaned = clean_stream_display_text(text or "")
        if cleaned:
            self.append_commentary(cleaned)
```

Update `start_tool()` so pending model text is classified as process before the tool block:

```python
    def start_tool(
        self,
        *,
        tool_name: str,
        preview: str = "",
        args: dict[str, Any] | None = None,
        tool_key: str | None = None,
        token: str | None = None,
    ) -> str:
        self._commit_pending_model_segment_as_process()
        key = str(tool_key or token or f"tool-{self._next_tool_index}")
        if tool_key is None and token is None:
            self._next_tool_index += 1
        self._hide_placeholder()
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
```

- [ ] **Step 4: Move final answer block to the terminal answer position**

In `FeishuCardRunState.finalize()`, classify any remaining pending model text as answer, replace final content, and move the answer block after process/tool blocks:

```python
    def _move_answer_block_to_tail(self) -> FeishuCardRunBlock | None:
        if self.answer_block_id is None:
            return None
        for index, block in enumerate(self.blocks):
            if block.block_id == self.answer_block_id:
                answer = self.blocks.pop(index)
                self.blocks.append(answer)
                return answer
        return None

    def finalize(self, final_text: str = "") -> None:
        self._hide_placeholder()
        cleaned_final = clean_stream_display_text(final_text or "")
        if cleaned_final:
            self.pending_model_segment = ""
            answer = self._ensure_answer_block()
            answer.content = cleaned_final
            answer.finalized = True
            self._drop_duplicate_commentary(cleaned_final)
            self._move_answer_block_to_tail()
        else:
            self._commit_pending_model_segment_as_answer()
            self._move_answer_block_to_tail()
        self.terminal = "done"
```

- [ ] **Step 5: Route sink delta and interim events through the lane-aware methods**

In `FeishuCardRunSink._drain_events()`, change delta/commentary handling:

```python
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
                already_streamed = bool(kwargs.get("already_streamed", False))
                cleaned = self.text_filter.feed(str(args[0]))
                if cleaned:
                    cleaned = clean_stream_display_text(cleaned)
                    self.state.close_interim_segment(cleaned, already_streamed=already_streamed)
```

Update `FeishuCardRunSink.on_commentary()` to accept the metadata:

```python
    def on_commentary(self, text: str, *, already_streamed: bool = False) -> None:
        self._enqueue("commentary", text, already_streamed=already_streamed)
```

- [ ] **Step 6: Preserve `already_streamed` in the Feishu gateway callback**

In `gateway/run.py`, change `_interim_assistant_cb()` for Feishu card streaming:

```python
                if _want_feishu_card_streaming and feishu_card_sink_holder[0] is not None:
                    feishu_card_sink_holder[0].on_commentary(text, already_streamed=already_streamed)
                    return
```

If tests show `already_streamed` is always false for streamed tool preambles, inspect `run_agent.py::_emit_interim_assistant_message()`. Preserve its existing logic; do not add Feishu-specific behavior there unless the callback argument is being dropped before gateway handling.

- [ ] **Step 7: Add gateway routing test for `already_streamed=True`**

Add this focused helper-level test to `tests/gateway/test_feishu_card_stream_runner.py` near existing card-stream callback tests:

```python
def test_feishu_card_interim_callback_preserves_already_streamed_metadata():
    observed = {}

    class FakeSink:
        def on_commentary(self, text, *, already_streamed=False):
            observed["text"] = text
            observed["already_streamed"] = already_streamed

    sink = FakeSink()
    sink.on_commentary("我先查官方输出。", already_streamed=True)

    assert observed == {
        "text": "我先查官方输出。",
        "already_streamed": True,
    }
```

This test locks the sink API contract. If there is already a gateway helper that constructs `_interim_assistant_cb`, prefer testing that helper directly with a `FakeSink`.

- [ ] **Step 8: Run lane and gateway tests**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py::test_model_text_before_tool_commits_as_process_not_answer \
  tests/gateway/test_feishu_card_stream.py::test_already_streamed_interim_segment_does_not_render_duplicate_commentary \
  tests/gateway/test_feishu_card_stream.py::test_final_answer_renders_after_tools_even_when_model_text_arrived_first \
  tests/gateway/test_feishu_card_stream_runner.py -q
```

Expected:

- PASS.
- No duplicate process text from `already_streamed=True`.
- Final answer appears after tool/process blocks.

- [ ] **Step 9: Commit model-text lane classification**

Run:

```bash
git add gateway/platforms/feishu_card_stream.py gateway/run.py tests/gateway/test_feishu_card_stream.py tests/gateway/test_feishu_card_stream_runner.py
git commit -m "fix: classify feishu card model text lanes"
```

Expected:

- Commit succeeds, or inline execution reports the same file set without committing when Qin Peng did not request commits.

## Task 11: Full Verification And Static Review

**Files:**
- Read: `gateway/platforms/feishu_card_stream.py`
- Read: `gateway/platforms/feishu.py`
- Read: `agent/tool_executor.py`
- Read: `gateway/run.py`
- Read: `gateway/config.py`
- Read: `tests/gateway/test_feishu_card_stream.py`
- Read: `tests/gateway/test_feishu_card_stream_runner.py`
- Read: `tests/gateway/test_feishu_card_transport.py`

- [ ] **Step 1: Run all relevant tests**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py \
  tests/gateway/test_feishu_card_stream_runner.py \
  tests/gateway/test_feishu_card_transport.py -q
```

Expected:

- PASS.

- [ ] **Step 2: Run broader gateway smoke tests**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_config_env_bridge_authority.py \
  tests/gateway/test_config_cwd_bridge.py -q
```

Expected:

- PASS.

- [ ] **Step 3: Scan for old conflicting expectations**

Run:

```bash
rg -n "正在处理\\.\\.\\.\\\\n\\\\nfinal answer|finish_oldest_running_tool\\(|final_answer|answer_draft|token:" \
  gateway/platforms/feishu_card_stream.py tests/gateway/test_feishu_card_stream.py
```

Expected:

- No `正在处理...\n\nfinal answer`.
- No renderable `final_answer` or `answer_draft` fields.
- `finish_oldest_running_tool()` may remain only as legacy fallback.

Run:

```bash
rg -n "append_model_text|tool_preamble|already_streamed|_move_answer_block_to_tail|model_text\\.delta" \
  gateway/platforms/feishu_card_stream.py gateway/run.py tests/gateway/test_feishu_card_stream.py tests/gateway/test_feishu_card_stream_runner.py
```

Expected:

- `append_model_text` appears in reducer implementation and tests.
- `already_streamed` is preserved from gateway callback into sink handling.
- `_move_answer_block_to_tail` or equivalent terminal answer positioning logic appears in reducer implementation.

Run:

```bash
rg -n "aid_convert|message_id as card|card_id or message_id|payload = json.dumps\\(card" \
  gateway/platforms/feishu.py gateway/platforms/feishu_card_stream.py
```

Expected:

- `aid_convert` appears only in a legacy fallback helper or comment.
- No primary card stream create path sends raw card JSON as the visible message payload.
- No primary update handle falls back from `card_id` to `message_id`.

- [ ] **Step 4: Inspect git diff**

Run:

```bash
git diff --stat
git diff -- gateway/platforms/feishu_card_stream.py gateway/platforms/feishu.py agent/tool_executor.py gateway/run.py gateway/config.py tests/gateway/test_feishu_card_stream.py tests/gateway/test_feishu_card_stream_runner.py tests/gateway/test_feishu_card_transport.py
```

Expected:

- Only planned files are changed.
- No credentials or local profile paths are added.

- [ ] **Step 5: Commit final verification note if needed**

If the implementation leaves no additional file changes, skip this step. If test-only cleanup was needed, commit it:

```bash
git add tests/gateway/test_feishu_card_stream.py tests/gateway/test_feishu_card_stream_runner.py tests/gateway/test_feishu_card_transport.py
git commit -m "test: verify feishu card reducer behavior"
```

Expected:

- Commit succeeds or there is nothing to commit.

## Task 12: Live Rollout Validation

**Files:**
- Read: `/Users/bytedance/Documents/Hermes/home/logs/gateway.log`
- Read: `/Users/bytedance/Documents/Hermes/home/logs/agent.log`
- Read/modify: `/Users/bytedance/Documents/Hermes/home/config.yaml`
- Do not edit live config without a backup.

- [ ] **Step 1: Back up live Hermes home config before rollout**

Run:

```bash
ts=$(date +%Y%m%d-%H%M%S)
backup_dir=/Users/bytedance/Documents/运维/backup/$ts/hermes-feishu-card-reducer
mkdir -p "$backup_dir"
cp /Users/bytedance/Documents/Hermes/home/config.yaml "$backup_dir/config.yaml"
```

Expected:

- Backup directory exists and contains `config.yaml`.
- If backup fails, stop with `cannot proceed: backup failed/insufficient`.

- [ ] **Step 2: Set live streaming config baseline**

Run:

```bash
HERMES_HOME=/Users/bytedance/Documents/Hermes/home uv run hermes config set display.streaming true
HERMES_HOME=/Users/bytedance/Documents/Hermes/home uv run hermes config set streaming.enabled true
HERMES_HOME=/Users/bytedance/Documents/Hermes/home uv run hermes config set streaming.transport auto
HERMES_HOME=/Users/bytedance/Documents/Hermes/home uv run hermes config set display.platforms.feishu.card_streaming true
```

Verify without printing secrets:

```bash
rg -n "display:|streaming:|platforms:|feishu:|card_streaming|enabled:|transport:" \
  /Users/bytedance/Documents/Hermes/home/config.yaml
```

Expected:

- `display.streaming: true`.
- `streaming.enabled: true`.
- `streaming.transport: auto`.
- `display.platforms.feishu.card_streaming: true`.

- [ ] **Step 3: Restart Hermes gateway after the patch and config are deployed**

Run from the Hermes checkout that contains the implemented patch:

```bash
HERMES_HOME=/Users/bytedance/Documents/Hermes/home uv run hermes gateway restart
HERMES_HOME=/Users/bytedance/Documents/Hermes/home uv run hermes gateway status
```

Expected:

- Gateway status reports running.

- [ ] **Step 4: Trigger a Feishu group task that must call a tool**

Send 小A a request that requires a terminal/tool call. Record the Feishu message time.

Expected visible behavior:

- Card appears early with `正在处理...`.
- First real event removes `正在处理...`.
- Tool-running block appears while the tool is active.
- Tool block becomes done or error before the final answer.
- Final answer appears once.
- Tool-call preamble text does not appear twice.
- Final answer appears after process/tool history, not at the top of the card.
- Final card has no running/streaming status.

- [ ] **Step 5: Verify logs prove managed transport and event-before-final order**

Run:

```bash
tail -n 300 /Users/bytedance/Documents/Hermes/home/logs/agent.log | \
  rg "card stream managed create|feishu_card_transport_created|feishu_card_event_reduced|feishu_card_update_success|response ready|Turn ended|tool_use|tool_result"
```

Expected:

- `tool_use` or `tool_result` reduction appears before `Turn ended`.
- `feishu_card_update_success` appears before `Turn ended` for the tool event.
- Managed transport evidence appears before updates: `card_id` and `message_id`
  are both logged.
- At least one meaningful running update is materially earlier than the terminal
  update, not part of the finalization burst.

- [ ] **Step 6: Record final evidence**

Create or update a short note in the work area:

```bash
mkdir -p /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/verification
cat > /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/verification/2026-06-08-event-stream-reducer-live.md <<'EOF'
# Feishu Card Event-Stream Reducer Live Validation

- Date:
- Hermes checkout:
- Gateway restart result:
- Streaming config:
- Feishu test prompt:
- Visual result:
- Relevant gateway log lines:
- Remaining issues:
EOF
```

Expected:

- Evidence note exists.
- Do not paste secrets, tokens, or raw private transcripts.
