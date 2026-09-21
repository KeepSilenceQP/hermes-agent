from gateway.platforms.feishu_card_stream import FeishuCardRunRenderer, FeishuCardRunState


def test_renderer_streaming_card_contains_text_and_running_tool():
    state = FeishuCardRunState()
    state.append_text("I will inspect the repo.")
    state.start_tool(tool_name="terminal", preview="rg card_streaming", args={"command": "rg card_streaming"})

    card = FeishuCardRunRenderer().render(state)

    assert card["schema"] == "2.0"
    assert card["config"]["streaming_mode"] is True
    assert card["config"]["enable_forward_interaction"] is False
    assert card["config"]["streaming_config"]["print_strategy"] == "fast"
    assert card["body"]["elements"] == [
        {
            "tag": "markdown",
            "element_id": "stream_md",
            "content": (
                "I will inspect the repo.\n\n"
                "> ⏳ **command_execution** — `rg card_streaming`\n\n"
                "_calling tools_"
            ),
        }
    ]
    body = str(card["body"])
    assert "I will inspect the repo." in body
    assert "command_execution" in body
    assert "rg card_streaming" in body
    assert "⏳" in body


def test_renderer_final_card_disables_streaming():
    state = FeishuCardRunState()
    state.append_text("Done.")
    state.finalize()

    card = FeishuCardRunRenderer().render(state)

    assert card["config"]["streaming_mode"] is False
    assert card["body"]["elements"][0]["element_id"] == "stream_md"
    assert "Done." in str(card["body"])


def test_tool_output_is_not_rendered():
    state = FeishuCardRunState()
    token = state.start_tool(tool_name="terminal", preview="cat secret.txt", args={"command": "cat secret.txt"})
    state.finish_tool(token, ok=True, output="SECRET_OUTPUT_SHOULD_NOT_RENDER")

    card = FeishuCardRunRenderer().render(state)

    assert "SECRET_OUTPUT_SHOULD_NOT_RENDER" not in str(card)
    assert "command_execution" in str(card)
    assert "✅" in str(card)


def test_error_tool_output_keeps_every_line_inside_quote_block():
    state = FeishuCardRunState()
    token = state.start_tool(
        tool_name="web_extract",
        preview="https://www.anthropic.com/news/context-management",
    )
    state.finish_tool(
        token,
        ok=False,
        output='{\n"results": [\n{"error": "Payment Required"}\n]\n}',
    )

    content = FeishuCardRunRenderer().content(state, include_running_status=False)

    assert "❌ **web_extract**" in content
    assert "> **Output**" in content
    assert "```" not in content
    assert "> {" in content
    assert '> "results": [' in content
    assert '> {"error": "Payment Required"}' in content
    assert "\n\"results\"" not in content


def test_repeated_same_name_tools_get_distinct_tokens():
    state = FeishuCardRunState()
    first = state.start_tool(tool_name="terminal", preview="one")
    second = state.start_tool(tool_name="terminal", preview="two")

    state.finish_tool(first, ok=True)

    assert first != second
    assert state.tools[0].status == "done"
    assert state.tools[1].status == "running"


def test_finish_oldest_matching_running_tool_when_no_token():
    state = FeishuCardRunState()
    first = state.start_tool(tool_name="terminal", preview="one")
    second = state.start_tool(tool_name="terminal", preview="two")

    matched = state.finish_oldest_running_tool(tool_name="terminal", ok=False)

    assert matched == first
    assert state.tools[0].status == "error"
    assert state.tools[1].status == "running"


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


def test_model_text_before_tool_commits_as_process_not_answer():
    state = FeishuCardRunState()

    state.append_model_text("我先查官方输出。")
    state.start_tool(tool_key="call-1", tool_name="web_search", preview="site:openai.com memory")

    content = FeishuCardRunRenderer().content(state, include_running_status=False)
    assert "我先查官方输出。" in content
    assert "web_search" in content
    assert any(
        block.kind == "process" and "我先查官方输出。" in block.content
        for block in state.blocks
    )
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
    assert content.index("web_search") < content.index(
        "结论：OpenAI 和 Anthropic 都在强化 Agent Memory。"
    )


import asyncio
import threading
import time
from types import SimpleNamespace

from gateway.platforms.feishu_card_stream import FeishuCardRunSink


class _FakeFeishuCardAdapter:
    def __init__(self):
        self.created = []
        self.updated = []
        self.text_updated = []
        self.sent_text = []
        self.raw_text = []

    async def create_card_stream_message(self, chat_id, card, metadata=None, reply_to=None):
        self.created.append((chat_id, card, metadata, reply_to))
        return SimpleNamespace(success=True, message_id="om_card_1", card_id="card_1")

    async def update_card_stream_message(self, update_handle, card, sequence=None):
        self.updated.append((update_handle, card, sequence))
        return SimpleNamespace(success=True)

    async def update_card_stream_text(self, update_handle, element_id, content, sequence=None):
        self.text_updated.append((update_handle, element_id, content, sequence))
        return SimpleNamespace(success=True)

    async def send(self, chat_id, content, metadata=None, reply_to=None):
        self.sent_text.append((chat_id, content, metadata, reply_to))
        return SimpleNamespace(success=True, message_id="om_text_1")

    async def send_raw_text(self, chat_id, text, metadata=None, reply_to=None):
        self.raw_text.append((chat_id, text, metadata, reply_to))
        return SimpleNamespace(success=True, message_id=f"om_raw_{len(self.raw_text)}")


def test_sink_finalize_marks_final_delivery():
    adapter = _FakeFeishuCardAdapter()
    sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1")
    sink.on_delta("final answer")

    delivered = asyncio.run(sink.finalize("final answer"))

    assert delivered is True
    assert sink.message_id == "om_card_1"
    assert sink.final_response_sent is True
    assert sink.final_content_delivered is True


def test_sink_delta_schedules_update_before_finalize():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        # First delta creates the card (no update needed — already current).
        sink.on_delta("streaming")
        await sink.drain_pending_updates()
        assert adapter.created
        assert sink.final_response_sent is False

        # Second delta triggers an actual update (card already exists).
        sink.on_delta(" more")
        await sink.drain_pending_updates()
        assert adapter.text_updated

    asyncio.run(run())


def test_sink_continues_updates_for_deltas_queued_during_flush():
    class SlowUpdateAdapter(_FakeFeishuCardAdapter):
        def __init__(self):
            super().__init__()
            self.first_update_started = asyncio.Event()

        async def update_card_stream_text(self, update_handle, element_id, content, sequence=None):
            self.text_updated.append((update_handle, element_id, content, sequence))
            if len(self.text_updated) == 1:
                self.first_update_started.set()
                await asyncio.sleep(0.01)
            return SimpleNamespace(success=True)

    async def run():
        adapter = SlowUpdateAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        sink.on_delta("one")
        await sink.drain_pending_updates()
        assert adapter.created

        sink.on_delta(" two")
        await adapter.first_update_started.wait()
        sink.on_delta(" three")
        await sink.drain_pending_updates()

        assert len(adapter.text_updated) >= 2
        assert "one two three" in adapter.text_updated[-1][2]

    asyncio.run(run())


def test_sink_finalize_replaces_compact_streamed_block_with_final_text():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        compact = "流式卡片验证****当前时间： 2026-06-08 当前运行状态- 平台：Feishu 总结：完成。"
        final_text = (
            "流式卡片验证\n\n"
            "**当前时间：** 2026-06-08\n\n"
            "**当前运行状态**\n"
            "- 平台：Feishu\n\n"
            "**总结：** 完成。"
        )
        sink.on_delta(compact)
        await sink.drain_pending_updates()

        delivered = await sink.finalize(final_text)

        assert delivered is True
        assert adapter.updated
        final_card = adapter.updated[-1][1]
        rendered_content = final_card["body"]["elements"][0]["content"]
        assert rendered_content == final_text
        assert compact not in rendered_content

    asyncio.run(run())


def test_sink_finalize_without_prior_delta_creates_empty_streaming_card_then_text_update():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        delivered = await sink.finalize("final answer")

        assert delivered is True
        assert adapter.created
        created_card = adapter.created[-1][1]
        assert created_card["config"]["streaming_mode"] is True
        assert created_card["body"]["elements"][0]["content"] == ""
        # Terminal flush goes through full card update (update_card_stream_message)
        assert adapter.updated
        final_card = adapter.updated[-1][1]
        final_content = final_card["body"]["elements"][0]["content"]
        assert final_content == "final answer"
        assert final_card["config"]["streaming_mode"] is False

    asyncio.run(run())


def test_sink_start_creates_process_card_and_finalize_appends_answer():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        started = await sink.start("正在处理...")
        delivered = await sink.finalize("final answer")

        assert started is True
        assert delivered is True
        # start() uses start_placeholder, so the initial text appears in the created card
        assert "正在处理..." in adapter.created[-1][1]["body"]["elements"][0]["content"]
        # Terminal flush goes through full card update — placeholder is cleared by finalize
        assert adapter.updated
        final_card = adapter.updated[-1][1]
        final_content = final_card["body"]["elements"][0]["content"]
        assert "正在处理..." not in final_content
        assert final_content == "final answer"

    asyncio.run(run())


def test_sink_late_start_does_not_restore_placeholder_after_real_event():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        sink.on_delta("real answer")
        await sink.drain_pending_updates()

        started = await sink.start("正在处理...")

        assert started is True
        created_content = adapter.created[-1][1]["body"]["elements"][0]["content"]
        assert "real answer" in created_content
        assert "正在处理..." not in created_content

    asyncio.run(run())


def test_sink_start_reduces_queued_lifecycle_before_creating_card():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        sink.on_commentary("已收到请求，正在准备上下文。")
        sink.on_commentary("正在请求模型，等待首个事件。")

        started = await sink.start("正在处理...")

        assert started is True
        created_content = adapter.created[-1][1]["body"]["elements"][0]["content"]
        assert created_content.index("已收到请求，正在准备上下文。") < created_content.index(
            "正在请求模型，等待首个事件。"
        )
        assert "正在处理..." not in created_content

    asyncio.run(run())


def test_sink_routes_thinking_progress_to_process_blocks():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        sink.on_tool_progress("_thinking", tool_name="我先检查当前状态")
        await sink.drain_pending_updates()

        assert adapter.created
        assert "我先检查当前状态" in adapter.created[-1][1]["body"]["elements"][0]["content"]

    asyncio.run(run())


def test_sink_tool_progress_schedules_update_before_finalize():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        # First event creates the card.
        sink.on_tool_progress("tool.started", tool_name="exec_command", preview="ls")
        await sink.drain_pending_updates()
        assert adapter.created

        # Second event triggers an update.
        sink.on_delta("output")
        await sink.drain_pending_updates()
        assert adapter.text_updated

    asyncio.run(run())


def test_sink_flush_threadsafe_updates_tool_progress_before_finalize():
    class SlowUpdateAdapter(_FakeFeishuCardAdapter):
        async def update_card_stream_text(self, update_handle, element_id, content, sequence=None):
            await asyncio.sleep(0.2)
            return await super().update_card_stream_text(update_handle, element_id, content, sequence=sequence)

    async def run():
        adapter = SlowUpdateAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=60)

        await sink.start("正在处理...")

        def worker():
            sink.on_tool_progress("tool.started", tool_name="terminal", preview="pwd")
            started_at = time.monotonic()
            accepted = sink.flush_threadsafe(timeout_sec=2)
            return accepted, time.monotonic() - started_at

        accepted, elapsed = await asyncio.to_thread(worker)
        assert accepted is True
        assert elapsed < 0.1
        assert not adapter.text_updated

        for _ in range(20):
            if adapter.text_updated:
                break
            await asyncio.sleep(0.05)
        assert adapter.text_updated
        assert "command_execution" in adapter.text_updated[-1][2]
        assert "pwd" in adapter.text_updated[-1][2]
        assert sink.final_response_sent is False

    asyncio.run(run())


def test_sink_worker_thread_process_event_updates_before_finalize():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=60)
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
        worker.join(timeout=2)

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


def test_sink_created_in_worker_thread_uses_explicit_loop_for_realtime_updates():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        loop = asyncio.get_running_loop()
        sink = await asyncio.to_thread(
            lambda: FeishuCardRunSink(
                adapter=adapter,
                chat_id="oc_1",
                update_interval_sec=60,
                loop=loop,
            )
        )
        await sink.start("正在处理...")

        ready = threading.Event()

        def emit_tool():
            sink.on_tool_progress(
                "tool.started",
                tool_name="terminal",
                preview="pwd",
                tool_call_id="call-worker-loop",
            )
            ready.set()

        worker = threading.Thread(target=emit_tool)
        worker.start()
        assert ready.wait(2)
        worker.join(timeout=2)

        for _ in range(20):
            if adapter.text_updated:
                break
            await asyncio.sleep(0.05)

        assert adapter.text_updated
        content = adapter.text_updated[-1][2]
        assert "command_execution" in content
        assert "pwd" in content
        assert sink.final_response_sent is False

    asyncio.run(run())


def test_sink_accepts_delta_from_worker_thread():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        # First delta from worker creates the card.
        worker = threading.Thread(target=lambda: sink.on_delta("from worker"))
        worker.start()
        worker.join(timeout=2)
        await sink.drain_pending_updates()
        assert adapter.created

        # Second delta triggers an update.
        sink.on_delta(" more text")
        await sink.drain_pending_updates()
        assert adapter.text_updated

    asyncio.run(run())


def test_sink_filters_internal_stream_markers_before_render():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        sink.on_delta("<think>hidden</think>visible\nMEDIA:/tmp/a.mp3\n[[audio_as_voice]]")
        await sink.drain_pending_updates()

        # First drain only creates the card — check created content.
        rendered = str(adapter.created[-1][1])
        assert "visible" in rendered
        assert "hidden" not in rendered
        assert "MEDIA:/tmp/a.mp3" not in rendered
        assert "[[audio_as_voice]]" not in rendered

    asyncio.run(run())


def test_sink_disables_card_updates_after_repeated_update_failures():
    class FailingUpdateAdapter(_FakeFeishuCardAdapter):
        async def update_card_stream_text(self, update_handle, element_id, content, sequence=None):
            self.text_updated.append((update_handle, element_id, content, sequence))
            return SimpleNamespace(success=False, error="update failed")

    async def run():
        adapter = FailingUpdateAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0, max_update_failures=2)

        sink.on_delta("one")
        await sink.drain_pending_updates()
        sink.on_delta("two")
        await sink.drain_pending_updates()
        sink.on_delta("three")
        await sink.drain_pending_updates()

        assert sink.card_updates_disabled is True
        assert len(adapter.text_updated) == 2

    asyncio.run(run())


def test_sink_card_create_failure_falls_back_to_text():
    class BrokenAdapter(_FakeFeishuCardAdapter):
        async def create_card_stream_message(self, chat_id, card, metadata=None, reply_to=None):
            return SimpleNamespace(success=False, error="card failed")

    adapter = BrokenAdapter()
    sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", reply_to="om_parent")
    sink.on_delta("visible answer")

    delivered = asyncio.run(sink.finalize("visible answer"))

    assert delivered is True
    assert sink.fallback_sent is True
    assert sink.final_response_sent is True
    assert adapter.sent_text == [("oc_1", "visible answer", None, "om_parent")]


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


def test_sink_reduces_tool_event_queued_before_final():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=60)

        await sink.start("正在处理...")
        sink.on_tool_progress("tool.started", tool_name="terminal", preview="pwd", tool_call_id="call-1")

        delivered = await sink.finalize("final answer")

        assert delivered is True
        assert adapter.updated
        final_card = adapter.updated[-1][1]
        final_content = final_card["body"]["elements"][0]["content"]
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


def test_native_at_extractor_selects_legal_paragraphs_in_order():
    from gateway.platforms.feishu_card_stream import extract_native_at_paragraphs

    text = (
        "先说明背景。\n\n"
        "<at user_id=\"ou_bot_a\">小A</at> 第一件事。\n\n"
        "<at user_id=\"ou_bot_b\">小B</at> 第二件事。"
    )

    paragraphs = extract_native_at_paragraphs(text, max_messages=5)

    assert [item.text for item in paragraphs] == [
        "<at user_id=\"ou_bot_a\">小A</at> 第一件事。",
        "<at user_id=\"ou_bot_b\">小B</at> 第二件事。",
    ]
    assert [item.target_open_ids for item in paragraphs] == [
        ("ou_bot_a",),
        ("ou_bot_b",),
    ]


def test_native_at_extractor_keeps_multiple_mentions_in_one_paragraph():
    from gateway.platforms.feishu_card_stream import extract_native_at_paragraphs

    text = (
        "<at user_id=\"ou_bot_a\">小A</at> 和 "
        "<at user_id=\"ou_bot_b\">小B</at> 请一起处理。"
    )

    paragraphs = extract_native_at_paragraphs(text, max_messages=5)

    assert len(paragraphs) == 1
    assert paragraphs[0].text == text
    assert paragraphs[0].target_open_ids == ("ou_bot_a", "ou_bot_b")


def test_native_at_extractor_dedupes_by_normalized_paragraph_not_target():
    from gateway.platforms.feishu_card_stream import extract_native_at_paragraphs

    first = "<at user_id=\"ou_bot_a\">小A</at> 处理构建失败。"
    second = "<at user_id=\"ou_bot_a\">小A</at> 处理权限失败。"
    text = f"{first}\n\n{first}\n\n{second}"

    paragraphs = extract_native_at_paragraphs(text, max_messages=5)

    assert [item.text for item in paragraphs] == [first, second]
    assert [item.target_open_ids for item in paragraphs] == [
        ("ou_bot_a",),
        ("ou_bot_a",),
    ]


def test_native_at_extractor_ignores_code_blocks_and_illegal_aliases():
    from gateway.platforms.feishu_card_stream import extract_native_at_paragraphs

    text = (
        "```text\n"
        "<at user_id=\"ou_in_code\">小P</at> 示例。\n"
        "```\n\n"
        "<at id=\"ou_bad_id\">Bad</at> wrong field.\n\n"
        "<at open_id=\"ou_bad_open\">Bad</at> wrong field.\n\n"
        "<at user_id=\"cli_bad\">Bad</at> wrong value.\n\n"
        "<at user_id=\"ou_good\">小P</at> real message."
    )

    paragraphs = extract_native_at_paragraphs(text, max_messages=5)

    assert [item.text for item in paragraphs] == [
        "<at user_id=\"ou_good\">小P</at> real message."
    ]
    assert paragraphs[0].target_open_ids == ("ou_good",)


def test_native_at_extractor_applies_message_cap():
    from gateway.platforms.feishu_card_stream import extract_native_at_paragraphs

    text = "\n\n".join(
        f"<at user_id=\"ou_bot_{index}\">Bot {index}</at> message {index}"
        for index in range(4)
    )

    paragraphs = extract_native_at_paragraphs(text, max_messages=2)

    assert [item.text for item in paragraphs] == [
        "<at user_id=\"ou_bot_0\">Bot 0</at> message 0",
        "<at user_id=\"ou_bot_1\">Bot 1</at> message 1",
    ]


def test_native_at_forwarder_sends_each_selected_paragraph():
    import asyncio
    from types import SimpleNamespace

    from gateway.platforms.feishu_card_stream import NativeBotAtForwarder

    class Adapter:
        def __init__(self):
            self.raw_texts = []

        async def send_raw_text(self, chat_id, text, metadata=None, reply_to=None):
            self.raw_texts.append((chat_id, text, metadata, reply_to))
            return SimpleNamespace(success=True, message_id=f"om_{len(self.raw_texts)}")

    async def run():
        adapter = Adapter()
        forwarder = NativeBotAtForwarder(
            adapter=adapter,
            chat_id="oc_1",
            metadata={"thread_id": "omt_1"},
            reply_to="om_parent",
            enabled=True,
            max_messages=5,
        )
        text = (
            "<at user_id=\"ou_a\">小A</at> 第一件事。\n\n"
            "<at user_id=\"ou_b\">小B</at> 第二件事。"
        )

        count = await forwarder.forward_from_final_text(text)

        assert count == 2
        assert adapter.raw_texts == [
            ("oc_1", "<at user_id=\"ou_a\">小A</at> 第一件事。", {"thread_id": "omt_1"}, "om_parent"),
            ("oc_1", "<at user_id=\"ou_b\">小B</at> 第二件事。", {"thread_id": "omt_1"}, "om_parent"),
        ]

    asyncio.run(run())


def test_native_at_forwarder_disabled_sends_nothing():
    import asyncio
    from types import SimpleNamespace

    from gateway.platforms.feishu_card_stream import NativeBotAtForwarder

    class Adapter:
        def __init__(self):
            self.raw_texts = []

        async def send_raw_text(self, chat_id, text, metadata=None, reply_to=None):
            self.raw_texts.append(text)
            return SimpleNamespace(success=True, message_id="om_1")

    async def run():
        adapter = Adapter()
        forwarder = NativeBotAtForwarder(adapter=adapter, chat_id="oc_1", enabled=False)

        count = await forwarder.forward_from_final_text("<at user_id=\"ou_a\">小A</at> hi")

        assert count == 0
        assert adapter.raw_texts == []

    asyncio.run(run())


def test_native_at_forwarder_failure_does_not_raise(caplog):
    import asyncio
    import logging
    from types import SimpleNamespace

    from gateway.platforms.feishu_card_stream import NativeBotAtForwarder

    class Adapter:
        async def send_raw_text(self, chat_id, text, metadata=None, reply_to=None):
            return SimpleNamespace(success=False, error="send failed")

    async def run():
        forwarder = NativeBotAtForwarder(adapter=Adapter(), chat_id="oc_1", enabled=True)
        return await forwarder.forward_from_final_text("<at user_id=\"ou_a\">小A</at> hi")

    with caplog.at_level(logging.WARNING):
        count = asyncio.run(run())

    assert count == 0
    assert "feishu_native_bot_at_forward_failed" in caplog.text
    assert "ou_a" in caplog.text


def test_raw_answer_text_returns_only_final_answer_block():
    state = FeishuCardRunState()
    state.append_commentary("<at user_id=\"ou_process\">Process</at> should not forward.")
    state.set_footer("<at user_id=\"ou_footer\">Footer</at> should not forward.")

    final = "<at user_id=\"ou_answer\">小P</at> 只转发最终正文。"
    state.finalize(final)

    assert state.raw_answer_text() == final


def test_raw_answer_text_is_not_renderer_content():
    state = FeishuCardRunState()
    state.append_commentary("process text")
    state.set_footer("footer text")
    final = "<at user_id=\"ou_answer\">小P</at> 只转发最终正文。"
    state.finalize(final)

    rendered = FeishuCardRunRenderer().content(state, include_running_status=False)

    assert state.raw_answer_text() == final
    assert "process text" in rendered
    assert "footer text" in rendered
    assert rendered != state.raw_answer_text()


def test_sink_forwards_native_at_after_successful_final_card_update():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(
            adapter=adapter,
            chat_id="oc_1",
            metadata={"thread_id": "omt_1"},
            reply_to="om_parent",
            update_interval_sec=0,
            native_bot_at_forward=True,
        )

        sink.on_commentary("<at user_id=\"ou_process\">Process</at> should not forward.")
        await sink.drain_pending_updates()
        final = "<at user_id=\"ou_answer\">小P</at> 请接手。"
        delivered = await sink.finalize(final)

        assert delivered is True
        assert adapter.raw_text == [("oc_1", final, {"thread_id": "omt_1"}, "om_parent")]

    asyncio.run(run())


def test_sink_forwards_multiple_native_at_paragraphs_after_final_card_update():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(
            adapter=adapter,
            chat_id="oc_1",
            update_interval_sec=0,
            native_bot_at_forward=True,
            native_bot_at_forward_max_messages=5,
        )

        first = "<at user_id=\"ou_a\">小A</at> 第一件事。"
        second = "<at user_id=\"ou_a\">小A</at> 第二件事。"
        delivered = await sink.finalize(f"{first}\n\n{second}")

        assert delivered is True
        assert [item[1] for item in adapter.raw_text] == [first, second]

    asyncio.run(run())


def test_sink_does_not_forward_native_at_when_feature_disabled():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(
            adapter=adapter,
            chat_id="oc_1",
            update_interval_sec=0,
            native_bot_at_forward=False,
        )

        delivered = await sink.finalize("<at user_id=\"ou_answer\">小P</at> 请接手。")

        assert delivered is True
        assert adapter.raw_text == []

    asyncio.run(run())


def test_sink_native_forward_failure_does_not_break_card_delivery():
    from types import SimpleNamespace

    class FailingRawAdapter(_FakeFeishuCardAdapter):
        async def send_raw_text(self, chat_id, text, metadata=None, reply_to=None):
            self.raw_text.append((chat_id, text, metadata, reply_to))
            return SimpleNamespace(success=False, error="raw send failed")

    async def run():
        adapter = FailingRawAdapter()
        sink = FeishuCardRunSink(
            adapter=adapter,
            chat_id="oc_1",
            update_interval_sec=0,
            native_bot_at_forward=True,
        )

        delivered = await sink.finalize("<at user_id=\"ou_answer\">小P</at> 请接手。")

        assert delivered is True
        assert sink.final_response_sent is True
        assert adapter.updated
        assert adapter.raw_text

    asyncio.run(run())


def test_card_display_rewrites_single_native_at_to_human_text():
    from gateway.platforms.feishu_card_stream import render_native_at_as_human_text

    rendered = render_native_at_as_human_text("<at user_id=\"ou_target\">小P</at> 你好")

    assert rendered == "对 小P 说，你好"


def test_card_display_rewrites_multiple_native_ats_to_human_text():
    from gateway.platforms.feishu_card_stream import render_native_at_as_human_text

    rendered = render_native_at_as_human_text(
        "<at user_id=\"ou_a\">小P</at> <at user_id=\"ou_b\">小C</at> 请看"
    )

    assert rendered == "对 小P、小C 说，请看"


def test_card_display_rewrite_ignores_incomplete_native_at():
    from gateway.platforms.feishu_card_stream import render_native_at_as_human_text

    partial = "<at user_id=\"ou_target\">小"

    assert render_native_at_as_human_text(partial) == partial


def test_raw_answer_survives_human_display_rewrite():
    state = FeishuCardRunState()
    raw = "<at user_id=\"ou_target\">小P</at> 你好"

    state.finalize(raw)
    rendered = FeishuCardRunRenderer().content(state, include_running_status=False)

    assert state.raw_answer_text() == raw
    assert "对 小P 说，你好" in rendered
    assert "<at user_id=\"ou_target\">小P</at>" not in rendered


def test_human_display_rewrite_does_not_apply_to_process_footer_or_tool_text():
    state = FeishuCardRunState()
    raw = "<at user_id=\"ou_target\">小P</at> 你好"
    process = "<at user_id=\"ou_process\">Process</at> process text"
    footer = "<at user_id=\"ou_footer\">Footer</at> footer text"

    state.append_commentary(process)
    state.set_footer(footer)
    state.finalize(raw)

    rendered = FeishuCardRunRenderer().content(state, include_running_status=False)

    assert "对 小P 说，你好" in rendered
    assert process in rendered
    assert footer in rendered
    assert "对 Process 说" not in rendered
    assert "对 Footer 说" not in rendered


def test_card_display_rewrite_protects_fenced_code_blocks():
    from gateway.platforms.feishu_card_stream import render_native_at_as_human_text

    text = (
        "```text\n"
        "<at user_id=\"ou_example\">小P</at> 示例。\n"
        "```\n\n"
        "<at user_id=\"ou_real\">小C</at> 真实消息。"
    )

    rendered = render_native_at_as_human_text(text)

    assert "<at user_id=\"ou_example\">小P</at> 示例。" in rendered
    assert "对 小C 说，真实消息。" in rendered
    assert "对 小P 说" not in rendered


def test_native_at_extractor_logs_warning_when_cap_exceeded(caplog):
    import logging
    from gateway.platforms.feishu_card_stream import extract_native_at_paragraphs

    text = "\n\n".join(
        f"<at user_id=\"ou_bot_{i}\">Bot {i}</at> message {i}"
        for i in range(10)
    )

    with caplog.at_level(logging.WARNING):
        paragraphs = extract_native_at_paragraphs(text, max_messages=3)

    assert len(paragraphs) == 3
    assert "feishu_native_bot_at_cap_reached" in caplog.text
    assert "max=3" in caplog.text
    assert "skipped=" in caplog.text


def test_sink_does_not_forward_twice_after_finalize():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(
            adapter=adapter,
            chat_id="oc_1",
            update_interval_sec=0,
            native_bot_at_forward=True,
        )

        delivered = await sink.finalize("<at user_id=\"ou_answer\">小P</at> 请接手。")
        assert delivered is True

        first_count = len(adapter.raw_text)
        assert first_count == 1

        # Call finalize a second time — should not forward again
        delivered2 = await sink.finalize("<at user_id=\"ou_answer\">小P</at> 请接手。")
        assert delivered2 is True
        assert len(adapter.raw_text) == first_count

    asyncio.run(run())
