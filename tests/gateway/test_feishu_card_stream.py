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
                "**terminal** — `rg card_streaming`\n\n"
                "Status: running\n\n"
                "I will inspect the repo.\n\n"
                "_calling tools_"
            ),
        }
    ]
    body = str(card["body"])
    assert "I will inspect the repo." in body
    assert "terminal" in body
    assert "rg card_streaming" in body
    assert "running" in body.lower()


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
    assert "terminal" in str(card)


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


import asyncio
import threading
from types import SimpleNamespace

from gateway.platforms.feishu_card_stream import FeishuCardRunSink


class _FakeFeishuCardAdapter:
    def __init__(self):
        self.created = []
        self.updated = []
        self.text_updated = []
        self.sent_text = []

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
        rendered_content = adapter.text_updated[-1][2]
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
        assert adapter.text_updated == [("card_1", "stream_md", "final answer", 2)]
        assert not adapter.updated

    asyncio.run(run())


def test_sink_start_creates_process_card_and_finalize_appends_answer():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        started = await sink.start("正在处理...")
        delivered = await sink.finalize("final answer")

        assert started is True
        assert delivered is True
        assert "正在处理..." in adapter.created[-1][1]["body"]["elements"][0]["content"]
        assert adapter.text_updated[-1][2] == "正在处理...\n\nfinal answer"

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
