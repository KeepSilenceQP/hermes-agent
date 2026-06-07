from gateway.platforms.feishu_card_stream import FeishuCardRunRenderer, FeishuCardRunState


def test_renderer_streaming_card_contains_text_and_running_tool():
    state = FeishuCardRunState()
    state.append_text("I will inspect the repo.")
    state.start_tool(tool_name="terminal", preview="rg card_streaming", args={"command": "rg card_streaming"})

    card = FeishuCardRunRenderer().render(state)

    assert card["schema"] == "2.0"
    assert card["config"]["streaming_mode"] is True
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
        self.sent_text = []

    async def create_card_stream_message(self, chat_id, card, metadata=None, reply_to=None):
        self.created.append((chat_id, card, metadata, reply_to))
        return SimpleNamespace(success=True, message_id="om_card_1", card_id="card_1")

    async def update_card_stream_message(self, update_handle, card, sequence=None):
        self.updated.append((update_handle, card, sequence))
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

        sink.on_delta("streaming")
        await sink.drain_pending_updates()

        assert adapter.created
        assert adapter.updated
        assert sink.final_response_sent is False

    asyncio.run(run())


def test_sink_tool_progress_schedules_update_before_finalize():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        sink.on_tool_progress("tool.started", tool_name="exec_command", preview="ls")
        await sink.drain_pending_updates()

        assert adapter.created
        assert adapter.updated

    asyncio.run(run())


def test_sink_accepts_delta_from_worker_thread():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        worker = threading.Thread(target=lambda: sink.on_delta("from worker"))
        worker.start()
        worker.join(timeout=2)
        await sink.drain_pending_updates()

        assert adapter.created
        assert adapter.updated

    asyncio.run(run())


def test_sink_filters_internal_stream_markers_before_render():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        sink.on_delta("<think>hidden</think>visible\nMEDIA:/tmp/a.mp3\n[[audio_as_voice]]")
        await sink.drain_pending_updates()

        rendered = str(adapter.updated[-1][1])
        assert "visible" in rendered
        assert "hidden" not in rendered
        assert "MEDIA:/tmp/a.mp3" not in rendered
        assert "[[audio_as_voice]]" not in rendered

    asyncio.run(run())


def test_sink_disables_card_updates_after_repeated_update_failures():
    class FailingUpdateAdapter(_FakeFeishuCardAdapter):
        async def update_card_stream_message(self, update_handle, card, sequence=None):
            self.updated.append((update_handle, card, sequence))
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
        assert len(adapter.updated) == 2

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
