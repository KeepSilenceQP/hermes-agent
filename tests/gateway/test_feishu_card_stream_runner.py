from types import SimpleNamespace


def _feishu_card_streaming_baseline_config():
    return {
        "display": {
            "streaming": True,
            "interim_assistant_messages": True,
            "platforms": {"feishu": {"card_streaming": True}},
        },
        "streaming": {"enabled": True, "transport": "auto"},
    }


def test_card_streaming_flag_forces_stream_delta_capture():
    from gateway.run import _should_use_feishu_card_streaming

    assert _should_use_feishu_card_streaming(
        platform_key="feishu",
        user_config=_feishu_card_streaming_baseline_config(),
    ) is True


def test_card_streaming_flag_requires_full_streaming_baseline():
    from gateway.run import _should_use_feishu_card_streaming

    assert _should_use_feishu_card_streaming(
        platform_key="feishu",
        user_config={"display": {"platforms": {"feishu": {"card_streaming": True}}}},
    ) is False
    assert _should_use_feishu_card_streaming(
        platform_key="feishu",
        user_config={
            "display": {"streaming": True, "platforms": {"feishu": {"card_streaming": True}}},
            "streaming": {"enabled": False, "transport": "auto"},
        },
    ) is False
    assert _should_use_feishu_card_streaming(
        platform_key="feishu",
        user_config={
            "display": {"streaming": True, "platforms": {"feishu": {"card_streaming": True}}},
            "streaming": {"enabled": True, "transport": "off"},
        },
    ) is False


def test_card_streaming_false_for_non_feishu():
    from gateway.run import _should_use_feishu_card_streaming

    assert _should_use_feishu_card_streaming(
        platform_key="telegram",
        user_config=_feishu_card_streaming_baseline_config(),
    ) is False


def test_card_sink_delivery_suppresses_final_send():
    from gateway.run import _card_sink_delivered_final

    sink = SimpleNamespace(final_response_sent=True, final_content_delivered=True, fallback_sent=False)

    assert _card_sink_delivered_final(sink) is True


def test_runtime_footer_is_rendered_as_card_block_not_final_answer_text():
    from gateway.platforms.feishu_card_stream import (
        FeishuCardRunRenderer,
        FeishuCardRunState,
    )

    answer = "final answer"
    footer = "gpt-5.5 · 19% · ~"
    state = FeishuCardRunState()

    state.append_commentary(answer)
    state.append_text(answer)
    state.set_footer(footer)
    state.finalize(answer)

    rendered = FeishuCardRunRenderer().content(state, include_running_status=False)
    assert rendered.count(answer) == 1
    assert footer in rendered


def test_card_delivered_runtime_footer_suppresses_trailing_footer():
    from gateway.run import _should_send_trailing_runtime_footer

    assert _should_send_trailing_runtime_footer(
        footer_line="gpt-5.5 · 19% · ~",
        agent_result={"already_sent": True, "runtime_footer_delivered": True},
    ) is False
    assert _should_send_trailing_runtime_footer(
        footer_line="gpt-5.5 · 19% · ~",
        agent_result={"already_sent": True},
    ) is True


def test_feishu_card_lifecycle_uses_commentary_event_chain():
    from gateway.run import _emit_feishu_card_lifecycle

    class Sink:
        def __init__(self):
            self.commentary = []
            self.flushes = 0

        def on_commentary(self, text):
            self.commentary.append(text)

        def flush_threadsafe(self):
            self.flushes += 1
            return True

    sink = Sink()

    assert _emit_feishu_card_lifecycle(sink, " 正在请求模型，等待首个事件。 ") is True
    assert sink.commentary == ["正在请求模型，等待首个事件。"]
    assert sink.flushes == 1


def test_feishu_card_streaming_config_file_loads_required_baseline(tmp_path, monkeypatch):
    import gateway.run as gateway_run

    (tmp_path / "config.yaml").write_text(
        """
display:
  streaming: true
  interim_assistant_messages: true
  platforms:
    feishu:
      card_streaming: true
streaming:
  enabled: true
  transport: auto
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(gateway_run, "_hermes_home", tmp_path)

    user_config = gateway_run._load_gateway_config()

    assert user_config["display"]["streaming"] is True
    assert user_config["streaming"]["enabled"] is True
    assert user_config["streaming"]["transport"] == "auto"
    assert user_config["display"]["platforms"]["feishu"]["card_streaming"] is True
    assert gateway_run._should_use_feishu_card_streaming(
        platform_key="feishu",
        user_config=user_config,
    ) is True


def test_card_streaming_keeps_tool_callback_without_progress_bubbles():
    from gateway.run import _should_attach_tool_progress_callback

    assert _should_attach_tool_progress_callback(
        tool_progress_enabled=False,
        want_feishu_card_streaming=True,
    ) is True


def test_route_feishu_card_tool_progress_calls_sink():
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
    ) is True
    assert sink.calls == [
        ("tool.started", "terminal", "pwd", {"command": "pwd"}, {})
    ]
    assert sink.flushes == 1


def test_route_feishu_card_tool_progress_handles_sink_error(caplog):
    import logging

    from gateway.run import _route_feishu_card_tool_progress

    class Sink:
        def on_tool_progress(self, event_type, tool_name, preview, args, **kwargs):
            raise RuntimeError("boom")

    logger = logging.getLogger("test-feishu-card-route")

    with caplog.at_level(logging.WARNING):
        assert _route_feishu_card_tool_progress(
            Sink(),
            "tool.started",
            "terminal",
            "pwd",
            {},
            logger_obj=logger,
        ) is True

    assert "feishu_card_tool_progress_route_failed" in caplog.text


def test_card_streaming_keeps_interim_callback_without_interim_messages():
    from gateway.run import _should_attach_interim_callback

    assert _should_attach_interim_callback(
        want_interim_messages=False,
        want_feishu_card_streaming=True,
    ) is True


def test_card_streaming_does_not_create_gateway_stream_consumer():
    from gateway.run import _should_create_gateway_stream_consumer

    assert _should_create_gateway_stream_consumer(
        streaming_enabled=False,
        want_interim_messages=False,
        want_feishu_card_streaming=True,
    ) is False


# ── Integration-level callback routing tests ──────────────────────────
# These verify that the FeishuCardRunSink correctly routes callbacks
# (delta, tool_progress, commentary) through to the adapter, exercising
# the full thread-safe drain-and-flush pipeline with a fake adapter.


def test_card_sink_callback_routing_delta_to_text():
    """Full pipeline: delta -> create card -> finalize -> already_sent."""
    import asyncio
    from types import SimpleNamespace

    from gateway.platforms.feishu_card_stream import FeishuCardRunSink

    class _Adapter:
        def __init__(self):
            self.created = []
            self.updated = []
            self.sent = []

        async def create_card_stream_message(self, chat_id, card, metadata=None, reply_to=None):
            self.created.append(card)  # store just the card dict
            return SimpleNamespace(success=True, message_id="om_1", card_id="card_1")

        async def update_card_stream_message(self, update_handle, card, sequence=None):
            self.updated.append(card)
            return SimpleNamespace(success=True)

        async def send(self, chat_id, content, metadata=None, reply_to=None):
            self.sent.append(content)
            return SimpleNamespace(success=True, message_id="om_fb")

    async def run():
        adapter = _Adapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_test", update_interval_sec=0)

        # Simulate agent streaming
        sink.on_delta("streaming text ")
        sink.on_delta("arrives in parts")
        await sink.drain_pending_updates()

        assert adapter.created, "card should be created on first drain"
        assert "streaming text" in str(adapter.created[0])
        assert "arrives in parts" in str(adapter.created[0])

        # Finalize — card delivers, no fallback needed
        delivered = await sink.finalize("streaming text arrives in parts")
        assert delivered is True
        assert sink.final_response_sent is True
        assert sink.fallback_sent is False, "should not fallback on success"
        assert not adapter.sent, "no text fallback sent"

    asyncio.run(run())


def test_card_sink_finalize_does_not_duplicate_streamed_final_text():
    """Final card should not repeat the same assistant answer twice."""
    import asyncio
    from types import SimpleNamespace

    from gateway.platforms.feishu_card_stream import FeishuCardRunSink

    class _Adapter:
        def __init__(self):
            self.created = []
            self.updated = []
            self.sent = []

        async def create_card_stream_message(self, chat_id, card, metadata=None, reply_to=None):
            self.created.append(card)
            return SimpleNamespace(success=True, message_id="om_1", card_id="card_1")

        async def update_card_stream_message(self, update_handle, card, sequence=None):
            self.updated.append(card)
            return SimpleNamespace(success=True)

        async def send(self, chat_id, content, metadata=None, reply_to=None):
            self.sent.append(content)
            return SimpleNamespace(success=True, message_id="om_fb")

    async def run():
        adapter = _Adapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_test", update_interval_sec=0)

        final_text = "streaming text arrives in parts"
        sink.on_delta(final_text)
        await sink.drain_pending_updates()

        delivered = await sink.finalize(final_text)

        assert delivered is True
        assert adapter.updated, "existing card should be updated at finalize"
        rendered = str(adapter.updated[-1])
        assert rendered.count(final_text) == 1
        assert not adapter.sent

    asyncio.run(run())


def test_card_run_state_footer_does_not_break_final_answer_dedupe():
    """Footer is a reducer block; final text remains answer-only for dedupe."""
    from gateway.platforms.feishu_card_stream import (
        FeishuCardRunRenderer,
        FeishuCardRunState,
    )

    answer = "当前北京时间 2026-06-08 18:13:58，/Users/bytedance/Desktop 下共有 20 个文件/项目。"
    footer = "gpt-5.5 · 19% · ~"
    state = FeishuCardRunState()

    state.append_text(answer)
    state.append_commentary(answer)
    state.set_footer(footer)
    state.finalize(answer)

    rendered = FeishuCardRunRenderer().content(state, include_running_status=False)
    assert rendered.count(answer) == 1
    assert footer in rendered


def test_card_sink_footer_event_preserves_single_final_answer():
    import asyncio
    from types import SimpleNamespace

    from gateway.platforms.feishu_card_stream import FeishuCardRunSink

    class _Adapter:
        def __init__(self):
            self.created = []
            self.updated = []
            self.text_updated = []

        async def create_card_stream_message(self, chat_id, card, metadata=None, reply_to=None):
            self.created.append(card)
            return SimpleNamespace(success=True, message_id="om_1", card_id="card_1")

        async def update_card_stream_text(self, update_handle, element_id, content, sequence=None):
            self.text_updated.append(content)
            return SimpleNamespace(success=True)

        async def update_card_stream_message(self, update_handle, card, sequence=None):
            self.updated.append(card)
            return SimpleNamespace(success=True)

        async def send(self, chat_id, content, metadata=None, reply_to=None):
            return SimpleNamespace(success=True, message_id="om_fb")

    async def run():
        answer = "当前北京时间 2026-06-08 18:13:58，/Users/bytedance/Desktop 下共有 20 个文件/项目。"
        footer = "gpt-5.5 · 19% · ~"
        adapter = _Adapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_test", update_interval_sec=0)

        sink.on_delta(answer)
        sink.on_commentary(answer)
        sink.on_footer(footer)
        await sink.drain_pending_updates()

        delivered = await sink.finalize(answer)

        assert delivered is True
        final_card = adapter.updated[-1]
        rendered = final_card["body"]["elements"][0]["content"]
        assert rendered.count(answer) == 1
        assert footer in rendered
        assert final_card["config"]["streaming_mode"] is False

    asyncio.run(run())


def test_card_sink_ignores_none_delta_boundaries():
    """Tool-boundary None deltas must not render as literal 'None' text."""
    import asyncio
    from types import SimpleNamespace

    from gateway.platforms.feishu_card_stream import FeishuCardRunSink

    class _Adapter:
        def __init__(self):
            self.created = []
            self.updated = []

        async def create_card_stream_message(self, chat_id, card, metadata=None, reply_to=None):
            self.created.append(card)
            return SimpleNamespace(success=True, message_id="om_1", card_id="card_1")

        async def update_card_stream_message(self, update_handle, card, sequence=None):
            self.updated.append(card)
            return SimpleNamespace(success=True)

        async def send(self, chat_id, content, metadata=None, reply_to=None):
            return SimpleNamespace(success=True, message_id="om_fb")

    async def run():
        adapter = _Adapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_test", update_interval_sec=0)

        sink.on_delta(None)
        sink.on_delta(None)
        sink.on_delta("visible answer")
        sink.on_delta(None)
        await sink.drain_pending_updates()

        rendered = str(adapter.created[-1])
        assert "visible answer" in rendered
        assert "None" not in rendered

    asyncio.run(run())


def test_card_sink_callback_routing_tool_progress():
    """Tool progress events route through sink and appear in card."""
    import asyncio
    from types import SimpleNamespace

    from gateway.platforms.feishu_card_stream import FeishuCardRunSink

    class _Adapter:
        def __init__(self):
            self.created = []
            self.updated = []

        async def create_card_stream_message(self, chat_id, card, metadata=None, reply_to=None):
            self.created.append(card)
            return SimpleNamespace(success=True, message_id="om_1", card_id="card_1")

        async def update_card_stream_message(self, update_handle, card, sequence=None):
            self.updated.append(card)
            return SimpleNamespace(success=True)

        async def send(self, chat_id, content, metadata=None, reply_to=None):
            return SimpleNamespace(success=True, message_id="om_fb")

    async def run():
        adapter = _Adapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_test", update_interval_sec=0)

        sink.on_tool_progress("tool.started", tool_name="search", preview="searching...")
        sink.on_tool_progress("tool.completed", tool_name="search")
        await sink.drain_pending_updates()

        # Card was created with tool state
        rendered = str(adapter.created[0])
        assert "search" in rendered
        assert "done" in rendered or "running" in rendered

        # Finalize
        await sink.finalize("results found")
        assert sink.final_response_sent

    asyncio.run(run())


def test_card_sink_renders_tool_result_output_and_error_state():
    """Tool completion carries output/error state into the rendered card."""
    import asyncio
    from types import SimpleNamespace

    from gateway.platforms.feishu_card_stream import FeishuCardRunSink

    class _Adapter:
        def __init__(self):
            self.created = []
            self.updated = []

        async def create_card_stream_message(self, chat_id, card, metadata=None, reply_to=None):
            self.created.append(card)
            return SimpleNamespace(success=True, message_id="om_1", card_id="card_1")

        async def update_card_stream_message(self, update_handle, card, sequence=None):
            self.updated.append(card)
            return SimpleNamespace(success=True)

        async def send(self, chat_id, content, metadata=None, reply_to=None):
            return SimpleNamespace(success=True, message_id="om_fb")

    async def run():
        adapter = _Adapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_test", update_interval_sec=0)

        sink.on_tool_progress("tool.started", tool_name="terminal", preview="ls")
        sink.on_tool_progress(
            "tool.completed",
            tool_name="terminal",
            is_error=True,
            result="permission denied",
        )
        await sink.drain_pending_updates()

        rendered = str(adapter.created[0])
        assert "command_execution" in rendered
        assert "permission denied" in rendered
        assert "❌" in rendered

    asyncio.run(run())


def test_card_sink_callback_routing_interim_commentary():
    """Interim commentary routes via on_commentary, not on_delta."""
    import asyncio
    from types import SimpleNamespace

    from gateway.platforms.feishu_card_stream import FeishuCardRunSink

    class _Adapter:
        def __init__(self):
            self.created = []
            self.updated = []

        async def create_card_stream_message(self, chat_id, card, metadata=None, reply_to=None):
            self.created.append(card)
            return SimpleNamespace(success=True, message_id="om_1", card_id="card_1")

        async def update_card_stream_message(self, update_handle, card, sequence=None):
            self.updated.append(card)
            return SimpleNamespace(success=True)

        async def send(self, chat_id, content, metadata=None, reply_to=None):
            return SimpleNamespace(success=True, message_id="om_fb")

    async def run():
        adapter = _Adapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_test", update_interval_sec=0)

        sink.on_commentary("I will inspect the repo first.")
        await sink.drain_pending_updates()

        rendered = str(adapter.created[0])
        assert "inspect the repo" in rendered

    asyncio.run(run())


def test_gateway_feishu_interim_callback_preserves_already_streamed_flag():
    import inspect

    import gateway.run as gateway_run

    source = inspect.getsource(gateway_run)

    assert ".on_commentary(" in source
    assert "already_streamed=already_streamed" in source


def test_card_sink_already_streamed_interim_does_not_duplicate_delta():
    import asyncio
    from types import SimpleNamespace

    from gateway.platforms.feishu_card_stream import FeishuCardRunSink

    class _Adapter:
        def __init__(self):
            self.created = []

        async def create_card_stream_message(self, chat_id, card, metadata=None, reply_to=None):
            self.created.append(card)
            return SimpleNamespace(success=True, message_id="om_1", card_id="card_1")

        async def update_card_stream_message(self, update_handle, card, sequence=None):
            return SimpleNamespace(success=True)

        async def send(self, chat_id, content, metadata=None, reply_to=None):
            return SimpleNamespace(success=True, message_id="om_fb")

    async def run():
        adapter = _Adapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_test", update_interval_sec=0)

        sink.on_delta("我先查官方输出。")
        sink.on_commentary("我先查官方输出。", already_streamed=True)
        await sink.drain_pending_updates()

        rendered = adapter.created[0]["body"]["elements"][0]["content"]
        assert rendered.count("我先查官方输出。") == 1

    asyncio.run(run())


def test_card_sink_fallback_on_create_failure_sends_text():
    """When card creation fails, finalize falls back to plain text send."""
    import asyncio
    from types import SimpleNamespace

    from gateway.platforms.feishu_card_stream import FeishuCardRunSink

    class _BrokenAdapter:
        def __init__(self):
            self.sent = []

        async def create_card_stream_message(self, chat_id, card, metadata=None, reply_to=None):
            return SimpleNamespace(success=False, error="card create failed")

        async def update_card_stream_message(self, update_handle, card, sequence=None):
            return SimpleNamespace(success=True)

        async def send(self, chat_id, content, metadata=None, reply_to=None):
            self.sent.append((chat_id, content))
            return SimpleNamespace(success=True, message_id="om_fb")

    async def run():
        adapter = _BrokenAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_test", reply_to="om_parent")

        sink.on_delta("visible answer")
        delivered = await sink.finalize("visible answer")

        assert delivered is True
        assert sink.fallback_sent is True
        assert adapter.sent == [("oc_test", "visible answer")]

    asyncio.run(run())


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


def test_card_sink_delivery_suppresses_final_send_after_terminal_card_update():
    from gateway.run import _card_sink_delivered_final, _should_use_feishu_card_streaming

    user_config = _feishu_card_streaming_baseline_config()
    sink = type(
        "Sink",
        (),
        {
            "final_response_sent": True,
            "final_content_delivered": True,
            "fallback_sent": False,
        },
    )()

    assert _should_use_feishu_card_streaming(platform_key="feishu", user_config=user_config) is True
    assert _card_sink_delivered_final(sink) is True
