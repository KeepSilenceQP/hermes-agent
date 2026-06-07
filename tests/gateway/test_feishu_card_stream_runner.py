from types import SimpleNamespace


def test_card_streaming_flag_forces_stream_delta_capture():
    from gateway.run import _should_use_feishu_card_streaming

    assert _should_use_feishu_card_streaming(
        platform_key="feishu",
        user_config={"display": {"platforms": {"feishu": {"card_streaming": True}}}},
    ) is True


def test_card_streaming_false_for_non_feishu():
    from gateway.run import _should_use_feishu_card_streaming

    assert _should_use_feishu_card_streaming(
        platform_key="telegram",
        user_config={"display": {"platforms": {"feishu": {"card_streaming": True}}}},
    ) is False


def test_card_sink_delivery_suppresses_final_send():
    from gateway.run import _card_sink_delivered_final

    sink = SimpleNamespace(final_response_sent=True, final_content_delivered=True, fallback_sent=False)

    assert _card_sink_delivered_final(sink) is True


def test_card_streaming_keeps_tool_callback_without_progress_bubbles():
    from gateway.run import _should_attach_tool_progress_callback

    assert _should_attach_tool_progress_callback(
        tool_progress_enabled=False,
        want_feishu_card_streaming=True,
    ) is True


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
