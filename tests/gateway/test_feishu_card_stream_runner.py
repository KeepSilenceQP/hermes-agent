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
