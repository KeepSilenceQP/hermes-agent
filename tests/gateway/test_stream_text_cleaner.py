from gateway.stream_text_cleaner import StreamDisplayTextFilter, clean_stream_display_text


def test_clean_stream_display_text_removes_media_and_voice_directive():
    raw = "hello\nMEDIA:/tmp/audio.mp3\n[[audio_as_voice]]\nworld"

    assert clean_stream_display_text(raw) == "hello\n\nworld"


def test_stream_display_text_filter_suppresses_complete_think_block():
    filt = StreamDisplayTextFilter()

    assert filt.feed("<think>hidden</think>visible") == "visible"


def test_stream_display_text_filter_suppresses_split_think_block():
    filt = StreamDisplayTextFilter()

    assert filt.feed("<thi") == ""
    assert filt.feed("nk>hidden</think>visible") == "visible"


def test_stream_display_text_filter_keeps_visible_text_around_think_block():
    filt = StreamDisplayTextFilter()

    assert filt.feed("before\n<think>hidden</think>\nafter") == "before\n\nafter"


def test_stream_display_text_filter_preserves_prose_think_mention():
    """<think> mid-line in prose should NOT be treated as a think block."""
    filt = StreamDisplayTextFilter()

    result = filt.feed("The <think> tag is used for reasoning")
    assert "<think>" in result
    assert "reasoning" in result
