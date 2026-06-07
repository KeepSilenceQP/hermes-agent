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
