from types import SimpleNamespace

class TestAssistantReasoningProgress:
    def _agent_with_progress(self, delegate_depth=0):
        calls = []
        agent = SimpleNamespace(
            tool_progress_callback=lambda *args, **kwargs: calls.append((args, kwargs)),
            _delegate_depth=delegate_depth,
        )
        return agent, calls

    def test_root_plain_assistant_content_does_not_emit_reasoning(self):
        from agent.turn_response_intake import _emit_assistant_reasoning_progress

        agent, calls = self._agent_with_progress(delegate_depth=0)

        _emit_assistant_reasoning_progress(
            agent,
            SimpleNamespace(content="The conclusion is ready; I will verify next."),
        )

        assert calls == []

    def test_root_inline_reasoning_content_emits_reasoning_available(self):
        from agent.turn_response_intake import _emit_assistant_reasoning_progress

        agent, calls = self._agent_with_progress(delegate_depth=0)

        _emit_assistant_reasoning_progress(
            agent,
            SimpleNamespace(content="<think>inspect current state</think>\nfinal answer."),
        )

        assert calls == [
            (("reasoning.available", "_thinking", "inspect current state", None), {})
        ]

    def test_root_provider_reasoning_field_emits_reasoning_available(self):
        from agent.turn_response_intake import _emit_assistant_reasoning_progress

        agent, calls = self._agent_with_progress(delegate_depth=0)

        _emit_assistant_reasoning_progress(
            agent,
            SimpleNamespace(content="Final answer.", reasoning="provider side reasoning"),
        )

        assert calls == [
            (("reasoning.available", "_thinking", "provider side reasoning", None), {})
        ]

    def test_delegate_plain_assistant_content_keeps_thinking_first_line(self):
        from agent.turn_response_intake import _emit_assistant_reasoning_progress

        agent, calls = self._agent_with_progress(delegate_depth=1)

        _emit_assistant_reasoning_progress(
            agent,
            SimpleNamespace(content="I will inspect the repo first.\nThen summarize."),
        )

        assert calls == [(("_thinking", "I will inspect the repo first."), {})]

