# Feishu Bot-at-Bot Native Forwarding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** After a Feishu CardKit streaming reply finishes, forward explicit native bot-at-bot paragraphs from the raw final assistant answer as separate Feishu `text` messages.

**Architecture:** Keep the existing card streaming reducer and renderer as the visible display path. Add a post-final one-shot `NativeBotAtForwarder` that reads only raw answer state, extracts legal native `<at user_id="ou_xxx">...</at>` paragraphs, and calls a Feishu adapter method that forces `msg_type="text"`. Card display uses answer-only plain-text projection such as `对 小P 说，你好`; CardKit `<person>` is not part of this implementation.

**Tech Stack:** Python, Hermes gateway, Feishu IM text messages, Feishu CardKit 2.0 display cards, pytest, `uv run --extra dev --extra feishu python -m pytest`.

---

## Command Convention

Run all implementation and verification commands from:

```bash
cd /Users/bytedance/repo/hermes-feishu-card-streaming/worktrees/hermes-agent
```

Use:

```bash
uv run --extra dev --extra feishu python -m pytest ...
```

Do not use bare `python` or bare `pytest` for verification.

Each task ends with a checkpoint. Show `git diff --stat` and `git status --short`; only commit when Qin Peng explicitly asks for commits.

## Current Baseline

The worktree starts from the clean code baseline for:

- `gateway/platforms/feishu_card_stream.py`
- `tests/gateway/test_feishu_card_stream_runner.py`

小A's previous global `<at>` to `<person>` code and tests have been reverted. The expected untracked files before implementation are:

- `docs/design/2026-06-09-feishu-bot-at-native-forwarding-design.md`
- `docs/plans/2026-06-09-feishu-bot-at-native-forwarding-implementation-plan.md`

Do not reintroduce the old global display conversion.

## File Structure

- Modify `gateway/platforms/feishu_card_stream.py`
  - Add native at paragraph extraction helpers.
  - Add answer-only human-readable display rewrite helpers.
  - Add `NativeBotAtForwarder`.
  - Add `FeishuCardRunState.raw_answer_text()`.
  - Wire post-final forwarding into `FeishuCardRunSink.finalize(...)` and `FeishuCardRunSink.update_final_after_transform(...)`.
  - Keep forwarding disabled unless explicitly enabled through constructor/config.
- Modify `gateway/platforms/feishu.py`
  - Add `FeishuAdapter.send_raw_text(...)`.
  - Force `msg_type="text"` and preserve the raw `<at user_id="...">...</at>` payload.
- Modify `gateway/run.py`
  - Read `display.platforms.feishu.native_bot_at_forward` and `display.platforms.feishu.native_bot_at_forward_max_messages`.
  - Pass those values into `FeishuCardRunSink`.
- Modify `tests/gateway/test_feishu_card_stream.py`
  - Add parser, forwarder, raw-answer, and sink integration tests.
- Modify `tests/gateway/test_feishu_card_transport.py`
  - Add `send_raw_text` transport tests.
- Modify `tests/gateway/test_feishu_card_stream_runner.py`
  - Add config-routing tests for the new feature gate.
- Keep `docs/design/2026-06-09-feishu-bot-at-native-forwarding-design.md`
  - Existing design source. Do not move it.

## Task 1: Add Native At Extraction Tests

**Files:**
- Modify: `tests/gateway/test_feishu_card_stream.py`
- Modify: `gateway/platforms/feishu_card_stream.py`
- Test: `tests/gateway/test_feishu_card_stream.py`

- [ ] **Step 1: Add failing parser tests**

Append these tests near the other reducer/state tests in `tests/gateway/test_feishu_card_stream.py`:

```python
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
```

- [ ] **Step 2: Run the parser tests and verify they fail**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py::test_native_at_extractor_selects_legal_paragraphs_in_order \
  tests/gateway/test_feishu_card_stream.py::test_native_at_extractor_keeps_multiple_mentions_in_one_paragraph \
  tests/gateway/test_feishu_card_stream.py::test_native_at_extractor_dedupes_by_normalized_paragraph_not_target \
  tests/gateway/test_feishu_card_stream.py::test_native_at_extractor_ignores_code_blocks_and_illegal_aliases \
  tests/gateway/test_feishu_card_stream.py::test_native_at_extractor_applies_message_cap -q
```

Expected: FAIL with `ImportError` or `AttributeError` because `extract_native_at_paragraphs` does not exist.

- [ ] **Step 3: Add the extraction implementation**

Add these imports and helpers near the top of `gateway/platforms/feishu_card_stream.py`, after the existing imports:

```python
import re
```

Add these definitions before `FeishuCardRunState`:

```python
_NATIVE_AT_TAG_RE = re.compile(
    r"<at\s+user_id=([\"'])(ou_[A-Za-z0-9_-]+)\1\s*>.*?</at>",
    re.DOTALL,
)


@dataclass(frozen=True)
class NativeAtParagraph:
    text: str
    target_open_ids: tuple[str, ...]


def _strip_fenced_code_blocks(text: str) -> str:
    output_lines: list[str] = []
    in_fence = False
    for line in text.splitlines():
        if line.strip().startswith("```"):
            in_fence = not in_fence
            continue
        if not in_fence:
            output_lines.append(line)
    return "\n".join(output_lines)


def _split_paragraphs(text: str) -> list[str]:
    paragraphs: list[str] = []
    current: list[str] = []
    for line in text.splitlines():
        if line.strip():
            current.append(line)
            continue
        if current:
            paragraphs.append("\n".join(current).strip())
            current = []
    if current:
        paragraphs.append("\n".join(current).strip())
    return paragraphs


def _normalize_native_at_paragraph(text: str) -> str:
    return "\n".join(line.rstrip() for line in text.strip().splitlines())


def extract_native_at_paragraphs(text: str, *, max_messages: int = 5) -> list[NativeAtParagraph]:
    if max_messages <= 0:
        return []

    cleaned = _strip_fenced_code_blocks(text or "")
    selected: list[NativeAtParagraph] = []
    seen: set[str] = set()

    for paragraph in _split_paragraphs(cleaned):
        matches = list(_NATIVE_AT_TAG_RE.finditer(paragraph))
        if not matches:
            continue
        normalized = _normalize_native_at_paragraph(paragraph)
        if normalized in seen:
            continue
        seen.add(normalized)
        selected.append(
            NativeAtParagraph(
                text=paragraph,
                target_open_ids=tuple(match.group(2) for match in matches),
            )
        )
        if len(selected) >= max_messages:
            break

    return selected
```

- [ ] **Step 4: Run the parser tests and verify they pass**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py::test_native_at_extractor_selects_legal_paragraphs_in_order \
  tests/gateway/test_feishu_card_stream.py::test_native_at_extractor_keeps_multiple_mentions_in_one_paragraph \
  tests/gateway/test_feishu_card_stream.py::test_native_at_extractor_dedupes_by_normalized_paragraph_not_target \
  tests/gateway/test_feishu_card_stream.py::test_native_at_extractor_ignores_code_blocks_and_illegal_aliases \
  tests/gateway/test_feishu_card_stream.py::test_native_at_extractor_applies_message_cap -q
```

Expected: PASS.

- [ ] **Step 5: Checkpoint parser work**

```bash
git diff --stat
git status --short
```

## Task 2: Add NativeBotAtForwarder Tests And Implementation

**Files:**
- Modify: `tests/gateway/test_feishu_card_stream.py`
- Modify: `gateway/platforms/feishu_card_stream.py`
- Test: `tests/gateway/test_feishu_card_stream.py`

- [ ] **Step 1: Add failing forwarder tests**

Append these tests after the extractor tests in `tests/gateway/test_feishu_card_stream.py`:

```python
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
```

- [ ] **Step 2: Run the forwarder tests and verify they fail**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py::test_native_at_forwarder_sends_each_selected_paragraph \
  tests/gateway/test_feishu_card_stream.py::test_native_at_forwarder_disabled_sends_nothing \
  tests/gateway/test_feishu_card_stream.py::test_native_at_forwarder_failure_does_not_raise -q
```

Expected: FAIL because `NativeBotAtForwarder` does not exist.

- [ ] **Step 3: Add `NativeBotAtForwarder`**

Add this class after `extract_native_at_paragraphs(...)` in `gateway/platforms/feishu_card_stream.py`:

```python
class NativeBotAtForwarder:
    def __init__(
        self,
        *,
        adapter: Any,
        chat_id: str,
        metadata: dict[str, Any] | None = None,
        reply_to: str | None = None,
        enabled: bool = False,
        max_messages: int = 5,
    ):
        self.adapter = adapter
        self.chat_id = chat_id
        self.metadata = metadata
        self.reply_to = reply_to
        self.enabled = enabled
        self.max_messages = max(0, int(max_messages))

    async def forward_from_final_text(self, text: str) -> int:
        if not self.enabled or not text or not hasattr(self.adapter, "send_raw_text"):
            return 0

        paragraphs = extract_native_at_paragraphs(text, max_messages=self.max_messages)
        sent = 0
        for paragraph in paragraphs:
            try:
                result = await self.adapter.send_raw_text(
                    self.chat_id,
                    paragraph.text,
                    metadata=self.metadata,
                    reply_to=self.reply_to,
                )
            except Exception as exc:
                logger.warning(
                    "feishu_native_bot_at_forward_failed chat_id=%s targets=%s error=%s",
                    self.chat_id,
                    ",".join(paragraph.target_open_ids),
                    exc,
                )
                continue
            if getattr(result, "success", False):
                sent += 1
                logger.info(
                    "feishu_native_bot_at_forward_sent chat_id=%s targets=%s",
                    self.chat_id,
                    ",".join(paragraph.target_open_ids),
                )
            else:
                logger.warning(
                    "feishu_native_bot_at_forward_failed chat_id=%s targets=%s error=%s",
                    self.chat_id,
                    ",".join(paragraph.target_open_ids),
                    getattr(result, "error", None) or "unknown error",
                )
        return sent
```

- [ ] **Step 4: Run the forwarder tests and verify they pass**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py::test_native_at_forwarder_sends_each_selected_paragraph \
  tests/gateway/test_feishu_card_stream.py::test_native_at_forwarder_disabled_sends_nothing \
  tests/gateway/test_feishu_card_stream.py::test_native_at_forwarder_failure_does_not_raise -q
```

Expected: PASS.

- [ ] **Step 5: Checkpoint forwarder work**

```bash
git diff --stat
git status --short
```

## Task 3: Add Raw Answer Boundary Tests And Accessor

**Files:**
- Modify: `tests/gateway/test_feishu_card_stream.py`
- Modify: `gateway/platforms/feishu_card_stream.py`
- Test: `tests/gateway/test_feishu_card_stream.py`

- [ ] **Step 1: Add failing raw answer tests**

Append these tests near existing state tests in `tests/gateway/test_feishu_card_stream.py`:

```python
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
```

- [ ] **Step 2: Run the raw answer tests and verify they fail**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py::test_raw_answer_text_returns_only_final_answer_block \
  tests/gateway/test_feishu_card_stream.py::test_raw_answer_text_is_not_renderer_content -q
```

Expected: FAIL with `AttributeError` because `raw_answer_text` does not exist.

- [ ] **Step 3: Add `FeishuCardRunState.raw_answer_text()`**

Add this method to `FeishuCardRunState` after `_ensure_answer_block(...)`:

```python
    def raw_answer_text(self) -> str:
        if self.answer_block_id is None:
            return ""
        for block in self.blocks:
            if block.block_id == self.answer_block_id and block.kind == "answer":
                return block.content
        return ""
```

- [ ] **Step 4: Run the raw answer tests and verify they pass**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py::test_raw_answer_text_returns_only_final_answer_block \
  tests/gateway/test_feishu_card_stream.py::test_raw_answer_text_is_not_renderer_content -q
```

Expected: PASS.

- [ ] **Step 5: Checkpoint raw answer boundary work**

```bash
git diff --stat
git status --short
```

## Task 4: Add Feishu Adapter Raw Text Transport

**Files:**
- Modify: `tests/gateway/test_feishu_card_transport.py`
- Modify: `gateway/platforms/feishu.py`
- Test: `tests/gateway/test_feishu_card_transport.py`

- [ ] **Step 1: Add failing `send_raw_text` transport test**

Append this test to `tests/gateway/test_feishu_card_transport.py`:

```python
@pytest.mark.asyncio
async def test_send_raw_text_forces_text_payload_and_preserves_native_at(monkeypatch):
    adapter = FeishuAdapter(PlatformConfig())
    adapter._client = _FakeClient()
    sent = {}

    async def fake_send_with_retry(*, chat_id, msg_type, payload, reply_to=None, metadata=None):
        sent["chat_id"] = chat_id
        sent["msg_type"] = msg_type
        sent["payload"] = payload
        sent["reply_to"] = reply_to
        sent["metadata"] = metadata
        return SimpleNamespace(success=lambda: True, code=0, data=SimpleNamespace(message_id="om_raw"))

    def fail_build_outbound_payload(content):
        raise AssertionError("_build_outbound_payload must not be used for native at forwarding")

    monkeypatch.setattr(adapter, "_feishu_send_with_retry", fake_send_with_retry)
    monkeypatch.setattr(adapter, "_build_outbound_payload", fail_build_outbound_payload)

    raw = "<at user_id=\"ou_target\">小P</at> 请接手。"
    result = await adapter.send_raw_text(
        "oc_1",
        raw,
        metadata={"thread_id": "omt_1"},
        reply_to="om_parent",
    )

    assert result.success is True
    assert result.message_id == "om_raw"
    assert sent == {
        "chat_id": "oc_1",
        "msg_type": "text",
        "payload": json.dumps({"text": raw}, ensure_ascii=False),
        "reply_to": "om_parent",
        "metadata": {"thread_id": "omt_1"},
    }
```

- [ ] **Step 2: Run the transport test and verify it fails**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_transport.py::test_send_raw_text_forces_text_payload_and_preserves_native_at -q
```

Expected: FAIL with `AttributeError` because `send_raw_text` does not exist.

- [ ] **Step 3: Implement `FeishuAdapter.send_raw_text(...)`**

Add this method in `gateway/platforms/feishu.py` immediately after `send(...)` and before `edit_message(...)`:

```python
    async def send_raw_text(
        self,
        chat_id: str,
        text: str,
        *,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a Feishu text message without markdown/post conversion."""
        if not self._client:
            return SendResult(success=False, error="Not connected")

        try:
            response = await self._feishu_send_with_retry(
                chat_id=chat_id,
                msg_type="text",
                payload=json.dumps({"text": text}, ensure_ascii=False),
                reply_to=reply_to,
                metadata=metadata,
            )
            return self._finalize_send_result(response, "send raw text failed")
        except Exception as exc:
            logger.error("[Feishu] Raw text send error: %s", exc, exc_info=True)
            return SendResult(success=False, error=str(exc))
```

- [ ] **Step 4: Run the transport test and verify it passes**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_transport.py::test_send_raw_text_forces_text_payload_and_preserves_native_at -q
```

Expected: PASS.

- [ ] **Step 5: Checkpoint raw text transport**

```bash
git diff --stat
git status --short
```

## Task 5: Integrate Forwarder Into Card Sink Finalization

**Files:**
- Modify: `tests/gateway/test_feishu_card_stream.py`
- Modify: `gateway/platforms/feishu_card_stream.py`
- Test: `tests/gateway/test_feishu_card_stream.py`

- [ ] **Step 1: Extend the fake adapter for raw text forwarding**

In `_FakeFeishuCardAdapter` in `tests/gateway/test_feishu_card_stream.py`, add `raw_text` storage and `send_raw_text(...)`:

```python
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
```

- [ ] **Step 2: Add failing sink integration tests**

Append these tests near existing sink finalization tests in `tests/gateway/test_feishu_card_stream.py`:

```python
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
```

- [ ] **Step 3: Run the sink integration tests and verify they fail**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py::test_sink_forwards_native_at_after_successful_final_card_update \
  tests/gateway/test_feishu_card_stream.py::test_sink_forwards_multiple_native_at_paragraphs_after_final_card_update \
  tests/gateway/test_feishu_card_stream.py::test_sink_does_not_forward_native_at_when_feature_disabled \
  tests/gateway/test_feishu_card_stream.py::test_sink_native_forward_failure_does_not_break_card_delivery -q
```

Expected: FAIL because `FeishuCardRunSink.__init__` does not accept the native forwarding arguments.

- [ ] **Step 4: Add sink constructor options and forwarder instance**

Update `FeishuCardRunSink.__init__(...)` in `gateway/platforms/feishu_card_stream.py` to accept the new options:

```python
        native_bot_at_forward: bool = False,
        native_bot_at_forward_max_messages: int = 5,
```

Add this field initialization after existing state/renderer setup:

```python
        self.native_at_forwarder = NativeBotAtForwarder(
            adapter=self.adapter,
            chat_id=self.chat_id,
            metadata=self.metadata,
            reply_to=self.reply_to,
            enabled=native_bot_at_forward,
            max_messages=native_bot_at_forward_max_messages,
        )
```

- [ ] **Step 5: Add finalization forwarding helper**

Add this method to `FeishuCardRunSink` after `_build_fallback_text(...)`:

```python
    async def _forward_native_bot_at_from_raw_answer(self) -> None:
        raw_answer = self.state.raw_answer_text()
        if not raw_answer:
            return
        await self.native_at_forwarder.forward_from_final_text(raw_answer)
```

- [ ] **Step 6: Call the helper after successful final card delivery**

In `FeishuCardRunSink.finalize(...)`, update the successful `flush()` path:

```python
        if await self.flush():
            await self._forward_native_bot_at_from_raw_answer()
            self._close()
            self.final_response_sent = True
            self.final_content_delivered = True
            return True
```

In `FeishuCardRunSink.update_final_after_transform(...)`, update the successful `flush()` path:

```python
        if self.update_handle and await self.flush():
            await self._forward_native_bot_at_from_raw_answer()
            self._close()
            self.final_response_sent = True
            self.final_content_delivered = True
            return True
```

Leave fallback text behavior unchanged. Native forwarding is tied to the completed card-stream success path.

- [ ] **Step 7: Run the sink integration tests and verify they pass**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py::test_sink_forwards_native_at_after_successful_final_card_update \
  tests/gateway/test_feishu_card_stream.py::test_sink_forwards_multiple_native_at_paragraphs_after_final_card_update \
  tests/gateway/test_feishu_card_stream.py::test_sink_does_not_forward_native_at_when_feature_disabled \
  tests/gateway/test_feishu_card_stream.py::test_sink_native_forward_failure_does_not_break_card_delivery -q
```

Expected: PASS.

- [ ] **Step 8: Checkpoint sink integration**

```bash
git diff --stat
git status --short
```

## Task 6: Add Config Routing For The Feature Gate

**Files:**
- Modify: `tests/gateway/test_feishu_card_stream_runner.py`
- Modify: `gateway/run.py`
- Test: `tests/gateway/test_feishu_card_stream_runner.py`

- [ ] **Step 1: Add failing config helper tests**

Append these tests near the existing Feishu card streaming config tests in `tests/gateway/test_feishu_card_stream_runner.py`:

```python
def test_native_bot_at_forwarding_config_defaults_disabled():
    from gateway.run import _feishu_native_bot_at_forwarding_config

    enabled, max_messages = _feishu_native_bot_at_forwarding_config({})

    assert enabled is False
    assert max_messages == 5


def test_native_bot_at_forwarding_config_reads_feishu_display_platform():
    from gateway.run import _feishu_native_bot_at_forwarding_config

    enabled, max_messages = _feishu_native_bot_at_forwarding_config(
        {
            "display": {
                "platforms": {
                    "feishu": {
                        "native_bot_at_forward": True,
                        "native_bot_at_forward_max_messages": 3,
                    }
                }
            }
        }
    )

    assert enabled is True
    assert max_messages == 3


def test_native_bot_at_forwarding_config_clamps_invalid_cap_to_default():
    from gateway.run import _feishu_native_bot_at_forwarding_config

    enabled, max_messages = _feishu_native_bot_at_forwarding_config(
        {
            "display": {
                "platforms": {
                    "feishu": {
                        "native_bot_at_forward": True,
                        "native_bot_at_forward_max_messages": "not-a-number",
                    }
                }
            }
        }
    )

    assert enabled is True
    assert max_messages == 5
```

- [ ] **Step 2: Run the config helper tests and verify they fail**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream_runner.py::test_native_bot_at_forwarding_config_defaults_disabled \
  tests/gateway/test_feishu_card_stream_runner.py::test_native_bot_at_forwarding_config_reads_feishu_display_platform \
  tests/gateway/test_feishu_card_stream_runner.py::test_native_bot_at_forwarding_config_clamps_invalid_cap_to_default -q
```

Expected: FAIL because `_feishu_native_bot_at_forwarding_config` does not exist.

- [ ] **Step 3: Add config helper in `gateway/run.py`**

Add this helper after `_should_use_feishu_card_streaming(...)`:

```python
def _feishu_native_bot_at_forwarding_config(user_config: dict) -> tuple[bool, int]:
    display = user_config.get("display") if isinstance(user_config, dict) else {}
    platforms = display.get("platforms") if isinstance(display, dict) else {}
    feishu = platforms.get("feishu") if isinstance(platforms, dict) else {}
    if not isinstance(feishu, dict):
        return False, 5

    enabled = feishu.get("native_bot_at_forward") is True
    raw_max = feishu.get("native_bot_at_forward_max_messages", 5)
    try:
        max_messages = int(raw_max)
    except (TypeError, ValueError):
        max_messages = 5
    if max_messages < 0:
        max_messages = 0
    return enabled, max_messages
```

- [ ] **Step 4: Pass config into `FeishuCardRunSink`**

In `gateway/run.py`, immediately before constructing `FeishuCardRunSink`, compute:

```python
                        (
                            _native_bot_at_forward,
                            _native_bot_at_forward_max_messages,
                        ) = _feishu_native_bot_at_forwarding_config(user_config)
```

Then pass the values into the constructor:

```python
                            native_bot_at_forward=_native_bot_at_forward,
                            native_bot_at_forward_max_messages=_native_bot_at_forward_max_messages,
```

The resulting constructor block should still pass `adapter`, `chat_id`, `metadata`, `reply_to`, and `loop` exactly as before.

- [ ] **Step 5: Run the config helper tests and verify they pass**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream_runner.py::test_native_bot_at_forwarding_config_defaults_disabled \
  tests/gateway/test_feishu_card_stream_runner.py::test_native_bot_at_forwarding_config_reads_feishu_display_platform \
  tests/gateway/test_feishu_card_stream_runner.py::test_native_bot_at_forwarding_config_clamps_invalid_cap_to_default -q
```

Expected: PASS.

- [ ] **Step 6: Checkpoint config routing**

```bash
git diff --stat
git status --short
```

## Task 7: Add Answer-Only Human-Readable Card Display Rewrite

**Files:**
- Modify: `tests/gateway/test_feishu_card_stream.py`
- Modify: `gateway/platforms/feishu_card_stream.py`
- Test: `tests/gateway/test_feishu_card_stream.py`

- [ ] **Step 1: Add failing display rewrite tests**

Append these tests in `tests/gateway/test_feishu_card_stream.py`:

```python
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
```

- [ ] **Step 2: Run display rewrite tests and verify they fail**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py::test_card_display_rewrites_single_native_at_to_human_text \
  tests/gateway/test_feishu_card_stream.py::test_card_display_rewrites_multiple_native_ats_to_human_text \
  tests/gateway/test_feishu_card_stream.py::test_card_display_rewrite_ignores_incomplete_native_at \
  tests/gateway/test_feishu_card_stream.py::test_raw_answer_survives_human_display_rewrite \
  tests/gateway/test_feishu_card_stream.py::test_human_display_rewrite_does_not_apply_to_process_footer_or_tool_text -q
```

Expected: FAIL because `render_native_at_as_human_text` does not exist and renderer does not rewrite answer display text.

- [ ] **Step 3: Implement display-only rewrite helper**

Add this function after the native at regex in `gateway/platforms/feishu_card_stream.py`:

```python
def render_native_at_as_human_text(text: str) -> str:
    def rewrite_paragraph(paragraph: str) -> str:
        matches = list(_NATIVE_AT_TAG_RE.finditer(paragraph))
        if not matches:
            return paragraph
        names = [at_match.group(3).strip() for at_match in matches]
        cleaned = _NATIVE_AT_TAG_RE.sub("", paragraph).strip()
        if not cleaned:
            return f"对 {'、'.join(names)} 说"
        return f"对 {'、'.join(names)} 说，{cleaned}"

    paragraphs = _split_paragraphs(text or "")
    if not paragraphs:
        return text or ""
    rewritten: list[str] = []
    for paragraph in paragraphs:
        if _NATIVE_AT_TAG_RE.search(paragraph):
            rewritten.append(rewrite_paragraph(paragraph))
        else:
            rewritten.append(paragraph)
    return "\n\n".join(rewritten)
```

Adjust `_NATIVE_AT_TAG_RE` from Task 1 so it captures the display label:

```python
_NATIVE_AT_TAG_RE = re.compile(
    r"<at\s+user_id=([\"'])(ou_[A-Za-z0-9_-]+)\1\s*>(.*?)</at>",
    re.DOTALL,
)
```

Update the extractor's target id access from `match.group(2)` to keep using the
open id capture. Do not change `FeishuCardRunState.raw_answer_text()`.

- [ ] **Step 4: Apply rewrite only while rendering answer blocks**

In `FeishuCardRunRenderer.content(...)`, do not wrap the final joined mixed
string. Instead, rewrite only the answer block display copy:

```python
            rendered_block_content = block.content
            if block.kind == "answer":
                rendered_block_content = render_native_at_as_human_text(block.content)
            content_parts.append(rendered_block_content)
```

Do not apply this to process, tool, footer, placeholder, or
`pending_model_segment`. Incomplete stream chunks remain unchanged until they are
classified into an answer block and contain a complete native at tag.

- [ ] **Step 5: Run display rewrite tests and verify they pass**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py::test_card_display_rewrites_single_native_at_to_human_text \
  tests/gateway/test_feishu_card_stream.py::test_card_display_rewrites_multiple_native_ats_to_human_text \
  tests/gateway/test_feishu_card_stream.py::test_card_display_rewrite_ignores_incomplete_native_at \
  tests/gateway/test_feishu_card_stream.py::test_raw_answer_survives_human_display_rewrite \
  tests/gateway/test_feishu_card_stream.py::test_human_display_rewrite_does_not_apply_to_process_footer_or_tool_text -q
```

Expected: PASS.

- [ ] **Step 6: Checkpoint display rewrite**

```bash
git diff --stat
git status --short
```

## Task 8: Full Regression Verification

**Files:**
- Verify: `gateway/platforms/feishu_card_stream.py`
- Verify: `gateway/platforms/feishu.py`
- Verify: `gateway/run.py`
- Verify: `tests/gateway/test_feishu_card_stream.py`
- Verify: `tests/gateway/test_feishu_card_stream_runner.py`
- Verify: `tests/gateway/test_feishu_card_transport.py`

- [ ] **Step 1: Run focused Feishu card tests**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py \
  tests/gateway/test_feishu_card_stream_runner.py \
  tests/gateway/test_feishu_card_transport.py -q
```

Expected: all tests pass.

- [ ] **Step 2: Run broader Feishu adapter tests**

Run:

```bash
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu.py \
  tests/gateway/test_feishu_card_stream.py \
  tests/gateway/test_feishu_card_stream_runner.py \
  tests/gateway/test_feishu_card_transport.py -q
```

Expected: all tests pass. If unrelated pre-existing failures appear in `tests/gateway/test_feishu.py`, capture the failing test names and rerun the focused Feishu card tests from Step 1 before reporting.

- [ ] **Step 3: Inspect the final diff for the agreed boundaries**

Run:

```bash
git diff -- gateway/platforms/feishu_card_stream.py gateway/platforms/feishu.py gateway/run.py tests/gateway/test_feishu_card_stream.py tests/gateway/test_feishu_card_stream_runner.py tests/gateway/test_feishu_card_transport.py
```

Expected in diff:

- `NativeBotAtForwarder` reads `state.raw_answer_text()`, not `renderer.content(...)`.
- `send_raw_text(...)` sends `msg_type="text"`.
- `render_native_at_as_human_text(...)` is display-only and applies only to answer block display copies.
- Multiple at paragraphs are capped and forwarded in order.
- Feature is gated by `display.platforms.feishu.native_bot_at_forward`.

- [ ] **Step 4: Checkpoint final verification state**

```bash
git status --short
git diff --stat
```

## Self-Review

**Spec coverage**

- Verified behavior is covered by Task 4 `send_raw_text(...)` and Task 5 sink forwarding.
- Post-final one-shot behavior is covered by Task 5. There is no stream delta scanner.
- Raw answer boundary is covered by Task 3 and used by Task 5.
- Multiple at messages are covered by Task 1 and Task 5.
- Native `msg_type="text"` trigger is covered by Task 4.
- Human-readable answer display rewrite is covered by Task 7.
- Config gating and cap are covered by Task 6.
- Failure behavior is covered by Task 2 and Task 5.

**Stub-content scan**

- The plan contains no stub implementation steps.
- Every code-changing step includes concrete code or exact insertion text.
- Every verification step has an exact command and expected result.

**Type consistency**

- `NativeAtParagraph.text` and `NativeAtParagraph.target_open_ids` are defined in Task 1 and used consistently in Tasks 1 and 2.
- `NativeBotAtForwarder.forward_from_final_text(...)` returns an integer send count in Tasks 2 and 5.
- `FeishuCardRunState.raw_answer_text()` is defined in Task 3 and used in Task 5.
- `FeishuAdapter.send_raw_text(...)` signature matches the fake adapter methods in Tasks 2 and 5.
- `native_bot_at_forward` and `native_bot_at_forward_max_messages` constructor names match the `gateway/run.py` config routing in Task 6.
