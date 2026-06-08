import json
from types import SimpleNamespace

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.feishu import FeishuAdapter


class _FakeCardApi:
    def __init__(self, card_id="card_1", create_success=True, create_code=0, create_msg=""):
        self.card_id = card_id
        self.create_success = create_success
        self.create_code = create_code
        self.create_msg = create_msg
        self.created = []
        self.updated = []
        self.converted = []

    async def acreate(self, request):
        self.created.append(request)
        return SimpleNamespace(
            success=lambda: self.create_success,
            code=self.create_code,
            msg=self.create_msg,
            data=SimpleNamespace(card_id=self.card_id),
        )

    async def aupdate(self, request):
        self.updated.append(request)
        return SimpleNamespace(success=lambda: True, code=0)

    async def aid_convert(self, request):
        self.converted.append(request)
        return SimpleNamespace(
            success=lambda: True,
            code=0,
            data=SimpleNamespace(card_id="legacy_card_1"),
        )


class _FakeCardElementApi:
    def __init__(self):
        self.content_updates = []

    async def acontent(self, request):
        self.content_updates.append(request)
        return SimpleNamespace(success=lambda: True, code=0)


class _FakeCardKit:
    def __init__(self, card_id="card_1", create_success=True, create_code=0, create_msg=""):
        self.v1 = SimpleNamespace(
            card=_FakeCardApi(
                card_id=card_id,
                create_success=create_success,
                create_code=create_code,
                create_msg=create_msg,
            ),
            card_element=_FakeCardElementApi(),
        )


class _FakeClient:
    def __init__(self, card_id="card_1", create_success=True, create_code=0, create_msg=""):
        self.cardkit = _FakeCardKit(
            card_id=card_id,
            create_success=create_success,
            create_code=create_code,
            create_msg=create_msg,
        )


def _field_value(request, field):
    if hasattr(request, field):
        return getattr(request, field)
    if hasattr(request, f"_{field}"):
        return getattr(request, f"_{field}")
    return None


@pytest.mark.asyncio
async def test_card_stream_create_uses_cardkit_create_then_card_id_reference(monkeypatch):
    adapter = FeishuAdapter(PlatformConfig())
    adapter._client = _FakeClient()
    sent = {}

    async def fake_send_with_retry(*, chat_id, msg_type, payload, reply_to=None, metadata=None):
        sent["chat_id"] = chat_id
        sent["msg_type"] = msg_type
        sent["payload"] = payload
        sent["reply_to"] = reply_to
        return SimpleNamespace(success=lambda: True, code=0, data=SimpleNamespace(message_id="om_1"))

    monkeypatch.setattr(adapter, "_feishu_send_with_retry", fake_send_with_retry)
    monkeypatch.setattr(adapter, "_response_succeeded", lambda response: True)
    monkeypatch.setattr(
        adapter,
        "_extract_response_field",
        lambda response, field: "om_1" if field == "message_id" else None,
    )

    result = await adapter.create_card_stream_message(
        "oc_1",
        {"schema": "2.0", "body": {"elements": []}},
    )

    assert result.success is True
    assert result.message_id == "om_1"
    assert result.update_handle == "card_1"
    assert adapter._client.cardkit.v1.card.created
    assert adapter._client.cardkit.v1.card.converted == []
    assert sent["msg_type"] == "interactive"
    assert json.loads(sent["payload"]) == {"type": "card", "data": {"card_id": "card_1"}}


@pytest.mark.asyncio
async def test_card_stream_create_fails_when_cardkit_create_returns_no_card_id(monkeypatch):
    adapter = FeishuAdapter(PlatformConfig())
    adapter._client = _FakeClient(card_id="")
    sent = []

    async def fake_send_with_retry(**kwargs):
        sent.append(kwargs)
        return SimpleNamespace(success=lambda: True, code=0, data=SimpleNamespace(message_id="om_1"))

    monkeypatch.setattr(adapter, "_feishu_send_with_retry", fake_send_with_retry)

    result = await adapter.create_card_stream_message(
        "oc_1",
        {"schema": "2.0", "body": {"elements": []}},
    )

    assert result.success is False
    assert "card_id" in result.error
    assert getattr(result, "update_handle", None) is None
    assert sent == []


@pytest.mark.asyncio
async def test_card_stream_create_error_includes_cardkit_code_and_message(monkeypatch):
    adapter = FeishuAdapter(PlatformConfig())
    adapter._client = _FakeClient(create_success=False, create_code=230099, create_msg="permission denied")
    sent = []

    async def fake_send_with_retry(**kwargs):
        sent.append(kwargs)
        return SimpleNamespace(success=lambda: True, code=0, data=SimpleNamespace(message_id="om_1"))

    monkeypatch.setattr(adapter, "_feishu_send_with_retry", fake_send_with_retry)

    result = await adapter.create_card_stream_message(
        "oc_1",
        {"schema": "2.0", "body": {"elements": []}},
    )

    assert result.success is False
    assert "230099" in result.error
    assert "permission denied" in result.error
    assert sent == []


@pytest.mark.asyncio
async def test_card_stream_updates_use_card_id_not_message_id():
    adapter = FeishuAdapter(PlatformConfig())
    adapter._client = _FakeClient()

    result = await adapter.update_card_stream_message("card_1", {"schema": "2.0"}, sequence=7)

    assert result.success is True
    updated = adapter._client.cardkit.v1.card.updated
    assert updated
    assert _field_value(updated[0], "card_id") == "card_1"


@pytest.mark.asyncio
async def test_card_stream_text_updates_use_card_id_not_message_id():
    adapter = FeishuAdapter(PlatformConfig())
    adapter._client = _FakeClient()

    result = await adapter.update_card_stream_text("card_1", "stream_md", "hello", sequence=8)

    assert result.success is True
    content_updates = adapter._client.cardkit.v1.card_element.content_updates
    assert content_updates
    assert _field_value(content_updates[0], "card_id") == "card_1"
    assert _field_value(content_updates[0], "element_id") == "stream_md"
