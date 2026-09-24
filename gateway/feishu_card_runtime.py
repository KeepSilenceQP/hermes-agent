from __future__ import annotations
import logging
import os

logger = logging.getLogger(__name__)

def _should_use_feishu_card_streaming(*, platform_key: str, user_config: dict) -> bool:
    """Return True when Feishu card streaming is explicitly enabled."""
    if platform_key != "feishu":
        return False
    display = user_config.get("display") if isinstance(user_config, dict) else {}
    streaming = user_config.get("streaming") if isinstance(user_config, dict) else {}
    platforms = display.get("platforms") if isinstance(display, dict) else {}
    feishu = platforms.get("feishu") if isinstance(platforms, dict) else {}
    return bool(
        isinstance(display, dict)
        and display.get("streaming") is True
        and isinstance(streaming, dict)
        and streaming.get("enabled") is True
        and streaming.get("transport", "auto") != "off"
        and isinstance(feishu, dict)
        and feishu.get("card_streaming") is True
    )


def _feishu_native_bot_at_forwarding_config(user_config: dict) -> tuple[bool, int]:
    display = user_config.get("display") if isinstance(user_config, dict) else {}
    platforms = display.get("platforms") if isinstance(display, dict) else {}
    feishu = platforms.get("feishu") if isinstance(platforms, dict) else {}
    if not isinstance(feishu, dict):
        return False, 5
    enabled = feishu.get("native_bot_at_forward") is True
    try:
        max_messages = int(feishu.get("native_bot_at_forward_max_messages", 5))
    except (TypeError, ValueError):
        max_messages = 5
    return enabled, max(0, max_messages)


def _card_sink_delivered_final(sink: object | None) -> bool:
    if sink is None:
        return False
    return bool(
        getattr(sink, "final_response_sent", False)
        or getattr(sink, "final_content_delivered", False)
        or getattr(sink, "fallback_sent", False)
    )


def _should_send_trailing_runtime_footer(
    *, footer_line: str, agent_result: dict,
) -> bool:
    """Return True unless the streaming transport embedded the footer."""
    return bool(footer_line and not agent_result.get("runtime_footer_delivered"))


def _emit_feishu_card_lifecycle(
    sink: object | None,
    text: str,
    *,
    logger_obj: logging.Logger | None = None,
) -> bool:
    if sink is None or not str(text or "").strip():
        return False
    try:
        sink.on_commentary(str(text).strip())
        flush = getattr(sink, "flush_threadsafe", None)
        if callable(flush):
            flush()
        return True
    except Exception as exc:
        (logger_obj or logger).debug("feishu_card_lifecycle_emit_failed: %s", exc)
        return False


def _route_feishu_card_tool_progress(
    sink: object | None,
    event_type: str,
    tool_name: str | None = None,
    preview: str | None = None,
    args: dict | None = None,
    *,
    logger_obj=None,
    **kwargs,
) -> bool:
    if sink is None or event_type not in {
        "tool.started",
        "tool.completed",
        "_thinking",
        "reasoning.available",
    }:
        return False
    try:
        sink.on_tool_progress(event_type, tool_name, preview, args, **kwargs)
        flush_now = getattr(sink, "flush_threadsafe", None)
        if callable(flush_now):
            flush_now()
        if logger_obj is not None:
            logger_obj.info(
                "feishu_card_tool_progress_routed event=%s tool=%s",
                event_type,
                tool_name,
            )
        return True
    except Exception:
        if logger_obj is not None:
            logger_obj.warning(
                "feishu_card_tool_progress_route_failed event=%s tool=%s",
                event_type,
                tool_name,
                exc_info=True,
            )
        return True


def _should_attach_tool_progress_callback(
    *, tool_progress_enabled: bool, want_feishu_card_streaming: bool,
) -> bool:
    """Keep the tool-progress callback attached when card streaming needs it."""
    return bool(tool_progress_enabled or want_feishu_card_streaming)


def _should_attach_interim_callback(
    *, want_interim_messages: bool, want_feishu_card_streaming: bool,
) -> bool:
    """Keep interim commentary attached when the card sink consumes it."""
    return bool(want_interim_messages or want_feishu_card_streaming)


def _should_create_gateway_stream_consumer(
    *,
    streaming_enabled: bool,
    want_interim_messages: bool,
    want_feishu_card_streaming: bool,
) -> bool:
    return bool(
        (streaming_enabled or want_interim_messages)
        and not want_feishu_card_streaming
    )



async def finalize_feishu_card(response, turn_ctx):
    source = turn_ctx.source
    # Finalize a Feishu streaming card before the ordinary delivery path
    # decides whether another final message is still necessary.
    _card_sink = (
        turn_ctx.feishu_card_sink_holder[0]
    )
    _final_raw = (
        response.get("final_response") if isinstance(response, dict) else None
    )
    _is_empty_card_final = not _final_raw or _final_raw == "(empty)"
    if (
        _card_sink is not None
        and isinstance(response, dict)
        and not response.get("failed")
        and not _is_empty_card_final
    ):
        _final = _final_raw or ""
        _card_footer_line = ""
        try:
            from gateway.runtime_footer import build_footer_line as _bfl

            _card_footer_line = _bfl(
                user_config=turn_ctx.user_config,
                platform_key=source.platform.value,
                model=response.get("model"),
                context_tokens=response.get("last_prompt_tokens", 0) or 0,
                context_length=response.get("context_length") or None,
                cwd=os.environ.get("TERMINAL_CWD", ""),
            )
        except Exception as _footer_err:
            logger.debug(
                "runtime_footer build for Feishu card failed: %s",
                _footer_err,
            )
        if _card_footer_line:
            _card_sink.on_footer(_card_footer_line)
            await _card_sink.drain_pending_updates()
            response["runtime_footer_delivered"] = True
        try:
            if response.get("response_transformed"):
                _card_delivered_now = await _card_sink.update_final_after_transform(
                    _final
                )
            else:
                _card_delivered_now = await _card_sink.finalize(_final)
            if _card_delivered_now:
                response["already_sent"] = True
        except Exception as _card_err:
            logger.warning("Feishu card finalize failed: %s", _card_err)
    elif (
        _card_sink is not None
        and isinstance(response, dict)
        and response.get("failed")
    ):
        error_text = (
            response.get("final_response")
            or response.get("error")
            or "Agent failed"
        )
        try:
            if await _card_sink.finish_failed(error_text):
                response["already_sent"] = True
        except Exception as _card_err:
            logger.warning("Feishu card error delivery failed: %s", _card_err)

