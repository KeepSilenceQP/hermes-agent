# Hermes Feishu Card-Backed Streaming Design

## Problem

Qin Peng wants Hermes / 小A replies in Feishu to behave like the current lark-channel-bridge experience: one CardKit card represents the whole agent run, assistant text streams into that card, and tool-call state is contained in the same card instead of appearing as separate progress messages.

Hermes currently has enough low-level pieces to send Feishu messages and edit text/post messages. It also has structured stream-event modules, but those modules are not wired into the production Feishu runner path. The production path still uses direct agent callbacks in `gateway/run.py`.

The first implementation must therefore connect a Feishu card run sink to the current production callbacks before implementing card JSON rendering.

## Goals

- Render one Feishu CardKit card per Hermes agent run when enabled for Feishu.
- Stream assistant text into the same card.
- Show tool-call state in the same card.
- For the first version, show only tool name, argument summary, and running/done/error state. Do not render full tool output.
- Keep the existing Feishu text/post send and edit path as the fallback.
- Keep the change Feishu-only and off by default.

## Non-Goals

- Do not upgrade `lark-oapi` as the primary solution.
- Do not change Telegram, Weixin, Slack, CLI, or API server rendering.
- Do not expose full tool output in the Feishu card in the first version.
- Do not persist card-rendered tool state into conversation history.
- Do not redesign Hermes's whole gateway streaming architecture.

## Current Evidence

- `gateway/platforms/feishu.py` sends text/post messages through `send()` and edits them through `edit_message()` using `message.update`.
- Feishu interactive cards exist today for approval and update prompts, not as the general assistant-run renderer.
- `gateway/stream_events.py` defines presentation events such as `MessageChunk`, `Commentary`, `ToolCallChunk`, and `ToolCallFinished`, but these events are not currently wired into the production `gateway/run.py` Feishu path.
- `gateway/stream_dispatch.py` exists as a tested dispatcher, but production Feishu traffic still uses `stream_delta_callback`, `tool_progress_callback`, and `interim_assistant_callback` assigned directly in `gateway/run.py`.
- The existing `progress_callback` mostly ignores `tool.completed` except for long-tool hints. A card renderer that wants done/error state must define its own mapping from the current callbacks.
- lark-channel-bridge solves the same UX by maintaining a run state, reducing text/tool events into that state, rendering a CardKit 2.0 card, and updating the same card as state changes.

## Chosen Approach

Implement a Feishu-only card-backed streaming path inside the Feishu platform boundary and wire it to the current production callbacks in `gateway/run.py`.

The Feishu adapter should own CardKit-specific details: card creation, card update, card JSON rendering, sequence handling, and fallback when CardKit fails. Gateway shared code should create and drive a `FeishuCardRunSink` only when the feature is enabled for Feishu.

This keeps CardKit behavior out of generic streaming code and avoids changing other platforms.

## Rejected Alternatives

| Alternative | Benefit | Reason rejected for first version |
| --- | --- | --- |
| Add a generic CardKit-like sink to `GatewayStreamConsumer` | May help future platforms | It pushes Feishu-specific card lifecycle into shared gateway code and increases regression risk. |
| Only enable existing streaming/edit config | Fastest | It can stream or edit assistant text, but tool progress still remains a separate queue. |
| Upgrade Feishu SDK first | May expose newer API helpers | SDK helpers do not create the missing run-state renderer. The root gap is presentation logic. |
| Show full tool output in the card | Closest to lark-channel-bridge's richer display | Higher privacy and card-length risk. First version should show status only. |

## Phase 0: CardKit API Discovery

Before changing production runtime behavior, verify the CardKit API available through the installed Feishu dependency set.

The spike must record:

- Whether `lark-oapi==1.5.3` exposes CardKit card create/update methods.
- Whether the implementation should use SDK calls or raw HTTP.
- The minimum payload for a CardKit 2.0 card with `streaming_mode`.
- The update mechanism: full card update, element-level content update, or `message.update` fallback.
- Required scopes, expected error codes, and how failures are surfaced by the SDK or raw HTTP client.
- A minimal mock or fixture payload for unit tests.

The spike output belongs under:

- `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/verification/`

## Production Wiring

The first version should not depend on `GatewayEventDispatcher` being called in production. It should use the callbacks that `gateway/run.py` already assigns to the agent.

When `display.platforms.feishu.card_streaming` is true for a Feishu source:

1. Create a `FeishuCardRunSink` for the current run.
2. Treat card streaming as its own single switch. It must enable text delta capture for that run without requiring users to also set top-level `streaming.enabled` or `display.platforms.feishu.streaming`.
3. Route `stream_delta_callback(text)` to `FeishuCardRunSink.on_delta(text)`.
4. Route `interim_assistant_callback(text)` to `FeishuCardRunSink.on_commentary(text)` or a segment boundary equivalent.
5. Route `tool_progress_callback(event_type, tool_name, preview, args, **kwargs)` to `FeishuCardRunSink.on_tool_progress(...)`.
6. Suppress separate Feishu tool-progress bubbles for card-streaming runs. Tool status should live in the card.
7. Keep non-Feishu platforms and Feishu runs with the flag disabled on the existing path.

## FeishuCardRunSink Contract

`FeishuCardRunSink` should provide the same runner-facing delivery signals that `GatewayStreamConsumer` provides, so `gateway/run.py` can suppress duplicate final sends and handle transformed final responses correctly.

Required properties:

- `message_id: str | None`
- `final_response_sent: bool`
- `final_content_delivered: bool`
- `fallback_sent: bool`

Required methods:

- `on_delta(text: str) -> None`
- `on_commentary(text: str) -> None`
- `on_tool_progress(event_type: str, tool_name: str | None, preview: str | None, args: dict | None, **kwargs) -> None`
- `finalize(final_text: str) -> bool`
- `update_final_after_transform(final_text: str) -> bool`
- `finish_failed(error_text: str) -> bool`

Runner behavior:

- If `finalize()` confirms visible delivery, set the equivalent of `final_response_sent` and `final_content_delivered`.
- If a plugin transforms the final answer after streaming, call `update_final_after_transform(final_text)` instead of sending a duplicate ordinary Feishu message when possible.
- If the card path falls back to text/post delivery, mark `fallback_sent` and expose delivery state so normal final send is not duplicated.

## Target Runtime Flow

1. Gateway receives a Feishu message and starts the Hermes agent run.
2. If `display.platforms.feishu.card_streaming` is enabled, the Feishu run uses the card-backed path.
3. A Feishu card state object starts with:
   - text blocks
   - tool blocks
   - footer status
   - terminal state
   - Feishu card id / message id after first send
4. `stream_delta_callback(text)` appends assistant text to the active text block.
5. `interim_assistant_callback(text)` appends a distinct assistant text block or closes the current text segment.
6. `tool_progress_callback("tool.started", ...)` creates or updates a tool block with:
   - tool name
   - short argument preview
   - running state
7. `tool_progress_callback("tool.completed", ...)` changes the matching tool block to done or error when identity data is available. If identity data is missing, the sink should update the oldest matching running tool for the same tool name, and fall back to a best-effort completion state.
8. Each state change renders a CardKit 2.0 card and updates the same Feishu card.
9. On finalization, the renderer reconciles the final response text, removes the running footer, and sets `streaming_mode` to false.
10. If CardKit creation or update fails, the run falls back to existing text/post delivery.

## Tool Identity And Completion Mapping

The first implementation must not assume tool names are unique.

Preferred identity order:

1. A stable call id from callback kwargs, if present. Candidate keys should be confirmed during implementation, such as `tool_call_id`, `call_id`, `id`, or similar.
2. A monotonic per-run start index assigned by `FeishuCardRunSink` on `tool.started`.
3. As a last resort, the oldest running tool with the same tool name.

Completion behavior:

- `tool.started` creates an in-flight tool block.
- `tool.completed` marks the matching block as done if the callback indicates success.
- If callback data contains an error or failure signal, mark the block as error.
- If success/error is not available, mark completion as done with an unknown-success note omitted from the user-facing card.
- Repeated same-name tools and parallel tools must be covered by tests.

## Card Shape

The first version should use a compact CardKit 2.0 card:

- One markdown element for assistant text.
- One section or collapsible panel per recent tool call.
- A compact footer while running:
  - thinking
  - outputting
  - calling tools
- Summary text that matches the current run state.

Tool panels should contain only:

- status icon or text
- tool name
- argument summary
- completion state

They must not contain raw tool result text in the first version.

## Configuration

Add a Feishu-specific setting:

```yaml
display:
  platforms:
    feishu:
      card_streaming: false
```

Default remains `false`.

When disabled, Hermes must preserve current behavior.

When enabled, this setting is sufficient by itself for Feishu card streaming. Users should not need to also enable `streaming.enabled` or `display.platforms.feishu.streaming`.

## Failure Handling

- If card creation fails before any visible response, send the accumulated text through the existing Feishu text/post path.
- If card update fails after the card exists, try one conservative fallback update or send a final text/post message with the latest assistant text.
- Card failures must not break the agent run.
- Log CardKit failures without printing credentials, tokens, raw private messages, or full tool outputs.
- Emit counters or structured log events for:
  - `feishu_card_create_failed`
  - `feishu_card_update_failed`
  - `feishu_card_fallback_sent`
- Fallback delivery must update sink state so the ordinary final send path does not duplicate the same answer.

## Testing Plan

Write tests before implementation.

Unit tests:

- Reduces stream delta callbacks into assistant text blocks.
- Reduces tool-start callbacks into running tool blocks.
- Reduces tool-completed callbacks into done/error state.
- Correlates repeated same-name tools without collapsing them into one block.
- Handles parallel tools using call id when available.
- Renders CardKit 2.0 JSON with `streaming_mode: true` while running.
- Renders `streaming_mode: false` after finalization.
- Omits full tool output from rendered cards.
- Falls back to existing text/post path when card creation fails.
- Leaves existing Feishu `send()` and `edit_message()` tests passing.

Runner-level tests:

- With `card_streaming` false, Feishu uses the existing path.
- With only `card_streaming` true, text deltas reach `FeishuCardRunSink` without enabling top-level `streaming.enabled`.
- Tool progress does not emit separate Feishu progress messages when card streaming is active.
- A successfully finalized card suppresses the ordinary final send.
- A transformed final response updates the card when possible and does not create a duplicate message.
- Card fallback delivery also suppresses duplicate final send.

Integration or live validation:

- With the feature disabled, Feishu behavior is unchanged.
- With the feature enabled, a multi-tool Feishu prompt produces one visible card.
- Assistant text updates in the card.
- Tool-call state updates in the card.
- Run completion finalizes the card.
- A simulated CardKit error falls back without losing the final answer.

## Rollout

1. Run the CardKit API discovery spike and store output in `verification/`.
2. Implement behind `display.platforms.feishu.card_streaming: false`.
3. Run unit tests, runner-level tests, and existing Feishu gateway tests.
4. Back up `/Users/bytedance/Documents/Hermes/home/config.yaml`.
5. Enable only `display.platforms.feishu.card_streaming: true`.
6. Restart Hermes gateway.
7. Send one multi-tool Feishu prompt to 小A.
8. Verify that the visible Feishu result is one card, with no separate progress bubbles and no duplicate final text.
9. Verify Hermes gateway logs for create/update/fallback counters.
10. If delivery fails, disable the flag and restart gateway.

## Acceptance Criteria

- Existing default Feishu behavior remains unchanged when the flag is false.
- When the flag is true, one Feishu card represents a single agent run.
- Assistant text and tool-call state update inside that card.
- Full tool output is not shown in the card.
- `card_streaming` alone is sufficient to capture Feishu text deltas.
- Tool completion state updates without collapsing repeated same-name tools.
- A successful card response does not produce a duplicate final message.
- CardKit failure degrades to a visible text/post response.
- Relevant tests pass.

## Deferred Work

- Wiring `GatewayEventDispatcher` into the production runner can be a later architecture cleanup. It is not required for the first Feishu card-streaming version.
- Element-level streaming update can be a later optimization if the full-card update path works reliably.
