# Feishu Card Event-Stream Reducer Design

## Goal

Make Hermes Feishu card streaming match the observable behavior of the Codex
lark-channel bridge used by 小P.

The target is an agent run event stream, not a final-answer typewriter effect.
When a Feishu user mentions 小A, Hermes should create one card early, reduce
thinking, tool, model, answer, and terminal events into one shared run state, and
flush the same card as the run evolves.

The design must fix these user-visible failures:

1. The temporary `正在处理...` placeholder remains after the answer is done.
2. Tool calls are not shown while the run is active.
3. The final conclusion can appear twice.
4. The process does not feel streamed; it only appears after the run is over.
5. Tool-call preamble text can be rendered twice: once as streamed text and once
   as interim commentary.
6. The final conclusion can be rendered above the process/tool history because
   the answer block was created by an early pre-tool delta.

## Chosen Approach

Use a Python-native implementation of 小P bridge's event-stream reducer model.

Hermes keeps its current gateway and callback entry points, but the Feishu card
sink no longer treats `on_delta`, `on_commentary`, `on_tool_progress`, and
`finalize` as separate rendering paths. Each callback is normalized into a
`RunEvent`, reduced into one `RunState`, and rendered from that state.

This avoids a shallow patch set where placeholder cleanup, duplicate final text,
tool rendering, and streaming-mode transitions are handled by unrelated
conditionals.

## Reference Behavior

The lark-channel bridge follows this behavioral shape:

- `item.started command_execution` becomes a `tool_use` event.
- `item.completed command_execution` becomes a `tool_result` event.
- Text, tool, thinking, and terminal events update one shared state object.
- Each meaningful event triggers a card flush.
- The final event closes the running state without removing process evidence.
- The visible message is a CardKit-managed card: create the card with
  `cardkit.card.create`, send an interactive message that references the
  returned `card_id`, then update that same `card_id`.
- The card update producer runs alongside event consumption. It must not wait
  for the whole Hermes turn to finish before reducing queued events.

Hermes should match this behavior while keeping Hermes-specific event sources and
Feishu adapter APIs.

## Scope

In scope:

- Feishu CardKit streaming for 小A.
- A Feishu-local run event model, reducer, renderer, and flush policy.
- Normalization from existing Hermes callbacks to reducer events.
- Tests that prove the listed user-visible failures are fixed.
- Live validation in the Feishu group.

Out of scope:

- Replacing the full Hermes gateway event system.
- Changing non-Feishu platforms.
- Reworking provider APIs.
- Adding card button callbacks or forms.
- Persisting full raw tool output in long-term storage.

## Target Architecture

```text
Hermes agent callbacks
  on_delta / on_commentary / on_tool_progress / finalize
        |
        v
FeishuRunEventNormalizer
        |
        v
FeishuRunReducer  ----->  FeishuRunState
        |                       |
        |                       v
        +-------------->  FeishuCardRenderer
                                |
                                v
                        FeishuManagedCardTransport
                                |
                                v
              cardkit.card.create -> send card_id reference -> card update
```

The normalizer and reducer are owned by the Feishu card streaming path. They do
not change other platforms. If the existing Hermes callback shape cannot express
the model-text lane, the Feishu streaming path may add callback metadata or a
Feishu-local normalized event wrapper, but that metadata must preserve existing
non-Feishu behavior.

## Feishu Transport Contract

The Feishu transport must follow the same managed-card shape as 小P.

Creation:

1. Call CardKit `card.create` with the initial CardKit 2.0 card JSON and receive
   `card_id`.
2. Send the visible Feishu interactive message as a card reference:
   `{"type":"card","data":{"card_id":"<card_id>"}}`.
3. Store both `message_id` and `card_id` in the sink. `card_id` is the only
   update handle for CardKit updates.

Updates:

- Full-card updates call CardKit `card.update` with `card_id`, full card JSON,
  and monotonic `sequence`.
- Text-element updates may call CardKit `card_element.content` with `card_id`,
  the stable markdown element id, full current content, and monotonic `sequence`.
- The transport must not use raw interactive-card message content plus
  `message_id -> card_id` conversion as the primary path. `aid_convert` is a
  fallback only for legacy cards that were not created by this sink.
- A missing `card_id` after create is a hard create failure and must fall back to
  the text reply path; it must not silently use `message_id` as the CardKit update
  handle.

This is not just an implementation preference. It is part of the observable
streaming contract, because 小P's working behavior updates a CardKit-managed card
entity rather than a raw interactive message payload.

## Run Events

Use these normalized events inside the Feishu card sink:

| Event | Source | Reducer effect |
| --- | --- | --- |
| `placeholder.started` | `sink.start("正在处理...")` | Show temporary status only when no real event exists. |
| `thinking.delta` | `_thinking`, `reasoning.available`, or interim commentary | Append or update reasoning/process content. |
| `model.started` | Structured lifecycle signal when available | Add or update a model/API progress block. |
| `model.completed` | Structured lifecycle signal when available | Mark the model/API block complete with safe metadata. |
| `model_text.delta` | `agent.stream_delta_callback` | Buffer visible model text until the current model response is classified. |
| `tool_preamble.commit` | Model text from a response that also emits tool calls | Commit buffered model text as process/commentary, not answer. |
| `answer.delta` | Text from a response classified as answer-producing | Append to the current streaming answer draft. |
| `interim.commentary` | `interim_assistant_callback(already_streamed=False)` | Append a process/commentary block. |
| `interim.segment_end` | `interim_assistant_callback(already_streamed=True)` | Close or classify the already-streamed segment without rendering duplicate text. |
| `tool_use` | `tool.started` | Add a running tool block keyed by `tool_key`. |
| `tool_result` | `tool.completed` | Update the matching `tool_key` block to done or error. |
| `final` | final response from the turn | Replace the answer draft with authoritative final text and close the run. |
| `error` | failed turn or card sink failure | Mark the run failed and render a failure note. |

API/model blocks are optional until Hermes has structured lifecycle events.
They must not be inferred by scraping logs.

## Run State

`FeishuRunState` should contain:

- `placeholder`: temporary status text and whether it is still visible.
- `reasoning`: accumulated thinking/process text plus active/inactive state.
- `blocks`: ordered process, answer, and tool blocks, matching 小P bridge's mixed
  stream.
- `tool_index`: map from `tool_key` to a tool block.
- `answer_block_id`: id of the single renderable answer block, when answer text
  exists.
- `pending_model_segment`: current visible model text whose lane is not known yet.
- `footer`: `thinking`, `streaming`, `tool_running`, or empty.
- `terminal`: `running`, `done`, `error`, `interrupted`, or `fallback`.

The renderer must be a pure projection of this state. It should not decide
whether a callback is process text, answer text, placeholder text, or final text.

## Reducer Rules

### Placeholder

`正在处理...` is a placeholder, not a process block.

- It is visible only while there are no real process, tool, or answer events.
- The first `thinking.delta`, `model_text.delta`, `tool_use`, or `model.started`
  hides it.
- `final`, `error`, and `fallback` always hide it.
- It must never appear in the terminal card.

### Thinking And Commentary

Interim commentary belongs to the process area, not the answer block.

- `on_commentary` normalizes to `thinking.delta`.
- `thinking.delta` updates `reasoning` or a process block.
- If a commentary block duplicates the final answer by the deterministic rules
  below, the final reducer step removes or suppresses that commentary block.
- Commentary should not create a second answer block.

This prevents a full conclusion from appearing once as commentary and again as
the final answer.

### Model Text Lane Classification

Visible model text is not automatically answer text.

Hermes model responses can contain ordinary assistant text before tool calls. In
Codex-style agent semantics, that text is a tool-call preamble or process note,
not the final answer. 小P's card treats this as process stream content; Hermes
must not pin it into the answer area.

Rules:

- `agent.stream_delta_callback` normalizes to `model_text.delta`, not directly to
  `answer.delta`.
- `model_text.delta` appends to `pending_model_segment` and may be rendered
  temporarily as a running process segment while the response is active.
- When the same model response emits `tool_use`, the pending segment is committed
  as `tool_preamble.commit` and becomes a process/commentary block before the
  tool block.
- When the same model response completes without tool calls and is known to be
  answer-producing, the pending segment is committed as `answer.delta`.
- `interim_assistant_callback(..., already_streamed=True)` must not render text
  again. It is a segment boundary/classification signal only.
- `interim_assistant_callback(..., already_streamed=False)` may render
  `interim.commentary`, but only when the text was not already delivered through
  `model_text.delta`.
- If the normalizer cannot determine a response's lane at the time a text delta
  arrives, it must keep the segment reclassifiable until the response emits tool
  calls or reaches a final/answer-producing boundary.

This is the core rule that prevents tool-call preambles from appearing once in
the answer lane and again in the process lane.

Duplicate detection must be deterministic:

- Normalize both strings by trimming whitespace, collapsing repeated whitespace,
  and removing trailing streaming cursor/status markers.
- Suppress commentary when the normalized strings are exactly equal.
- Suppress commentary when one normalized string fully contains the other and the
  shorter string is at least 80% of the longer string.
- Do not use fuzzy semantic similarity or model calls for this decision.

### Answer Text

Answer text has one renderable source: the answer block in `blocks`.

- `answer.delta` creates the answer block only after the current model response is
  classified as answer-producing.
- Consecutive answer deltas append to the same answer block.
- Tool-call preambles, interim progress, and lifecycle status must not create the
  answer block.
- A tool event closes or classifies the active model text segment before adding
  the tool block.
- `final` replaces the answer block content with the authoritative final text.
- If there was no answer block before `final`, `final` creates one.
- If an answer block already exists before the final process/tool history is
  complete, `final` moves the answer block to the terminal answer position:
  after process/tool history and before footer or terminal error notes.
- The renderer must never render both a draft field and a final field. It renders
  the answer block once.
- The renderer test must cover
  `model_text.delta -> tool_use -> model_text.delta -> final`
  and assert that final answer text appears once.

### Tool Blocks

`tool_use` and `tool_result` update the same logical tool block by `tool_key`.

- Every normalized tool event must contain `tool_key`.
- Prefer provider or Codex item id when present.
- For ordinary Hermes executor callbacks, the normalizer must create and
  propagate a stable `tool_seq` or `tool_call_id` from `tool.started` through the
  matching `tool.completed`.
- Fallback matching by tool name is allowed only as a degradation path for legacy
  events; it is not the primary contract.
- `tool_use` creates a running tool block and sets footer to `tool_running`.
- `tool_result` updates the matching block to `done` or `error`.
- If completion arrives without a known start, create a completed synthetic tool
  block instead of dropping the event.
- Successful tool output is summarized or omitted by default; failed tool output
  may show a bounded diagnostic preview.

Tool events are process events and must flush immediately.

### Terminal Events

`final` and `error` close the run.

- `final` sets `terminal=done`, hides placeholder, deactivates reasoning, and
  sets `streaming_mode=false` in the final card.
- `error` sets `terminal=error`, hides placeholder, and preserves safe process
  context.
- Terminal rendering preserves tool and process blocks unless they duplicate the
  final answer.

## Renderer

Render one CardKit 2.0 card from `FeishuRunState`.

Recommended structure:

1. Reasoning/process block, when present.
2. Ordered process/tool history.
3. Answer block, only when the current classified answer draft or final answer
   exists.
4. Running footer while `terminal=running`.
5. Terminal note only for error, interruption, timeout, or empty response.

The card uses `streaming_mode=true` while the run is active. The final update
uses `streaming_mode=false` to remove the running cursor/status.

The renderer must not append `final_answer` after an already rendered identical
draft. It should choose one answer source for the answer area.

The terminal card must place the final answer after the process/tool history.
The answer block's earliest creation time must not control its terminal render
position.

## Flush Policy

Use two flush paths:

- Process events flush immediately: `thinking.delta`, `tool_use`,
  `tool_result`, `model.started`, `model.completed`, `final`, and `error`.
- Text deltas may be rate-limited, but the latest draft must be included in the
  next immediate process flush or final flush.

`tool_use` should normally produce a visible card update within one second.
`tool_result` should update the existing tool block before the final answer is
rendered.

The sink must have a producer-style update loop, matching 小P's `channel.stream`
behavior:

- Event consumption, reduction, and card update scheduling run concurrently with
  the Hermes agent turn.
- A process event must be reduced and flushed while the agent turn is still
  active; it must not wait for `finalize()` or terminal barrier drain.
- The agent worker should not block on Feishu network I/O. It should enqueue the
  event and signal the producer loop.
- A synchronous bridge helper may wait briefly only to confirm that the producer
  accepted the event, not to turn Feishu network I/O into the agent's critical
  path.
- If Hermes runs agent work in an executor thread, callbacks from that thread
  must still wake the gateway event loop immediately. A queued event whose
  update is first visible only during finalization is a streaming failure.

Flush ordering invariants:

- All callbacks enqueue normalized events into one run queue.
- `final` and `error` are barrier events: the sink must reduce all prior queued
  events before reducing the terminal event.
- Terminal state rejects new post-terminal events, but it must not discard events
  that were queued before the terminal event.
- Every card update uses a monotonic sequence. Older flush completions must not
  overwrite newer terminal content.
- Delayed text flush and immediate process flush share one serialized update
  pipeline.
- The producer records event-to-update latency. For process events, p95 latency
  should be under one second in normal network conditions.

## Configuration Baseline

Live validation must run with streaming explicitly enabled, so configuration
cannot mask implementation problems.

Required Hermes config values:

```yaml
display:
  streaming: true
  platforms:
    feishu:
      card_streaming: true
streaming:
  enabled: true
  transport: auto
```

Notes:

- `display.platforms.feishu.card_streaming=true` selects the Feishu CardKit card
  path.
- `display.streaming=true` and `streaming.enabled=true` keep Hermes' normal
  streaming gates open so provider text/reasoning deltas are captured when they
  exist.
- The card sink may still own final Feishu rendering and suppress the normal text
  final send; enabling global streaming must not create duplicate Feishu messages.
- Card JSON should set `config.streaming_mode=true` while `terminal=running` and
  `false` on the terminal update. `streaming_config` is optional and must not be
  treated as a substitute for the managed-card transport and producer loop.

## Error Handling

- If card creation fails, fall back to the existing text reply path and log the
  Feishu error code.
- If an update fails transiently, keep the run state and retry on the next event
  or finalization.
- If repeated updates fail, disable card updates for that run and send one final
  text fallback.
- If event normalization fails, log the event type and continue the stream.
- If a tool result is too large, truncate or summarize it before rendering.

## Observability

Logs should make event routing and rendering latency obvious:

- `feishu_card_event_received event=<type> source=<callback>`
- `feishu_card_event_reduced event=<type> blocks=<n> tools=<n> terminal=<state>`
- `feishu_card_flush_requested event=<type> immediate=<bool>`
- `feishu_card_update_success seq=<n> tools=<n> terminal=<state> latency_ms=<n> transport=<managed|legacy-convert>`
- `feishu_card_update_failed seq=<n> error=<code-or-message>`
- `feishu_card_transport_created card_id=<id> message_id=<id>`

The key proof pattern is:

```text
tool.started routed at T1
tool_use reduced at T2
feishu_card_update_success at T3
Turn ended at T4
```

`T3` must be before `T4`.

A pattern where `tool.started` and `tool.completed` are routed early but the
first meaningful `feishu_card_update_success` appears only next to `final` is a
failure, even if the terminal card content is correct.

## Testing

Add or update focused tests for the reducer, renderer, sink, and gateway routing.

Reducer tests:

- Placeholder is hidden by the first real event.
- Placeholder is absent after `final`.
- `answer.delta` followed by `final` renders the answer once.
- `model_text.delta` followed by `tool_use` commits that text as process, not as
  answer.
- `interim.segment_end` for already-streamed text does not render duplicate
  commentary.
- Commentary identical or near-identical to final is not rendered as a duplicate
  conclusion.
- `tool_use` creates a running tool block.
- `tool_result` updates the same tool block to done or error.
- Two concurrent same-name tools update their own blocks by `tool_key`.
- Missing tool start creates a completed synthetic tool block.
- `model_text.delta -> tool_use -> model_text.delta -> tool_use -> final`
  renders tool-call preambles as process blocks and final answer once from the
  answer block.
- Final answer appears after all process/tool blocks and before footer.

Sink/flush tests:

- `tool_use` schedules or performs an update without calling `finalize()`.
- `tool_result` updates before `final`.
- Process-event flush bypasses the text delta debounce.
- A process event enqueued from a worker thread wakes the producer and updates
  the fake adapter before `finalize()` is called.
- Final update switches `streaming_mode` to false.
- `tool_use` queued immediately before `final` is reduced before the terminal
  card closes.
- A stale lower-sequence update cannot overwrite a newer terminal update.
- A fake adapter captures this sequence: create card with placeholder, update
  with tool-running and no placeholder, final update with `streaming_mode=false`
  and final answer count equal to one.

Gateway routing tests:

- Feishu card streaming attaches tool progress callback even when normal progress
  bubbles are disabled.
- Feishu card streaming routes interim commentary into the sink.
- Feishu card streaming respects `already_streamed=True` by sending a segment
  boundary/classification event, not another renderable commentary block.
- Feishu card streaming does not route tool-call preamble deltas directly to the
  answer lane.
- Feishu card streaming suppresses the normal final send after card finalization
  succeeds.
- Feishu card streaming keeps provider delta callbacks attached when
  `card_streaming=true` and global streaming is enabled.

Transport tests:

- Card creation calls CardKit `card.create` before sending the visible Feishu
  message.
- The visible message content references `card_id`; it does not inline the raw
  card JSON.
- Full updates and text-element updates use the stored `card_id`, never
  `message_id`.
- `aid_convert` is covered only as a legacy fallback and is not used in the
  primary create path.

Configuration tests:

- The live config fixture or migration path sets `display.streaming=true`,
  `streaming.enabled=true`, and
  `display.platforms.feishu.card_streaming=true` for Feishu card streaming
  validation.
- Enabling global streaming with Feishu card streaming does not create duplicate
  final messages.

Live validation:

1. Restart Hermes gateway.
2. Confirm live config has `display.streaming=true`, `streaming.enabled=true`,
   and `display.platforms.feishu.card_streaming=true`.
3. Ask 小A in the Feishu group to run a task that must call a tool and then
   produce a short final answer.
4. Verify the card appears early with a temporary status.
5. Verify the temporary status disappears after the first real event.
6. Verify tool-running appears while the tool is active, not only after it
   completes.
7. Verify the tool block changes to done or error before final answer.
8. Verify the final conclusion appears once.
9. Verify the final card no longer has streaming/running status.
10. Check logs for `tool_use reduced` and `feishu_card_update_success` before
    `Turn ended`.
11. Check event-to-update timestamps: at least one meaningful running update must
    happen materially before the terminal update, not within the finalization
    burst.

## Acceptance Criteria

| Problem | Required behavior | Verification |
| --- | --- | --- |
| `正在处理...` remains | Placeholder is not rendered in the terminal card. | Unit test and Feishu screenshot. |
| Tool calls missing | `tool_use` renders before finalization. | Fake adapter update before `finalize()` and live log order. |
| Conclusion duplicated | Final answer is rendered from one source only. | Count exact final text once in rendered card. |
| Tool preamble duplicated | Already-streamed interim text is not rendered again as commentary. | Unit test for `already_streamed=True` and live card screenshot. |
| Conclusion above tools | Final answer renders after process/tool history. | Renderer test over multi-tool sequence and live card screenshot. |
| No streamed process | Each process event reduces state and flushes the active card. | Logs show event-to-update latency and live card changes. |
| Raw-card transport drift | Primary path uses CardKit managed card references like 小P. | Transport test asserts `card.create -> send card_id reference -> card.update`. |
| Config masks streaming | Live Feishu config keeps card and global streaming enabled. | Config check before live validation. |
| Finalization burst only | Running updates occur during the turn, not only next to terminal update. | Timestamp assertions over logs and fake producer test. |

## Risks

- Feishu update rate limits may reject overly frequent text updates. Mitigation:
  keep text deltas rate-limited while process events flush immediately.
- Card size can grow during long runs. Mitigation: cap process blocks, collapse
  older tool groups, and truncate output previews.
- Hermes may not expose structured model/API lifecycle events. Mitigation:
  implement model blocks only when structured events exist.
- Immediate flushing from worker threads can add latency or deadlocks.
  Mitigation: schedule flushes on the gateway loop and keep blocking waits short
  and bounded.

## Implementation Boundary

The implementation should first establish the reducer contract and tests, then
wire existing callbacks into it. Rendering changes should follow the reducer
state rather than adding one-off conditions to existing callback methods.
