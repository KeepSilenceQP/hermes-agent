# Feishu Card Process Stream Design

## Goal

Make Hermes Feishu card streaming match the observable behavior of the Codex
lark-channel bridge: when a Feishu user mentions 小A, the reply card should open
early and continuously show process events, including thinking, API/model
progress, tool start, tool completion, and the final answer.

This is not a final-answer typewriter effect. The card must show the agent's
run process while the run is still active.

## Current Evidence

The current implementation already receives tool progress events, but it does
not display them in the card at the right time.

Observed live sequence for session `20260607_183942_78c8292b`:

- `03:36:21` gateway routed `tool.started tool=terminal`.
- `03:36:23` gateway routed `tool.completed tool=terminal`.
- `03:36:33` the agent turn ended.
- `03:36:33` `FeishuCardRunSink` finally logged `feishu_card_tool_event`.
- `03:36:34` the card update logged `tools=1 terminal=running`.

This proves the event source and callback attachment are not the main failure.
The failure is between tool-progress routing and immediate card rendering.

## Reference Behavior

The lark-channel bridge used by 小P follows an event-stream reducer model:

- `item.started command_execution` becomes a `tool_use` block.
- `item.completed command_execution` becomes a `tool_result` block.
- Text, tool, and reasoning events update one shared state object.
- Each event triggers a card flush immediately.
- The final answer is appended to the same card without removing prior process
  blocks.

Hermes should use the same behavioral shape even if the implementation remains
Python-native.

## Scope

In scope:

- Feishu CardKit streaming for 小A.
- Process events from existing agent callbacks and observable agent lifecycle
  points.
- Card rendering for thinking, API/model progress, tool started, tool completed,
  partial answer, and final answer.
- Tests that prove process events update the card before finalization.
- Focused live validation in the Feishu group.

Out of scope:

- Replacing the whole Hermes gateway event system.
- Changing non-Feishu platforms.
- Reworking provider APIs.
- Adding button callbacks or interactive form behavior.
- Persisting full raw tool output in long-term storage.

## Design Overview

Introduce a small process-stream layer inside the Feishu card streaming path.
The layer converts agent progress callbacks into ordered process blocks and
renders those blocks into one CardKit card.

Data flow:

```text
agent/codex/tool events
  -> gateway.run progress callback
  -> FeishuCardRunSink.on_process_event(...)
  -> ProcessStateReducer
  -> CardKit renderer
  -> immediate update_card
```

The process layer should be local to Feishu card streaming. Existing plain text
fallbacks and non-card paths should keep their current behavior.

## Components

### Process Event

Create a normalized process event shape for the Feishu card sink:

- `reasoning.available`: thinking or reasoning preview is available.
- `api_call.started`: model/API call started when that signal is available.
- `api_call.completed`: model/API call completed when usage or latency is known.
- `tool.started`: a tool call has begun.
- `tool.completed`: a tool call has completed or failed.
- `text.delta`: assistant answer text is streaming.
- `final`: final answer is available.

The initial implementation can use the events already available in Hermes:
`reasoning.available`, `tool.started`, `tool.completed`, stream deltas, and
final text. API call blocks can be added from existing conversation-loop
observability only when the event can be emitted without scraping logs.

### Process State Reducer

Add a reducer owned by `FeishuCardRunSink`.

The reducer keeps:

- An ordered list of process blocks.
- A map from tool call id or stable fallback key to tool block.
- The current answer draft.
- The final answer.
- Terminal state: `running`, `done`, `failed`, or `fallback`.

Tool block identity should prefer an explicit call id when present. If no call
id exists, use a stable fallback of event order, tool name, and start timestamp.

### Renderer

Render one CardKit 2.0 card with:

- Header or top markdown block for the referenced task summary.
- Process area for thinking, API/model progress, and tools.
- Answer area for draft/final answer.
- Footer for model/status metadata when available.

The renderer must not collapse process blocks when final text arrives. Final
text updates only the answer area.

### Immediate Flush

Tool and reasoning events must trigger a card update immediately after the
reducer mutates state.

Text deltas may still be rate-limited to avoid excessive Feishu updates, but
process events should bypass the normal text debounce.

Expected timing target:

- `tool.started` should result in `feishu_card_update_success` within one
  second under normal network conditions.
- `tool.completed` should update the existing tool block before the final
  answer is rendered.

## Error Handling

- If card update fails with a transient Feishu error, keep the process state and
  retry on the next event or finalization.
- If card creation fails, fall back to the existing text reply path and log the
  failure with the Feishu error code.
- If a tool completion arrives without a known start block, create a completed
  tool block instead of dropping the event.
- If tool output is too large, show a preview and mark the block as truncated.
- If an event cannot be normalized, log it at debug or warning level and keep
  the rest of the stream alive.

## Observability

Add focused logs that explain timing and routing:

- Event received by gateway callback.
- Event accepted by `FeishuCardRunSink`.
- Reducer mutation result: block type, block count, tool count.
- Immediate flush requested.
- Flush completed, failed, or timed out.
- Card update success with sequence, tool count, terminal state, and elapsed
  milliseconds from process event to update.

The logs should make this failure mode obvious:

```text
tool.started routed at T1
tool.started rendered at T2
card update success at T3
```

## Testing

Add or update focused tests:

- `tool.started` renders a tool-running block before finalization.
- `tool.completed` updates the same tool block to completed.
- A failed tool completion renders an error state.
- `reasoning.available` renders a process block.
- Final text preserves prior process blocks.
- Text delta rate limiting does not delay process-event flush.
- Missing tool start does not drop a tool completion.

At least one test should assert that a fake card sink receives an update after
`tool.started` without calling `finalize()`.

## Live Validation

After implementation and deployment:

1. Restart Hermes gateway.
2. Ask 小A in the Feishu group to perform a task that must call a tool.
3. Verify the card opens before final answer.
4. Verify a tool-running block appears while the tool is active.
5. Verify the tool block changes to completed or failed before final answer.
6. Verify final answer appears without removing process blocks.
7. Check logs for event-to-card-update latency.

Passing evidence should include both Feishu visual confirmation and log lines
showing `tool.started` followed by `feishu_card_update_success` before
`Turn ended`.

## Risks

- Feishu update rate limits may reject overly frequent updates. Mitigation:
  process events flush immediately; text deltas remain rate-limited.
- Card size can grow during long runs. Mitigation: truncate previews and cap
  process blocks.
- Current callbacks may not expose API call start/completion as structured
  events. Mitigation: implement API blocks only when structured signals exist;
  do not scrape logs.
- Blocking the agent thread while flushing can introduce latency. Mitigation:
  avoid synchronous waits from the agent worker; schedule card updates on the
  gateway loop and log completion timing.

## Approval Gate

Implementation should proceed only after this design is reviewed and approved.
