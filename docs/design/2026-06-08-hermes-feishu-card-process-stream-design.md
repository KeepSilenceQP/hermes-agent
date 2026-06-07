# Feishu Card Process Stream Design

## Goal

Make Hermes Feishu card streaming match the observable behavior of the Codex
lark-channel bridge: when a Feishu user mentions 小A, the reply card should open
early and continuously show process events, including thinking, tool start,
tool completion, answer deltas, and the final answer.

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
- Card rendering for thinking, tool started, tool completed, partial answer,
  and final answer.
- Tests that prove process events update the card before finalization.
- Focused live validation in the Feishu group.

Out of scope:

- Replacing the whole Hermes gateway event system.
- Changing non-Feishu platforms.
- Reworking provider APIs.
- Adding button callbacks or interactive form behavior.
- Persisting full raw tool output in long-term storage.
- Making API/model progress a first-version acceptance requirement. It can be
  added only when Hermes exposes structured callback events for it.

## Design Overview

Introduce a small process-stream layer inside the Feishu card streaming path.
The layer converts agent progress callbacks into ordered process blocks and
renders those blocks into one CardKit card.

This layer must be implemented inside the existing `FeishuCardRunSink` contract.
Do not add a new runner-facing `on_process_event(...)` API unless a later design
also defines the compatibility migration for existing callers.

Data flow:

```text
agent/codex/tool events
  -> gateway.run progress callback
  -> existing FeishuCardRunSink callbacks
  -> internal ProcessEvent normalization
  -> existing FeishuCardRunState reducer/update methods
  -> CardKit renderer
  -> scheduled immediate card update
```

The process layer should be local to Feishu card streaming. Existing plain text
fallbacks and non-card paths should keep their current behavior.

## Components

### Internal Process Event

Create a normalized process event shape inside the Feishu card sink:

- `reasoning.available`: thinking or reasoning preview is available.
- `tool.started`: a tool call has begun.
- `tool.completed`: a tool call has completed or failed.
- `text.delta`: assistant answer text is streaming.
- `final`: final answer is available.

The initial implementation can use the events already available in Hermes:
`reasoning.available`, `tool.started`, `tool.completed`, stream deltas, and
final text. API call blocks can be added from existing conversation-loop
observability only when the event can be emitted as structured callbacks without
scraping logs.

### Existing Sink State

Reuse the current `FeishuCardRunSink` and `FeishuCardRunState` shape. The
implementation should refine the existing queue/drain/reducer behavior instead
of adding a second reducer beside it.

The state keeps:

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
- Process area for thinking and tools.
- Answer area for draft/final answer.
- Footer for model/status metadata when available.

The renderer must not collapse process blocks when final text arrives. Final
text updates only the answer area.

Tool blocks in the first version must contain only:

- tool name
- parameter or command summary
- running/done/error state
- short error category or short error summary when the tool failed

They must not render raw tool output. Error summaries must be capped and
sanitized before display.

### Immediate Process Updates

Tool and reasoning events must schedule a card update immediately after the sink
mutates state.

Text deltas may still be rate-limited to avoid excessive Feishu updates, but
process events should bypass the normal text debounce.

The implementation should avoid synchronous waits from the agent worker thread.
The preferred first-version strategy is to schedule the update onto the gateway
loop and log whether it completes within the target window. A bounded synchronous
wait can be used only if the implementation proves it does not block frequent
tool callbacks under slow Feishu responses.

Expected timing target:

- `tool.started` should result in `feishu_card_update_success` within one
  second under normal network conditions. This is an observability target, not a
  promise that the agent callback blocks until success.
- `tool.completed` should update the existing tool block before the final
  answer is rendered.

## Error Handling

- If card update fails with a transient Feishu error, keep the process state and
  retry on the next event or finalization.
- If card creation fails, fall back to the existing text reply path and log the
  failure with the Feishu error code.
- If a tool completion arrives without a known start block, create a completed
  tool block instead of dropping the event.
- If tool output is present, do not render the raw output in the card. For
  failures, render only a capped and sanitized error category or short summary.
- If an event cannot be normalized, log it at debug or warning level and keep
  the rest of the stream alive.

## Observability

Add focused logs that explain timing and routing:

- Process event id and monotonic timestamp.
- Event received by gateway callback.
- Event accepted by `FeishuCardRunSink`.
- Reducer mutation result: block type, block count, tool count.
- Immediate update scheduled.
- Update completed, failed, or timed out.
- Card update success with sequence, tool count, terminal state, and elapsed
  milliseconds from process event to update.

The logs should make this failure mode obvious:

```text
tool.started routed at T1
tool.started rendered at T2
card update success at T3
```

The same `event_id` should appear in each log line so live validation does not
require manually correlating unrelated timestamps.

## Testing

Add or update focused tests:

- `tool.started` renders a tool-running block before finalization.
- `tool.completed` updates the same tool block to completed.
- A failed tool completion renders an error state.
- `reasoning.available` renders a process block.
- Final text preserves prior process blocks.
- Text delta rate limiting does not delay process-event flush.
- Missing tool start does not drop a tool completion.
- Raw successful tool output is not rendered.
- Failed tool output is reduced to a capped sanitized error summary.
- Existing public sink callbacks remain the runner-facing API.

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
7. Verify there are no separate Feishu progress bubbles.
8. Verify there is no duplicate final text message.
9. Check logs for event-to-card-update latency with a shared event id.

Passing evidence should include both Feishu visual confirmation and log lines
showing `tool.started` followed by `feishu_card_update_success` before
`Turn ended`.

## Risks

- Feishu update rate limits may reject overly frequent updates. Mitigation:
  process events schedule immediate updates; text deltas remain rate-limited.
- Card size can grow during long runs. Mitigation: truncate previews and cap
  process blocks.
- Current callbacks may not expose API call start/completion as structured
  events. Mitigation: implement API blocks only when structured signals exist;
  do not scrape logs.
- Blocking the agent thread while updating cards can introduce latency.
  Mitigation: avoid synchronous waits from the agent worker; schedule card
  updates on the gateway loop and log completion timing.

## Approval Gate

Implementation should proceed only after this design is reviewed and approved.
