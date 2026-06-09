# Feishu Bot-at-Bot Native Forwarding Design

## Goal

When 小A finishes a Feishu CardKit streaming reply, Hermes should detect any
explicit native bot-at-bot message in the raw final assistant answer and send it
as a separate Feishu `text` message. This satisfies Feishu's documented trigger
condition for one bot at-mentioning another bot.

The trigger mechanism is the native IM message content:

```text
<at user_id="ou_xxx">Name</at> message
```

Card display rendering is display-only. It may rewrite native at text into a
human-readable sentence, but it must not be treated as the bot-at-bot trigger.

## Verified Behavior

Live validation confirmed that when 小A sends a native `msg_type=text` message
containing:

```text
<at user_id="ou_cc7a2bbc1be9e7f6054282ae918b9249">小P</at> bot-at-bot live proof
```

小P receives the structured mention event. If the target bot then refuses to
handle the message because of its own group allowlist or response policy, that is
outside the sending path and still proves the at trigger succeeded.

## Chosen Approach

Use a post-final one-shot forwarder.

Hermes should not scan stream deltas while the answer is still being generated.
The at message is explicitly provided by the assistant output, and correctness is
more important than waking the target bot a few seconds earlier. Waiting until
the final answer is stable avoids partial tags, split stream chunks, incomplete
paragraphs, and premature forwarding.

```text
assistant stream
  -> Feishu card state updates and CardKit rendering continue normally
  -> final answer arrives
  -> read raw final assistant answer from run state
  -> NativeBotAtForwarder.forward_from_final_text(raw_answer)
  -> send native Feishu text messages for matching at paragraphs
```

## Current Code Shape

The current card renderer does not render from a standalone answer-only source.
`FeishuCardRunRenderer.content(state)` builds one mixed markdown string from:

- placeholder
- process blocks
- tool lines
- answer block
- footer
- pending model segment
- running status text

小A's previous change applied `render_card_markdown_mentions(...)` to the whole
mixed string. That function converted Hermes-style
`<at user_id="ou_xxx">...</at>` tags into CardKit `<person ...></person>` tags
for display. That change has been reverted and must not be reintroduced as a
global mixed-content conversion.

This means the current rendered card content is not a valid input for native
forwarding. It may contain process, tool, footer, or example text that should
not trigger a bot.

## Text Boundaries

The implementation must keep two separate text surfaces:

```text
raw_final_answer_text
  Raw assistant answer only.
  Preserves native <at user_id="ou_xxx">...</at>.
  Used only by NativeBotAtForwarder.

rendered_card_content
  Mixed CardKit display markdown.
  May rewrite answer-only native at text into human-readable text.
  Used only for card create/update.
```

Hard constraints:

- `NativeBotAtForwarder` must not read `renderer.content(state)`.
- `NativeBotAtForwarder` must not read `renderer.render(state)`.
- `NativeBotAtForwarder` must not read the CardKit update payload.
- `NativeBotAtForwarder` must not read footer, process, tool, or placeholder
  text.
- The forwarder input must preserve the original `<at user_id="...">...</at>`
  tags exactly as emitted in the final answer.

The code should add a raw answer accessor on the run state, for example:

```python
def raw_answer_text(self) -> str:
    if self.answer_block_id is None:
        return ""
    for block in self.blocks:
        if block.block_id == self.answer_block_id and block.kind == "answer":
            return block.content
    return ""
```

`FeishuCardRunSink.finalize(...)` and
`FeishuCardRunSink.update_final_after_transform(...)` should call the forwarder
after the final answer has been written into the answer block.

## Card Display Rewrite

Card display should not depend on unverified CardKit `<person>` behavior. The
implementation should instead use a plain-text display rewrite:

```text
<at user_id="ou_xxx">小P</at> 你好
  -> 对 小P 说，你好
```

If one paragraph contains multiple at tags, preserve all display names:

```text
<at user_id="ou_a">小P</at> <at user_id="ou_b">小C</at> 请看
  -> 对 小P、小C 说，请看
```

The rewrite timing is the CardKit display projection phase:

1. Assistant text is stored in run state unchanged.
2. `NativeBotAtForwarder` reads raw answer state unchanged.
3. The card renderer builds an answer-block display copy.
4. Only that answer-block display copy is rewritten to human-readable text.
5. The rewritten text is sent to CardKit as display markdown.

The rewrite must not run when appending stream deltas, finalizing answer state,
or processing native forwarding. It also must not run on `renderer.content(...)`
after mixed content has already been joined. Process text, tool output, footer
text, placeholder text, and code examples must not be rewritten.

Incomplete stream chunks are safe to leave unchanged. The renderer should only
rewrite complete tags that match:

```text
<at user_id="ou_xxx">display name</at>
```

## Native Forwarder Rules

`NativeBotAtForwarder` receives only the raw final answer text.

Processing rules:

1. Remove or ignore fenced code block content.
2. Split the remaining text into paragraphs.
3. Select paragraphs that contain at least one legal native at tag.
4. A legal tag is only:

```text
<at user_id="ou_xxx">display name</at>
```

5. `user_id` must start with `ou_`.
6. Do not accept `id=`, `open_id=`, or `cli_xxx`.
7. Forward the selected paragraph text unchanged as Feishu native text.

## Multiple At Messages

Multiple explicit at messages are allowed.

The forwarding unit is a paragraph:

- If one paragraph contains one at tag, send that paragraph as one text message.
- If one paragraph contains multiple at tags, send that paragraph once with all
  original tags preserved.
- If the final answer contains multiple separate at paragraphs, send multiple
  native text messages in the original paragraph order.

Deduplication should be based on the normalized full paragraph text, not only on
the target `open_id`. The same target bot may legitimately receive two distinct
instructions in one final answer; target-level dedupe would incorrectly drop the
second instruction.

To prevent runaway side effects, cap forwarding to a small number per run, such
as 5 native at messages. If the final answer contains more matching paragraphs,
send the first 5 in order and log a warning with the skipped count.

## Feishu Adapter Contract

Add a Feishu adapter method that forces native text sending, for example:

```python
async def send_raw_text(
    self,
    chat_id: str,
    text: str,
    *,
    reply_to: str | None = None,
    metadata: dict | None = None,
) -> SendResult:
    ...
```

This method must send:

```json
{
  "msg_type": "text",
  "content": "{\"text\":\"<at user_id=\\\"ou_xxx\\\">Name</at> message\"}"
}
```

It must bypass `_build_outbound_payload(...)` so native at content is not
converted into `post` or any CardKit representation.

## Failure Behavior

Native at forwarding is a side effect of the completed card reply.

- Failure to send native at text must not break the final card display.
- Log warning details including run id, chat id, target open ids, and error.
- Do not add production card footer noise for this failure by default.

## Configuration

Gate the behavior behind the existing Feishu display platform config shape:

```yaml
display:
  platforms:
    feishu:
      native_bot_at_forward: true
      native_bot_at_forward_max_messages: 5
```

Enable it first for 小A's profile. This feature sends extra visible group
messages, so it should be easy to disable during rollout.

## Tests

Parser and forwarder tests:

- A legal raw final answer paragraph triggers `send_raw_text`.
- A paragraph with multiple legal at tags sends one native text message.
- Multiple separate legal at paragraphs send multiple native text messages in
  order.
- Duplicate normalized paragraphs are sent once.
- The same target bot in two different paragraphs is sent twice.
- More than the configured cap sends only the first capped set and logs a
  warning.
- Fenced code block content does not trigger forwarding.
- `id=`, `open_id=`, and `cli_xxx` do not trigger forwarding.

Renderer and state tests:

- The forwarder reads raw answer state, not `renderer.content(state)`.
- Card display content may show `对 小P 说，...`, while raw answer state still
  contains `<at user_id="...">`.
- Human-readable display rewrite applies only to answer block display copies.
- Process, tool, footer, placeholder, and code-example text are not rewritten.

Transport tests:

- `send_raw_text` sends `msg_type="text"`.
- The outgoing payload preserves the native `<at user_id="...">...</at>` tag.
- `_build_outbound_payload(...)` is not used for native forwarding.

## Acceptance Criteria

1. 小A's final card reply still renders normally.
2. If the raw final answer contains legal native at paragraphs, 小A sends
   additional native text messages after the final card update.
3. The target bot receives a structured mention event.
4. Multiple at paragraphs are forwarded in order, subject to the configured cap.
5. Human-readable card display rewrite is not used as the trigger source.
