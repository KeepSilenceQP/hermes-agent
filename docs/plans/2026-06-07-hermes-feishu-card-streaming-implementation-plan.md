# Hermes Feishu Card Streaming Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an off-by-default Feishu CardKit run-card renderer for Hermes / 小A so one Feishu card contains assistant text and tool-call state for a single agent run.

**Architecture:** Use a Feishu-only `FeishuCardRunSink` wired to the existing production callbacks in `gateway/run.py`: `stream_delta_callback`, `interim_assistant_callback`, and `tool_progress_callback`. Keep CardKit create/update inside the Feishu platform boundary and expose delivery-state properties equivalent to `GatewayStreamConsumer` so the runner can avoid duplicate final sends.

**Tech Stack:** Python, Hermes gateway, Feishu `lark-oapi==1.5.3` or raw Feishu OpenAPI HTTP, pytest, unittest mocks.

**Command convention:** Run all implementation and verification commands from the clean Hermes worktree created in Task 0:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
```

Then use the project environment:

```bash
uv run --extra dev --extra feishu python -m pytest ...
uv run --extra dev --extra feishu python <script.py>
```

If `uv` is unavailable in the worktree, stop and identify the existing Hermes virtualenv before continuing. Do not use bare `python` or bare `pytest` for verification.

**Live checkout rule:** Do not implement directly in `/Users/bytedance/.hermes/hermes-agent`. Use that checkout only as the source repository for creating the worktree and, after tests pass, for final controlled rollout if needed.

---

## Execution Workspace And Patch Stack

The implementation must leave behind a replayable patch stack in the work-area repository. Do not rely on copying changed files back into Hermes after upgrades.

- Source Hermes repo: `/Users/bytedance/.hermes/hermes-agent`
- Implementation worktree: `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent`
- Implementation branch: `qinpeng/feishu-card-streaming`
- Patch artifacts: `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/patches/feishu-card-streaming-<timestamp>/`

Rules:

- All code edits happen in the implementation worktree.
- Every logical change should be committed in the worktree with a focused message.
- After the implementation is complete, export `git format-patch` artifacts plus a combined patch into the patch artifact directory.
- Record the base commit in `BASE_COMMIT`, before/after status snapshots, and a verification summary.
- After future Hermes upgrades, reapply with `git am -3` or `git apply --3way`. Do not use `cp` as the normal restoration mechanism, because it can silently overwrite upstream fixes.
- `cp` is only an emergency fallback after manual review.
- The feature remains off by default. Live Hermes config is changed only after tests pass and a config backup exists.

## File Structure

Hermes code paths below are repository-relative logical paths. Implement them under `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent/`, not directly under the live checkout.

- Create `gateway/platforms/feishu_card_stream.py`
  - Owns `FeishuCardRunState`, `FeishuCardToolBlock`, `FeishuCardRunRenderer`, and `FeishuCardRunSink`.
  - Contains no Feishu credentials and no direct config loading.
- Create `gateway/stream_text_cleaner.py`
  - Owns shared streaming display cleanup: think-block filtering, `MEDIA:<path>` cleanup, and internal directive cleanup.
- Modify `gateway/stream_consumer.py`
  - Reuses `stream_text_cleaner.py` so existing streaming and Feishu card streaming display the same cleaned text.
- Modify `gateway/platforms/feishu.py`
  - Adds small CardKit transport methods used by the sink.
  - Keeps existing `send()` and `edit_message()` behavior unchanged.
- Modify `gateway/run.py`
  - Creates a `FeishuCardRunSink` only for Feishu when `display.platforms.feishu.card_streaming` is true.
  - Routes existing callbacks into the sink.
  - Suppresses separate progress bubbles and duplicate final sends for card-streaming runs.
- Create `tests/gateway/test_feishu_card_stream.py`
  - Unit tests for state reduction, rendering, tool identity, fallback, and sink delivery flags.
- Create `tests/gateway/test_stream_text_cleaner.py`
  - Unit tests for shared think-block and media directive cleanup.
- Create `tests/gateway/test_feishu_card_stream_runner.py`
  - Runner-level tests for the callback wiring and final-send contract.
- Use `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent/`
  - Local clean worktree for this implementation. Do not treat it as the durable artifact.
- Use `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/patches/feishu-card-streaming-<timestamp>/`
  - Durable patch artifact directory containing `BASE_COMMIT`, status snapshots, exported commits, and verification notes.
- Create `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/scripts/export-patches.sh`
  - Re-exports the committed worktree patch stack into the latest patch artifact directory.
- Create `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/scripts/apply-to-hermes.sh`
  - Applies the exported patch stack to a Hermes checkout using three-way patch application.
- Create `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/scripts/verify.sh`
  - Runs the fixed verification commands from the worktree.
- Use `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/verification/`
  - Stores Phase 0 CardKit API discovery output and live validation notes.

## Task 0: Prepare Clean Hermes Worktree And Patch Stack

**Files:**
- Create or reuse: `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent`
- Create: `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/patches/feishu-card-streaming-<timestamp>/`
- Create: `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/scripts/export-patches.sh`
- Create: `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/scripts/apply-to-hermes.sh`
- Create: `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/scripts/verify.sh`

- [ ] **Step 1: Create or reuse the clean Hermes worktree**

Run:

```bash
src=/Users/bytedance/.hermes/hermes-agent
worktree=/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
branch=qinpeng/feishu-card-streaming

if [ -d "$worktree/.git" ]; then
  git -C "$worktree" status --short
else
  git -C "$src" worktree add -b "$branch" "$worktree" main
fi
```

Expected:

- Existing worktree prints a clean `git status --short`, or a new worktree is created.
- If the worktree is dirty, stop and inspect before implementation.
- Do not continue by editing `/Users/bytedance/.hermes/hermes-agent` directly.

- [ ] **Step 2: Create the patch artifact directory and record the base commit**

Run:

```bash
worktree=/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
ts="$(date +%Y%m%d-%H%M%S)"
patch_dir="/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/patches/feishu-card-streaming-${ts}"

mkdir -p "$patch_dir/commits"
git -C "$worktree" rev-parse HEAD > "$patch_dir/BASE_COMMIT"
git -C "$worktree" status --short > "$patch_dir/status-before.txt"
printf '%s\n' "$patch_dir" > /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/patches/latest-feishu-card-streaming-dir
```

Expected:

- `BASE_COMMIT` contains exactly one commit hash.
- `status-before.txt` is empty for a clean worktree.
- `latest-feishu-card-streaming-dir` points to the new artifact directory.

- [ ] **Step 3: Create patch export helper**

Create `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/scripts/export-patches.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

worktree=${WORKTREE:-/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent}
latest_file=/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/patches/latest-feishu-card-streaming-dir
patch_dir=${PATCH_DIR:-$(cat "$latest_file")}
base=${BASE_COMMIT:-$(cat "$patch_dir/BASE_COMMIT")}

mkdir -p "$patch_dir/commits"
git -C "$worktree" status --short > "$patch_dir/status-after.txt"
git -C "$worktree" log --oneline "$base"..HEAD > "$patch_dir/commits/series.txt"
git -C "$worktree" format-patch "$base"..HEAD -o "$patch_dir/commits"
git -C "$worktree" format-patch "$base"..HEAD --stdout > "$patch_dir/combined-feishu-card-streaming.patch"
```

Run:

```bash
chmod +x /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/scripts/export-patches.sh
```

- [ ] **Step 4: Create three-way apply helper**

Create `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/scripts/apply-to-hermes.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

target=${1:-/Users/bytedance/.hermes/hermes-agent}
patch_dir=${2:-$(cat /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/patches/latest-feishu-card-streaming-dir)}

git -C "$target" status --short
echo "Applying patch stack from: $patch_dir"
git -C "$target" am -3 "$patch_dir"/commits/[0-9][0-9][0-9][0-9]-*.patch
```

Run:

```bash
chmod +x /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/scripts/apply-to-hermes.sh
```

Expected:

- The helper uses `git am -3`.
- If `git am -3` conflicts after a Hermes upgrade, stop and resolve manually. Do not replace files with `cp`.

- [ ] **Step 5: Create verification helper**

Create `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/scripts/verify.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

worktree=${WORKTREE:-/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent}
cd "$worktree"

uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py \
  tests/gateway/test_stream_text_cleaner.py \
  tests/gateway/test_feishu_card_stream_runner.py \
  tests/gateway/test_feishu.py::TestFeishuCardStreamTransport \
  tests/gateway/test_stream_consumer.py \
  tests/gateway/test_display_config.py \
  -q
```

Run:

```bash
chmod +x /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/scripts/verify.sh
```

## Task 1: CardKit API Discovery Gate

**Files:**
- Create: `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/code/discover_feishu_cardkit.py`
- Create: `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/verification/cardkit-api-discovery-2026-06-07.md`

- [ ] **Step 1: Write the discovery script**

Create `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/code/discover_feishu_cardkit.py`:

```python
#!/usr/bin/env python3
from __future__ import annotations

import importlib
import pkgutil
import json
from pathlib import Path


def public_attrs(obj: object) -> list[str]:
    return sorted(name for name in dir(obj) if not name.startswith("_"))


def discover_modules(root_module: object) -> list[dict[str, object]]:
    root_path = getattr(root_module, "__path__", None)
    if not root_path:
        return []
    matches: list[dict[str, object]] = []
    for module_info in pkgutil.walk_packages(root_path, prefix=f"{root_module.__name__}."):
        name = module_info.name
        lowered = name.lower()
        if not any(token in lowered for token in ("card", "cardkit", "im.v1.message", "message")):
            continue
        entry: dict[str, object] = {"module": name, "is_package": module_info.ispkg}
        try:
            module = importlib.import_module(name)
            attrs = public_attrs(module)
            entry["attrs_containing_card"] = [attr for attr in attrs if "card" in attr.lower()]
            entry["attrs_containing_message"] = [attr for attr in attrs if "message" in attr.lower()]
        except Exception as exc:
            entry["import_error"] = repr(exc)
        matches.append(entry)
    return matches


def main() -> None:
    report: dict[str, object] = {}
    try:
        lark_oapi = importlib.import_module("lark_oapi")
        report["lark_oapi_imported"] = True
        report["lark_oapi_file"] = getattr(lark_oapi, "__file__", "")
        report["lark_oapi_version"] = getattr(lark_oapi, "__version__", "")
        client_cls = getattr(lark_oapi, "Client", None)
        report["has_client"] = client_cls is not None
        report["top_level_attrs_containing_card"] = [
            name for name in public_attrs(lark_oapi) if "card" in name.lower()
        ]
        if client_cls is not None:
            report["client_class_attrs_containing_card"] = [
                name for name in public_attrs(client_cls) if "card" in name.lower()
            ]
        report["candidate_modules"] = discover_modules(lark_oapi)
    except Exception as exc:
        report["lark_oapi_imported"] = False
        report["import_error"] = repr(exc)

    out = Path("/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/verification/cardkit-api-discovery-raw.json")
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the discovery script**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
uv run --extra dev --extra feishu python /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/code/discover_feishu_cardkit.py
```

Expected:

- The command prints JSON.
- The file `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/verification/cardkit-api-discovery-raw.json` exists.

- [ ] **Step 3: Write the discovery result note**

Create `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/verification/cardkit-api-discovery-2026-06-07.md` with this structure:

```markdown
# CardKit API Discovery 2026-06-07

## Environment

- Hermes source: `/Users/bytedance/.hermes/hermes-agent`
- Feishu dependency: `lark-oapi==1.5.3`
- Raw discovery JSON: `cardkit-api-discovery-raw.json`

## Result

- `lark_oapi` import: recorded in raw JSON.
- SDK CardKit surface: recorded in raw JSON.
- Candidate nested modules: recorded in `candidate_modules`.
- Implementation transport decision: use SDK methods only if raw JSON proves CardKit create/update methods are available; otherwise implement raw Feishu OpenAPI HTTP inside `FeishuAdapter`.

## Gate Decision

Proceed only when one of these paths is confirmed:

- SDK path: card create/update methods are visible and mockable.
- Raw HTTP path: tenant access token retrieval is available in the adapter and request signing/auth headers are clear from existing Feishu adapter code.
```

- [ ] **Step 4: Gate before implementation**

If neither SDK nor raw HTTP path is clear after this task, stop. Revise the design before writing Hermes production code.

## Task 2: Feishu Card State And Renderer

**Files:**
- Create: `/Users/bytedance/.hermes/hermes-agent/gateway/stream_text_cleaner.py`
- Create: `/Users/bytedance/.hermes/hermes-agent/gateway/platforms/feishu_card_stream.py`
- Modify: `/Users/bytedance/.hermes/hermes-agent/gateway/stream_consumer.py`
- Test: `/Users/bytedance/.hermes/hermes-agent/tests/gateway/test_stream_text_cleaner.py`
- Test: `/Users/bytedance/.hermes/hermes-agent/tests/gateway/test_feishu_card_stream.py`

- [ ] **Step 1: Write failing shared text-cleaner tests**

Create `/Users/bytedance/.hermes/hermes-agent/tests/gateway/test_stream_text_cleaner.py`:

```python
from gateway.stream_text_cleaner import StreamDisplayTextFilter, clean_stream_display_text


def test_clean_stream_display_text_removes_media_and_voice_directive():
    raw = "hello\nMEDIA:/tmp/audio.mp3\n[[audio_as_voice]]\nworld"

    assert clean_stream_display_text(raw) == "hello\nworld"


def test_stream_display_text_filter_suppresses_complete_think_block():
    filt = StreamDisplayTextFilter()

    assert filt.feed("<think>hidden</think>visible") == "visible"


def test_stream_display_text_filter_suppresses_split_think_block():
    filt = StreamDisplayTextFilter()

    assert filt.feed("<thi") == ""
    assert filt.feed("nk>hidden</think>visible") == "visible"


def test_stream_display_text_filter_keeps_visible_text_around_think_block():
    filt = StreamDisplayTextFilter()

    assert filt.feed("before <think>hidden</think> after") == "before  after"
```

- [ ] **Step 2: Verify text-cleaner tests fail**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_stream_text_cleaner.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'gateway.stream_text_cleaner'`.

- [ ] **Step 3: Implement shared stream text cleaner**

Create `/Users/bytedance/.hermes/hermes-agent/gateway/stream_text_cleaner.py`:

```python
from __future__ import annotations

import re

from gateway.platforms.base import MEDIA_TAG_CLEANUP_RE


def clean_stream_display_text(text: str) -> str:
    if not text:
        return ""
    cleaned = text.replace("[[audio_as_voice]]", "")
    cleaned = MEDIA_TAG_CLEANUP_RE.sub("", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.rstrip()


class StreamDisplayTextFilter:
    _OPEN_TAGS = ("<think>", "<thinking>", "<reasoning>")
    _CLOSE_TAGS = ("</think>", "</thinking>", "</reasoning>")

    def __init__(self) -> None:
        self._in_think_block = False
        self._pending = ""

    def feed(self, text: str) -> str:
        if not text:
            return ""
        buf = self._pending + text
        self._pending = ""
        output: list[str] = []

        while buf:
            if self._in_think_block:
                close_idx, close_len = self._find_earliest(buf, self._CLOSE_TAGS)
                if close_idx == -1:
                    self._pending = self._suffix_that_may_start_tag(buf, self._CLOSE_TAGS)
                    return clean_stream_display_text("".join(output))
                buf = buf[close_idx + close_len:]
                self._in_think_block = False
                continue

            open_idx, open_len = self._find_earliest(buf, self._OPEN_TAGS)
            if open_idx == -1:
                safe, pending = self._split_safe_suffix(buf, self._OPEN_TAGS)
                output.append(safe)
                self._pending = pending
                break

            output.append(buf[:open_idx])
            buf = buf[open_idx + open_len:]
            self._in_think_block = True

        return clean_stream_display_text("".join(output))

    @staticmethod
    def _find_earliest(buf: str, tags: tuple[str, ...]) -> tuple[int, int]:
        best_idx = -1
        best_len = 0
        for tag in tags:
            idx = buf.find(tag)
            if idx != -1 and (best_idx == -1 or idx < best_idx):
                best_idx = idx
                best_len = len(tag)
        return best_idx, best_len

    @staticmethod
    def _split_safe_suffix(buf: str, tags: tuple[str, ...]) -> tuple[str, str]:
        max_pending = 0
        for size in range(1, min(len(buf), max(map(len, tags)) - 1) + 1):
            suffix = buf[-size:]
            if any(tag.startswith(suffix) for tag in tags):
                max_pending = size
        if max_pending:
            return buf[:-max_pending], buf[-max_pending:]
        return buf, ""

    @classmethod
    def _suffix_that_may_start_tag(cls, buf: str, tags: tuple[str, ...]) -> str:
        return cls._split_safe_suffix(buf, tags)[1]
```

- [ ] **Step 4: Wire existing stream consumer to shared cleaner**

In `/Users/bytedance/.hermes/hermes-agent/gateway/stream_consumer.py`, replace the private think-block and media cleanup logic with `StreamDisplayTextFilter` / `clean_stream_display_text`, keeping public behavior unchanged:

```python
from gateway.stream_text_cleaner import StreamDisplayTextFilter, clean_stream_display_text
```

Construct one `StreamDisplayTextFilter` per `GatewayStreamConsumer` instance and delegate `_filter_and_accumulate()` / `_clean_for_display()` to the shared helper. Keep existing `GatewayStreamConsumer` tests passing.

- [ ] **Step 5: Verify text-cleaner and stream consumer tests pass**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_stream_text_cleaner.py tests/gateway/test_stream_consumer.py -q
```

Expected: PASS.

- [ ] **Step 6: Write failing renderer tests**

Create `/Users/bytedance/.hermes/hermes-agent/tests/gateway/test_feishu_card_stream.py`:

```python
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
```

- [ ] **Step 7: Verify renderer tests fail**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu_card_stream.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'gateway.platforms.feishu_card_stream'`.

- [ ] **Step 8: Implement minimal state and renderer**

Create `/Users/bytedance/.hermes/hermes-agent/gateway/platforms/feishu_card_stream.py`:

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FeishuCardToolBlock:
    token: str
    tool_name: str
    preview: str = ""
    args: dict[str, Any] = field(default_factory=dict)
    status: str = "running"


@dataclass
class FeishuCardRunState:
    text_blocks: list[str] = field(default_factory=list)
    tools: list[FeishuCardToolBlock] = field(default_factory=list)
    terminal: str = "running"
    _next_tool_index: int = 0

    def append_text(self, text: str) -> None:
        if not text:
            return
        if self.text_blocks:
            self.text_blocks[-1] += text
        else:
            self.text_blocks.append(text)

    def append_commentary(self, text: str) -> None:
        if text:
            self.text_blocks.append(text)

    def start_tool(self, *, tool_name: str, preview: str = "", args: dict[str, Any] | None = None, token: str | None = None) -> str:
        if token is None:
            token = f"tool-{self._next_tool_index}"
            self._next_tool_index += 1
        self.tools.append(FeishuCardToolBlock(token=token, tool_name=tool_name, preview=preview or "", args=args or {}))
        return token

    def finish_tool(self, token: str, *, ok: bool = True, output: str | None = None) -> None:
        for tool in self.tools:
            if tool.token == token:
                tool.status = "done" if ok else "error"
                return

    def finalize(self) -> None:
        self.terminal = "done"


class FeishuCardRunRenderer:
    def render(self, state: FeishuCardRunState) -> dict[str, Any]:
        elements: list[dict[str, Any]] = []
        for block in state.text_blocks:
            if block.strip():
                elements.append({"tag": "markdown", "content": block})
        for tool in state.tools:
            preview = f" — `{tool.preview}`" if tool.preview else ""
            elements.append({
                "tag": "markdown",
                "content": f"**{tool.tool_name}**{preview}\n\nStatus: {tool.status}",
            })
        if state.terminal == "running":
            elements.append({"tag": "markdown", "content": "_calling tools_" if state.tools else "_outputting_"})
        return {
            "schema": "2.0",
            "config": {
                "streaming_mode": state.terminal == "running",
                "summary": {"content": "running" if state.terminal == "running" else "done"},
            },
            "body": {"elements": elements or [{"tag": "markdown", "content": "_running_"}]},
        }
```

- [ ] **Step 9: Verify renderer tests pass**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu_card_stream.py -q
```

Expected: PASS.

- [ ] **Step 10: Commit renderer and shared cleaner**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
git add gateway/stream_text_cleaner.py gateway/stream_consumer.py gateway/platforms/feishu_card_stream.py tests/gateway/test_stream_text_cleaner.py tests/gateway/test_feishu_card_stream.py
git commit -m "feat(feishu): add card stream renderer"
```

Expected: commit succeeds if the Hermes source tree is clean enough for this work. If unrelated dirty files exist, stage only the two listed files.

## Task 3: Tool Identity And Completion Mapping

**Files:**
- Modify: `/Users/bytedance/.hermes/hermes-agent/gateway/platforms/feishu_card_stream.py`
- Test: `/Users/bytedance/.hermes/hermes-agent/tests/gateway/test_feishu_card_stream.py`

- [ ] **Step 1: Add failing identity tests**

Append to `tests/gateway/test_feishu_card_stream.py`:

```python
def test_repeated_same_name_tools_get_distinct_tokens():
    state = FeishuCardRunState()
    first = state.start_tool(tool_name="terminal", preview="one")
    second = state.start_tool(tool_name="terminal", preview="two")

    state.finish_tool(first, ok=True)

    assert first != second
    assert state.tools[0].status == "done"
    assert state.tools[1].status == "running"


def test_finish_oldest_matching_running_tool_when_no_token():
    state = FeishuCardRunState()
    first = state.start_tool(tool_name="terminal", preview="one")
    second = state.start_tool(tool_name="terminal", preview="two")

    matched = state.finish_oldest_running_tool(tool_name="terminal", ok=False)

    assert matched == first
    assert state.tools[0].status == "error"
    assert state.tools[1].status == "running"
```

- [ ] **Step 2: Verify identity tests fail**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu_card_stream.py::test_finish_oldest_matching_running_tool_when_no_token -q
```

Expected: FAIL with `AttributeError: 'FeishuCardRunState' object has no attribute 'finish_oldest_running_tool'`.

- [ ] **Step 3: Implement oldest-running fallback**

Add this method to `FeishuCardRunState`:

```python
    def finish_oldest_running_tool(self, *, tool_name: str, ok: bool = True) -> str | None:
        for tool in self.tools:
            if tool.tool_name == tool_name and tool.status == "running":
                tool.status = "done" if ok else "error"
                return tool.token
        return None
```

- [ ] **Step 4: Verify identity tests pass**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu_card_stream.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit identity mapping**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
git add gateway/platforms/feishu_card_stream.py tests/gateway/test_feishu_card_stream.py
git commit -m "test(feishu): cover card tool identity mapping"
```

Expected: commit succeeds.

## Task 4: FeishuCardRunSink Delivery Contract

**Files:**
- Modify: `/Users/bytedance/.hermes/hermes-agent/gateway/platforms/feishu_card_stream.py`
- Test: `/Users/bytedance/.hermes/hermes-agent/tests/gateway/test_feishu_card_stream.py`

- [ ] **Step 1: Add failing sink tests**

Append to `tests/gateway/test_feishu_card_stream.py`:

```python
import asyncio
import threading
from types import SimpleNamespace

from gateway.platforms.feishu_card_stream import FeishuCardRunSink


class _FakeFeishuCardAdapter:
    def __init__(self):
        self.created = []
        self.updated = []
        self.sent_text = []

    async def create_card_stream_message(self, chat_id, card, metadata=None, reply_to=None):
        self.created.append((chat_id, card, metadata, reply_to))
        return SimpleNamespace(success=True, message_id="om_card_1", card_id="card_1")

    async def update_card_stream_message(self, update_handle, card, sequence=None):
        self.updated.append((update_handle, card, sequence))
        return SimpleNamespace(success=True)

    async def send(self, chat_id, content, metadata=None, reply_to=None):
        self.sent_text.append((chat_id, content, metadata, reply_to))
        return SimpleNamespace(success=True, message_id="om_text_1")


def test_sink_finalize_marks_final_delivery():
    adapter = _FakeFeishuCardAdapter()
    sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1")
    sink.on_delta("final answer")

    delivered = asyncio.run(sink.finalize("final answer"))

    assert delivered is True
    assert sink.message_id == "om_card_1"
    assert sink.final_response_sent is True
    assert sink.final_content_delivered is True


def test_sink_delta_schedules_update_before_finalize():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        sink.on_delta("streaming")
        await sink.drain_pending_updates()

        assert adapter.created
        assert adapter.updated
        assert sink.final_response_sent is False

    asyncio.run(run())


def test_sink_tool_progress_schedules_update_before_finalize():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        sink.on_tool_progress("tool.started", tool_name="exec_command", preview="ls")
        await sink.drain_pending_updates()

        assert adapter.created
        assert adapter.updated

    asyncio.run(run())


def test_sink_accepts_delta_from_worker_thread():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        worker = threading.Thread(target=lambda: sink.on_delta("from worker"))
        worker.start()
        worker.join(timeout=2)
        await sink.drain_pending_updates()

        assert adapter.created
        assert adapter.updated

    asyncio.run(run())


def test_sink_filters_internal_stream_markers_before_render():
    async def run():
        adapter = _FakeFeishuCardAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0)

        sink.on_delta("<think>hidden</think>visible\nMEDIA:/tmp/a.mp3\n[[audio_as_voice]]")
        await sink.drain_pending_updates()

        rendered = str(adapter.updated[-1][1])
        assert "visible" in rendered
        assert "hidden" not in rendered
        assert "MEDIA:/tmp/a.mp3" not in rendered
        assert "[[audio_as_voice]]" not in rendered

    asyncio.run(run())


def test_sink_disables_card_updates_after_repeated_update_failures():
    class FailingUpdateAdapter(_FakeFeishuCardAdapter):
        async def update_card_stream_message(self, update_handle, card, sequence=None):
            self.updated.append((update_handle, card, sequence))
            return SimpleNamespace(success=False, error="update failed")

    async def run():
        adapter = FailingUpdateAdapter()
        sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", update_interval_sec=0, max_update_failures=2)

        sink.on_delta("one")
        await sink.drain_pending_updates()
        sink.on_delta("two")
        await sink.drain_pending_updates()
        sink.on_delta("three")
        await sink.drain_pending_updates()

        assert sink.card_updates_disabled is True
        assert len(adapter.updated) == 2

    asyncio.run(run())


def test_sink_card_create_failure_falls_back_to_text():
    class BrokenAdapter(_FakeFeishuCardAdapter):
        async def create_card_stream_message(self, chat_id, card, metadata=None, reply_to=None):
            return SimpleNamespace(success=False, error="card failed")

    adapter = BrokenAdapter()
    sink = FeishuCardRunSink(adapter=adapter, chat_id="oc_1", reply_to="om_parent")
    sink.on_delta("visible answer")

    delivered = asyncio.run(sink.finalize("visible answer"))

    assert delivered is True
    assert sink.fallback_sent is True
    assert sink.final_response_sent is True
    assert adapter.sent_text == [("oc_1", "visible answer", None, "om_parent")]
```

- [ ] **Step 2: Verify sink tests fail**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu_card_stream.py::test_sink_finalize_marks_final_delivery -q
```

Expected: FAIL with `ImportError` or `AttributeError` because `FeishuCardRunSink` does not exist.

- [ ] **Step 3: Implement minimal sink**

Add to `gateway/platforms/feishu_card_stream.py`:

```python
# Requires imports: asyncio, queue, threading, Any, StreamDisplayTextFilter.

class FeishuCardRunSink:
    def __init__(
        self,
        *,
        adapter: Any,
        chat_id: str,
        metadata: dict[str, Any] | None = None,
        reply_to: str | None = None,
        update_interval_sec: float = 0.25,
        max_update_failures: int = 3,
        loop: asyncio.AbstractEventLoop | None = None,
    ):
        self.adapter = adapter
        self.chat_id = chat_id
        self.metadata = metadata
        self.reply_to = reply_to
        self.update_interval_sec = update_interval_sec
        self.max_update_failures = max_update_failures
        self._loop = loop
        if self._loop is None:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                self._loop = None
        self.state = FeishuCardRunState()
        self.renderer = FeishuCardRunRenderer()
        self.text_filter = StreamDisplayTextFilter()
        self.message_id: str | None = None
        self.update_handle: str | None = None
        self.final_response_sent = False
        self.final_content_delivered = False
        self.fallback_sent = False
        self.card_updates_disabled = False
        self._closed = False
        self._event_queue: queue.Queue[tuple[str, tuple[Any, ...], dict[str, Any]]] = queue.Queue()
        self._drain_task: asyncio.Task[None] | None = None
        self._schedule_lock = threading.Lock()
        self._flush_lock = asyncio.Lock()
        self._sequence = 0
        self._update_failures = 0

    def on_delta(self, text: str) -> None:
        self._enqueue("delta", text)

    def on_commentary(self, text: str) -> None:
        self._enqueue("commentary", text)

    def on_tool_progress(self, event_type: str, tool_name: str | None = None, preview: str | None = None, args: dict[str, Any] | None = None, **kwargs: Any) -> None:
        self._enqueue("tool", event_type, tool_name, preview, args, **kwargs)

    def _enqueue(self, kind: str, *args: Any, **kwargs: Any) -> None:
        if self._closed:
            return
        self._event_queue.put((kind, args, kwargs))
        self._schedule_drain_threadsafe()

    def _schedule_drain_threadsafe(self) -> None:
        if self._loop is None:
            return
        with self._schedule_lock:
            self._loop.call_soon_threadsafe(self._ensure_drain_task)

    def _ensure_drain_task(self) -> None:
        if self._closed:
            return
        if self._drain_task is None or self._drain_task.done():
            self._drain_task = asyncio.create_task(self._drain_and_flush())

    async def _drain_and_flush(self) -> None:
        if self.update_interval_sec > 0:
            await asyncio.sleep(self.update_interval_sec)
        changed = self._drain_events()
        if changed and not self.card_updates_disabled:
            ok = await self.flush()
            if not ok:
                self._record_update_failure()

    def _drain_events(self) -> bool:
        changed = False
        while True:
            try:
                kind, args, kwargs = self._event_queue.get_nowait()
            except queue.Empty:
                return changed
            changed = True
            if kind == "delta":
                cleaned = self.text_filter.feed(str(args[0]))
                if cleaned:
                    self.state.append_text(cleaned)
            elif kind == "commentary":
                cleaned = self.text_filter.feed(str(args[0]))
                if cleaned:
                    self.state.append_commentary(cleaned)
            elif kind == "tool":
                self._apply_tool_progress(*args, **kwargs)

    def _apply_tool_progress(self, event_type: str, tool_name: str | None = None, preview: str | None = None, args: dict[str, Any] | None = None, **kwargs: Any) -> None:
        if not tool_name:
            return
        token = kwargs.get("tool_call_id") or kwargs.get("call_id") or kwargs.get("id")
        if event_type == "tool.started":
            self.state.start_tool(tool_name=tool_name, preview=preview or "", args=args, token=token)
        elif event_type == "tool.completed":
            ok = not bool(kwargs.get("error") or kwargs.get("failed"))
            if token:
                self.state.finish_tool(str(token), ok=ok)
            else:
                self.state.finish_oldest_running_tool(tool_name=tool_name, ok=ok)

    def _record_update_failure(self) -> None:
        self._update_failures += 1
        if self._update_failures >= self.max_update_failures:
            self.card_updates_disabled = True

    async def drain_pending_updates(self) -> None:
        task = self._drain_task
        if task is not None and not task.done():
            await task
        if not self._event_queue.empty():
            if self.card_updates_disabled:
                self._drain_events()
            else:
                await self._drain_and_flush()

    async def _send_fallback_text(self, text: str) -> bool:
        result = await self.adapter.send(self.chat_id, text, metadata=self.metadata, reply_to=self.reply_to)
        if getattr(result, "success", False):
            self.message_id = getattr(result, "message_id", None)
            self.fallback_sent = True
            self.final_response_sent = True
            self.final_content_delivered = True
            return True
        return False

    async def _ensure_card(self) -> bool:
        if self.update_handle:
            return True
        card = self.renderer.render(self.state)
        result = await self.adapter.create_card_stream_message(self.chat_id, card, metadata=self.metadata, reply_to=self.reply_to)
        if getattr(result, "success", False):
            self.message_id = getattr(result, "message_id", None)
            self.update_handle = (
                getattr(result, "update_handle", None)
                or getattr(result, "card_id", None)
                or getattr(result, "message_id", None)
            )
            return bool(self.update_handle)
        return False

    async def flush(self) -> bool:
        if self.card_updates_disabled:
            return False
        async with self._flush_lock:
            seq = self._sequence = self._sequence + 1
            if not await self._ensure_card():
                return False
            result = await self.adapter.update_card_stream_message(self.update_handle, self.renderer.render(self.state), sequence=seq)
            return bool(getattr(result, "success", False))

    async def finalize(self, final_text: str) -> bool:
        self._closed = True
        await self.drain_pending_updates()
        if final_text and not "".join(self.state.text_blocks).strip():
            self.state.append_text(final_text)
        self.state.finalize()
        if await self.flush():
            self.final_response_sent = True
            self.final_content_delivered = True
            return True
        return await self._send_fallback_text(final_text or "".join(self.state.text_blocks))

    async def update_final_after_transform(self, final_text: str) -> bool:
        self._closed = True
        await self.drain_pending_updates()
        self.state.text_blocks = [final_text] if final_text else self.state.text_blocks
        self.state.finalize()
        if self.update_handle and await self.flush():
            self.final_response_sent = True
            self.final_content_delivered = True
            return True
        return await self._send_fallback_text(final_text)

    async def finish_failed(self, error_text: str) -> bool:
        self._closed = True
        await self.drain_pending_updates()
        self.state.append_commentary(error_text)
        self.state.terminal = "error"
        if self.update_handle and await self.flush():
            self.final_response_sent = True
            self.final_content_delivered = True
            return True
        return await self._send_fallback_text(error_text)
```

- [ ] **Step 4: Verify sink tests pass**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu_card_stream.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit sink**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
git add gateway/platforms/feishu_card_stream.py tests/gateway/test_feishu_card_stream.py
git commit -m "feat(feishu): add card run sink"
```

Expected: commit succeeds.

## Task 5: Feishu Adapter CardKit Transport

**Files:**
- Modify: `/Users/bytedance/.hermes/hermes-agent/gateway/platforms/feishu.py`
- Test: `/Users/bytedance/.hermes/hermes-agent/tests/gateway/test_feishu.py`

- [ ] **Step 1: Add failing adapter transport tests**

Append to `tests/gateway/test_feishu.py`:

```python
class TestFeishuCardStreamTransport(unittest.TestCase):
    def test_create_card_stream_message_returns_update_handle_from_discovered_transport(self):
        from gateway.config import PlatformConfig
        from gateway.platforms.feishu import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        adapter._create_card_stream_transport = AsyncMock(
            return_value=SimpleNamespace(success=True, message_id="om_1", update_handle="om_1")
        )

        result = asyncio.run(adapter.create_card_stream_message("oc_1", {"schema": "2.0"}))

        self.assertTrue(result.success)
        self.assertEqual(result.message_id, "om_1")
        self.assertEqual(result.update_handle, "om_1")
        adapter._create_card_stream_transport.assert_awaited_once()

    def test_update_card_stream_message_reports_failure(self):
        from gateway.config import PlatformConfig
        from gateway.platforms.feishu import FeishuAdapter

        adapter = FeishuAdapter(PlatformConfig())
        adapter._update_card_stream_transport = AsyncMock(return_value=False)

        result = asyncio.run(adapter.update_card_stream_message("om_1", {"schema": "2.0"}))

        self.assertFalse(result.success)
        self.assertIn("card update failed", result.error)
```

- [ ] **Step 2: Verify adapter tests fail**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu.py::TestFeishuCardStreamTransport -q
```

Expected: FAIL with `AttributeError: 'FeishuAdapter' object has no attribute 'create_card_stream_message'`.

- [ ] **Step 3: Implement public adapter transport methods**

Add methods to `FeishuAdapter` in `gateway/platforms/feishu.py` near `send()` / `edit_message()`. The public contract is transport-neutral: the sink receives an `update_handle` and must not care whether that handle is a message ID, card ID, element ID, or another value proven by Phase 0.

```python
    async def create_card_stream_message(
        self,
        chat_id: str,
        card: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
        reply_to: Optional[str] = None,
    ) -> SendResult:
        try:
            result = await self._create_card_stream_transport(
                chat_id=chat_id,
                card=card,
                metadata=metadata,
                reply_to=reply_to,
            )
            if result.success:
                update_handle = (
                    getattr(result, "update_handle", None)
                    or getattr(result, "card_id", None)
                    or getattr(result, "message_id", None)
                )
                setattr(result, "update_handle", update_handle)
            return result
        except Exception as exc:
            logger.warning("[Feishu] card stream create failed: %s", exc)
            return SendResult(success=False, error=str(exc))

    async def update_card_stream_message(self, update_handle: str, card: Dict[str, Any], sequence: Optional[int] = None) -> SendResult:
        try:
            ok = await self._update_card_stream_transport(update_handle, card, sequence=sequence)
            if ok:
                return SendResult(success=True)
            return SendResult(success=False, error="card update failed")
        except Exception as exc:
            logger.warning("[Feishu] card stream update failed: %s", exc)
            return SendResult(success=False, error=str(exc))
```

Also add private `_create_card_stream_transport` and `_update_card_stream_transport` using exactly one of the concrete branches below. Record the selected branch in `verification/cardkit-api-discovery-2026-06-07.md` before writing these methods.

Phase 0 must decide one of these concrete shapes before implementation:

- Message-update path: send a CardKit/interactive message directly, return `message_id` as `update_handle`, update with the existing or raw `message.update` path.
- Card-entity path: create a CardKit card entity, send a reference message, return `card_id` or element handle as `update_handle`, update with the CardKit update API.
- Element-stream path: create/send a card and return the precise element/content handle needed by `cardkit/v1/cards/:card_id/elements/:element_id/content`.

Do not implement a hard-coded `_create_cardkit_card` / `_send_cardkit_reference` layer until discovery proves it is required. Keep the public methods above unchanged across all transport variants.

- [ ] **Step 4: Verify adapter transport tests pass**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu.py::TestFeishuCardStreamTransport -q
```

Expected: PASS.

- [ ] **Step 5: Commit adapter transport**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
git add gateway/platforms/feishu.py tests/gateway/test_feishu.py
git commit -m "feat(feishu): add card stream transport"
```

Expected: commit succeeds.

## Task 6: Runner Wiring And Single-Switch Streaming

**Files:**
- Modify: `/Users/bytedance/.hermes/hermes-agent/gateway/run.py`
- Test: `/Users/bytedance/.hermes/hermes-agent/tests/gateway/test_feishu_card_stream_runner.py`

- [ ] **Step 1: Add failing runner-level tests with a focused helper**

Create `/Users/bytedance/.hermes/hermes-agent/tests/gateway/test_feishu_card_stream_runner.py`:

```python
from types import SimpleNamespace


def test_card_streaming_flag_forces_stream_delta_capture():
    from gateway.run import _should_use_feishu_card_streaming

    assert _should_use_feishu_card_streaming(
        platform_key="feishu",
        user_config={"display": {"platforms": {"feishu": {"card_streaming": True}}}},
    ) is True


def test_card_streaming_false_for_non_feishu():
    from gateway.run import _should_use_feishu_card_streaming

    assert _should_use_feishu_card_streaming(
        platform_key="telegram",
        user_config={"display": {"platforms": {"feishu": {"card_streaming": True}}}},
    ) is False


def test_card_sink_delivery_suppresses_final_send():
    from gateway.run import _card_sink_delivered_final

    sink = SimpleNamespace(final_response_sent=True, final_content_delivered=True, fallback_sent=False)

    assert _card_sink_delivered_final(sink) is True


def test_card_streaming_keeps_tool_callback_without_progress_bubbles():
    from gateway.run import _should_attach_tool_progress_callback

    assert _should_attach_tool_progress_callback(
        tool_progress_enabled=False,
        want_feishu_card_streaming=True,
    ) is True


def test_card_streaming_keeps_interim_callback_without_interim_messages():
    from gateway.run import _should_attach_interim_callback

    assert _should_attach_interim_callback(
        want_interim_messages=False,
        want_feishu_card_streaming=True,
    ) is True


def test_card_streaming_does_not_create_gateway_stream_consumer():
    from gateway.run import _should_create_gateway_stream_consumer

    assert _should_create_gateway_stream_consumer(
        streaming_enabled=False,
        want_interim_messages=False,
        want_feishu_card_streaming=True,
    ) is False
```

Also add a focused wiring test around the smallest extracted callback helper or closure seam available in `run.py`:

- `progress_queue=None` and `want_feishu_card_streaming=True` still calls `sink.on_tool_progress(...)`.
- `tool_progress_enabled=False` prevents standalone queue sender startup, but does not prevent assigning `agent.tool_progress_callback`.
- `_want_interim_messages=False` and `want_feishu_card_streaming=True` still assigns `agent.interim_assistant_callback`.
- The card-streaming interim path calls `sink.on_commentary(...)` and does not call the normal adapter-send interim path.

- [ ] **Step 2: Verify runner tests fail**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu_card_stream_runner.py -q
```

Expected: FAIL with import errors for `_should_use_feishu_card_streaming`, `_card_sink_delivered_final`, `_should_attach_tool_progress_callback`, `_should_attach_interim_callback`, or `_should_create_gateway_stream_consumer`.

- [ ] **Step 3: Add runner helper functions**

Add near related display helper code in `gateway/run.py`:

```python
def _should_use_feishu_card_streaming(*, platform_key: str, user_config: dict) -> bool:
    if platform_key != "feishu":
        return False
    display = user_config.get("display") if isinstance(user_config, dict) else {}
    platforms = display.get("platforms") if isinstance(display, dict) else {}
    feishu = platforms.get("feishu") if isinstance(platforms, dict) else {}
    return bool(feishu.get("card_streaming")) if isinstance(feishu, dict) else False


def _card_sink_delivered_final(sink: object | None) -> bool:
    if sink is None:
        return False
    return bool(
        getattr(sink, "final_response_sent", False)
        or getattr(sink, "final_content_delivered", False)
        or getattr(sink, "fallback_sent", False)
    )


def _should_attach_tool_progress_callback(*, tool_progress_enabled: bool, want_feishu_card_streaming: bool) -> bool:
    return bool(tool_progress_enabled or want_feishu_card_streaming)


def _should_attach_interim_callback(*, want_interim_messages: bool, want_feishu_card_streaming: bool) -> bool:
    return bool(want_interim_messages or want_feishu_card_streaming)


def _should_create_gateway_stream_consumer(
    *,
    streaming_enabled: bool,
    want_interim_messages: bool,
    want_feishu_card_streaming: bool,
) -> bool:
    return bool((streaming_enabled or want_interim_messages) and not want_feishu_card_streaming)
```

- [ ] **Step 4: Wire sink into callback creation**

In `gateway/run.py`, in the same scope that computes `_want_stream_deltas`, add logic equivalent to:

```python
_want_feishu_card_streaming = _should_use_feishu_card_streaming(
    platform_key=platform_key,
    user_config=user_config,
)
_want_agent_stream_delta_callback = _streaming_enabled or _want_feishu_card_streaming
_want_interim_consumer = _should_attach_interim_callback(
    want_interim_messages=_want_interim_messages,
    want_feishu_card_streaming=_want_feishu_card_streaming,
)
_want_gateway_stream_consumer = _should_create_gateway_stream_consumer(
    streaming_enabled=_streaming_enabled,
    want_interim_messages=_want_interim_messages,
    want_feishu_card_streaming=_want_feishu_card_streaming,
)
```

Create `feishu_card_sink_holder = [None]` near `stream_consumer_holder`.

Only create `GatewayStreamConsumer` when `_want_gateway_stream_consumer` is true. Do not let `card_streaming=true` create an otherwise empty normal stream consumer.

When `_want_feishu_card_streaming` is true and the adapter is Feishu, construct:

```python
from gateway.platforms.feishu_card_stream import FeishuCardRunSink

feishu_card_sink_holder[0] = FeishuCardRunSink(
    adapter=_adapter,
    chat_id=_status_chat_id,
    metadata=_status_thread_metadata,
    reply_to=event_message_id,
)
```

Route callbacks. The card path must call `on_delta()`, which schedules a throttled async flush; do not wait until finalization to update the card.

```python
if _want_feishu_card_streaming and feishu_card_sink_holder[0] is not None:
    def _stream_delta_cb(text: str) -> None:
        if _run_still_current():
            feishu_card_sink_holder[0].on_delta(text)
elif _want_gateway_stream_consumer:
    # keep existing stream-consumer callback
else:
    _stream_delta_cb = None
```

In `progress_callback`, before putting tool lines into `progress_queue`, add:

```python
if _want_feishu_card_streaming and feishu_card_sink_holder[0] is not None:
    feishu_card_sink_holder[0].on_tool_progress(event_type, tool_name, preview, args, **kwargs)
    return
```

This branch must be placed before the existing `if not progress_queue or not _run_still_current(): return` guard. Card streaming suppresses separate Feishu progress bubbles, but it must not suppress the agent's tool-progress callback.

When assigning the callback to the agent, use:

```python
agent.tool_progress_callback = (
    progress_callback
    if _should_attach_tool_progress_callback(
        tool_progress_enabled=tool_progress_enabled,
        want_feishu_card_streaming=_want_feishu_card_streaming,
    )
    else None
)
```

Keep the existing `progress_task` / queue sender gated by `tool_progress_enabled`; do not start the separate progress bubble sender solely because `card_streaming` is true.

In `_interim_assistant_cb`, route to card sink before stream consumer when card streaming is enabled:

```python
if _want_feishu_card_streaming and feishu_card_sink_holder[0] is not None:
    feishu_card_sink_holder[0].on_commentary(text)
    return
```

When assigning the callback to the agent, use `_want_interim_consumer` rather than `_want_interim_messages`:

```python
agent.interim_assistant_callback = _interim_assistant_cb if _want_interim_consumer else None
```

When `card_streaming` is true and `_want_interim_messages` is false, interim/commentary events go only into the card sink and must not produce standalone Feishu messages.

- [ ] **Step 5: Wire final delivery and transformed final handling**

Where `run.py` checks `_sc.final_response_sent`, include the card sink:

```python
_card_sink = feishu_card_sink_holder[0]
_card_delivered = _card_sink_delivered_final(_card_sink)
```

Before normal final send, call:

```python
if _card_sink is not None and not response.get("failed"):
    _final = response.get("final_response") or ""
    _transformed = bool(response.get("response_transformed"))
    if _transformed:
        if await _card_sink.update_final_after_transform(_final):
            response["already_sent"] = True
    elif _final and await _card_sink.finalize(_final):
        response["already_sent"] = True
```

For failed responses:

```python
if _card_sink is not None and response.get("failed"):
    error_text = response.get("final_response") or response.get("error") or "Agent failed"
    if await _card_sink.finish_failed(error_text):
        response["already_sent"] = True
```

- [ ] **Step 6: Verify runner helper tests pass**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu_card_stream_runner.py -q
```

Expected: PASS.

- [ ] **Step 7: Run focused gateway tests**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_stream_text_cleaner.py tests/gateway/test_feishu_card_stream.py tests/gateway/test_feishu_card_stream_runner.py tests/gateway/test_stream_consumer.py -q
```

Expected: PASS.

- [ ] **Step 8: Commit runner wiring**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
git add gateway/run.py tests/gateway/test_feishu_card_stream_runner.py
git commit -m "feat(feishu): wire card streaming into runner"
```

Expected: commit succeeds.

## Task 7: Observability And Rollback Notes

**Files:**
- Modify: `/Users/bytedance/.hermes/hermes-agent/gateway/platforms/feishu_card_stream.py`
- Create: `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/verification/live-validation-template.md`

- [ ] **Step 1: Add structured log events in the sink**

Add a logger to `feishu_card_stream.py`:

```python
import logging

logger = logging.getLogger("gateway.feishu_card_stream")
```

Emit these log messages:

```python
logger.warning("feishu_card_create_failed")
logger.warning("feishu_card_update_failed")
logger.warning("feishu_card_updates_disabled")
logger.info("feishu_card_fallback_sent")
```

Use them only at the exact create/update/update-disabled/fallback branches. `feishu_card_updates_disabled` fires once when `max_update_failures` is reached for a run.

- [ ] **Step 2: Create live validation template**

Create `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/verification/live-validation-template.md`:

```markdown
# Hermes Feishu Card Streaming Live Validation

## Preflight

- Hermes home: `/Users/bytedance/Documents/Hermes/home`
- Config backup path:
- Feature flag:
  - `display.platforms.feishu.card_streaming: true`

## Test Prompt

Use a Feishu prompt that triggers at least two tools and one final answer.

## Expected Feishu Behavior

- One visible card for the run.
- Assistant text appears in the card.
- Tool state appears in the card.
- No separate tool-progress message.
- No duplicate final text message.

## Logs To Check

```bash
HERMES_HOME=/Users/bytedance/Documents/Hermes/home rg -n "feishu_card_|CardKit|card_streaming" /Users/bytedance/Documents/Hermes/home/logs
```

## Rollback

Disable:

```yaml
display:
  platforms:
    feishu:
      card_streaming: false
```

Restart Hermes gateway.
```

- [ ] **Step 3: Run sink tests again**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu_card_stream.py -q
```

Expected: PASS.

- [ ] **Step 4: Commit observability notes**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
git add gateway/platforms/feishu_card_stream.py
git commit -m "chore(feishu): add card stream observability"
```

Then commit the work-area verification template if `/Users/bytedance/Documents/运维` is inside a git repository. If it is not, leave it as an uncommitted local artifact.

## Task 8: Full Verification And Work-Area Closeout

**Files:**
- Modify: `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/verification/implementation-verification.md`

- [ ] **Step 1: Run focused tests**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
uv run --extra dev --extra feishu python -m pytest \
  tests/gateway/test_feishu_card_stream.py \
  tests/gateway/test_stream_text_cleaner.py \
  tests/gateway/test_feishu_card_stream_runner.py \
  tests/gateway/test_feishu.py::TestFeishuCardStreamTransport \
  tests/gateway/test_stream_consumer.py \
  tests/gateway/test_display_config.py \
  -q
```

Expected: PASS.

- [ ] **Step 2: Run existing Feishu tests**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu.py tests/gateway/test_feishu_comment.py tests/gateway/test_feishu_onboard.py -q
```

Expected: PASS.

- [ ] **Step 3: Record verification output**

Create `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/verification/implementation-verification.md`:

```markdown
# Implementation Verification

## Focused Tests

Command:

```bash
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_stream_text_cleaner.py tests/gateway/test_feishu_card_stream.py tests/gateway/test_feishu_card_stream_runner.py tests/gateway/test_feishu.py::TestFeishuCardStreamTransport tests/gateway/test_stream_consumer.py tests/gateway/test_display_config.py -q
```

Result: paste the exact pytest summary line from the run, including pass/fail count and runtime.

## Existing Feishu Tests

Command:

```bash
uv run --extra dev --extra feishu python -m pytest tests/gateway/test_feishu.py tests/gateway/test_feishu_comment.py tests/gateway/test_feishu_onboard.py -q
```

Result: paste the exact pytest summary line from the run, including pass/fail count and runtime.

## Live Validation

- Config backup: paste the created backup path.
- Feature flag enabled: record the exact YAML key and value.
- Feishu visible behavior: record whether one card was visible.
- Duplicate final message: record `none` or paste the duplicate message timestamp.
- Separate progress messages: record `none` or paste the progress message timestamp.
- Rollback needed: record `no` or paste the rollback command that was run.
```

- [ ] **Step 4: Inspect git diff**

Run:

```bash
cd /Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent
git status --short
git diff --stat
```

Expected:

- Only intended Hermes files are changed if commits were not made.
- No secrets, tokens, raw private transcripts, or large logs appear in diff.

- [ ] **Step 5: Prepare live rollout only after test pass**

Before touching live config:

```bash
USER_HOME="$(cd ~ && pwd)"
echo "$USER_HOME"
ts="$(date +%Y%m%d-%H%M%S)"
backup_dir="/Users/bytedance/Documents/运维/backup/${ts}-hermes-feishu-card-streaming"
mkdir -p "$backup_dir"
cp /Users/bytedance/Documents/Hermes/home/config.yaml "$backup_dir/config.yaml"
```

Expected:

- `USER_HOME` is `/Users/bytedance`.
- Backup command succeeds before live config is changed.

- [ ] **Step 6: Export the replayable patch stack**

Run after all implementation commits and verification commands have completed:

```bash
/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/scripts/export-patches.sh
```

Expected:

- The latest patch artifact directory contains:
  - `BASE_COMMIT`
  - `status-before.txt`
  - `status-after.txt`
  - `commits/series.txt`
  - one or more numbered `*.patch` files
  - `combined-feishu-card-streaming.patch`
- `commits/series.txt` lists focused implementation commits, not unrelated Hermes changes.

- [ ] **Step 7: Record implementation and patch verification summary**

Create or update `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/verification/implementation-verification.md` with a final section:

````markdown
## Patch Stack

- Worktree: `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/worktrees/hermes-agent`
- Patch directory: paste the path from `patches/latest-feishu-card-streaming-dir`.
- Base commit: paste the content of `BASE_COMMIT`.
- Export command: `/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/scripts/export-patches.sh`
- Split patches: `$patch_dir/commits/[0-9][0-9][0-9][0-9]-*.patch`
- Combined patch: `$patch_dir/combined-feishu-card-streaming.patch`
- Reapply command after future Hermes upgrades:

```bash
/Users/bytedance/Documents/运维/hermes-feishu-card-streaming/scripts/apply-to-hermes.sh /path/to/upgraded/hermes "$patch_dir"
```

- Reapply policy: use `git am -3` / `git apply --3way`; do not restore by copying files unless explicitly doing an emergency manual recovery.
````

Expected:

- A future implementer can identify the base commit, exported patches, verification result, and reapply command without reading the chat history.

## Self-Review Checklist

- Spec coverage:
  - Clean worktree and patch stack preparation is Task 0.
  - Phase 0 discovery is Task 1.
  - Feishu-only renderer is Task 2.
  - Tool identity and done/error mapping is Task 3.
  - Final delivery contract is Task 4 and Task 6.
  - Single-switch streaming is Task 6.
  - Observability and rollback are Task 7 and Task 8.
  - Replayable upgrade-safe patch export is Task 8.
- Placeholder scan:
  - This plan contains no unresolved implementation placeholders.
  - Discovery is a gate, not an implementation placeholder.
- Type consistency:
  - `FeishuCardRunSink` methods match the spec.
  - Runner helper names are consistent across tests and implementation steps.
  - `card_streaming` is the only new config key.
- Patch-stack consistency:
  - The plan tells 小C to implement in the worktree, not in the live Hermes checkout.
  - The plan records `BASE_COMMIT`, before/after status, exported commit patches, and a combined patch outside the `commits/` glob.
  - The plan uses three-way patch application for future Hermes upgrades and marks raw file copying as emergency-only.
