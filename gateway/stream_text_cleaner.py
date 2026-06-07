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
    """Stateful filter that suppresses reasoning/thinking tags from streamed text.

    Tag set mirrors the old GatewayStreamConsumer think-block filter to keep
    existing behavior unchanged.  Supports partial-tag buffering and flush on
    empty-string feed so callers can drain held-back text at stream end.
    """

    # Must stay in sync with cli.py _OPEN_TAGS/_CLOSE_TAGS and
    # run_agent.py _strip_think_blocks() tag variants.
    _OPEN_TAGS = (
        "<REASONING_SCRATCHPAD>", "<think>", "<reasoning>",
        "<THINKING>", "<thinking>", "<thought>",
    )
    _CLOSE_TAGS = (
        "</REASONING_SCRATCHPAD>", "</think>", "</reasoning>",
        "</THINKING>", "</thinking>", "</thought>",
    )

    def __init__(self) -> None:
        self._in_think_block = False
        self._pending = ""

    def feed(self, text: str) -> str:
        """Feed text through the think-block filter.

        Returns displayable text.  Pass ``""`` to flush any held-back
        partial-tag text (e.g. at stream end).
        """
        if not text:
            if self._pending and not self._in_think_block:
                pending = self._pending
                self._pending = ""
                return pending
            return ""

        buf = self._pending + text
        self._pending = ""
        output: list[str] = []

        while buf:
            if self._in_think_block:
                close_idx, close_len = self._find_earliest(buf, self._CLOSE_TAGS)
                if close_idx == -1:
                    self._pending = self._suffix_that_may_start_tag(buf, self._CLOSE_TAGS)
                    return "".join(output)
                buf = buf[close_idx + close_len:]
                self._in_think_block = False
                continue

            open_idx, open_len = self._find_earliest_at_boundary(buf, self._OPEN_TAGS, output)
            if open_idx == -1:
                safe, pending = self._split_safe_suffix(buf, self._OPEN_TAGS)
                output.append(safe)
                self._pending = pending
                break

            output.append(buf[:open_idx])
            buf = buf[open_idx + open_len:]
            self._in_think_block = True

        return "".join(output)

    def _find_earliest_at_boundary(
        self, buf: str, tags: tuple[str, ...], output: list[str],
    ) -> tuple[int, int]:
        """Find earliest opening tag that appears at a block boundary.

        A block boundary is: start of text, after a newline, or preceded only
        by whitespace on the current line (with accumulated output ending in
        newline or empty).  This prevents false positives when models mention
        tags in prose (e.g. "the <think> tag is used for…").
        """
        accumulated = "".join(output)
        best_idx = -1
        best_len = 0
        for tag in tags:
            search_start = 0
            while True:
                idx = buf.find(tag, search_start)
                if idx == -1:
                    break
                # Block-boundary check (mirrors old stream_consumer logic)
                if idx == 0:
                    is_boundary = (
                        not accumulated
                        or accumulated.endswith("\n")
                    )
                else:
                    preceding = buf[:idx]
                    last_nl = preceding.rfind("\n")
                    if last_nl == -1:
                        is_boundary = (
                            (not accumulated or accumulated.endswith("\n"))
                            and preceding.strip() == ""
                        )
                    else:
                        is_boundary = preceding[last_nl + 1:].strip() == ""

                if is_boundary and (best_idx == -1 or idx < best_idx):
                    best_idx = idx
                    best_len = len(tag)
                    break  # first boundary hit for this tag is enough
                search_start = idx + 1

        return best_idx, best_len

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
