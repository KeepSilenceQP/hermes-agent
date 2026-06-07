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

    def finish_oldest_running_tool(self, *, tool_name: str, ok: bool = True) -> str | None:
        for tool in self.tools:
            if tool.tool_name == tool_name and tool.status == "running":
                tool.status = "done" if ok else "error"
                return tool.token
        return None

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
