"""Anthropic (Claude) provider — supports one-shot completion and tool-use loop."""

from __future__ import annotations

import json
import logging
import os
from typing import Callable

from .base import LLMProvider, LLMUsage, Tool, ToolLoopResult, ToolTrace


DEFAULT_MODEL = "claude-sonnet-4-6"
log = logging.getLogger(__name__)


class AnthropicProvider(LLMProvider):
    name = "anthropic"

    def __init__(self, api_key: str | None = None, model: str = DEFAULT_MODEL):
        try:
            from anthropic import Anthropic
        except ImportError as e:
            raise RuntimeError(
                "anthropic SDK not installed. Install with: pip install 'sap-process-mining[anthropic]'"
            ) from e
        key = api_key or os.getenv("ANTHROPIC_API_KEY")
        if not key:
            raise RuntimeError("ANTHROPIC_API_KEY not set")
        self.client = Anthropic(api_key=key)
        self.model = model

    def complete(
        self,
        system: str,
        user: str,
        *,
        max_tokens: int = 2048,
        temperature: float = 0.2,
    ) -> tuple[str, LLMUsage]:
        resp = self.client.messages.create(
            model=self.model,
            system=system,
            messages=[{"role": "user", "content": user}],
            max_tokens=max_tokens,
            temperature=temperature,
        )
        text_parts = [b.text for b in resp.content if getattr(b, "type", None) == "text"]
        text = "".join(text_parts)
        usage = LLMUsage(
            input_tokens=resp.usage.input_tokens,
            output_tokens=resp.usage.output_tokens,
        )
        return text, usage

    def run_with_tools(
        self,
        system: str,
        user: str,
        tools: list[Tool],
        *,
        max_turns: int = 10,
        max_tokens: int = 2048,
        temperature: float = 0.2,
        on_tool_call: Callable[[ToolTrace], None] | None = None,
    ) -> ToolLoopResult:
        """Anthropic Messages API tool-use loop.

        On each turn the model either returns `stop_reason='end_turn'` with a final
        text answer, or `stop_reason='tool_use'` with one or more `tool_use` blocks.
        We execute each tool, send results back as `tool_result` blocks, and loop.
        """
        tool_specs = [
            {"name": t.name, "description": t.description, "input_schema": t.input_schema}
            for t in tools
        ]
        tools_by_name = {t.name: t for t in tools}

        messages: list[dict] = [{"role": "user", "content": user}]
        traces: list[ToolTrace] = []
        total = LLMUsage()
        stopped = "max_turns"

        for turn in range(1, max_turns + 1):
            resp = self.client.messages.create(
                model=self.model,
                system=system,
                messages=messages,
                tools=tool_specs,
                max_tokens=max_tokens,
                temperature=temperature,
            )
            total = total + LLMUsage(
                input_tokens=resp.usage.input_tokens,
                output_tokens=resp.usage.output_tokens,
            )

            if resp.stop_reason != "tool_use":
                text = "".join(
                    b.text for b in resp.content if getattr(b, "type", None) == "text"
                )
                return ToolLoopResult(
                    text=text, traces=traces, usage=total, turns=turn,
                    stopped_because=resp.stop_reason or "end_turn",
                )

            # Otherwise, resp.content contains one or more tool_use blocks (possibly
            # interleaved with text thoughts). Execute each tool, append results.
            messages.append({"role": "assistant", "content": resp.content})

            tool_results: list[dict] = []
            for block in resp.content:
                if getattr(block, "type", None) != "tool_use":
                    continue
                tool = tools_by_name.get(block.name)
                if tool is None:
                    err = f"unknown tool: {block.name}"
                    trace = ToolTrace(name=block.name, args=dict(block.input), result=None, error=err)
                    traces.append(trace)
                    if on_tool_call:
                        on_tool_call(trace)
                    tool_results.append({
                        "type": "tool_result", "tool_use_id": block.id,
                        "content": json.dumps({"error": err}), "is_error": True,
                    })
                    continue
                try:
                    result = tool.fn(dict(block.input))
                    trace = ToolTrace(name=block.name, args=dict(block.input), result=result)
                    traces.append(trace)
                    if on_tool_call:
                        on_tool_call(trace)
                    tool_results.append({
                        "type": "tool_result", "tool_use_id": block.id,
                        "content": json.dumps(result, default=str),
                    })
                except Exception as e:  # noqa: BLE001 — return errors to the model
                    log.warning("tool %s raised: %s", block.name, e)
                    trace = ToolTrace(name=block.name, args=dict(block.input), result=None, error=str(e))
                    traces.append(trace)
                    if on_tool_call:
                        on_tool_call(trace)
                    tool_results.append({
                        "type": "tool_result", "tool_use_id": block.id,
                        "content": json.dumps({"error": str(e)}), "is_error": True,
                    })

            messages.append({"role": "user", "content": tool_results})

        return ToolLoopResult(
            text="(max_turns reached before the agent produced a final answer)",
            traces=traces, usage=total, turns=max_turns, stopped_because=stopped,
        )
