"""OpenAI provider — supports one-shot completion and tool-use loop."""

from __future__ import annotations

import json
import logging
import os
from typing import Callable

from .base import LLMProvider, LLMUsage, Tool, ToolLoopResult, ToolTrace


DEFAULT_MODEL = "gpt-5"
log = logging.getLogger(__name__)


class OpenAIProvider(LLMProvider):
    name = "openai"

    def __init__(self, api_key: str | None = None, model: str = DEFAULT_MODEL, base_url: str | None = None):
        try:
            from openai import OpenAI
        except ImportError as e:
            raise RuntimeError(
                "openai SDK not installed. Install with: pip install 'sap-process-mining[openai]'"
            ) from e
        key = api_key or os.getenv("OPENAI_API_KEY")
        if not key:
            raise RuntimeError("OPENAI_API_KEY not set")
        self.client = OpenAI(api_key=key, base_url=base_url) if base_url else OpenAI(api_key=key)
        self.model = model

    def complete(
        self,
        system: str,
        user: str,
        *,
        max_tokens: int = 2048,
        temperature: float = 0.2,
    ) -> tuple[str, LLMUsage]:
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            max_completion_tokens=max_tokens,
            temperature=temperature,
        )
        text = resp.choices[0].message.content or ""
        u = resp.usage
        usage = LLMUsage(
            input_tokens=getattr(u, "prompt_tokens", 0) or 0,
            output_tokens=getattr(u, "completion_tokens", 0) or 0,
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
        """OpenAI Chat Completions tool-use loop.

        On each turn the model either returns `finish_reason='stop'` with a final text
        answer, or `finish_reason='tool_calls'` with one or more `tool_calls`. We
        execute each tool, append the assistant message plus one `role='tool'` message
        per call, and loop.

        Note on temperature: most chat models (gpt-4o, gpt-4.1, gpt-5) accept any
        temperature in [0, 2]. Reasoning-mode models (o1/o3/o4) require temperature=1.
        If you point at one of those, override temperature via config or swap the
        provider default.
        """
        tool_specs = [
            {
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.input_schema,
                },
            }
            for t in tools
        ]
        tools_by_name = {t.name: t for t in tools}

        messages: list[dict] = [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]
        traces: list[ToolTrace] = []
        total = LLMUsage()

        for turn in range(1, max_turns + 1):
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                tools=tool_specs,
                tool_choice="auto",
                max_completion_tokens=max_tokens,
                temperature=temperature,
            )
            u = resp.usage
            total = total + LLMUsage(
                input_tokens=getattr(u, "prompt_tokens", 0) or 0,
                output_tokens=getattr(u, "completion_tokens", 0) or 0,
            )

            choice = resp.choices[0]
            msg = choice.message
            finish = choice.finish_reason

            if finish != "tool_calls" or not getattr(msg, "tool_calls", None):
                return ToolLoopResult(
                    text=msg.content or "",
                    traces=traces,
                    usage=total,
                    turns=turn,
                    stopped_because=finish or "stop",
                )

            # Replay the assistant message (with tool_calls) as-is for the next turn.
            messages.append({
                "role": "assistant",
                "content": msg.content,
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        },
                    }
                    for tc in msg.tool_calls
                ],
            })

            # Execute every tool call and feed results back as `role: tool` messages.
            for tc in msg.tool_calls:
                name = tc.function.name
                raw_args = tc.function.arguments or "{}"
                try:
                    args = json.loads(raw_args)
                except json.JSONDecodeError as e:
                    err = f"invalid JSON arguments from model: {e}"
                    trace = ToolTrace(name=name, args={"_raw": raw_args}, result=None, error=err)
                    traces.append(trace)
                    if on_tool_call:
                        on_tool_call(trace)
                    messages.append({
                        "role": "tool", "tool_call_id": tc.id,
                        "content": json.dumps({"error": err}),
                    })
                    continue

                tool = tools_by_name.get(name)
                if tool is None:
                    err = f"unknown tool: {name}"
                    trace = ToolTrace(name=name, args=args, result=None, error=err)
                    traces.append(trace)
                    if on_tool_call:
                        on_tool_call(trace)
                    messages.append({
                        "role": "tool", "tool_call_id": tc.id,
                        "content": json.dumps({"error": err}),
                    })
                    continue

                try:
                    result = tool.fn(args)
                    trace = ToolTrace(name=name, args=args, result=result)
                    traces.append(trace)
                    if on_tool_call:
                        on_tool_call(trace)
                    messages.append({
                        "role": "tool", "tool_call_id": tc.id,
                        "content": json.dumps(result, default=str),
                    })
                except Exception as e:  # noqa: BLE001 — feed errors back to the model
                    log.warning("tool %s raised: %s", name, e)
                    trace = ToolTrace(name=name, args=args, result=None, error=str(e))
                    traces.append(trace)
                    if on_tool_call:
                        on_tool_call(trace)
                    messages.append({
                        "role": "tool", "tool_call_id": tc.id,
                        "content": json.dumps({"error": str(e)}),
                    })

        return ToolLoopResult(
            text="(max_turns reached before the agent produced a final answer)",
            traces=traces,
            usage=total,
            turns=max_turns,
            stopped_because="max_turns",
        )
