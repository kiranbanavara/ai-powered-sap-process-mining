"""Unified LLM interface.

Two modes:
  * complete(system, user)              — one-shot text generation, used by the Flagger
  * run_with_tools(system, user, tools) — agentic loop with tool use, used by the
                                          Investigator

Tool-use shapes vary enough across SDKs that we keep the interface narrow and
provider-agnostic: tools are JSON-schema + Python callable, the loop lives in each
provider.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass(frozen=True)
class LLMUsage:
    input_tokens: int = 0
    output_tokens: int = 0

    def __add__(self, other: "LLMUsage") -> "LLMUsage":
        return LLMUsage(
            input_tokens=self.input_tokens + other.input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
        )


@dataclass
class Tool:
    """A callable the agent can invoke. `fn(args_dict) -> JSON-serialisable result`."""
    name: str
    description: str
    input_schema: dict             # JSON Schema for args
    fn: Callable[[dict], Any]


@dataclass
class ToolTrace:
    """One tool call with args and result — useful for audit in the RCA report."""
    name: str
    args: dict
    result: Any
    error: str | None = None


@dataclass
class ToolLoopResult:
    """Final answer from an agentic tool-use loop."""
    text: str
    traces: list[ToolTrace] = field(default_factory=list)
    usage: LLMUsage = field(default_factory=LLMUsage)
    turns: int = 0
    stopped_because: str = "end_turn"


class LLMProvider(ABC):
    """Minimal provider interface."""

    name: str = "base"

    @abstractmethod
    def complete(
        self,
        system: str,
        user: str,
        *,
        max_tokens: int = 2048,
        temperature: float = 0.2,
    ) -> tuple[str, LLMUsage]:
        """Return (text, usage) for a single-shot generation."""
        raise NotImplementedError

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
        """Run an agentic tool-use loop. Default: raise — providers opt in.

        The loop: the model receives tools → may emit tool calls → we execute them →
        feed results back → repeat until the model returns a final text answer or
        we hit `max_turns`.

        `on_tool_call`, if given, is invoked after each tool executes. The UI uses this
        to stream progress: tool name, args, and result become visible immediately
        instead of waiting for the whole loop to finish.
        """
        raise NotImplementedError(
            f"Provider '{self.name}' does not yet support tool-use. "
            f"Use Anthropic for now (pip install 'sap-process-mining[anthropic]')."
        )
