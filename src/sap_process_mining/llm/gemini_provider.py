"""Google Gemini provider — supports one-shot completion and tool-use loop.

Uses the `google-genai` SDK (v1+, the newer one that replaced `google-generativeai`).

Gemini's tool-use is function-calling: we declare tool schemas via
`FunctionDeclaration`, the model returns `function_call` parts, we execute each and
feed results back as `function_response` parts in the next turn's content list.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Callable

from .base import LLMProvider, LLMUsage, Tool, ToolLoopResult, ToolTrace


DEFAULT_MODEL = "gemini-2.5-pro"
log = logging.getLogger(__name__)


# Gemini's Schema type is a strict subset of JSON Schema. In particular it rejects
# `default`, `additionalProperties`, and `$schema`. Strip those recursively before
# passing to FunctionDeclaration.
_GEMINI_SCHEMA_STRIP = {"default", "additionalProperties", "$schema"}


def _clean_schema_for_gemini(schema: dict) -> dict:
    if not isinstance(schema, dict):
        return schema
    cleaned = {k: v for k, v in schema.items() if k not in _GEMINI_SCHEMA_STRIP}
    if "properties" in cleaned and isinstance(cleaned["properties"], dict):
        cleaned["properties"] = {
            k: _clean_schema_for_gemini(v) for k, v in cleaned["properties"].items()
        }
    if "items" in cleaned:
        cleaned["items"] = _clean_schema_for_gemini(cleaned["items"])
    return cleaned


def _json_safe(obj):
    """Coerce arbitrary Python values (e.g. numpy / pandas scalars) to pure JSON types.
    Gemini's FunctionResponse.response is a pydantic model that trips on exotic types."""
    return json.loads(json.dumps(obj, default=str))


class GeminiProvider(LLMProvider):
    name = "gemini"

    def __init__(self, api_key: str | None = None, model: str = DEFAULT_MODEL):
        try:
            from google import genai
        except ImportError as e:
            raise RuntimeError(
                "google-genai SDK not installed. Install with: pip install 'sap-process-mining[gemini]'"
            ) from e
        key = api_key or os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not key:
            raise RuntimeError("GEMINI_API_KEY not set")
        self._genai = genai
        self.client = genai.Client(api_key=key)
        self.model = model

    def complete(
        self,
        system: str,
        user: str,
        *,
        max_tokens: int = 2048,
        temperature: float = 0.2,
    ) -> tuple[str, LLMUsage]:
        from google.genai import types
        resp = self.client.models.generate_content(
            model=self.model,
            contents=user,
            config=types.GenerateContentConfig(
                system_instruction=system,
                max_output_tokens=max_tokens,
                temperature=temperature,
            ),
        )
        text = resp.text or ""
        meta = getattr(resp, "usage_metadata", None)
        usage = LLMUsage(
            input_tokens=getattr(meta, "prompt_token_count", 0) or 0,
            output_tokens=getattr(meta, "candidates_token_count", 0) or 0,
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
        """Gemini function-calling loop.

        The conversation history (`contents`) grows each turn:
          - turn 1:   [user]
          - turn 2:   [user, model(function_call), user(function_response)]
          - turn n:   … until the model returns text with no function_call.
        """
        from google.genai import types

        function_declarations = [
            types.FunctionDeclaration(
                name=t.name,
                description=t.description,
                parameters=_clean_schema_for_gemini(t.input_schema),
            )
            for t in tools
        ]
        gemini_tools = [types.Tool(function_declarations=function_declarations)]
        tools_by_name = {t.name: t for t in tools}

        contents: list = [
            types.Content(role="user", parts=[types.Part(text=user)])
        ]
        traces: list[ToolTrace] = []
        total = LLMUsage()

        for turn in range(1, max_turns + 1):
            resp = self.client.models.generate_content(
                model=self.model,
                contents=contents,
                config=types.GenerateContentConfig(
                    system_instruction=system,
                    tools=gemini_tools,
                    max_output_tokens=max_tokens,
                    temperature=temperature,
                ),
            )
            meta = getattr(resp, "usage_metadata", None)
            total = total + LLMUsage(
                input_tokens=getattr(meta, "prompt_token_count", 0) or 0,
                output_tokens=getattr(meta, "candidates_token_count", 0) or 0,
            )

            candidate = resp.candidates[0] if resp.candidates else None
            content = candidate.content if candidate else None
            parts = list(content.parts) if content and content.parts else []
            function_calls = [p.function_call for p in parts if getattr(p, "function_call", None)]

            if not function_calls:
                # Final text answer.
                text_parts = [getattr(p, "text", "") or "" for p in parts]
                text = "".join(text_parts) or (getattr(resp, "text", "") or "")
                finish = getattr(candidate, "finish_reason", None) if candidate else None
                finish_name = (
                    finish.name if hasattr(finish, "name") else str(finish or "stop")
                ).lower()
                return ToolLoopResult(
                    text=text,
                    traces=traces,
                    usage=total,
                    turns=turn,
                    stopped_because=finish_name,
                )

            # Append the model turn (with function_calls) to history and execute calls.
            contents.append(content)
            response_parts: list = []

            for fc in function_calls:
                name = fc.name
                args = dict(fc.args) if fc.args else {}
                tool = tools_by_name.get(name)

                if tool is None:
                    err = f"unknown tool: {name}"
                    trace = ToolTrace(name=name, args=args, result=None, error=err)
                    traces.append(trace)
                    if on_tool_call:
                        on_tool_call(trace)
                    response_parts.append(types.Part(
                        function_response=types.FunctionResponse(
                            name=name, response={"error": err}
                        )
                    ))
                    continue

                try:
                    result = tool.fn(args)
                    trace = ToolTrace(name=name, args=args, result=result)
                    traces.append(trace)
                    if on_tool_call:
                        on_tool_call(trace)
                    # FunctionResponse.response must be a dict. Wrap scalars and coerce
                    # to pure JSON types so pydantic doesn't choke on numpy/pandas scalars.
                    payload = result if isinstance(result, dict) else {"result": result}
                    response_parts.append(types.Part(
                        function_response=types.FunctionResponse(
                            name=name, response=_json_safe(payload)
                        )
                    ))
                except Exception as e:  # noqa: BLE001 — feed errors back to the model
                    log.warning("tool %s raised: %s", name, e)
                    trace = ToolTrace(name=name, args=args, result=None, error=str(e))
                    traces.append(trace)
                    if on_tool_call:
                        on_tool_call(trace)
                    response_parts.append(types.Part(
                        function_response=types.FunctionResponse(
                            name=name, response={"error": str(e)}
                        )
                    ))

            contents.append(types.Content(role="user", parts=response_parts))

        return ToolLoopResult(
            text="(max_turns reached before the agent produced a final answer)",
            traces=traces,
            usage=total,
            turns=max_turns,
            stopped_because="max_turns",
        )
