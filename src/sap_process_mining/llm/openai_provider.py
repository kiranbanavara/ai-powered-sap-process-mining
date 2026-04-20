"""OpenAI provider."""

from __future__ import annotations

import os

from .base import LLMProvider, LLMUsage


DEFAULT_MODEL = "gpt-5"


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
