"""Google Gemini provider (via google-genai SDK)."""

from __future__ import annotations

import os

from .base import LLMProvider, LLMUsage


DEFAULT_MODEL = "gemini-2.5-pro"


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
