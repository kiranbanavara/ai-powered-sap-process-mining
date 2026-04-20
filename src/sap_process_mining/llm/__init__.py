"""Pluggable LLM providers.

Customers can choose their trusted model family without changing analysis code. The core
interface is `LLMProvider.complete(system, user) -> str`. Provider modules import their
SDK lazily so installing `sap-process-mining[openai]` doesn't drag in Anthropic and
vice-versa.
"""

from .base import LLMProvider

_PROVIDERS = {
    "anthropic": ("sap_process_mining.llm.anthropic_provider", "AnthropicProvider"),
    "openai": ("sap_process_mining.llm.openai_provider", "OpenAIProvider"),
    "gemini": ("sap_process_mining.llm.gemini_provider", "GeminiProvider"),
}


def get_provider(name: str, **kwargs) -> LLMProvider:
    try:
        module_path, class_name = _PROVIDERS[name]
    except KeyError:
        raise ValueError(
            f"Unknown LLM provider '{name}'. Available: {sorted(_PROVIDERS)}"
        ) from None
    import importlib
    module = importlib.import_module(module_path)
    return getattr(module, class_name)(**kwargs)


__all__ = ["LLMProvider", "get_provider"]
