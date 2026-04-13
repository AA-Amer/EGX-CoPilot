"""
backend/data/llm_client.py

Multi-provider LLM router for EGX Copilot.
All providers are accessed through an OpenAI-compatible client,
so switching providers is a single line in config.json.

Provider priority for a given task:
  1. task_routing map in config  (e.g. "simple_chat" → "ollama")
  2. active_provider fallback     (e.g. "groq")
  3. On any exception → retry once with ollama

Claude raises NotImplementedError until manually enabled in config.json.
"""
from __future__ import annotations

import logging
import os
from typing import Generator

from dotenv import load_dotenv
from openai import OpenAI

from backend.data.config_loader import load_config

load_dotenv()
logger = logging.getLogger(__name__)

# Maps provider name → environment variable that holds its API key
_ENV_KEYS: dict[str, str | None] = {
    "groq": "GROQ_API_KEY",
    "gemini": "GEMINI_API_KEY",
    "claude": "ANTHROPIC_API_KEY",
    "ollama": None,  # no key required
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resolve_provider(task: str) -> tuple[str, dict]:
    """
    Return (provider_name, provider_config_dict) for the given task.
    Falls back to active_provider if the task isn't in task_routing or
    the routed provider is disabled.
    """
    cfg = load_config()
    ai = cfg["ai"]
    routing = ai.get("task_routing", {})
    providers = ai["providers"]

    name = routing.get(task, ai["active_provider"])

    # Ensure the resolved provider exists and is enabled
    if name not in providers or not providers[name].get("enabled", False):
        name = ai["active_provider"]

    return name, providers[name]


def _get_client(provider_name: str, provider_cfg: dict) -> tuple[OpenAI, str]:
    """
    Build an OpenAI-compatible client for the given provider.
    Returns (client, model_name).
    """
    if provider_name == "claude":
        raise NotImplementedError(
            "Claude not yet activated — set enabled: true in config.json"
        )

    base_url: str = provider_cfg["base_url"]
    model: str = provider_cfg["model"]

    # Ollama's OpenAI-compatible endpoint requires a /v1 suffix
    if provider_name == "ollama" and not base_url.rstrip("/").endswith("/v1"):
        base_url = base_url.rstrip("/") + "/v1"

    env_key = _ENV_KEYS.get(provider_name)
    api_key = os.getenv(env_key) if env_key else None

    if not api_key:
        if provider_name == "ollama":
            api_key = "ollama"  # any non-empty string satisfies the openai client
        else:
            raise ValueError(
                f"Missing environment variable {env_key} for provider '{provider_name}'. "
                f"Add it to your .env file."
            )

    return OpenAI(base_url=base_url, api_key=api_key), model


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ask_llm(
    system_prompt: str,
    user_message: str,
    task: str = "general",
) -> str:
    """
    Send a prompt to the configured LLM and return the full response string.

    Provider is chosen by task_routing in config.json.
    On any exception (network, quota, etc.) retries once with ollama.

    Args:
        system_prompt: The system/context instruction.
        user_message:  The user's query.
        task:          Task key for routing (e.g. "simple_chat", "news_analysis").

    Returns:
        Response text from the model.

    Raises:
        NotImplementedError: If Claude is selected but not yet activated.
        RuntimeError:        If both primary provider and ollama fallback fail.
    """
    cfg = load_config()
    ai = cfg["ai"]
    max_tokens: int = ai.get("max_tokens", 4096)
    temperature: float = ai.get("temperature", 0.1)

    provider_name, provider_cfg = _resolve_provider(task)

    def _call(pname: str, pcfg: dict) -> str:
        client, model = _get_client(pname, pcfg)
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            max_tokens=max_tokens,
            temperature=temperature,
        )
        return resp.choices[0].message.content.strip()

    try:
        result = _call(provider_name, provider_cfg)
        logger.debug("ask_llm: used '%s' for task '%s'", provider_name, task)
        return result
    except NotImplementedError:
        raise
    except Exception as primary_exc:
        logger.warning(
            "ask_llm: '%s' failed (%s) — retrying with ollama",
            provider_name, primary_exc,
        )
        ollama_cfg = cfg["ai"]["providers"]["ollama"]
        try:
            result = _call("ollama", ollama_cfg)
            logger.info("ask_llm: ollama fallback succeeded")
            return result
        except Exception as fallback_exc:
            logger.error("ask_llm: ollama fallback also failed: %s", fallback_exc)
            raise RuntimeError(
                f"All LLM providers failed. "
                f"Primary ({provider_name}): {primary_exc}. "
                f"Fallback (ollama): {fallback_exc}"
            ) from fallback_exc


def stream_llm(
    system_prompt: str,
    user_message: str,
    task: str = "general",
) -> Generator[str, None, None]:
    """
    Stream LLM response tokens as a generator of string chunks.
    Designed for use with Streamlit's st.write_stream().

    Falls back to a single non-streamed ask_llm() call on any error,
    so st.write_stream() always receives a valid generator.

    Args:
        system_prompt: The system/context instruction.
        user_message:  The user's query.
        task:          Task key for routing.

    Yields:
        String chunks of the model response.
    """
    cfg = load_config()
    ai = cfg["ai"]
    max_tokens: int = ai.get("max_tokens", 4096)
    temperature: float = ai.get("temperature", 0.1)

    provider_name, provider_cfg = _resolve_provider(task)

    try:
        client, model = _get_client(provider_name, provider_cfg)
        stream = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            max_tokens=max_tokens,
            temperature=temperature,
            stream=True,
        )
        for chunk in stream:
            content = chunk.choices[0].delta.content
            if content:
                yield content
    except Exception as exc:
        logger.warning("stream_llm: streaming failed (%s) — falling back to ask_llm", exc)
        yield ask_llm(system_prompt, user_message, task=task)
