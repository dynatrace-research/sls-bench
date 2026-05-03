from __future__ import annotations

from pydantic import BaseModel
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage


class TokenUsage(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0
    reasoning_tokens: int = 0

    def __iadd__(self, other: TokenUsage) -> TokenUsage:
        self.input_tokens += other.input_tokens
        self.output_tokens += other.output_tokens
        self.cached_input_tokens += other.cached_input_tokens
        self.reasoning_tokens += other.reasoning_tokens
        return self


class LLMResponse(BaseModel):
    final_answer: str
    token_usage: TokenUsage


def extract_final_answer(response: AIMessage) -> str:
    content = response.content
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                return block.get("text", "")
    return ""


def extract_token_usage(response: AIMessage) -> TokenUsage:
    usage = response.usage_metadata
    if usage is None:
        return TokenUsage()

    in_details = usage.get("input_token_details") or {}
    out_details = usage.get("output_token_details") or {}

    return TokenUsage(
        input_tokens=usage["input_tokens"],
        output_tokens=usage["output_tokens"],
        cached_input_tokens=in_details.get("cache_read", 0),
        reasoning_tokens=out_details.get("reasoning", 0),
    )


def extract_response(response: AIMessage) -> LLMResponse:
    return LLMResponse(
        final_answer=extract_final_answer(response),
        token_usage=extract_token_usage(response),
    )


async def ainvoke_model(
    model: BaseChatModel,
    system_prompt: str,
    user_content: str,
) -> LLMResponse:
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_content),
    ]
    response = await model.ainvoke(messages)
    return extract_response(response)
