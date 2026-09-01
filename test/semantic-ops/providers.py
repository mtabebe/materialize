# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Model providers for the enrichment worker.

Materialize never makes these calls. The worker does, which is the whole point of
the two-layer design: the credential, the spend and the rate limit stay in the
customer's process.

Two mock modes exist and the second one is the interesting one. `deterministic`
answers from keyword rules and is what you want while setting a demo up.
`chaotic` returns a *random* label for any body it has not seen before in this
process, which is what makes "delete a ticket and re-insert it, watch the label
stay identical" a result rather than a claim: a mock that always answers the same
way would make the store look decorative.
"""

from __future__ import annotations

import hashlib
import os
import random
from dataclasses import dataclass


@dataclass(frozen=True)
class EnrichmentSpec:
    """One row of the generated `<source>_ai_spec` view.

    The worker never hardcodes what `severity` means; it reads it from here, which
    is why a worker written once serves any `ENRICH WITH` clause.
    """

    column_name: str
    kind: str
    input_column: str
    prompt: str | None
    labels: list[str] | None
    prompt_version: str


class ProviderError(Exception):
    """A call failed in a way that is worth retrying."""


class Provider:
    def call(self, spec: EnrichmentSpec, body: str) -> object:
        raise NotImplementedError


class MockProvider(Provider):
    """Keyword rules (`deterministic`) or coin flips (`chaotic`)."""

    def __init__(self, mode: str = "deterministic", seed: int | None = None):
        if mode not in ("deterministic", "chaotic"):
            raise ValueError(f"unknown mock mode {mode!r}")
        self.mode = mode
        self._rng = random.Random(seed)
        # Only consulted in chaotic mode, and deliberately process-local: a restart
        # forgets it, which is what makes the store rather than the mock the thing
        # holding the answer stable.
        self._seen: dict[tuple[str, str], object] = {}

    def call(self, spec: EnrichmentSpec, body: str) -> object:
        key = (spec.column_name, body)
        if self.mode == "chaotic" and key in self._seen:
            return self._seen[key]
        value = self._answer(spec, body)
        if self.mode == "chaotic":
            self._seen[key] = value
        return value

    def _answer(self, spec: EnrichmentSpec, body: str) -> object:
        if spec.kind == "classify":
            labels = spec.labels or []
            if not labels:
                raise ProviderError("classify spec carries no labels")
            if self.mode == "chaotic":
                return self._rng.choice(labels)
            lowered = body.lower()
            if any(w in lowered for w in ("outage", "down", "cannot", "data loss")):
                return labels[0]
            if any(w in lowered for w in ("slow", "degraded", "error")):
                return labels[min(1, len(labels) - 1)]
            return labels[-1]
        if spec.kind == "extract":
            if self.mode == "chaotic":
                return self._rng.choice(["Acme", "Globex", "Initech", "Umbrella"])
            # Deterministic rule: the first capitalized token that is not one of the
            # words a ticket tends to open with. Crude on purpose; the mock is a
            # fixture, not a model.
            openers = {"The", "A", "An", "Our", "We", "I", "This", "They", "Customer"}
            for token in body.replace(",", " ").split():
                stripped = token.strip(".:;'\"")
                if stripped[:1].isupper() and stripped not in openers:
                    return stripped
            return None
        if spec.kind == "score":
            if self.mode == "chaotic":
                return round(self._rng.random(), 4)
            digest = hashlib.sha256(body.encode()).digest()
            return round(digest[0] / 255.0, 4)
        if spec.kind == "generate":
            return body[:120]
        if spec.kind == "embed":
            # A deterministic pseudo-embedding: stable per body, 8 dimensions, and
            # good enough for `cosine_similarity` to order things sensibly in a demo.
            digest = hashlib.sha256(body.encode()).digest()
            return [round(b / 255.0, 6) for b in digest[:8]]
        raise ProviderError(f"unknown enrichment kind {spec.kind!r}")


class AnthropicProvider(Provider):
    """A real provider, used for beat 1 where a real label is persuasive.

    Structured output constrains `classify` to the declared label set rather than
    parsing a label out of prose. A response that still does not match is a parse
    failure, and the caller turns that into an error row rather than retrying
    forever.
    """

    MODEL = "claude-sonnet-5"

    def __init__(self, api_key: str | None = None, model: str | None = None):
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY is not set")
        self.model = model or self.MODEL
        import anthropic  # imported lazily so the mock path needs no dependency

        self._client = anthropic.Anthropic(api_key=self.api_key)

    def call(self, spec: EnrichmentSpec, body: str) -> object:
        if spec.kind == "classify":
            labels = spec.labels or []
            tool = {
                "name": "label",
                "description": f"Assign a value for {spec.column_name}.",
                "input_schema": {
                    "type": "object",
                    "properties": {"value": {"type": "string", "enum": labels}},
                    "required": ["value"],
                },
            }
        elif spec.kind in ("extract", "generate"):
            tool = {
                "name": "label",
                "description": spec.prompt or spec.column_name,
                "input_schema": {
                    "type": "object",
                    "properties": {"value": {"type": "string"}},
                    "required": ["value"],
                },
            }
        elif spec.kind == "score":
            tool = {
                "name": "label",
                "description": spec.prompt or spec.column_name,
                "input_schema": {
                    "type": "object",
                    "properties": {"value": {"type": "number"}},
                    "required": ["value"],
                },
            }
        else:
            raise ProviderError(f"{spec.kind} is not supported by this provider")

        instruction = spec.prompt or f"Determine {spec.column_name}."
        try:
            resp = self._client.messages.create(
                model=self.model,
                max_tokens=256,
                tools=[tool],
                tool_choice={"type": "tool", "name": "label"},
                messages=[{"role": "user", "content": f"{instruction}\n\n{body}"}],
            )
        except Exception as e:  # noqa: BLE001 - retried by the caller
            raise ProviderError(str(e)) from e

        for block in resp.content:
            if getattr(block, "type", None) == "tool_use":
                return block.input["value"]
        raise ProviderError("provider returned no structured output")


def make_provider(name: str, mode: str) -> Provider:
    if name == "mock":
        return MockProvider(mode=mode)
    if name == "anthropic":
        return AnthropicProvider()
    raise ValueError(f"unknown provider {name!r}")
