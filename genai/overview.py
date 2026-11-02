"""
genai/overview.py

Writes the opening paragraph of the digest with Gemini, under strict rules.
Gemini sees the facts but must refer to them only through placeholders such as
{n_aggiornati}. Python checks the paragraph, then substitutes the values. If
the call fails, or the check finds any problem, the deterministic paragraph
from genai/render.py is used instead. Gemini can never make the run fail.

Run from the repository root:
    python -m genai.overview --facts scratch/facts_2026-11.json --dry-run
    python -m genai.overview --facts scratch/facts_2026-11.json --out scratch/digest_llm.md
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from genai.render import (load_labels, numero, overview_counts, render_markdown,
                          template_overview)

logger = logging.getLogger("genai.overview")

PROMPT_VERSION = "2026-11-02.2"

# Overridden by the 'genai' section of config/settings.yaml, if present.
DEFAULTS = {
    "models": ["gemini-3.8-flash", "gemini-3.6-flash", "gemini-3.6-flash-lite"],   # tried in order
    "thinking_level": "low",
    "temperature": 0.3,
    "max_output_tokens": 8192,   # includes thinking tokens
    "retry_attempts": 3,         # per model, first try included
    "max_chars": 700,            # final paragraph, after substitution
}

DESCRIZIONI = {
    "mese": "mese di riferimento del riepilogo",
    "mese_precedente": "mese dell'elaborazione con cui si confronta",
    "n_dataset": "numero di dataset monitorati",
    "n_aggiornati": "dataset scaricati di nuovo dalla fonte in questo aggiornamento",
    "n_nuovi_periodi": "dataset che contengono periodi di riferimento nuovi",
    "elenco_nuovi_periodi": "nomi dei dataset che contengono periodi nuovi",
    "n_non_aggiornati": ("dataset non aggiornati: il download non è riuscito e restano "
                         "i dati di un'estrazione precedente"),
    "n_mancanti": "dataset attesi ma assenti dai dati elaborati",
    "n_nuovi_dataset": "dataset presenti per la prima volta",
}

SYSTEM_INSTRUCTION = """\
You write the opening paragraph of a monthly digest, in Italian, for researchers \
at AICCON who use a statistical database built from ISTAT and Eurostat data. \
The digest says which datasets were refreshed. It never reports statistical values. \
Detailed lists follow below your paragraph.

Rules:
1. Write one paragraph of two to four sentences in clear, professional Italian. \
No headings, lists, markdown or line breaks.
2. Refer to every fact only through the placeholders provided, written exactly as \
shown, for example {n_aggiornati}. Use every placeholder at least once. Never write \
the values themselves.
3. Do not write digits, number words (such as due, tre, dieci), month names, or \
quantifiers such as tutti, nessuno, la maggior parte, metà, entrambi.
4. State only the facts provided. Do not describe trends, causes or values, and do \
not use words such as aumento, calo, crescita, miglioramento.
5. The values are shown only so you can choose singular or plural agreement, for \
example "contiene" when the value is 1.
6. Do not start a sentence with a placeholder.
7. Write plainly, as in a short note to colleagues. Avoid bureaucratic phrasing \
such as "il presente", "procedure" or "flussi", and do not call the digest statistical.
8. Describe each fact with the meaning given for its placeholder. Do not suggest \
causes: for example, do not say that missing datasets were not delivered by the source.
Return only the paragraph."""

PLACEHOLDER_RE = re.compile(r"\{([a-z_]+)\}")
BANNED = {
    "number word": re.compile(
        r"\b(?:due|tre|quattro|cinque|sei|sette|otto|nove|dieci|undici|dodici|tredici|"
        r"quattordici|quindici|sedici|diciassette|diciotto|diciannove|venti|trenta|"
        r"quaranta|cinquanta|cento|mille|mila|milion[ei]|dozzin[ae]|decin[ae]|"
        r"centinai[oa])\b", re.IGNORECASE),
    "quantifier": re.compile(
        r"\b(?:tutt[ie]|nessun[oa]?|metà|maggioranza|maggior parte|entramb[ie]|totalità)\b",
        re.IGNORECASE),
    "trend word": re.compile(
        r"\b(?:aument\w*|diminu\w*|cresc\w*|cal[oi]|riduzion\w*|ridott\w*|miglior\w*|"
        r"peggior\w*|record)\b", re.IGNORECASE),
    "month name": re.compile(
        r"\b(?:gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|"
        r"ottobre|novembre|dicembre)\b", re.IGNORECASE),
}


# ── Pure functions ────────────────────────────────────────────────────────────

def offered_placeholders(facts: dict, labels: dict) -> dict[str, str] | None:
    """Placeholder -> formatted value. None for a baseline month (nothing to compare)."""
    c = overview_counts(facts)
    if c["prima_rilevazione"]:
        return None
    vals = {"mese": c["mese"], "n_dataset": numero(c["n_dataset"])}
    if c["mese_precedente"]:
        vals["mese_precedente"] = c["mese_precedente"]
    for key in ("n_aggiornati", "n_nuovi_periodi", "n_non_aggiornati",
                "n_mancanti", "n_nuovi_dataset"):
        if c[key]:
            vals[key] = numero(c[key])
    codes = [ds["dataset_code"] for d in facts["domains"] for ds in d["datasets"]
             if ds.get("new_periods")]
    names = list(dict.fromkeys(labels[x]["label_it"] if x in labels else x for x in codes))
    if 1 <= len(names) <= 3:
        vals["elenco_nuovi_periodi"] = (names[0] if len(names) == 1
                                        else ", ".join(names[:-1]) + " e " + names[-1])
    return vals


def build_prompt(vals: dict[str, str]) -> str:
    lines = ["Segnaposto disponibili (nome: significato; valore, "
             "solo per l'accordo grammaticale):"]
    lines += [f"- {{{k}}}: {DESCRIZIONI[k]}; valore: {v}" for k, v in vals.items()]
    lines += ["", "Scrivi il paragrafo introduttivo."]
    return "\n".join(lines)


def substitute(text: str, vals: dict[str, str]) -> str:
    return PLACEHOLDER_RE.sub(lambda m: vals[m.group(1)], text)


def verify(text: str, vals: dict[str, str], max_chars: int = DEFAULTS["max_chars"]) -> list[str]:
    """Every reason to reject the paragraph. An empty list means it may be used."""
    t = (text or "").strip()
    if not t:
        return ["empty response"]
    problems = []
    if re.search(r"\d", t):
        problems.append("contains digits")
    for name, pattern in BANNED.items():
        m = pattern.search(t)
        if m:
            problems.append(f"{name}: '{m.group(0)}'")
    if re.search(r"[\n\r#*|`\[\]<>]", t):
        problems.append("formatting characters")
    found = set(PLACEHOLDER_RE.findall(t))
    unknown, missing = found - vals.keys(), vals.keys() - found
    if unknown:
        problems.append(f"unknown placeholders: {sorted(unknown)}")
    if missing:
        problems.append(f"missing placeholders: {sorted(missing)}")
    if re.search(r"[{}]", PLACEHOLDER_RE.sub("", t)):
        problems.append("stray braces")
    if not unknown:
        n = len(substitute(t, vals))
        if n > max_chars:
            problems.append(f"too long: {n} characters")
        elif n < 80:
            problems.append(f"too short: {n} characters")
    return problems


# ── Gemini calls ──────────────────────────────────────────────────────────────

def make_client(s: dict):
    from google import genai
    from google.genai import types

    # Reads GEMINI_API_KEY from the environment. Retries only transient errors,
    # with backoff, and at most retry_attempts tries per model (first try included).
    return genai.Client(http_options=types.HttpOptions(
        timeout=120_000,
        retry_options=types.HttpRetryOptions(
            attempts=s["retry_attempts"], initial_delay=5.0, max_delay=60.0,
            http_status_codes=[408, 429, 500, 502, 503, 504],
        ),
    ))


def call_model(client, model: str, prompt: str, s: dict) -> tuple[str, dict]:
    from google.genai import types

    response = client.models.generate_content(
        model=model,
        contents=prompt,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            temperature=s["temperature"],
            max_output_tokens=s["max_output_tokens"],
            thinking_config=types.ThinkingConfig(thinking_level=s["thinking_level"]),
            automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
        ),
    )
    u = response.usage_metadata
    usage = ({"prompt": u.prompt_token_count, "output": u.candidates_token_count,
              "thinking": u.thoughts_token_count} if u else {})
    return response.text or "", usage


def write_overview(facts: dict, labels: dict, settings: dict | None = None,
                   client=None) -> dict:
    """
    Returns {'text', 'source' ('gemini' or 'template'), 'model', 'prompt_version',
    'attempts', 'reason'}. 'text' is always usable.
    """
    s = {**DEFAULTS, **(settings or {})}
    result = {"text": template_overview(overview_counts(facts)), "source": "template",
              "model": None, "prompt_version": PROMPT_VERSION, "attempts": [], "reason": None}

    vals = offered_placeholders(facts, labels)
    if vals is None:
        result["reason"] = "baseline month: nothing to compare"
        return result
    if client is None:
        if not (os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")):
            result["reason"] = "no API key in the environment"
            return result
        try:
            client = make_client(s)
        except Exception as e:  # noqa: BLE001  (any failure means: use the template)
            result["reason"] = f"client error: {type(e).__name__}"
            return result

    prompt = build_prompt(vals)
    for model in dict.fromkeys(m for m in s["models"] if m):
        attempt: dict = {"model": model}
        try:
            raw, attempt["usage"] = call_model(client, model, prompt, s)
        except Exception as e:  # noqa: BLE001
            code = getattr(e, "code", None)       # HTTP status for API errors
            attempt["error"] = f"{type(e).__name__}{f' {code}' if code else ''}: {str(e)[:200]}"
            result["attempts"].append(attempt)
            logger.warning("Gemini %s failed: %s", model, attempt["error"].split(":")[0])
            continue
        attempt["raw"] = raw
        attempt["problems"] = verify(raw, vals, s["max_chars"])
        result["attempts"].append(attempt)
        if not attempt["problems"]:
            result.update(text=substitute(raw.strip(), vals), source="gemini", model=model)
            return result
        logger.warning("Gemini %s paragraph rejected: %s", model, attempt["problems"])

    result["reason"] = "no model produced an acceptable paragraph"
    return result


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Write the digest's opening paragraph.")
    parser.add_argument("--facts", required=True, help="JSON written by genai.facts.")
    parser.add_argument("--out", default=None, help="Also write the full digest here.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the prompt without calling Gemini.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    from ingestion.loaders.base_loader import load_config
    config = load_config()                       # also loads config/.env locally
    facts = json.loads(Path(args.facts).read_text(encoding="utf-8"))
    labels = load_labels()

    if args.dry_run:
        vals = offered_placeholders(facts, labels)
        print(SYSTEM_INSTRUCTION, "\n\n", build_prompt(vals) if vals else "(baseline: no call)")
        return

    result = write_overview(facts, labels, config.get("genai"))
    print(json.dumps({k: v for k, v in result.items() if k != "text"},
                     indent=2, ensure_ascii=False))
    print("\n" + result["text"])
    if args.out:
        Path(args.out).write_text(render_markdown(facts, labels, overview=result["text"]),
                                  encoding="utf-8")
        print(f"\nDigest written to {args.out}")


if __name__ == "__main__":
    main()