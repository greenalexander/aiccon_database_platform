"""
genai/render.py

Turns the facts dictionary from genai/facts.py into the Italian markdown digest.
Fully deterministic: every list, table, date and number is produced here.

The opening paragraph comes from template_overview() unless another paragraph
is passed in. In step 5 that paragraph will be written by Gemini and checked
before use; this template remains the fallback.

Run from the repository root:
    python -m genai.render --facts scratch/facts_2026-11.json --out scratch/digest_2026-11.md
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path

LABELS_PATH = Path(__file__).with_name("dataset_labels.csv")

MESI = ["gennaio", "febbraio", "marzo", "aprile", "maggio", "giugno", "luglio",
        "agosto", "settembre", "ottobre", "novembre", "dicembre"]
ROMANI = ["I", "II", "III", "IV"]
DOMINI = {"social_economy": "Economia sociale", "labour": "Lavoro"}
FREQ_ORDER = ["M", "Q", "A", "?"]


# ── Formatting helpers ────────────────────────────────────────────────────────

def numero(n: int) -> str:
    """Italian thousands separator: 40906 -> '40.906'."""
    return f"{n:,}".replace(",", ".")


def variazione(n: int) -> str:
    return "nessuna" if n == 0 else f"{'+' if n > 0 else '−'}{numero(abs(n))}"


def mese_anno(ym: str) -> str:
    """'2026-11' -> 'novembre 2026'."""
    y, m = ym.split("-")
    return f"{MESI[int(m) - 1]} {y}"


def data(iso_date: str) -> str:
    """'2026-06-01' -> '1 giugno 2026'."""
    y, m, d = iso_date.split("-")
    giorno = "1º" if int(d) == 1 else str(int(d))
    return f"{giorno} {MESI[int(m) - 1]} {y}"


def _runs(indices: list[int]) -> list[tuple[int, int]]:
    """Compress sorted integers into (start, end) runs of consecutive values."""
    runs: list[tuple[int, int]] = []
    for i in sorted(indices):
        if runs and i == runs[-1][1] + 1:
            runs[-1] = (runs[-1][0], i)
        else:
            runs.append((i, i))
    return runs


def _mesi(periods: list[str]) -> str:
    idx = [int(p[:4]) * 12 + int(p[5:7]) - 1 for p in periods]
    parts = []
    for a, b in _runs(idx):
        (ya, ma), (yb, mb) = divmod(a, 12), divmod(b, 12)
        if a == b:
            parts.append(f"{MESI[ma]} {ya}")
        elif ya == yb:
            parts.append(f"da {MESI[ma]} a {MESI[mb]} {ya}")
        else:
            parts.append(f"da {MESI[ma]} {ya} a {MESI[mb]} {yb}")
    n = len(periods)
    return ", ".join(parts) + (f" ({n} mesi)" if n > 1 else "")


def _trimestri(periods: list[str]) -> str:
    idx = [int(p[:4]) * 4 + int(p[-1]) - 1 for p in periods]
    parts = []
    for a, b in _runs(idx):
        (ya, qa), (yb, qb) = divmod(a, 4), divmod(b, 4)
        if a == b:
            parts.append(f"{ROMANI[qa]} trimestre {ya}")
        elif ya == yb and b == a + 1:
            parts.append(f"{ROMANI[qa]} e {ROMANI[qb]} trimestre {ya}")
        elif ya == yb:
            parts.append(f"dal {ROMANI[qa]} al {ROMANI[qb]} trimestre {ya}")
        else:
            parts.append(f"dal {ROMANI[qa]} trimestre {ya} al {ROMANI[qb]} trimestre {yb}")
    return ", ".join(parts)


def _anni(periods: list[str]) -> str:
    parts = []
    for a, b in _runs([int(p) for p in periods]):
        if a == b:
            parts.append(f"anno {a}")
        elif b == a + 1:
            parts.append(f"anni {a} e {b}")
        else:
            parts.append(f"dal {a} al {b}")
    return ", ".join(parts)


def periodi(by_freq: dict[str, list[str]]) -> str:
    """{'M': ['2026-05', ..., '2026-09']} -> 'da maggio a settembre 2026 (5 mesi)'."""
    fmt = {"M": _mesi, "Q": _trimestri, "A": _anni}
    parts = []
    for f in FREQ_ORDER:
        if by_freq.get(f):
            parts.append(fmt[f](by_freq[f]) if f in fmt else ", ".join(by_freq[f]))
    return "; ".join(parts)


def ultimo_periodo(latest: dict[str, str]) -> str:
    """{'A': '2025', 'Q': '2026-Q2'} -> 'II trimestre 2026 / 2025'."""
    out = []
    for f in FREQ_ORDER:
        p = latest.get(f)
        if p is None:
            continue
        if f == "M":
            out.append(mese_anno(p))
        elif f == "Q":
            out.append(f"{ROMANI[int(p[-1]) - 1]} trimestre {p[:4]}")
        else:
            out.append(p)
    return " / ".join(out)


# ── Labels ────────────────────────────────────────────────────────────────────

def load_labels(path: Path = LABELS_PATH) -> dict[str, dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as f:
        return {row["dataset_code"]: row for row in csv.DictReader(f)}


def nome(code: str, labels: dict, dominio: str | None = None) -> str:
    """'**Label** (Lavoro, ISTAT, `code`)'; falls back to the code when unlabelled."""
    row = labels.get(code)
    extra = [x for x in (dominio, row["fonte"] if row else None) if x]
    inside = ", ".join(extra + [f"`{code}`"])
    return f"**{row['label_it']}** ({inside})" if row else f"`{code}` ({inside})"


# ── Content ───────────────────────────────────────────────────────────────────

def _all(facts: dict):
    """Yield (domain_record, dataset_record) pairs."""
    for d in facts["domains"]:
        for ds in d["datasets"]:
            yield d, ds


def overview_counts(facts: dict) -> dict:
    """The numbers the opening paragraph may mention. Step 5 passes these to Gemini."""
    statuses = Counter(ds["status"] for _, ds in _all(facts))
    prev = {d["previous_month"] for d in facts["domains"] if d["previous_month"]}
    return {
        "mese": mese_anno(facts["month"]),
        "mese_precedente": mese_anno(prev.pop()) if len(prev) == 1 else None,
        "n_dataset": sum(statuses.values()),
        "n_aggiornati": statuses["updated"],
        "n_nuovi_periodi": facts["datasets_with_new_periods"],
        "n_non_aggiornati": statuses["not_updated"],
        "n_mancanti": statuses["missing"] + statuses["not_processed"],
        "n_nuovi_dataset": statuses["new_dataset"],
        "prima_rilevazione": statuses["baseline"] > 0,
    }


def _n(n: int, singolare: str, plurale: str) -> str:
    return singolare if n == 1 else plurale


def template_overview(c: dict) -> str:
    """Deterministic opening paragraph, used whenever no checked LLM paragraph exists."""
    tot = numero(c["n_dataset"])
    if c["prima_rilevazione"]:
        return (f"Questo è il primo riepilogo per {c['mese']}: non esiste un'elaborazione "
                f"precedente con cui confrontare i {tot} dataset monitorati.")
    rispetto = (f"rispetto a {c['mese_precedente']}" if c["mese_precedente"]
                else "rispetto all'elaborazione precedente")
    frasi = [f"Riepilogo dell'aggiornamento dati di {c['mese']}, {rispetto}."]

    agg, nuovi = c["n_aggiornati"], c["n_nuovi_periodi"]
    if agg == 0:
        frasi.append(f"Nessuno dei {tot} dataset monitorati è stato scaricato di nuovo dalla fonte.")
    else:
        frase = (f"Dei {tot} dataset monitorati, {numero(agg)} "
                 f"{_n(agg, 'è stato scaricato', 'sono stati scaricati')} di nuovo dalla fonte")
        if nuovi:
            frase += f" e {numero(nuovi)} {_n(nuovi, 'contiene', 'contengono')} periodi nuovi."
        else:
            frase += ", ma nessuno contiene periodi nuovi."
        frasi.append(frase)
    if agg == 0 and nuovi:
        frasi.append(f"Periodi nuovi in {numero(nuovi)} dataset.")

    problemi = []
    if c["n_non_aggiornati"]:
        k = c["n_non_aggiornati"]
        problemi.append(f"{numero(k)} dataset non {_n(k, 'aggiornato', 'aggiornati')}")
    if c["n_mancanti"]:
        k = c["n_mancanti"]
        problemi.append(f"{numero(k)} {_n(k, 'mancante', 'mancanti')}")
    if c["n_nuovi_dataset"]:
        k = c["n_nuovi_dataset"]
        problemi.append(f"{numero(k)} {_n(k, 'nuovo', 'nuovi')}")
    if problemi:
        elenco = problemi[0] if len(problemi) == 1 else ", ".join(problemi[:-1]) + " e " + problemi[-1]
        frasi.append(f"Da verificare: {elenco}.")
    return " ".join(frasi)


def render_markdown(facts: dict, labels: dict, overview: str | None = None) -> str:
    counts = overview_counts(facts)
    lines = [f"# Aggiornamento dati AICCON: {counts['mese']}", "",
             overview if overview is not None else template_overview(counts), ""]

    nuovi = [(d, ds) for d, ds in _all(facts) if ds.get("new_periods")]
    if nuovi:
        lines += ["## Nuovi periodi disponibili", ""]
        for d, ds in nuovi:
            lines.append(f"- {nome(ds['dataset_code'], labels, DOMINI.get(d['domain'], d['domain']))}: "
                         f"{periodi(ds['new_periods'])}.")
        lines.append("")

    verifiche = []
    for d in facts["domains"]:
        if d["status"] == "not_processed":
            verifiche.append(f"- Il dominio {DOMINI.get(d['domain'], d['domain'])} "
                             f"non è stato elaborato in {counts['mese']}.")
    for d, ds in _all(facts):
        if ds["status"] == "not_updated":
            verifiche.append(f"- {nome(ds['dataset_code'], labels, DOMINI.get(d['domain'], d['domain']))}: non aggiornato. "
                             f"I dati risalgono all'estrazione del {data(ds['extracted_on'])} "
                             f"(ultimo periodo: {ultimo_periodo(ds['latest_period'])}).")
        elif ds["status"] == "missing":
            if ds.get("last_extracted_on"):
                quando = f"ultima estrazione disponibile: {data(ds['last_extracted_on'])}"
            elif d["previous_file"]:
                quando = "assente anche nell'elaborazione precedente"
            else:
                quando = "nessuna elaborazione precedente"
            verifiche.append(f"- {nome(ds['dataset_code'], labels, DOMINI.get(d['domain'], d['domain']))}: assente dal file elaborato "
                             f"({quando}).")
        elif ds["status"] == "new_dataset":
            verifiche.append(f"- {nome(ds['dataset_code'], labels, DOMINI.get(d['domain'], d['domain']))}: nuovo dataset, "
                             f"assente nell'elaborazione precedente.")
    if verifiche:
        lines += ["## Da verificare", ""] + verifiche + [""]

    altri = [(d, ds) for d, ds in _all(facts)
             if ds["status"] in ("updated", "baseline") and not ds.get("new_periods")]
    if altri:
        lines += ["## Altri dataset", "",
                  "| Dominio | Dataset | Ultimo periodo | Variazione righe |",
                  "|---|---|---|---|"]
        for d, ds in altri:
            var = variazione(ds["rows_change"]) if "rows_change" in ds else "n.d."
            lines.append(f"| {DOMINI.get(d['domain'], d['domain'])} | "
                         f"{nome(ds['dataset_code'], labels)} | "
                         f"{ultimo_periodo(ds['latest_period'])} | {var} |")
        lines += ["", "Le variazioni nel numero di righe possono derivare da dati arrivati in "
                      "ritardo alla fonte oppure da modifiche alla pipeline.", ""]

    sha = facts.get("commit_sha")
    origine = f"commit {sha[:7]}" if sha else "esecuzione locale"
    lines += ["---", "",
              f"_Generato automaticamente il {data(facts['generated_at'][:10])} "
              f"({origine}). Il riepilogo descrive quali dati sono stati aggiornati "
              f"e non riporta valori statistici._", ""]
    return "\n".join(lines)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Render the Italian markdown digest.")
    parser.add_argument("--facts", required=True, help="JSON written by genai.facts.")
    parser.add_argument("--out", default=None, help="Markdown file (default: stdout).")
    args = parser.parse_args()

    facts = json.loads(Path(args.facts).read_text(encoding="utf-8"))
    text = render_markdown(facts, load_labels())
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
        print(f"Digest written to {args.out}")
    else:
        print(text)


if __name__ == "__main__":
    main()
