"""
Tests for genai/overview.py. No network access and no API key needed.
Run from the repository root:  uv run pytest tests/test_overview.py -v
"""
from genai import overview as ov

VALS = {"mese": "novembre 2026", "mese_precedente": "ottobre 2026", "n_dataset": "29",
        "n_aggiornati": "27", "n_nuovi_periodi": "1", "n_mancanti": "2"}

GOOD = ("L'aggiornamento di {mese}, confrontato con {mese_precedente}, riguarda "
        "{n_dataset} dataset monitorati. Di questi, {n_aggiornati} sono stati scaricati "
        "di nuovo dalla fonte e {n_nuovi_periodi} contiene periodi nuovi. "
        "Restano da verificare {n_mancanti} dataset mancanti.")

FACTS = {
    "month": "2026-11", "datasets_with_new_periods": 1,
    "domains": [{"domain": "labour", "previous_month": "2026-10", "previous_file": "x",
                 "status": "processed", "datasets": [
                     {"dataset_code": "A", "status": "updated", "new_periods": {"M": ["2026-09"]}},
                     {"dataset_code": "B", "status": "updated", "new_periods": {}},
                     {"dataset_code": "C", "status": "missing"},
                 ]}],
}
LABELS = {"A": {"label_it": "Forze di lavoro", "fonte": "ISTAT"}}


def test_good_paragraph_passes_and_substitutes():
    assert ov.verify(GOOD, VALS) == []
    out = ov.substitute(GOOD, VALS)
    assert "27 sono stati scaricati" in out and "{" not in out


def test_rejections():
    cases = {
        "contains digits": GOOD.replace("{n_dataset}", "29"),
        "number word": GOOD + " Due dataset sono nuovi.",
        "month name": GOOD.replace("{mese}", "novembre"),
        "quantifier": GOOD + " Tutti i dati sono pronti.",
        "trend word": GOOD + " Si nota un aumento.",
        "missing placeholders": GOOD.replace("{n_mancanti} ", ""),
        "unknown placeholders": GOOD + " Vedi {n_inventato}.",
        "formatting characters": "**" + GOOD + "**",
    }
    for expected, text in cases.items():
        problems = ov.verify(text, VALS)
        assert any(p.startswith(expected) for p in problems), (expected, problems)
    assert ov.verify("", VALS) == ["empty response"]


def test_offered_placeholders_only_nonzero_counts():
    vals = ov.offered_placeholders(FACTS, LABELS)
    assert vals["n_aggiornati"] == "2" and vals["n_mancanti"] == "1"
    assert "n_non_aggiornati" not in vals and "n_nuovi_dataset" not in vals
    assert vals["elenco_nuovi_periodi"] == "Forze di lavoro"


def _stub(responses):
    """A fake call_model that returns or raises the given items in order."""
    it = iter(responses)

    def call(client, model, prompt, s):
        r = next(it)
        if isinstance(r, Exception):
            raise r
        return r, {"prompt": 1, "output": 1, "thinking": 0}
    return call


GOOD_FOR_FACTS = ("L'aggiornamento di {mese}, rispetto a {mese_precedente}, riguarda "
                  "{n_dataset} dataset: {n_aggiornati} risultano scaricati di nuovo e "
                  "{n_nuovi_periodi} contiene periodi nuovi, cioè {elenco_nuovi_periodi}. "
                  "Sono da verificare {n_mancanti} dataset mancanti.")


def test_fallback_model_used_when_primary_fails(monkeypatch):
    monkeypatch.setattr(ov, "call_model", _stub([RuntimeError("503"), GOOD_FOR_FACTS]))
    r = ov.write_overview(FACTS, LABELS, client=object())
    assert r["source"] == "gemini" and r["model"] == ov.DEFAULTS["models"][1]
    assert "Forze di lavoro" in r["text"]


def test_template_used_when_both_rejected(monkeypatch):
    monkeypatch.setattr(ov, "call_model", _stub(["Ci sono 3 dataset.", "Tutto bene."]))
    r = ov.write_overview(FACTS, LABELS, client=object())
    assert r["source"] == "template" and len(r["attempts"]) == 2
    assert r["text"].startswith("Riepilogo dell'aggiornamento")


def test_no_key_means_template(monkeypatch):
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    r = ov.write_overview(FACTS, LABELS)
    assert r["source"] == "template" and r["reason"] == "no API key in the environment"