"""
Parse experimental conditions from a free-text run/state description.

Turns a human description such as
``"OCV measurement in D2O electrolyte (pH 8.25, 0.1 M NaHCO3, N2 sparged)"``
into the structured electrochemical fields carried by ENVIRONMENT_SCHEMA:
``potential``, ``potential_scale``, ``control_mode``, ``electrolyte``, ``pH``.

This is the canonical home for condition parsing — downstream consumers (e.g.
the ISAAC writer) should read the structured fields rather than re-parsing text.
"""

from __future__ import annotations

import re
from typing import Any

# Open-circuit conditions.
_OCV_RE = re.compile(r"\bOCV\b|\bOCP\b|open[\s-]?circuit", re.IGNORECASE)
# Numeric applied potential, e.g. "-1 V", "+0.5 V", "0 V" (ASCII or unicode minus).
_POTENTIAL_RE = re.compile(r"([+\-−]?\d+(?:\.\d+)?)\s*V\b")
# "pH 8.25" / "pH=8.25".
_PH_RE = re.compile(r"\bpH\s*=?\s*(\d+(?:\.\d+)?)", re.IGNORECASE)
# "0.1 M NaHCO3" → concentration + electrolyte name.
_ELECTROLYTE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*M\s+([A-Za-z][A-Za-z0-9()]*)")
# Reference scales. Acronyms case-sensitive so the word "she" is never matched;
# slash-notation electrodes case-insensitive. First match wins.
_SCALE_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bRHE\b"), "RHE"),
    (re.compile(r"\bSHE\b"), "SHE"),
    (re.compile(r"\bNHE\b"), "SHE"),
    (re.compile(r"\bSCE\b"), "SCE"),
    (re.compile(r"Ag\s*/\s*AgCl", re.IGNORECASE), "Ag/AgCl"),
    (re.compile(r"Hg\s*/\s*HgSO4", re.IGNORECASE), "Hg/HgSO4"),
    (re.compile(r"Hg\s*/\s*HgO", re.IGNORECASE), "Hg/HgO"),
)


def parse_conditions(text: str | None) -> dict[str, Any]:
    """Extract structured electrochemical conditions from free text.

    Returns a dict with any of ``control_mode``, ``potential``,
    ``potential_scale``, ``pH``, ``electrolyte`` that could be recognised.
    Numeric potentials default to the SHE scale unless another is named.
    Open-circuit (OCV/OCP) takes precedence over a quoted value.
    Returns an empty dict when nothing is recognised.
    """
    if not text:
        return {}

    cond: dict[str, Any] = {}

    if _OCV_RE.search(text):
        cond["control_mode"] = "open_circuit"
    else:
        m = _POTENTIAL_RE.search(text)
        if m:
            cond["control_mode"] = "potentiostatic"
            cond["potential"] = float(m.group(1).replace("−", "-"))
            scale = "SHE"
            for pattern, name in _SCALE_PATTERNS:
                if pattern.search(text):
                    scale = name
                    break
            cond["potential_scale"] = scale

    mph = _PH_RE.search(text)
    if mph:
        cond["pH"] = float(mph.group(1))

    mel = _ELECTROLYTE_RE.search(text)
    if mel:
        cond["electrolyte"] = {"name": mel.group(2), "concentration_M": float(mel.group(1))}

    return cond
