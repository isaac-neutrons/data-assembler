"""Tests for free-text experimental-condition parsing."""

from assembler.parsers.conditions import parse_conditions


def test_ocv_to_open_circuit():
    cond = parse_conditions("OCV measurement in D2O electrolyte (pH 8.25, 0.1 M NaHCO3, N2 sparged)")
    assert cond["control_mode"] == "open_circuit"
    assert cond["pH"] == 8.25
    assert cond["electrolyte"] == {"name": "NaHCO3", "concentration_M": 0.1}
    # OCV has no applied setpoint
    assert "potential" not in cond


def test_numeric_potential_defaults_to_she():
    cond = parse_conditions("Held at -1 V in 0.5 M KOH")
    assert cond["control_mode"] == "potentiostatic"
    assert cond["potential"] == -1.0
    assert cond["potential_scale"] == "SHE"
    assert cond["electrolyte"] == {"name": "KOH", "concentration_M": 0.5}


def test_explicit_scale_overrides_default():
    cond = parse_conditions("Measured at +0.5 V vs RHE, pH=7")
    assert cond["potential"] == 0.5
    assert cond["potential_scale"] == "RHE"
    assert cond["pH"] == 7.0


def test_ocv_takes_precedence_over_quoted_value():
    cond = parse_conditions("At OCV (about 0.2 V vs RHE)")
    assert cond["control_mode"] == "open_circuit"
    assert "potential" not in cond


def test_no_conditions_returns_empty():
    assert parse_conditions("Dry film measured in air, back reflection") == {}
    assert parse_conditions(None) == {}
    assert parse_conditions("") == {}


def test_she_not_matched_in_lowercase_word():
    # the English word "she" must not be read as the SHE scale
    cond = parse_conditions("she prepared the sample at -1 V")
    assert cond["potential_scale"] == "SHE"  # default, not from the word "she"
