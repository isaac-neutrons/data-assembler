"""
Tests for instrument-specific handlers.
"""

import pytest

from assembler.enums import Facility
from assembler.instruments import Instrument, InstrumentRegistry, REF_L
from assembler.instruments.base import GenericInstrument, ExtractedEnvironment


class TestInstrumentRegistry:
    """Tests for the InstrumentRegistry."""

    def test_ref_l_is_registered(self):
        """Test REF_L is registered."""
        assert "REF_L" in InstrumentRegistry.list_instruments()

    def test_get_handler_for_ref_l(self):
        """Test getting handler for REF_L."""
        handler = InstrumentRegistry.get_handler("REF_L")
        assert handler == REF_L

    def test_get_handler_for_bl4b(self):
        """Test getting handler by alias BL4B."""
        handler = InstrumentRegistry.get_handler("BL4B")
        assert handler == REF_L

    def test_get_handler_unknown(self):
        """Test getting handler for unknown instrument returns GenericInstrument."""
        handler = InstrumentRegistry.get_handler("UNKNOWN_INSTRUMENT")
        assert handler == GenericInstrument

    def test_get_handler_none(self):
        """Test getting handler for None returns GenericInstrument."""
        handler = InstrumentRegistry.get_handler(None)
        assert handler == GenericInstrument


class TestREFL:
    """Tests for the REF_L instrument handler."""

    def test_name_and_aliases(self):
        """Test REF_L name and aliases."""
        assert REF_L.name == "REF_L"
        assert "BL4B" in REF_L.aliases
        assert "BL-4B" in REF_L.aliases

    def test_defaults(self):
        """Test REF_L default values."""
        assert REF_L.defaults.facility == Facility.SNS
        assert REF_L.defaults.probe == "neutrons"
        assert REF_L.defaults.technique == "reflectivity"
        assert REF_L.defaults.wavelength == 6.0

    def test_matches_ref_l(self):
        """Test REF_L matches REF_L identifier."""
        assert REF_L.matches("REF_L") is True
        assert REF_L.matches("ref_l") is True
        assert REF_L.matches("REF_L_123") is True

    def test_matches_bl4b(self):
        """Test REF_L matches BL4B identifier."""
        assert REF_L.matches("BL4B") is True
        assert REF_L.matches("BL-4B") is True

    def test_does_not_match_other(self):
        """Test REF_L does not match other instruments."""
        assert REF_L.matches("REF_M") is False
        assert REF_L.matches("CG-1D") is False
        assert REF_L.matches(None) is False


class TestGenericInstrument:
    """Tests for the GenericInstrument fallback handler."""

    def test_name(self):
        """Test GenericInstrument name."""
        assert GenericInstrument.name == "GENERIC"

    def test_defaults(self):
        """Test GenericInstrument defaults to SNS."""
        assert GenericInstrument.defaults.facility == Facility.SNS

    def test_matches_nothing(self):
        """Test GenericInstrument doesn't match any specific instrument."""
        assert GenericInstrument.matches("REF_L") is False
        assert GenericInstrument.matches("GENERIC") is True
