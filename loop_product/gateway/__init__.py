"""Gateway surfaces for structured control-object handling."""

from .accept import accept_control_envelope
from .classify import classify_envelope
from .normalize import normalize_control_envelope
from .safe_degrade import safe_degrade_envelope

__all__ = [
    "accept_control_envelope",
    "classify_envelope",
    "normalize_control_envelope",
    "safe_degrade_envelope",
]
