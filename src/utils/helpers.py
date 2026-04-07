"""Common helper utilities."""

import hashlib
import re
from typing import Any


def clean_text(text: str) -> str:
    """Clean and normalize text data.

    Args:
        text: Input text to clean

    Returns:
        Cleaned text string
    """
    if not isinstance(text, str):
        return str(text) if text is not None else ""

    # Replace literal and actual line breaks
    text = text.replace("\\n", " ")
    text = text.replace("\n", " ")

    # Replace non-breaking spaces
    text = text.replace("\u00a0", " ")

    # Collapse multiple whitespaces
    text = re.sub(r"\s{2,}", " ", text)

    return text.strip()


def hash_text(input_text: str) -> str:
    """Generate SHA256 hash of input text.

    Args:
        input_text: Text to hash

    Returns:
        Hexadecimal hash string
    """
    if not isinstance(input_text, str):
        input_text = str(input_text)

    input_bytes = input_text.encode("utf-8")
    sha256 = hashlib.sha256()
    sha256.update(input_bytes)
    return sha256.hexdigest()


def extract_numbers(text: str) -> list[float]:
    """Extract all numbers from a text string.

    Args:
        text: Input text

    Returns:
        List of numbers found in the text
    """
    if not text:
        return []

    # Pattern to match Italian number format (1.234.567,89)
    pattern = r"\d{1,3}(?:[\.\,\d{3}]*\d)?(?:,\d{2})?"
    matches = re.findall(pattern, text)

    numbers = []
    for match in matches:
        try:
            # Convert Italian format to standard format
            number = match.replace(".", "").replace(",", ".")
            numbers.append(float(number))
        except ValueError:
            continue

    return numbers


def normalize_cig_cup(code: str) -> str:
    """Normalize CIG/CUP codes.

    Args:
        code: Input code

    Returns:
        Normalized code string
    """
    if not isinstance(code, str):
        return ""

    code = code.strip().upper()

    # Check for empty or placeholder values
    if code in ["NON SPECIFICATO", "NON SPECIFICATA", "N/A", ""]:
        return ""

    # Check if alphanumeric
    if not code.isalnum():
        return ""

    return code


def safe_cast(value: Any, target_type: type, default: Any = None) -> Any:
    """Safely cast a value to target type.

    Args:
        value: Value to cast
        target_type: Target type to cast to
        default: Default value if casting fails

    Returns:
        Cast value or default
    """
    try:
        return target_type(value)
    except (ValueError, TypeError):
        return default
