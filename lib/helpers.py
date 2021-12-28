import hashlib


def strip_sscom(string: str) -> str:
    """Strip the https://www.ss.com/lv/ from url, for better readability."""
    if "https://www.ss.com/lv/" in string:
        return string.split("https://www.ss.com/lv/")[1]
    else:
        return string  # return unmodified string


def hash(string: str) -> str:
    return hashlib.sha256(string.encode("utf-8")).hexdigest()


def shorthash(string: str) -> str:
    return hash(string)[:10]
