import re
import unicodedata

MAX_DESCRIPTION_LENGTH = 24
MAX_CLIENT_NAME_LENGTH = 64

TRANSLIT_MAP = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
    'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'i', 'к': 'k', 'л': 'l', 'м': 'm',
    'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
    'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
    'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya'
}


def slugify_description(value: str, max_length: int = MAX_DESCRIPTION_LENGTH) -> str:
    text = unicodedata.normalize('NFKD', value or '').lower().strip()
    if not text:
        return ''

    result: list[str] = []
    for char in text:
        lower = char.lower()
        if lower in TRANSLIT_MAP:
            mapped = TRANSLIT_MAP[lower]
            if mapped:
                result.append(mapped)
            continue
        if char.isalnum():
            result.append(lower)
            continue
        if char in {' ', '-', '_', '.', ','}:
            if result and result[-1] != '-':
                result.append('-')
            elif not result:
                result.append('-')

    slug = ''.join(result)
    slug = re.sub(r'-{2,}', '-', slug).strip('-')
    return slug[:max_length]


def sanitize_owner_identifier(value: str, fallback_id: int) -> str:
    text = unicodedata.normalize('NFKD', value or '').lower()
    result: list[str] = []
    for char in text:
        lower = char.lower()
        if lower in TRANSLIT_MAP:
            mapped = TRANSLIT_MAP[lower]
            if mapped:
                result.append(mapped)
            continue
        if char.isalnum():
            result.append(lower)
            continue
        if char in {' ', '-', '_', '.', ','}:
            if result and result[-1] != '-':
                result.append('-')
            elif not result:
                result.append('-')

    sanitized = ''.join(result).strip('-')
    if not sanitized:
        sanitized = f"user{fallback_id}"
    return sanitized[:MAX_CLIENT_NAME_LENGTH]


def build_client_name(base: str, slug: str) -> str:
    base = (base or '').strip('-')
    slug = (slug or '').strip('-')
    if not slug:
        return base[:MAX_CLIENT_NAME_LENGTH]

    separator = '-' if base else ''
    max_len = MAX_CLIENT_NAME_LENGTH
    trimmed_slug = slug[:max_len]
    available_for_base = max_len - len(separator) - len(trimmed_slug)
    if available_for_base < 0:
        trimmed_slug = trimmed_slug[:max_len]
        available_for_base = max_len - len(separator) - len(trimmed_slug)

    trimmed_base = base[:max(0, available_for_base)]
    trimmed_base = trimmed_base.rstrip('-')
    if trimmed_base:
        separator = '-'
    else:
        separator = ''

    candidate = f"{trimmed_base}{separator}{trimmed_slug}"
    if len(candidate) <= max_len:
        return candidate
    return candidate[:max_len].rstrip('-')


def ensure_unique_slugged_name(base: str, slug: str, existing: set[str]) -> str:
    attempt = slug
    counter = 1
    while counter < 10000:
        candidate = build_client_name(base, attempt)
        if candidate and candidate not in existing:
            return candidate
        counter += 1
        attempt = f"{slug}-{counter}"
    raise RuntimeError("Не удалось подобрать уникальное имя клиента.")


def next_sequential_name(base: str, existing: set[str]) -> str:
    index = 1
    while index < 10000:
        candidate = build_client_name(base, str(index))
        if candidate and candidate not in existing:
            return candidate
        index += 1
    raise RuntimeError("Не удалось подобрать уникальный порядковый номер для клиента.")


def generate_client_name(base: str, slug: str, existing: set[str]) -> str:
    if slug:
        return ensure_unique_slugged_name(base, slug, existing)
    return next_sequential_name(base, existing)
