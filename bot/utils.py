from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Tuple

from aiogram.utils.keyboard import InlineKeyboardBuilder

DATE_CALLBACK_PREFIX = "date:"
REFRESH_CALLBACK_PREFIX = "refresh:"
DURATION_CALLBACK_PREFIX = "duration:"
DAY_PERIOD_CALLBACK_PREFIX = "period:"
TIME_CALLBACK_PREFIX = "time:"
NAVIGATION_CALLBACK_PREFIX = "nav:"
RESET_CALLBACK_DATA = f"{NAVIGATION_CALLBACK_PREFIX}reset"
AUTOBOOK_REQUEST = "autobook:start"
AUTOBOOK_STUDIO_PREFIX = "autobook:studio:"
SLOT_CALLBACK_PREFIX = "slot:"

DURATION_OPTIONS = (
    (60, "1 час"),
    (90, "1,5 часа"),
    (120, "2 часа"),
)

_DURATION_LABELS: Dict[int, str] = {
    minutes: label for minutes, label in DURATION_OPTIONS
}

DAY_PERIOD_OPTIONS = (
    ("any", "Любое время"),
    ("morning", "Утро"),
    ("day", "День"),
    ("evening", "Вечер"),
)

_DAY_PERIOD_LABELS: Dict[str, str] = {
    key: label for key, label in DAY_PERIOD_OPTIONS
}

DAY_PERIOD_RANGES: Dict[str, Tuple[int, int]] = {
    "any": (0, 24 * 60),
    "morning": (6 * 60, 12 * 60),
    "day": (12 * 60, 18 * 60),
    "evening": (18 * 60, 24 * 60),
}

STUDIO_LINKS: Dict[str, str] = {
    "Нагатинская": "https://padlhub.ru/padel_nagatinskaya",
    "Нагатинская Премиум": "https://padlhub.ru/padel_nagatinskayapremium",
    "Сколково": "https://padlhub.ru/padel_skolkovo",
    "Терехово": "https://padlhub.ru/padel_terehovo",
    "Ясенево": "https://padlhub.ru/padl_yas",
    "First Padel Club": "https://firstpadel.ru/#eZA",
}

_MONTHS_RU = (
    "января",
    "февраля",
    "марта",
    "апреля",
    "мая",
    "июня",
    "июля",
    "августа",
    "сентября",
    "октября",
    "ноября",
    "декабря",
)


def build_duration_keyboard() -> InlineKeyboardBuilder:
    builder = InlineKeyboardBuilder()
    for minutes, label in DURATION_OPTIONS:
        builder.button(
            text=label,
            callback_data=f"{DURATION_CALLBACK_PREFIX}{minutes}",
        )
    builder.adjust(len(DURATION_OPTIONS))
    return builder


def build_period_keyboard() -> InlineKeyboardBuilder:
    builder = InlineKeyboardBuilder()
    for key, label in DAY_PERIOD_OPTIONS:
        builder.button(
            text=label,
            callback_data=f"{DAY_PERIOD_CALLBACK_PREFIX}{key}",
        )
    builder.adjust(len(DAY_PERIOD_OPTIONS))
    return builder


def build_time_keyboard(period_key: str) -> InlineKeyboardBuilder:
    """Создает клавиатуру с вариантами времени для выбранного периода."""
    builder = InlineKeyboardBuilder()
    
    time_range = DAY_PERIOD_RANGES.get(period_key)
    if time_range is None:
        # Если период не определен, показываем все времена с шагом 30 минут
        start_min = 0
        end_min = 24 * 60
    else:
        start_min, end_min = time_range
    
    # Генерируем времена с шагом 30 минут
    current_min = start_min
    times = []
    while current_min < end_min:
        hours = current_min // 60
        minutes = current_min % 60
        time_str = f"{hours:02d}:{minutes:02d}"
        times.append((time_str, current_min))
        current_min += 30
    
    # Добавляем кнопки с временами
    for time_str, minutes in times:
        builder.button(
            text=time_str,
            callback_data=f"{TIME_CALLBACK_PREFIX}{time_str}",
        )
    
    # Располагаем кнопки по 4 в ряд для удобства
    builder.adjust(4)
    return builder


def build_date_keyboard(days: int = 7) -> InlineKeyboardBuilder:
    builder = InlineKeyboardBuilder()
    today = date.today()
    for offset in range(days):
        current = today + timedelta(days=offset)
        builder.button(
            text=current.strftime("%d.%m"),
            callback_data=f"{DATE_CALLBACK_PREFIX}{current.isoformat()}",
        )
    builder.adjust(3, 3, 1)
    return builder


def build_refresh_keyboard(date_str: str) -> InlineKeyboardBuilder:
    builder = InlineKeyboardBuilder()
    builder.button(
        text="🔁 Обновить",
        callback_data=f"{REFRESH_CALLBACK_PREFIX}{date_str}",
    )
    return builder


def build_results_keyboard(
    date_str: str,
    slot_buttons: Iterable[Tuple[str, str]] | None = None,
) -> InlineKeyboardBuilder:
    builder = InlineKeyboardBuilder()
    builder.button(
        text="🔁 Обновить",
        callback_data=f"{REFRESH_CALLBACK_PREFIX}{date_str}",
    )
    builder.button(
        text="🕒 Длительность",
        callback_data=f"{NAVIGATION_CALLBACK_PREFIX}duration",
    )
    builder.button(
        text="🌗 Период",
        callback_data=f"{NAVIGATION_CALLBACK_PREFIX}period",
    )
    builder.button(
        text="⏰ Время",
        callback_data=f"{NAVIGATION_CALLBACK_PREFIX}time",
    )
    builder.button(
        text="📆 Дата",
        callback_data=f"{NAVIGATION_CALLBACK_PREFIX}date",
    )
    builder.button(
        text="🔄 Сначала",
        callback_data=RESET_CALLBACK_DATA,
    )
    builder.adjust(1, 2, 2, 1)
    return builder


def build_autobook_keyboard(studios: Iterable[str]) -> InlineKeyboardBuilder:
    builder = InlineKeyboardBuilder()
    has_buttons = False
    for studio in studios:
        if studio not in STUDIO_LINKS:
            continue
        builder.button(
            text=studio,
            callback_data=f"{AUTOBOOK_STUDIO_PREFIX}{studio}",
        )
        has_buttons = True
    if not has_buttons:
        return builder
    builder.adjust(1)
    builder.button(
        text="⬅️ Назад",
        callback_data=f"{NAVIGATION_CALLBACK_PREFIX}date",
    )
    return builder


def humanize_date(date_str: str) -> str:
    parsed = datetime.strptime(date_str, "%Y-%m-%d").date()
    month = _MONTHS_RU[parsed.month - 1]
    return f"{parsed.day} {month}"


def humanize_duration(minutes: int) -> str:
    return _DURATION_LABELS.get(minutes, f"{minutes // 60} ч")


def humanize_period(period_key: str) -> str:
    return _DAY_PERIOD_LABELS.get(period_key, period_key)


def format_slots(studio: str, times: Iterable[str]) -> str:
    items = []
    link = STUDIO_LINKS.get(studio)
    for time in times:
        if link:
            items.append(f'• {time} (<a href="{link}">Ссылка</a>)')
        else:
            items.append(f"• {time}")
    if not items:
        return ""
    body = "\n".join(items)
    return f"<b>📍 {studio} — Панорамик 2x2</b>\n{body}"


