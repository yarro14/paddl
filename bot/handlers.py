from __future__ import annotations

import random
from contextlib import suppress
from datetime import datetime
from html import escape
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple

import aiohttp
from aiogram import Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from automation import BookingResult, BookingTask, BookingTaskManager, BookingTaskState

from .parser import PadlHubClient, ParserError, SLOT_STEP, SLOT_STEP_MINUTES, fetch_panoramic_slots
from .utils import (
    AUTOBOOK_REQUEST,
    AUTOBOOK_STUDIO_PREFIX,
    DATE_CALLBACK_PREFIX,
    DAY_PERIOD_CALLBACK_PREFIX,
    DAY_PERIOD_RANGES,
    DURATION_CALLBACK_PREFIX,
    NAVIGATION_CALLBACK_PREFIX,
    RESET_CALLBACK_DATA,
    REFRESH_CALLBACK_PREFIX,
    STUDIO_LINKS,
    SLOT_CALLBACK_PREFIX,
    TIME_CALLBACK_PREFIX,
    build_autobook_keyboard,
    build_date_keyboard,
    build_duration_keyboard,
    build_period_keyboard,
    build_results_keyboard,
    build_time_keyboard,
    format_slots,
    humanize_date,
    humanize_duration,
    humanize_period,
)

router = Router()
booking_manager = BookingTaskManager()

ERROR_MESSAGE = (
    "⚠️ Не удалось получить данные с сайта padlhub.ru. Попробуйте позже."
)
WELCOME_MESSAGE = (
    "👋 Привет! Я помогу найти свободные слоты <b>Панорамик 2x2</b>.\n"
    "Начнём с выбора длительности игры:"
)
SELECT_DURATION_MESSAGE = "Выберите длительность игры:"
SELECT_PERIOD_MESSAGE = "Выберите период дня:"
SELECT_TIME_MESSAGE = "Выберите время начала игры:"
SELECT_DATE_MESSAGE = "Выберите дату, когда хотите поиграть в падел:"
NO_SLOTS_MESSAGE = (
    "На выбранный период нет свободных слотов.\n"
    "Попробуйте выбрать другую дату или изменить параметры."
)


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    keyboard = build_duration_keyboard().as_markup()
    await message.answer(
        WELCOME_MESSAGE,
        reply_markup=keyboard,
    )


@router.callback_query(
    lambda c: c.data and c.data.startswith(DURATION_CALLBACK_PREFIX)
)
async def handle_duration(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    message = callback.message
    if message is None:
        return
    data = callback.data or ""
    raw_value = data[len(DURATION_CALLBACK_PREFIX) :]
    try:
        duration_minutes = int(raw_value)
    except ValueError:
        keyboard = build_duration_keyboard().as_markup()
        await _safe_edit(callback, SELECT_DURATION_MESSAGE, keyboard)
        return

    await state.update_data(duration=duration_minutes, period=None, selected_time=None, selected_date=None)
    keyboard = build_period_keyboard().as_markup()
    await message.answer(
        f"✅ Длительность: <b>{humanize_duration(duration_minutes)}</b>\n"
        f"{SELECT_PERIOD_MESSAGE}",
        reply_markup=keyboard,
    )
    with suppress(TelegramBadRequest):
        await callback.message.delete()


@router.callback_query(
    lambda c: c.data and c.data.startswith(DAY_PERIOD_CALLBACK_PREFIX)
)
async def handle_period(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    message = callback.message
    if message is None:
        return
    data = callback.data or ""
    period_key = data[len(DAY_PERIOD_CALLBACK_PREFIX) :]
    if period_key not in DAY_PERIOD_RANGES:
        keyboard = build_period_keyboard().as_markup()
        await _safe_edit(callback, SELECT_PERIOD_MESSAGE, keyboard)
        return

    current_state = await state.get_data()
    duration = current_state.get("duration")
    selected_time = current_state.get("selected_time")
    selected_date = current_state.get("selected_date")

    await state.update_data(period=period_key, selected_time=None)

    # Если выбрано "Любое время", пропускаем выбор времени
    if period_key == "any":
        if isinstance(duration, int) and isinstance(selected_date, str):
            await _send_slots(callback, state, selected_date, duration, period_key, None)
            return
        await state.update_data(selected_date=None)
        keyboard = build_date_keyboard().as_markup()
        await message.answer(
            f"🌗 Период: <b>{humanize_period(period_key)}</b>\n"
            f"{SELECT_DATE_MESSAGE}",
            reply_markup=keyboard,
        )
        with suppress(TelegramBadRequest):
            await callback.message.delete()
        return

    if isinstance(duration, int) and isinstance(selected_time, str) and isinstance(selected_date, str):
        await _send_slots(callback, state, selected_date, duration, period_key, selected_time)
        return

    keyboard = build_time_keyboard(period_key).as_markup()
    await message.answer(
        f"🌗 Период: <b>{humanize_period(period_key)}</b>\n"
        f"{SELECT_TIME_MESSAGE}",
        reply_markup=keyboard,
    )
    with suppress(TelegramBadRequest):
        await callback.message.delete()


@router.callback_query(lambda c: c.data and c.data.startswith(TIME_CALLBACK_PREFIX))
async def handle_time(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    message = callback.message
    if message is None:
        return
    data = callback.data or ""
    time_str = data[len(TIME_CALLBACK_PREFIX) :]
    
    # Проверяем формат времени
    try:
        datetime.strptime(time_str, "%H:%M")
    except ValueError:
        data_state = await state.get_data()
        period = data_state.get("period")
        if isinstance(period, str) and period in DAY_PERIOD_RANGES:
            keyboard = build_time_keyboard(period).as_markup()
            await _safe_edit(callback, SELECT_TIME_MESSAGE, keyboard)
        return

    current_state = await state.get_data()
    duration = current_state.get("duration")
    period = current_state.get("period")
    selected_date = current_state.get("selected_date")

    if not isinstance(duration, int):
        keyboard = build_duration_keyboard().as_markup()
        await _safe_edit(callback, SELECT_DURATION_MESSAGE, keyboard)
        return
    if not isinstance(period, str) or period not in DAY_PERIOD_RANGES:
        keyboard = build_period_keyboard().as_markup()
        await _safe_edit(callback, SELECT_PERIOD_MESSAGE, keyboard)
        return

    await state.update_data(selected_time=time_str)

    if isinstance(selected_date, str):
        await _send_slots(callback, state, selected_date, duration, period, time_str)
        return

    await state.update_data(selected_date=None)
    keyboard = build_date_keyboard().as_markup()
    await message.answer(
        f"⏰ Время: <b>{time_str}</b>\n"
        f"{SELECT_DATE_MESSAGE}",
        reply_markup=keyboard,
    )
    with suppress(TelegramBadRequest):
        await callback.message.delete()


@router.callback_query(lambda c: c.data and c.data.startswith(DATE_CALLBACK_PREFIX))
async def handle_date(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    date_str = callback.data[len(DATE_CALLBACK_PREFIX) :]  # type: ignore[index]
    data = await state.get_data()
    duration = data.get("duration")
    period = data.get("period")
    selected_time = data.get("selected_time")
    if not isinstance(duration, int):
        keyboard = build_duration_keyboard().as_markup()
        await _safe_edit(callback, SELECT_DURATION_MESSAGE, keyboard)
        return
    if not isinstance(period, str) or period not in DAY_PERIOD_RANGES:
        keyboard = build_period_keyboard().as_markup()
        await _safe_edit(callback, SELECT_PERIOD_MESSAGE, keyboard)
        return
    await state.update_data(selected_date=date_str)
    await _send_slots(callback, state, date_str, duration, period, selected_time if isinstance(selected_time, str) else None)


@router.callback_query(
    lambda c: c.data and c.data.startswith(REFRESH_CALLBACK_PREFIX)
)
async def handle_refresh(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer("Обновляю данные…")
    date_str = callback.data[len(REFRESH_CALLBACK_PREFIX) :]  # type: ignore[index]
    data = await state.get_data()
    duration = data.get("duration")
    period = data.get("period")
    selected_time = data.get("selected_time")
    if not isinstance(duration, int):
        keyboard = build_duration_keyboard().as_markup()
        await _safe_edit(callback, SELECT_DURATION_MESSAGE, keyboard)
        return
    if not isinstance(period, str) or period not in DAY_PERIOD_RANGES:
        keyboard = build_period_keyboard().as_markup()
        await _safe_edit(callback, SELECT_PERIOD_MESSAGE, keyboard)
        return
    await state.update_data(selected_date=date_str)
    await _send_slots(callback, state, date_str, duration, period, selected_time if isinstance(selected_time, str) else None)


@router.callback_query(
    lambda c: c.data and c.data.startswith(NAVIGATION_CALLBACK_PREFIX)
)
async def handle_navigation(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    if callback.data == RESET_CALLBACK_DATA:
        await cmd_start(callback.message, state)  # type: ignore[arg-type]
        return
    action = callback.data[len(NAVIGATION_CALLBACK_PREFIX) :]
    if action == "duration":
        await _prompt_duration(callback, state)
    elif action == "period":
        await _prompt_period(callback, state)
    elif action == "time":
        await _prompt_time(callback, state)
    elif action == "date":
        await _prompt_date(callback, state)
    else:
        await callback.answer("Недоступное действие.", show_alert=True)


async def _send_slots(
    callback: CallbackQuery,
    state: FSMContext,
    date_str: str,
    duration_minutes: int,
    period_key: str,
    selected_time: str | None = None,
) -> None:
    message = callback.message
    placeholder = None
    if message:
        with suppress(TelegramBadRequest):
            placeholder = await message.answer("⏳ Загружаю доступные слоты…")
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        await _safe_edit(callback, ERROR_MESSAGE)
        if placeholder:
            with suppress(TelegramBadRequest):
                await placeholder.delete()
        return

    try:
        slots = await fetch_panoramic_slots(date_str, duration_minutes)
    except ParserError as exc:
        message = str(exc).strip() or ERROR_MESSAGE
        await _safe_edit(
            callback,
            message,
            build_results_keyboard(date_str).as_markup(),
        )
        if placeholder:
            with suppress(TelegramBadRequest):
                await placeholder.delete()
        return

    filtered = _filter_slots_by_period(slots, period_key, date_str, selected_time)
    if not filtered:
        await _safe_edit(
            callback,
            NO_SLOTS_MESSAGE,
            build_results_keyboard(date_str).as_markup(),
        )
        if placeholder:
            with suppress(TelegramBadRequest):
                await placeholder.delete()
        return

    total_slots = 0
    for times in filtered.values():
        total_slots += len(times)

    lines = [
        "<b>Ваша подборка</b>",
        f"📅 {humanize_date(date_str)}",
        f"⏱ {humanize_duration(duration_minutes)}",
        f"🌗 {humanize_period(period_key)}",
    ]
    if selected_time:
        lines.append(f"⏰ {selected_time}")
    lines.extend([
        "",
        f"🔎 Найдено вариантов: {total_slots}",
    ])
    for studio, times in filtered.items():
        slot_line = format_slots(studio, times)
        if slot_line:
            lines.append("")
            lines.append(slot_line)
    await state.update_data(
        last_results={"studios": list(filtered.keys())},
        slot_mapping={},
        selected_slot=None,
    )
    text = "\n".join(lines)
    keyboard = build_results_keyboard(date_str).as_markup()
    await _safe_edit(callback, text, keyboard)
    if placeholder:
        with suppress(TelegramBadRequest):
            await placeholder.delete()


async def _safe_edit(
    callback: CallbackQuery,
    text: str,
    reply_markup=None,
) -> None:
    message = callback.message
    if message is None:
        return
    try:
        await message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest:
        await message.answer(text, reply_markup=reply_markup)


@router.callback_query(lambda c: c.data and c.data.startswith(SLOT_CALLBACK_PREFIX))
async def handle_slot_selection(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    data = await state.get_data()
    mapping = data.get("slot_mapping")
    if not isinstance(mapping, dict):
        if callback.message:
            await callback.message.answer("Не удалось определить выбранный слот. Попробуйте обновить список.")
        return
    token = callback.data[len(SLOT_CALLBACK_PREFIX) :]  # type: ignore[index]
    slot_info = mapping.get(token)
    if not isinstance(slot_info, dict):
        if callback.message:
            await callback.message.answer("Этот слот устарел. Обновите список и попробуйте снова.")
        return

    await state.update_data(selected_slot=slot_info)
    if callback.message:
        studio = slot_info.get("studio", "—")
        interval = slot_info.get("interval", "—")
        await callback.message.answer(
            f"🔔 Выбран слот: <b>{studio}</b> — {interval}.\n"
            "Теперь можно нажать «🤖 Автозапись (beta)»."
        )


@router.callback_query(lambda c: c.data == AUTOBOOK_REQUEST)
async def handle_autobook_request(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    data = await state.get_data()
    selected = data.get("selected_slot")
    if isinstance(selected, dict):
        studio = selected.get("studio")
        interval = selected.get("interval")
        duration_raw = selected.get("duration")
        duration = _coerce_int(duration_raw) or data.get("duration")
        date_str = selected.get("date") or data.get("selected_date")
        if isinstance(studio, str):
            link = STUDIO_LINKS.get(studio)
            if link:
                await _start_autobook(
                    callback,
                    state,
                    studio,
                    link,
                    interval,
                    date_str if isinstance(date_str, str) else None,
                    _coerce_int(duration),
                )
                return

    studios: List[str] = []
    results = data.get("last_results")
    if isinstance(results, dict):
        raw = results.get("studios")
        if isinstance(raw, list):
            studios = [
                studio
                for studio in raw
                if isinstance(studio, str) and studio in STUDIO_LINKS
            ]

    if not studios:
        if callback.message:
            await callback.message.answer(
                "Нет сохранённых локаций для автозаписи. Обновите список слотов и попробуйте снова."
            )
        return

    if callback.message:
        await callback.message.answer(
            "🤖 Выберите площадку, чтобы открыть её страницу в автономном режиме. "
            "Пока что сценарий только проверяет доступность сайта.",
            reply_markup=build_autobook_keyboard(studios).as_markup(),
        )


@router.callback_query(lambda c: c.data and c.data.startswith(AUTOBOOK_STUDIO_PREFIX))
async def handle_autobook_studio(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer("Запускаю автозапись…")
    studio = callback.data[len(AUTOBOOK_STUDIO_PREFIX) :]  # type: ignore[index]
    link = STUDIO_LINKS.get(studio)
    if not link:
        if callback.message:
            await callback.message.answer("Не удалось определить ссылку для выбранной локации.")
        return

    if callback.message is None:
        return

    data = await state.get_data()
    duration = _coerce_int(data.get("duration"))
    date_str = data.get("selected_date")
    await state.update_data(selected_slot={"studio": studio, "interval": None})
    await _start_autobook(
        callback,
        state,
        studio,
        link,
        interval=None,
        date_str=date_str if isinstance(date_str, str) else None,
        duration_minutes=duration,
    )


@router.message()
async def handle_autobook_input(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    autobook = data.get("autobook")
    if not isinstance(autobook, dict):
        return

    stage = autobook.get("stage")
    if stage == "phone":
        phone = _normalize_phone(message.text or "")
        if phone is None:
            await message.answer(
                "Не удалось распознать номер. Введите в формате +7XXXXXXXXXX."
            )
            return
        autobook["phone"] = phone

        studio = autobook.get("studio")
        link = autobook.get("link")
        interval = autobook.get("interval")
        date_str = autobook.get("date")
        duration_value = autobook.get("duration")
        duration_minutes = _coerce_int(duration_value) or 60

        if not isinstance(studio, str) or not isinstance(link, str):
            await message.answer(
                "Не удалось подготовить автозапись: отсутствуют данные о площадке."
            )
            await state.update_data(autobook=None)
            return

        await message.answer("⏳ Запрашиваю код подтверждения…")
        booking_manager.start()
        metadata = {
            "mode": "request_code",
            "studio": studio,
            "phone": phone,
            "date": date_str,
            "interval": interval,
            "duration": str(duration_minutes),
        }
        if autobook.get("room"):
            metadata["room"] = autobook["room"]

        task = BookingTask(
            location_url=link,
            description=f"Запрос кода для «{studio}»",
            metadata=metadata,
        )
        try:
            result = await booking_manager.submit(task)
        except Exception as exc:  # pragma: no cover - защитный слой
            await message.answer(
                f"Не удалось отправить код подтверждения: {escape(str(exc))}"
            )
            await state.update_data(autobook=None)
            return

        if result.state is not BookingTaskState.COMPLETED or not result.payload:
            reason = result.message or "Сервис не принял запрос на код."
            await message.answer(f"Не удалось отправить код подтверждения: {escape(reason)}")
            await state.update_data(autobook=None)
            return

        storage_state = result.payload.get("storage_state")
        resume_url = result.payload.get("resume_url")
        if not storage_state:
            await message.answer(
                "Не удалось сохранить сессию для подтверждения. Попробуйте заново."
            )
            await state.update_data(autobook=None)
            return

        autobook["storage_state"] = storage_state
        autobook["resume_url"] = resume_url
        autobook["stage"] = "code"
        await state.update_data(autobook=autobook)
        await message.answer(
            "📨 Код отправлен. Как только получите SMS или сообщение в WhatsApp — введите код здесь."
        )
        return

    if stage == "code":
        code = (message.text or "").strip()
        if not code:
            await message.answer("Код не может быть пустым. Попробуйте ещё раз.")
            return
        autobook["code"] = code
        autobook["stage"] = "processing"
        await state.update_data(autobook=autobook)
        await _execute_autobook(message, state)
        return

    # когда stage другой, игнорируем сообщение


def _filter_slots_by_period(
    slots: Dict[str, List[str]],
    period_key: str,
    date_str: str,
    selected_time: str | None = None,
) -> Dict[str, List[str]]:
    time_range = DAY_PERIOD_RANGES.get(period_key)
    if time_range is None:
        return slots
    start_min, end_min = time_range
    tz = ZoneInfo("Europe/Moscow")
    now = datetime.now(tz)
    filtered: Dict[str, List[str]] = {}
    
    # Если выбрано конкретное время, фильтруем по нему и времени +30 минут
    target_times: List[int] | None = None
    if selected_time:
        try:
            time_obj = datetime.strptime(selected_time, "%H:%M")
            target_minutes = time_obj.hour * 60 + time_obj.minute
            target_times = [target_minutes, target_minutes + 30]
        except ValueError:
            target_times = None
    
    for studio, items in slots.items():
        selected: List[str] = []
        for slot in items:
            start_part = slot.split("–", 1)[0]
            try:
                start_time = datetime.strptime(start_part, "%H:%M")
            except ValueError:
                continue
            minutes = start_time.hour * 60 + start_time.minute
            
            # Если выбрано конкретное время, проверяем только его и +30 минут
            if target_times is not None:
                if minutes not in target_times:
                    continue
            else:
                # Иначе фильтруем по периоду дня
                if not (start_min <= minutes < end_min):
                    continue
            
            start_dt = datetime.strptime(
                f"{date_str} {start_part}", "%Y-%m-%d %H:%M"
            ).replace(tzinfo=tz)
            if start_dt >= now:
                selected.append(slot)
        if selected:
            filtered[studio] = selected
    return filtered


async def _start_autobook(
    callback: CallbackQuery,
    state: FSMContext,
    studio: str,
    link: str,
    interval: str | None,
    date_str: str | None,
    duration_minutes: int | None,
) -> None:
    if duration_minutes is None:
        data = await state.get_data()
        duration_minutes = _coerce_int(data.get("duration"))
    await state.update_data(
        autobook={
            "stage": "phone",
            "studio": studio,
            "link": link,
            "interval": interval,
            "date": date_str,
            "duration": duration_minutes,
            "phone": None,
            "code": None,
            "storage_state": None,
            "resume_url": None,
        }
    )
    if callback.message:
        await callback.message.answer(
            "📱 Введите номер телефона, который обычно используете для бронирования "
            "через PadlHub/VivaCRM (формат +7XXXXXXXXXX)."
        )


async def _execute_autobook(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    autobook = data.get("autobook")
    if not isinstance(autobook, dict):
        return

    studio = autobook.get("studio")
    link = autobook.get("link")
    interval = autobook.get("interval")
    date_str = autobook.get("date")
    duration = autobook.get("duration")
    phone = autobook.get("phone")
    code = autobook.get("code")
    storage_state = autobook.get("storage_state")
    resume_url = autobook.get("resume_url")

    if not isinstance(studio, str) or not isinstance(link, str):
        await message.answer("Не удалось найти данные для автозаписи. Попробуйте заново.")
        await state.update_data(autobook=None)
        return
    if not isinstance(phone, str) or not isinstance(code, str):
        await message.answer("Недостаточно данных для автозаписи. Попробуйте заново.")
        await state.update_data(autobook=None)
        return
    if storage_state is None:
        await message.answer(
            "Сессия подтверждения не найдена. Пожалуйста, начните автозапись заново."
        )
        await state.update_data(autobook=None)
        return

    duration_minutes = _coerce_int(duration) or 60
    chosen_room = None
    if isinstance(studio, str) and isinstance(date_str, str) and isinstance(interval, str):
        try:
            chosen_room = await _choose_random_room(studio, date_str, interval, duration_minutes)
        except Exception as exc:  # pragma: no cover - защитный слой
            await message.answer(f"⚠️ Не удалось определить свободный корт автоматически: {exc}")

    booking_manager.start()
    metadata = {
        "studio": studio,
        "phone": phone,
        "code": code,
        "date": date_str,
        "duration": str(duration_minutes),
        "storage_state": storage_state,
        "resume_url": resume_url,
    }
    description = f"Страница {studio}"
    if isinstance(interval, str) and interval:
        metadata["interval"] = interval
        description += f" — {interval}"

    task = BookingTask(
        location_url=link,
        description=description,
        metadata=metadata,
    )
    waiting_parts = [f"⏳ Запускаю автозапись для «{studio}»."]  # noqa: RUF015
    if interval:
        waiting_parts.append(f"Слот: {interval}.")
    if chosen_room:
        waiting_parts.append(f"Выбран корт: {chosen_room}.")
        metadata["room"] = chosen_room
    waiting_parts.append("Это может занять до минуты…")
    status_message = await message.answer(" ".join(waiting_parts))

    try:
        result: BookingResult = await booking_manager.submit(task)
    except Exception as exc:  # pragma: no cover - защитный слой
        result = BookingResult(
            state=BookingTaskState.FAILED,
            message=f"Техническая ошибка: {escape(str(exc))}",
        )

    if result.state is BookingTaskState.COMPLETED:
        parts = ["✅ Страница успешно открыта."]
        if interval:
            parts.append(f"Слот: {interval}.")
        if chosen_room:
            parts.append(f"Корт: {chosen_room}.")
        parts.append(
            "Следующий этап — автоматизировать подтверждение и оплату."
        )
        text = "\n".join(parts)
    else:
        text = (
            "⚠️ Не удалось выполнить автозапись.\n"
            f"Причина: {escape(result.message)}"
        )

    try:
        await status_message.edit_text(text)
    except TelegramBadRequest:
        await status_message.answer(text)

    await state.update_data(autobook=None)


def _normalize_phone(raw: str) -> str | None:
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 11 and digits[0] in {"7", "8"}:
        digits = "7" + digits[1:]
    elif len(digits) == 10:
        digits = "7" + digits
    if len(digits) != 11 or not digits.startswith("7"):
        return None
    return f"+{digits}"


def _coerce_int(value) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


async def _choose_random_room(
    studio: str,
    date_str: str,
    interval: str,
    duration_minutes: int,
) -> str | None:
    start_part = interval.split()[0]
    if "–" not in start_part:
        return None
    start_time = start_part.split("–", 1)[0]
    required_slots = max(1, duration_minutes // SLOT_STEP_MINUTES)

    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        client = PadlHubClient(session)
        descriptors = await client.fetch_panoramic_rooms()
        candidates: List[str] = []

        for descriptor in descriptors:
            if descriptor.studio_name != studio:
                continue
            times = await client.fetch_room_slots(room=descriptor, date_str=date_str)
            if not times:
                continue

            for idx in range(len(times) - required_slots + 1):
                window = times[idx : idx + required_slots]
                if window[0].strftime("%H:%M") != start_time:
                    continue
                if not _is_consecutive(window):
                    continue
                candidates.append(descriptor.room_name)
                break

    if not candidates:
        return None
    return random.choice(candidates)


def _is_consecutive(window: List[datetime]) -> bool:
    if len(window) <= 1:
        return True
    for previous, current in zip(window, window[1:]):
        if current - previous != SLOT_STEP:
            return False
    return True


async def _prompt_duration(callback: CallbackQuery, state: FSMContext) -> None:
    await state.update_data(duration=None, period=None, selected_time=None, selected_date=None)
    keyboard = build_duration_keyboard().as_markup()
    await _safe_edit(callback, SELECT_DURATION_MESSAGE, keyboard)


async def _prompt_period(callback: CallbackQuery, state: FSMContext) -> None:
    await state.update_data(period=None, selected_time=None, selected_date=None)
    keyboard = build_period_keyboard().as_markup()
    await _safe_edit(callback, SELECT_PERIOD_MESSAGE, keyboard)


async def _prompt_time(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    period = data.get("period")
    if not isinstance(period, str) or period not in DAY_PERIOD_RANGES:
        keyboard = build_period_keyboard().as_markup()
        await _safe_edit(callback, SELECT_PERIOD_MESSAGE, keyboard)
        return
    # Если выбрано "Любое время", пропускаем выбор времени и переходим к выбору даты
    if period == "any":
        await state.update_data(selected_time=None, selected_date=None)
        keyboard = build_date_keyboard().as_markup()
        period_label = humanize_period(period)
        await _safe_edit(callback, f"🌗 Период: <b>{period_label}</b>\n{SELECT_DATE_MESSAGE}", keyboard)
        return
    await state.update_data(selected_time=None, selected_date=None)
    keyboard = build_time_keyboard(period).as_markup()
    period_label = humanize_period(period)
    await _safe_edit(callback, f"🌗 Период: <b>{period_label}</b>\n{SELECT_TIME_MESSAGE}", keyboard)


async def _prompt_date(callback: CallbackQuery, state: FSMContext) -> None:
    keyboard = build_date_keyboard().as_markup()
    data = await state.get_data()
    selected_time = data.get("selected_time")
    selected_date = data.get("selected_date")
    await state.update_data(selected_date=None)
    parts: List[str] = []
    if isinstance(selected_time, str):
        parts.append(f"⏰ Время: <b>{selected_time}</b>")
    if isinstance(selected_date, str):
        parts.append(f"Текущая дата: <b>{humanize_date(selected_date)}</b>")
    parts.append(SELECT_DATE_MESSAGE)
    await _safe_edit(callback, "\n".join(parts), keyboard)



