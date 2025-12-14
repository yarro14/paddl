from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import json
import re
from datetime import datetime
from enum import Enum, auto
from typing import Dict, Optional, Tuple

from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from .browser import HeadlessBrowser


CODE_INPUT_SELECTOR = (
    "input[placeholder*='код'], "
    "input[name*='code'], "
    "input[aria-label*='код'], "
    "input[maxlength='4'], "
    "input[maxlength='6'], "
    "[data-widget-component-name='VerificationCode'] input"
)


class BookingTaskState(Enum):
    PENDING = auto()
    IN_PROGRESS = auto()
    COMPLETED = auto()
    FAILED = auto()


@dataclass(slots=True)
class BookingTask:
    """Data required to initialise an automated booking attempt."""

    location_url: str
    description: str
    priority: int = 0
    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class BookingResult:
    state: BookingTaskState
    message: str
    payment_url: Optional[str] = None
    payload: Optional[Dict[str, str]] = None


class BookingTaskManager:
    """
    Manage a queue of booking automation tasks. A worker coroutine pulls jobs from
    the queue sequentially to avoid fighting over headless browser resources.
    """

    def __init__(
        self,
        *,
        headless: bool = True,
        default_timeout: float = 30.0,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        self._headless = headless
        self._timeout = default_timeout
        self._loop = loop
        self._queue: Optional[
            "asyncio.PriorityQueue[Tuple[int, int, BookingTask, asyncio.Future[BookingResult]]]"
        ] = None
        self._task_counter = 0
        self._worker_task: Optional[asyncio.Task[None]] = None
        self._shutdown = asyncio.Event()

    def start(self) -> None:
        if self._worker_task is not None and not self._worker_task.done():
            return
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        if self._queue is None:
            self._queue = asyncio.PriorityQueue()
        self._worker_task = self._loop.create_task(self._worker(), name="booking-task-worker")

    async def stop(self) -> None:
        self._shutdown.set()
        if self._worker_task is not None:
            await self._worker_task
        self._worker_task = None
        self._shutdown.clear()

    async def submit(self, task: BookingTask) -> BookingResult:
        """
        Enqueue a booking task and wait for the result.
        The manager must be started via start() beforehand.
        """
        if self._worker_task is None:
            self.start()
        if self._loop is None or self._queue is None:
            raise RuntimeError("BookingTaskManager is not initialised. Call start() first.")
        future: "asyncio.Future[BookingResult]" = self._loop.create_future()
        self._task_counter += 1
        await self._queue.put((task.priority, self._task_counter, task, future))
        return await future

    async def _worker(self) -> None:
        if self._queue is None:
            return
        while not self._shutdown.is_set():
            try:
                priority, counter, task, future = await asyncio.wait_for(self._queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

            if future.done():
                self._queue.task_done()
                continue

            try:
                result = await self._process_task(task)
            except Exception as exc:  # pragma: no cover - defensive catch
                result = BookingResult(
                    state=BookingTaskState.FAILED,
                    message=f"Не удалось обработать задачу «{task.description}»: {exc}",
                )
            finally:
                self._queue.task_done()

            if not future.done():
                future.set_result(result)

    async def _process_task(self, task: BookingTask) -> BookingResult:
        """
        Полноценный сценарий VivaCRM:
        - выбор услуги «Панорамик 2x2» (или «Ультрапанорамик 2x2») на нужной площадке;
        - выбор даты и слота;
        - выбор конкретного корта (если передан);
        - ввод телефона и кода подтверждения;
        - переход к оплате, выбор СБП и получение ссылки.
        """
        metadata = task.metadata
        mode = metadata.get("mode", "complete")
        phone = metadata.get("phone")
        code = metadata.get("code")
        date_str = metadata.get("date")
        interval = metadata.get("interval")
        duration_raw = metadata.get("duration")
        duration_minutes = _safe_int(duration_raw) or 60
        room_name = metadata.get("room")
        studio_name = metadata.get("studio")
        storage_state_raw = metadata.get("storage_state")
        resume_url = metadata.get("resume_url")
        storage_state = None
        if storage_state_raw:
            if isinstance(storage_state_raw, str):
                try:
                    storage_state = json.loads(storage_state_raw)
                except json.JSONDecodeError:
                    storage_state = storage_state_raw
            else:
                storage_state = storage_state_raw

        if not date_str:
            return BookingResult(
                state=BookingTaskState.FAILED,
                message="Не указана дата слота для автозаписи.",
            )
        if not interval:
            return BookingResult(
                state=BookingTaskState.FAILED,
                message="Не указан временной интервал слота для автозаписи.",
            )

        try:
            start_time = interval.split("–", 1)[0]
        except Exception:  # pragma: no cover - защитный слой
            start_time = interval

        try:
            weekday_token = _weekday_token(date_str)
        except ValueError:
            return BookingResult(
                state=BookingTaskState.FAILED,
                message=f"Некорректная дата слота: {date_str}.",
            )

        if mode == "request_code":
            if not phone:
                return BookingResult(
                    state=BookingTaskState.FAILED,
                    message="Не указан номер телефона для запроса кода.",
                )
            try:
                payload = await self._request_code(
                    task.location_url,
                    phone=phone,
                    studio_name=studio_name,
                    weekday_token=weekday_token,
                    start_time=start_time,
                    duration_minutes=duration_minutes,
                    room_name=room_name,
                )
            except BookingAutomationError as exc:
                return BookingResult(
                    state=BookingTaskState.FAILED,
                    message=str(exc),
                )
            except Exception as exc:  # pragma: no cover - защитный слой
                return BookingResult(
                    state=BookingTaskState.FAILED,
                    message=f"Не удалось запросить код подтверждения: {exc}",
                )

            return BookingResult(
                state=BookingTaskState.COMPLETED,
                message="Код подтверждения отправлен.",
                payload=payload,
            )

        if not phone or not code:
            return BookingResult(
                state=BookingTaskState.FAILED,
                message="Не указаны телефон или код подтверждения.",
            )

        try:
            payment_url = await self._run_booking_flow(
                task.location_url,
                phone=phone,
                code=code,
                studio_name=studio_name,
                weekday_token=weekday_token,
                start_time=start_time,
                duration_minutes=duration_minutes,
                room_name=room_name,
                storage_state=storage_state,
                resume_url=resume_url,
            )
        except BookingAutomationError as exc:
            return BookingResult(
                state=BookingTaskState.FAILED,
                message=str(exc),
            )
        except Exception as exc:  # pragma: no cover - защитный слой
            return BookingResult(
                state=BookingTaskState.FAILED,
                message=f"Неожиданная ошибка сценария автозаписи: {exc}",
            )

        message = "Ссылка на оплату СБП получена."
        return BookingResult(
            state=BookingTaskState.COMPLETED,
            message=message,
            payment_url=payment_url,
        )

    async def _run_booking_flow(
        self,
        location_url: str,
        *,
        phone: str,
        code: str,
        studio_name: Optional[str],
        weekday_token: str,
        start_time: str,
        duration_minutes: int,
        room_name: Optional[str],
        storage_state: Optional[str],
        resume_url: Optional[str],
    ) -> str:
        async with HeadlessBrowser(
            headless=self._headless,
            timeout=self._timeout,
            storage_state=storage_state,
        ) as browser:
            page = browser.page
            target_url = resume_url or location_url
            final_url = await browser.goto(target_url)
            await browser.wait_for_selector("body")

            try:
                if storage_state:
                    await page.wait_for_selector(
                        CODE_INPUT_SELECTOR,
                        timeout=20_000,
                    )
                else:
                    await self._ensure_widget_ready(browser)
                    await self._select_training_step(browser, studio_name, room_name)
                    await self._select_date(browser, weekday_token)
                    await self._select_slot(browser, start_time)
                    await self._select_room(browser, room_name)
                    await self._continue_to_contacts(browser)
                    await self._submit_phone(browser, phone)

                await self._submit_code(browser, code)
                await self._proceed_to_payment(browser)
                payment_url = await self._select_sbp_and_extract_url(browser)
            except BookingAutomationError:
                raise
            except Exception as exc:  # pragma: no cover - защитный слой
                raise BookingAutomationError(f"Ошибка пошагового сценария: {exc}") from exc

            if not payment_url:
                raise BookingAutomationError(
                    "Не удалось получить ссылку для оплаты СБП после завершения сценария."
                )

            return payment_url or final_url

    async def _ensure_widget_ready(self, browser: HeadlessBrowser) -> None:
        page = browser.page
        await page.wait_for_load_state("domcontentloaded")
        await page.wait_for_timeout(2_000)

        for attempt in range(3):
            try:
                await page.wait_for_selector(
                    '[data-widget-component-name="FormStep"]',
                    timeout=30_000,
                )
                break
            except PlaywrightTimeoutError:
                await self._dismiss_overlays(page)
                await page.wait_for_timeout(2_000)
                if attempt == 2:
                    raise BookingAutomationError(
                        "Не удалось загрузить виджет бронирования. Проверьте доступность страницы."
                    )

        service_selector = '[data-widget-component-name="ServicesListSubservice"]'
        attempt = 0
        while attempt < 3:
            try:
                await page.wait_for_selector(
                    service_selector,
                    timeout=20_000,
                )
                await page.wait_for_function(
                    """selector => {
                        const container = document.querySelector(selector);
                        if (!container) return false;
                                return container.querySelector('.services-list-subservice-module__subservice') !== null;
                    }""",
                    arg=service_selector,
                    timeout=10_000,
                )
                break
            except PlaywrightTimeoutError:
                attempt += 1
                if attempt >= 3:
                    raise BookingAutomationError(
                        "Не удалось загрузить список услуг «Панорамик 2x2». Сайт не успел отрисовать карточки."
                    )
                try:
                    step_button = page.get_by_role("button", name=re.compile("Выберите\\s+тренировку", re.IGNORECASE))
                    await step_button.click()
                except Exception:
                    await page.evaluate(
                        """
                        () => {
                            const buttons = Array.from(document.querySelectorAll('button'));
                            const target = buttons.find(btn => /\\b(Панорамик|Ультрапанорамик)\\b/i.test(btn.textContent));
                            if (target) target.dispatchEvent(new Event('mouseenter'));
                        }
                        """
                    )
                await page.wait_for_timeout(2_000)

    async def _select_training_step(
        self,
        browser: HeadlessBrowser,
        studio_name: Optional[str],
        room_name: Optional[str],
    ) -> None:
        page = browser.page
        step_button = page.get_by_role("button", name=re.compile("Выберите\\s+тренировку", re.IGNORECASE))
        await step_button.click()

        subservices = page.locator('[data-widget-component-name="ServicesListSubservice"]')
        await subservices.first.wait_for()

        preferred_tokens = ["Панорамик 2x2", "Ультрапанорамик 2x2"]
        if room_name and "ультра" in room_name.lower():
            preferred_tokens = ["Ультрапанорамик 2x2", "Панорамик 2x2"]

        target_locator = None
        total = await subservices.count()
        for token in preferred_tokens:
            for index in range(total):
                candidate = subservices.nth(index)
                text = await candidate.inner_text()
                if token.lower() not in text.lower():
                    continue
                if studio_name and studio_name.lower() not in text.lower():
                    continue
                target_locator = candidate
                break
            if target_locator:
                break

        if target_locator is None and total > 0:
            # fallback — первая карточка, чтобы не падать
            target_locator = subservices.first

        if target_locator is None:
            raise BookingAutomationError("Не удалось найти услугу «Панорамик 2x2» для автозаписи.")

        await target_locator.click()

    async def _dismiss_overlays(self, page) -> None:
        candidates = [
            "button:has-text('Принять')",
            "button:has-text('Хорошо')",
            "button:has-text('Понятно')",
            ".t-popup__close",
            "button:has-text('Ок')",
        ]
        for selector in candidates:
            locator = page.locator(selector)
            try:
                if await locator.count():
                    await locator.first.click()
                    await page.wait_for_timeout(500)
            except Exception:
                continue

    async def _select_date(self, browser: HeadlessBrowser, weekday_token: str) -> None:
        page = browser.page
        time_button = page.get_by_role("button", name=re.compile("Выберите\\s+время", re.IGNORECASE))
        await time_button.click()

        # Дат всего 7-14, ищем по сочетанию «пн3».
        day_buttons = page.locator('[class*="date-picker-day-styles__tabsTrigger"]')
        await day_buttons.first.wait_for()
        matched = None
        count = await day_buttons.count()
        token_lower = weekday_token.lower()
        for idx in range(count):
            candidate = day_buttons.nth(idx)
            text = (await candidate.inner_text()).strip().lower().replace(" ", "")
            if text == token_lower:
                matched = candidate
                break
        if matched is None:
            raise BookingAutomationError(f"Не удалось найти дату для токена «{weekday_token}».")
        await self._safe_click(matched)

    async def _select_slot(self, browser: HeadlessBrowser, start_time: str) -> None:
        page = browser.page
        slots = page.locator('[data-widget-component-name="TimeSlot"]')
        await slots.first.wait_for()

        desired = None
        count = await slots.count()
        start = start_time.strip()
        for idx in range(count):
            candidate = slots.nth(idx)
            text = await candidate.inner_text()
            if start in text:
                desired = candidate
                break
        if desired is None:
            raise BookingAutomationError(f"Не найден слот, начинающийся в {start_time}.")
        await self._safe_click(desired)

    async def _select_room(self, browser: HeadlessBrowser, room_name: Optional[str]) -> None:
        page = browser.page
        room_section = page.locator('[data-widget-component-name="SelectedOptionsList"]')
        await room_section.first.wait_for()

        select_buttons = room_section.locator('button', has_text="Выбрать")
        count = await select_buttons.count()
        if count == 0:
            # Бывает, что корт выбран автоматически — просто продолжаем.
            return

        await self._safe_click(select_buttons.first)

        rooms = page.locator('[data-widget-component-name="TimeSlotRoomItem"]')
        await rooms.first.wait_for()

        target = None
        total = await rooms.count()
        if room_name:
            room_norm = room_name.lower()
            for idx in range(total):
                candidate = rooms.nth(idx)
                text = await candidate.inner_text()
                if room_norm in text.lower():
                    target = candidate
                    break

        if target is None:
            target = rooms.first

        await self._safe_click(target)

    async def _continue_to_contacts(self, browser: HeadlessBrowser) -> None:
        page = browser.page
        continue_button = page.get_by_role("button", name=re.compile("Продолжить", re.IGNORECASE))
        await continue_button.wait_for()
        await self._click_when_enabled(continue_button)

    async def _submit_phone(self, browser: HeadlessBrowser, phone: str) -> None:
        page = browser.page
        phone_input = page.locator('input[type="tel"]')
        await phone_input.first.wait_for()
        await phone_input.first.click()
        await phone_input.first.fill("")
        await phone_input.first.type(phone)

        # Отмечаем чекбоксы согласия, если присутствуют
        consent_checkboxes = page.locator('input[type="checkbox"]')
        total_boxes = await consent_checkboxes.count()
        for idx in range(total_boxes):
            checkbox = consent_checkboxes.nth(idx)
            try:
                is_checked = await checkbox.is_checked()
            except Exception:
                continue
            if not is_checked:
                try:
                    await checkbox.click()
                except Exception:
                    continue

        # Если нужно выбрать канал доставки кода, предпочитаем SMS
        channel_order = ("SMS", "СМС", "WhatsApp", "Ватсап", "Ватсапп")
        for label in channel_order:
            option = page.locator(f"button:has-text('{label}')")
            if await option.count():
                try:
                    await option.first.click()
                    break
                except Exception:
                    continue

        submit_button = await _match_button(
            page,
            [
                "Получить код",
                "Получить код по SMS",
                "Получить код в WhatsApp",
                "Подтвердить",
                "Далее",
            ],
        )
        await self._click_when_enabled(submit_button)

        await page.wait_for_selector(CODE_INPUT_SELECTOR, timeout=20_000)

    async def _submit_code(self, browser: HeadlessBrowser, code: str) -> None:
        page = browser.page
        typed = False

        verification_container = page.locator("[data-widget-component-name='VerificationCode']")
        if await verification_container.count():
            code_inputs = verification_container.locator("input")
            count = await code_inputs.count()
            if count >= len(code):
                for idx, symbol in enumerate(code):
                    await code_inputs.nth(idx).fill(symbol)
                typed = True

        if not typed:
            candidates = page.locator(CODE_INPUT_SELECTOR)
            if await candidates.count() == 0:
                raise BookingAutomationError("Не удалось найти поле для ввода кода подтверждения.")
            target = candidates.first
            await target.fill("")
            await target.type(code)
            typed = True

        confirm_button = await _match_button(page, ["Подтвердить", "Продолжить", "Готово"])
        await self._click_when_enabled(confirm_button)

    async def _proceed_to_payment(self, browser: HeadlessBrowser) -> None:
        page = browser.page
        pay_button = page.get_by_role("button", name=re.compile("Оплатить", re.IGNORECASE))
        await pay_button.wait_for()
        await self._click_when_enabled(pay_button)

    async def _select_sbp_and_extract_url(self, browser: HeadlessBrowser) -> str:
        page = browser.page
        payment_url_holder: Dict[str, Optional[str]] = {"url": None}

        def _capture_response(response) -> None:
            url = response.url
            if "sbp" in url.lower() or "qr" in url.lower():
                payment_url_holder["url"] = url

        page.on("response", _capture_response)

        sbp_button = page.locator("text=СБП")
        await sbp_button.first.wait_for()
        await self._safe_click(sbp_button.first)

        payment_url: Optional[str] = None
        try:
            candidate_link = await page.wait_for_selector("a[href^='https://']", timeout=10_000)
        except Exception:
            candidate_link = None

        if candidate_link:
            candidate_url = await candidate_link.get_attribute("href")
            if candidate_url and "sbp" in candidate_url.lower():
                payment_url = candidate_url

        if not payment_url:
            payment_url = payment_url_holder["url"]

        if not payment_url:
            # В некоторых сценариях ссылка открывается в новом окне — ждём popup.
            try:
                popup = await page.wait_for_event("popup", timeout=5_000)
                payment_url = popup.url
            except Exception:
                pass

        if not payment_url or "http" not in payment_url:
            raise BookingAutomationError("Ссылка СБП не появилась после выбора способа оплаты.")

        return payment_url

    async def _request_code(
        self,
        location_url: str,
        *,
        phone: str,
        studio_name: Optional[str],
        weekday_token: str,
        start_time: str,
        duration_minutes: int,
        room_name: Optional[str],
    ) -> Dict[str, str]:
        async with HeadlessBrowser(headless=self._headless, timeout=self._timeout) as browser:
            page = browser.page
            await browser.goto(location_url)
            await browser.wait_for_selector("body")

            try:
                await self._ensure_widget_ready(browser)
                await self._select_training_step(browser, studio_name, room_name)
                await self._select_date(browser, weekday_token)
                await self._select_slot(browser, start_time)
                await self._select_room(browser, room_name)
                await self._continue_to_contacts(browser)
                await self._submit_phone(browser, phone)
            except BookingAutomationError:
                raise
            except Exception as exc:  # pragma: no cover - защитный слой
                raise BookingAutomationError(f"Не удалось запросить код подтверждения: {exc}") from exc

            state = await browser.storage_state()
            current_url = browser.page.url

        return {
            "storage_state": json.dumps(state),
            "resume_url": current_url,
        }

    async def _click_when_enabled(self, locator) -> None:
        timeout = self._timeout
        deadline = asyncio.get_event_loop().time() + timeout
        last_error: Optional[Exception] = None
        while True:
            try:
                await locator.click()
                return
            except Exception as exc:  # pragma: no cover - защитный слой
                last_error = exc
                if asyncio.get_event_loop().time() > deadline:
                    raise BookingAutomationError(f"Кнопка не активна: {exc}") from exc
                await asyncio.sleep(0.3)

    async def _safe_click(self, locator) -> None:
        await locator.scroll_into_view_if_needed()
        await locator.click()


class BookingAutomationError(RuntimeError):
    """Raised when the automated booking sequence cannot be completed."""


async def _match_button(page, candidates: Tuple[str, ...] | list[str]):
    patterns = [re.compile(name, re.IGNORECASE) for name in candidates]
    for pattern in patterns:
        locator = page.get_by_role("button", name=pattern)
        try:
            await locator.wait_for(timeout=2_000)
            return locator
        except Exception:
            continue

    fallback = page.get_by_role("button")
    await fallback.first.wait_for()
    return fallback.first


def _weekday_token(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    weekday_map = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]
    weekday = weekday_map[dt.weekday()]
    return f"{weekday}{dt.day}"


def _safe_int(value: Optional[str] | Optional[int]) -> Optional[int]:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        try:
            return int(value)
        except ValueError:
            return None
    return None

