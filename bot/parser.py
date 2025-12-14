from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import aiohttp
from aiohttp import ClientResponseError, ClientSession, ClientTimeout
from bs4 import BeautifulSoup

TENANT_KEY = "iSkq6G"
API_BASE_URL = "https://api.vivacrm.ru/end-user/api/v1"
LOCATIONS_URL = "https://padlhub.ru/locations"
SUPABASE_BASE_URL = "https://supadb.vivacrm.ru/"
EXTRA_WIDGET_PAGES: Tuple[str, ...] = ("https://firstpadel.ru/",)
FIRST_PADEL_TENANT = "4yMzOR"

SLOT_STEP_MINUTES = 30
SLOT_STEP = timedelta(minutes=SLOT_STEP_MINUTES)
MIN_DURATION_MINUTES = 60
MAX_DURATION_MINUTES = 120

_MASTER_SERVICE_CACHE: Optional[Set[Tuple[str, str]]] = None
_MASTER_SERVICE_LOCK = asyncio.Lock()


class ParserError(RuntimeError):
    """Общий класс ошибок при работе с API padlhub."""


@dataclass(frozen=True)
class RoomDescriptor:
    tenant_key: str
    master_service_id: str
    studio_id: str
    studio_name: str
    room_id: str
    room_name: str
    subservice_id: str
    subservice_name: str


class PadlHubClient:
    """Клиент для взаимодействия с публичным API padlhub."""

    def __init__(self, session: ClientSession) -> None:
        self._session = session
        self._tenant_default = TENANT_KEY

    async def fetch_panoramic_rooms(self) -> List[RoomDescriptor]:
        master_services = await self._collect_master_services()
        url = (
            f"{API_BASE_URL}/{{tenant}}/products/master-services/{{service}}/subServices"
        )
        payload_collections: List[Tuple[str, str, List[Dict[str, Any]]]] = []
        try:
            for tenant_key, master_service in master_services:
                request_url = url.format(tenant=tenant_key, service=master_service)
                async with self._session.get(request_url) as response:
                    response.raise_for_status()
                    payload = await response.json()
                if isinstance(payload, list):
                    payload_collections.append((tenant_key, master_service, payload))
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise ParserError("Не удалось загрузить список услуг.") from exc

        descriptors: List[RoomDescriptor] = []
        for tenant_key, master_service, payload in payload_collections:
            for group in payload:
                subservices = (
                    group.get("subServices") if isinstance(group, dict) else None
                )
                if not isinstance(subservices, list):
                    continue

                for subservice in subservices:
                    name = _safe_str(subservice.get("name"))
                    lowered_name = name.lower()
                    normalized_name = lowered_name.replace("х", "x")
                    is_ultra = "ультрапанорамик 2x2" in normalized_name
                    if is_ultra and tenant_key != FIRST_PADEL_TENANT:
                        continue
                    if not any(
                        keyword in normalized_name
                        for keyword in (
                            "панорамик 2x2",
                            "ультрапанорамик 2x2",
                        )
                    ):
                        continue
                    sub_id = _safe_str(subservice.get("id"))
                    rooms_data = subservice.get("availableStudioRooms")
                    if not sub_id or not isinstance(rooms_data, list):
                        continue
                    for entry in rooms_data:
                        studio = entry.get("studio") if isinstance(entry, dict) else None
                        rooms = entry.get("rooms") if isinstance(entry, dict) else None
                        if not isinstance(studio, dict) or not isinstance(rooms, list):
                            continue

                        studio_id = _safe_str(studio.get("id"))
                        studio_name = _safe_str(studio.get("name"))
                        if not studio_id or not studio_name:
                            continue

                        for room in rooms:
                            if not isinstance(room, dict):
                                continue
                            room_id = _safe_str(room.get("id"))
                            room_name = _safe_str(room.get("name"))
                            if not room_id or not room_name:
                                continue
                            descriptors.append(
                                RoomDescriptor(
                                    tenant_key=tenant_key,
                                    master_service_id=master_service,
                                    studio_id=studio_id,
                                    studio_name=studio_name,
                                    room_id=room_id,
                                    room_name=room_name,
                                    subservice_id=sub_id,
                                    subservice_name=name,
                                )
                            )

        if not descriptors:
            raise ParserError("Не найдены площадки «Панорамик 2x2».")
        return descriptors

    async def fetch_room_slots(
        self,
        *,
        room: RoomDescriptor,
        date_str: str,
    ) -> List[datetime]:
        url = f"{API_BASE_URL}/{room.tenant_key}/products/master-services/{room.master_service_id}/timeslots"
        payload = {
            "studioId": room.studio_id,
            "roomId": room.room_id,
            "date": date_str,
            "subServiceIds": [room.subservice_id],
            "trainers": {"type": "NO_TRAINER"},
        }
        try:
            async with self._session.post(
                url,
                json=payload,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
            ) as response:
                response.raise_for_status()
                data = await response.json()
        except ClientResponseError as exc:
            if exc.status == 404:
                return []
            raise ParserError("Ошибка при запросе слотов площадки.") from exc
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise ParserError("Не удалось получить список слотов площадки.") from exc

        slots = _extract_times(data)
        return sorted(_parse_iso_datetime(ts) for ts in slots)

    async def _collect_master_services(self) -> Set[Tuple[str, str]]:
        global _MASTER_SERVICE_CACHE
        async with _MASTER_SERVICE_LOCK:
            if _MASTER_SERVICE_CACHE is not None:
                return _MASTER_SERVICE_CACHE

            html = await self._fetch_text(LOCATIONS_URL)
            soup = BeautifulSoup(html, "html.parser")
            links: Set[str] = set()
            for anchor in soup.find_all("a", href=True):
                href = anchor["href"]
                if not isinstance(href, str):
                    continue
                if href.startswith("/padel_") or href.startswith("/padl_"):
                    links.add(urljoin(LOCATIONS_URL, href))
                elif href.startswith("https://padlhub.ru/padel_") or href.startswith(
                    "https://padlhub.ru/padl_"
                ):
                    links.add(href)

            links.update(EXTRA_WIDGET_PAGES)

            if not links:
                raise ParserError("Не удалось определить список локаций padlhub.ru.")

            master_pattern = re.compile(r'"masterServiceId"\s*:\s*"([^"]+)"')
            tenant_pattern = re.compile(r'"tenantKey"\s*:\s*"([^"]+)"')
            script_pattern = re.compile(
                r"storage/v1/object/public/widgets/[a-f0-9\-]+\.js", re.IGNORECASE
            )

            master_services: Set[Tuple[str, str]] = set()

            for link in sorted(links):
                page_text = await self._fetch_text(link)
                script_match = script_pattern.search(page_text)
                if script_match:
                    script_url = urljoin(SUPABASE_BASE_URL, script_match.group(0))
                    script_text = await self._fetch_text(script_url)

                    master_match = master_pattern.search(script_text)
                    tenant_match = tenant_pattern.search(script_text)
                    if master_match:
                        tenant_key = (
                            tenant_match.group(1)
                            if tenant_match
                            else self._tenant_default
                        )
                        master_services.add((tenant_key, master_match.group(1)))
                    continue

                inline_match = re.search(
                    r"_smBookingWidget\('init'\s*,\s*(\{.*?\})\);",
                    page_text,
                    re.DOTALL,
                )
                if inline_match:
                    raw_json = inline_match.group(1)
                    try:
                        config = json.loads(raw_json)
                    except json.JSONDecodeError:
                        continue
                    master_service = _safe_str(config.get("masterServiceId"))
                    if not master_service:
                        continue
                    tenant_key = (
                        _safe_str(config.get("tenantKey")) or self._tenant_default
                    )
                    master_services.add((tenant_key, master_service))

            if not master_services:
                raise ParserError("Не найдены masterServiceId для площадок.")

            _MASTER_SERVICE_CACHE = master_services
            return master_services

    async def _fetch_text(self, url: str) -> str:
        async with self._session.get(url) as response:
            response.raise_for_status()
            return await response.text()


async def fetch_panoramic_slots(
    date_str: str,
    duration_minutes: int,
) -> Dict[str, List[str]]:
    """Возвращает доступные промежутки Панорамик 2x2 по всем локациям на дату."""
    _validate_date_format(date_str)
    if (
        duration_minutes % SLOT_STEP_MINUTES != 0
        or duration_minutes < MIN_DURATION_MINUTES
        or duration_minutes > MAX_DURATION_MINUTES
    ):
        raise ParserError("Некорректная длительность бронирования.")

    timeout = ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        client = PadlHubClient(session)
        descriptors = await client.fetch_panoramic_rooms()

        slots_by_location: Dict[str, Dict[str, Dict[str, Set[str]]]] = {}
        for descriptor in descriptors:
            times = await client.fetch_room_slots(room=descriptor, date_str=date_str)
            if not times:
                continue
            slot_step_minutes = _detect_slot_step(times)
            if slot_step_minutes <= 0 or duration_minutes % slot_step_minutes != 0:
                continue
            sequences = _collect_sequences(times, duration_minutes, slot_step_minutes)
            if not sequences:
                continue
            store = slots_by_location.setdefault(descriptor.studio_name, {})
            for sequence in sequences:
                by_type = store.setdefault(sequence, {})
                rooms = by_type.setdefault(descriptor.subservice_name, set())
                rooms.add(descriptor.room_name)

    if not slots_by_location:
        raise ParserError(
            "На выбранную дату нет подряд идущих свободных слотов выбранной длительности."
        )

    result: Dict[str, List[str]] = {}
    for studio, slots in slots_by_location.items():
        entries: List[str] = []
        for interval, by_type in sorted(slots.items(), key=lambda item: item[0]):
            entries.append(_format_interval(interval, by_type))
        result[studio] = entries

    return {studio: result[studio] for studio in sorted(result.keys())}


def _extract_times(payload: Dict[str, Any]) -> List[str]:
    by_trainer = payload.get("byTrainer")
    if not isinstance(by_trainer, dict):
        return []

    trainer_block = by_trainer.get("NO_TRAINER")
    if trainer_block is None and by_trainer:
        trainer_block = next(iter(by_trainer.values()))
    if not isinstance(trainer_block, dict):
        return []

    slots = trainer_block.get("slots")
    if not isinstance(slots, list):
        return []

    result: List[str] = []
    for segment in slots:
        if isinstance(segment, list):
            for slot in segment:
                if isinstance(slot, dict):
                    time_from = slot.get("timeFrom")
                    if isinstance(time_from, str):
                        result.append(time_from)
    return result


def _collect_sequences(
    times: List[datetime],
    duration_minutes: int,
    slot_step_minutes: int,
) -> List[str]:
    if not times or slot_step_minutes <= 0 or duration_minutes <= 0:
        return []
    if duration_minutes % slot_step_minutes != 0:
        return []

    duration_delta = timedelta(minutes=duration_minutes)
    step_delta = timedelta(minutes=slot_step_minutes)
    required_slots = max(1, duration_minutes // slot_step_minutes)

    unique_sequences: Set[str] = set()

    if required_slots == 1:
        for start in times:
            end = start + duration_delta
            unique_sequences.add(f"{start.strftime('%H:%M')}–{end.strftime('%H:%M')}")
        return sorted(unique_sequences)

    count = len(times)
    if count < required_slots:
        return []

    for idx in range(count - required_slots + 1):
        window = times[idx : idx + required_slots]
        if _is_consecutive(window, step_delta):
            start = window[0]
            end = start + duration_delta
            unique_sequences.add(f"{start.strftime('%H:%M')}–{end.strftime('%H:%M')}")

    return sorted(unique_sequences)


def _is_consecutive(window: List[datetime], expected_step: timedelta) -> bool:
    return all(
        window[i] - window[i - 1] == expected_step for i in range(1, len(window))
    )


def _detect_slot_step(times: List[datetime]) -> int:
    min_delta: Optional[int] = None
    for earlier, later in zip(times, times[1:]):
        delta = later - earlier
        delta_minutes = int(delta.total_seconds() // 60)
        if delta_minutes <= 0:
            continue
        if min_delta is None or delta_minutes < min_delta:
            min_delta = delta_minutes
    return min_delta or SLOT_STEP_MINUTES


def _format_interval(
    interval: str,
    by_type: Dict[str, Set[str]],
) -> str:
    if not by_type:
        return interval

    if len(by_type) == 1:
        subtype, rooms = next(iter(by_type.items()))
        subtype = _normalize_subservice_label(subtype)
        count = len(rooms)
        subtype_lower = subtype.lower()
        if "ультра" not in subtype_lower and subtype_lower.startswith("панорамик"):
            if count <= 1:
                return interval
            suffix = _pluralize_court(count)
            return f"{interval} ({count} {suffix})"

        if count == 1:
            return f"{interval} ({subtype})"
        suffix = _pluralize_court(count)
        return f"{interval} ({subtype} — {count} {suffix})"

    parts: List[str] = []
    for subtype, rooms in sorted(by_type.items(), key=lambda item: item[0]):
        subtype = _normalize_subservice_label(subtype)
        count = len(rooms)
        suffix = _pluralize_court(count)
        if count == 1:
            parts.append(subtype)
        else:
            parts.append(f"{subtype} — {count} {suffix}")
    summary = "; ".join(parts)
    return f"{interval} ({summary})"


def _pluralize_court(count: int) -> str:
    if count % 10 == 1 and count % 100 != 11:
        return "корт"
    if count % 10 in (2, 3, 4) and count % 100 not in (12, 13, 14):
        return "корта"
    return "кортов"


def _normalize_subservice_label(name: str) -> str:
    cleaned = name.strip()
    if cleaned.endswith("."):
        cleaned = cleaned[:-1].strip()
    return cleaned or name


def _parse_iso_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise ParserError("Получено некорректное время слота.") from exc


def _validate_date_format(value: str) -> None:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ParserError("Дата должна быть в формате YYYY-MM-DD.") from exc


def _safe_str(value: Any) -> str:
    return str(value).strip() if isinstance(value, str) else ""


