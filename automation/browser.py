from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager
from typing import Optional

from playwright.async_api import (
    Browser,
    BrowserContext,
    ElementHandle,
    FrameLocator,
    Locator,
    Page,
    Playwright,
    async_playwright,
)


class HeadlessBrowser(AbstractAsyncContextManager["HeadlessBrowser"]):
    """Manage a Chromium headless browser instance with sane defaults."""

    def __init__(
        self,
        *,
        headless: bool = True,
        timeout: float = 30.0,
        storage_state: Optional[str | dict] = None,
    ) -> None:
        self._headless = headless
        self._timeout = timeout
        self._storage_state = storage_state
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> "HeadlessBrowser":
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=self._headless)
        self._context = await self._browser.new_context(
            ignore_https_errors=True,
            storage_state=self._storage_state,
        )
        self._page = await self._context.new_page()
        self._page.set_default_timeout(self._timeout * 1000)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        await self.close()

    @property
    def page(self) -> Page:
        if self._page is None:
            raise RuntimeError("Browser page is not initialized yet.")
        return self._page

    async def close(self) -> None:
        """Close page, context and browser gracefully."""
        async with self._lock:
            if self._page is not None:
                await self._page.close()
                self._page = None
            if self._context is not None:
                await self._context.close()
                self._context = None
            if self._browser is not None:
                await self._browser.close()
                self._browser = None
            if self._playwright is not None:
                await self._playwright.stop()
                self._playwright = None

    async def goto(self, url: str) -> str:
        """
        Navigate to a specified URL and wait until the network is idle.

        Returns the final URL the browser ends up at (after potential redirects).
        """
        page = self.page
        await page.goto(url, wait_until="commit")
        await page.wait_for_load_state("networkidle")
        return page.url

    async def wait_for_selector(self, selector: str, *, timeout: Optional[float] = None) -> None:
        """Convenience wrapper to wait for a selector to appear."""
        await self.page.wait_for_selector(selector, timeout=(timeout or self._timeout) * 1000)

    async def click(self, selector: str, *, timeout: Optional[float] = None) -> None:
        """Click on the specified selector once it is available."""
        await self.wait_for_selector(selector, timeout=timeout)
        await self.page.click(selector)

    async def fill(self, selector: str, value: str, *, timeout: Optional[float] = None) -> None:
        await self.wait_for_selector(selector, timeout=timeout)
        await self.page.fill(selector, value)

    def locator(self, selector: str) -> Locator:
        return self.page.locator(selector)

    def frame_locator(self, selector: str) -> FrameLocator:
        return self.page.frame_locator(selector)

    async def wait_for_navigation(self) -> None:
        await self.page.wait_for_load_state("networkidle")

    async def content(self) -> str:
        return await self.page.content()

    async def storage_state(self) -> dict:
        if self._context is None:
            raise RuntimeError("Browser context is not initialized yet.")
        return await self._context.storage_state()

