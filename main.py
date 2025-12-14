from __future__ import annotations

import asyncio

from bot.main import main as _run_bot


if __name__ == "__main__":
    asyncio.run(_run_bot())

