# -*- coding: utf-8 -*-
import os
import asyncio
import aiohttp

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()

async def main():
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        raise SystemExit("请先设置 TG_BOT_TOKEN 和 TG_CHAT_ID")
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data={"chat_id": TG_CHAT_ID, "text": "TG 测试成功：Hyperliquid Monitor"}) as resp:
            print("status:", resp.status)
            print(await resp.text())

if __name__ == "__main__":
    asyncio.run(main())
