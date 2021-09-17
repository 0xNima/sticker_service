import asyncio
import os
import uvloop
import config

from telethon import TelegramClient
from telethon.tl import functions, types
from telethon.errors.rpcerrorlist import StickersetInvalidError, FloodWaitError, FileMigrateError


class StickerService:

    def __init__(self, pool_size=3):
        self.loop = asyncio.get_event_loop()
        self.max_size = pool_size
        self.pool = asyncio.Queue(maxsize=pool_size * len(config.BOT_TOKEN))

    async def prepare(self):
        for i in range(self.max_size):
            for k, v in config.BOT_TOKEN.items():
                print("create connection {}-{}".format(i, k))
                await self.pool.put(
                    await TelegramClient('', api_id=config.API_ID, api_hash=config.API_HASH).start(bot_token=v)
                )

    async def process(self, sticker_name, writer):
        client = await self.pool.get()
        error = 0
        data = None
        try:
            sticker = await client(functions.messages.GetStickerSetRequest(types.InputStickerSetShortName(sticker_name)))
        except StickersetInvalidError:
            data = (404).to_bytes(4, 'big')
            error = 1
        except (FileMigrateError, FloodWaitError):
            data = (429).to_bytes(4, 'big')
            error = 1

        await self.pool.put(client)

        info = bytearray(5)
        info[0] = error

        if error:
            info[1:] = data
            writer.write(info)
            await writer.drain()
            return

        data = sticker.to_json().encode()
        size = len(data).to_bytes(4, 'big')
        info[1:] = size

        writer.write(info)
        await writer.drain()

        writer.write(data)
        await writer.drain()

    async def handler(self, reader, writer):
        print("received stream")
        payload_size = await reader.read(1)
        payload_size = int(payload_size.hex(), 16)
        payload = await reader.readexactly(payload_size)
        await self.process(payload.decode(), writer)

    def run(self):
        if os.path.exists(config.SOCKET_FILE_PATH):
            os.remove(config.SOCKET_FILE_PATH)

        uvloop.install()

        self.loop.run_until_complete(self.prepare())

        server = asyncio.start_unix_server(self.handler, path=config.SOCKET_FILE_PATH)

        self.loop.run_until_complete(server)
        self.loop.run_forever()


if __name__ == '__main__':
    service = StickerService(3)
    service.run()
