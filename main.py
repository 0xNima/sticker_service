import asyncio
import os
import pickle
import time
from enum import Enum

import uvloop
import config

from telethon import TelegramClient
from telethon.tl import functions, types
from telethon.errors.rpcerrorlist import StickersetInvalidError, FloodWaitError, FileMigrateError


class StickerService:

    class FnSchema(Enum):
        get_sticker = 0x01
        dl_sticker_set = 0x02
        dl_sticker = 0x03
        get_sticker_code = 0x04

    def __init__(self, pool_size=1, enable_dl_pool=False):
        self.loop = asyncio.get_event_loop()
        self.max_size = pool_size
        self.pool = asyncio.Queue(maxsize=pool_size * len(config.BOT_TOKEN))
        if enable_dl_pool:
            self.dl_pool = asyncio.Queue(maxsize=pool_size * len(config.BOT_TOKEN))

    async def prepare(self):
        for i in range(self.max_size):
            for k, v in config.BOT_TOKEN.items():
                print("create connection {}-{}".format(i, k))
                await self.pool.put(
                    await TelegramClient('', api_id=config.API_ID, api_hash=config.API_HASH).start(bot_token=v)
                )
                if hasattr(self, 'dl_pool'):
                    print("create dl connection {}-{}".format(i, k))
                    await self.dl_pool.put(
                        await TelegramClient('', api_id=config.API_ID, api_hash=config.API_HASH).start(bot_token=v)
                    )

    async def get_sticker_code(self, reader, writer):
        payload_size = await reader.readexactly(1)
        payload_size = int(payload_size.hex(), 16)
        payload = await reader.readexactly(payload_size)
        sticker_name = payload.decode()

        client = await self.pool.get()
        try:
            sticker = await client(functions.messages.GetStickerSetRequest(types.InputStickerSetShortName(sticker_name)))
            code = int(sticker.set.animated)
        except StickersetInvalidError:
            code = 2
        except (FileMigrateError, FloodWaitError):
            code = 3
        except:
            code = 4

        writer.write(code.to_bytes(1, 'big'))
        writer.close()

    async def get_sticker(self, reader, writer):
        payload_size = await reader.readexactly(1)
        payload_size = int(payload_size.hex(), 16)
        payload = await reader.readexactly(payload_size)
        sticker_name = payload.decode()

        client = await self.pool.get()
        error = 1
        try:
            sticker = await client(functions.messages.GetStickerSetRequest(types.InputStickerSetShortName(sticker_name)))
            data = pickle.dumps(sticker)
            error = 0
        except StickersetInvalidError:
            data = (404).to_bytes(4, 'big')
        except (FileMigrateError, FloodWaitError):
            data = (429).to_bytes(4, 'big')

        await self.pool.put(client)

        if error:
            payload = bytearray(5)
            payload[0] = error
            payload[1:] = data
            writer.write(payload)
            await writer.drain()
            return

        payload = bytearray(5+len(data))
        size = len(data).to_bytes(4, 'big')
        payload[0] = error
        payload[1:5] = size
        payload[5:] = data
        writer.write(payload)
        await writer.drain()

    async def __dl(self, sticker, path, client):
        try:
            _ = await client.download_media(sticker, path)
            return None
        except:
            return sticker.id

    async def dl_sticker(self, reader, writer):
        payload_size = await reader.readexactly(1)
        payload_size = int(payload_size.hex(), 16)
        payload = await reader.readexactly(payload_size)
        sticker_name = payload.decode()

        payload_size = await reader.readexactly(2)
        payload_size = int(payload_size.hex(), 16)
        payload = await reader.readexactly(payload_size)
        path = payload.decode()

        payload_size = await reader.readexactly(1)
        payload_size = int(payload_size.hex(), 16)
        payload = await reader.readexactly(payload_size)
        identifier = int(payload.decode(), 16)

        client = await self.pool.get()
        sticker_set = await client(
            functions.messages.GetStickerSetRequest(types.InputStickerSetShortName(sticker_name)))
        await self.pool.put(client)

        sticker = None
        for _sticker in sticker_set.documents:
            if _sticker.id == identifier:
                sticker = _sticker
                break

        client = await self.dl_pool.get()
        result = await self.__dl(sticker, path, client)
        await self.dl_pool.put(client)

        writer.write(int(result is None).to_bytes(1, 'big'))
        await writer.drain()

    async def dl_sticker_set(self, reader, writer):
        payload_size = await reader.readexactly(1)
        payload_size = int(payload_size.hex(), 16)
        payload = await reader.readexactly(payload_size)
        sticker_name = payload.decode()

        payload_size = await reader.readexactly(4)
        payload_size = int(payload_size.hex(), 16)
        payload = await reader.readexactly(payload_size)

        client = await self.pool.get()
        sticker_set = await client(functions.messages.GetStickerSetRequest(types.InputStickerSetShortName(sticker_name)))
        await self.pool.put(client)

        client = await self.dl_pool.get()
        paths = pickle.loads(payload)

        tasks = []
        for sticker in sticker_set.documents:
            tasks.append(
                asyncio.create_task(self.__dl(sticker, paths.get('0').get(sticker.id), client))
            )
            tray = paths.get('1').get(sticker.id)
            if tray:
                tasks.append(
                    asyncio.create_task(self.__dl(sticker, tray, client))
                )

        t0 = time.time()
        result = await asyncio.gather(*tasks)
        await self.dl_pool.put(client)
        print("download pack {} in {} sec".format(sticker_name, time.time() - t0))

        failures = list(filter(lambda z: z is not None, result))
        pickled = pickle.dumps(failures)
        payload = bytearray(4+len(pickled))
        payload[:4] = len(pickled).to_bytes(4, 'big')
        payload[4:] = pickled

        writer.write(payload)
        await writer.drain()

    async def handler(self, reader, writer):
        print("received stream from: {}".format(writer.transport))
        fn_schema = await reader.read(1)
        fn_schema = int(fn_schema.hex(), 16)
        fn = StickerService.FnSchema(fn_schema).name
        await getattr(self, fn)(reader, writer)

        writer.close()

    def run(self):
        if os.path.exists(config.SOCKET_FILE_PATH):
            os.remove(config.SOCKET_FILE_PATH)

        uvloop.install()

        self.loop.run_until_complete(self.prepare())

        server = asyncio.start_unix_server(self.handler, path=config.SOCKET_FILE_PATH)

        print("starting server...")

        self.loop.run_until_complete(server)
        self.loop.run_forever()


if __name__ == '__main__':
    service = StickerService()
    service.run()
