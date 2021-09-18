import asyncio
import pickle
import socket
import time

from config import SOCKET_FILE_PATH
import sys


async def get_sticker(sticker_name):
    sticker_name = sticker_name.encode()
    sticker_size = len(sticker_name)
    payload = bytearray(2 + sticker_size)
    payload[0] = 0x01   # function code
    payload[1] = sticker_size
    payload[2:] = sticker_name

    reader, writer = await asyncio.open_unix_connection(SOCKET_FILE_PATH)

    writer.write(payload)
    await writer.drain()

    buffer_size = 1024
    payload = b''

    info = await reader.readexactly(5)
    error = info[0]
    if error:
        code = int(info[1:].hex(), 16)
        print("error code: {}".format(code))
        writer.close()
        return code

    size = int(info[1:].hex(), 16)
    read = 0
    while read < size:
        chunk = await reader.read(buffer_size)
        read += len(chunk)
        payload += chunk

    writer.close()
    return pickle.loads(payload)


async def dl_sticker_set(sticker_name, paths):
    pickled = pickle.dumps(paths)
    sticker_name = sticker_name.encode()

    payload = bytearray(6 + len(pickled) + len(sticker_name))

    payload[0] = 0x02   # function code
    payload[1] = len(sticker_name)
    payload[2:len(sticker_name) + 2] = sticker_name
    payload[len(sticker_name) + 2: len(sticker_name) + 6] = len(pickled).to_bytes(4, 'big')
    payload[len(sticker_name) + 6:] = pickled

    reader, writer = await asyncio.open_unix_connection(SOCKET_FILE_PATH)

    writer.write(payload)
    await writer.drain()

    size = int((await reader.readexactly(4)).hex(), 16)
    read = 0
    buffer_size = 1024
    msg = b''
    while read < size:
        chunk = await reader.read(buffer_size)
        read += len(chunk)
        msg += chunk
    writer.close()
    return pickle.loads(msg)


async def dl_sticker(sticker_name, identifier, paths):
    sticker_name = sticker_name.encode()
    lsn = len(sticker_name)

    pickled = pickle.dumps(paths)
    lp = len(pickled)

    identifier = hex(identifier).encode()
    li = len(identifier)

    payload = bytearray(6 + lsn + lp + li)

    payload[0] = 0x03   # function code
    payload[1] = lsn
    payload[2:lsn + 2] = sticker_name
    payload[lsn + 2: lsn + 6] = lp.to_bytes(4, 'big')
    payload[lsn + 6: lsn + lp + 6] = pickled
    payload[lsn + lp + 6] = li
    payload[lsn + lp + 7:] = identifier

    reader, writer = await asyncio.open_unix_connection(SOCKET_FILE_PATH)

    writer.write(payload)
    await writer.drain()

    result = int((await reader.readexactly(1)).hex(), 16)
    writer.close()
    return bool(result)


async def do(st_name):
    t0 = time.time()
    sticker_set = await get_sticker(st_name)
    print("get sticker set {} in {} sec".format(st_name, time.time() - t0))
    if isinstance(sticker_set, int):
        print(sticker_set)
        return

    base = sticker_set.set.id
    path = {}
    map_ = {}
    for sticker in sticker_set.documents:
        path[sticker.id] = "{}/{}.webp".format(base, sticker.id)
        map_[sticker.id] = sticker

    t0 = time.time()
    failures = await dl_sticker_set(st_name, path)
    print("download sticker {} in {} sec - failures: {}".format(st_name, time.time() - t0, failures))
    # for id_ in failures:
    #     print(await dl_sticker(st_name, id_, path.get(id_)))


async def main():
    file = open('/home/nima/Desktop/links.txt', 'r')
    tasks = []
    for i, sticker_name in enumerate(file.readlines()):
        tasks.append(asyncio.create_task(do(sticker_name.strip())))
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
