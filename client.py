import pickle
import socket
from config import SOCKET_FILE_PATH
import sys


def get_sticker(sticker_name):
    sticker_name = sticker_name.encode()
    sticker_size = len(sticker_name)
    payload = bytearray(2 + sticker_size)
    payload[0] = 0x01   # function code
    payload[1] = sticker_size
    payload[2:] = sticker_name

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.connect(SOCKET_FILE_PATH)

    server.sendall(payload)

    buffer_size = 1024
    payload = b''

    info = server.recv(5)
    error = info[0]
    if error:
        code = int(info[1:].hex(), 16)
        print("error code: {}".format(code))
        server.close()
        exit()

    size = int(info[1:].hex(), 16)
    read = 0
    while read < size:
        chunk = server.recv(buffer_size)
        read += buffer_size
        payload += chunk

    server.close()
    return pickle.loads(payload)


def dl_sticker_set(sticker_name, paths):
    pickled = pickle.dumps(paths)
    sticker_name = sticker_name.encode()

    payload = bytearray(6 + len(pickled) + len(sticker_name))

    payload[0] = 0x02   # function code
    payload[1] = len(sticker_name)
    payload[2:len(sticker_name) + 2] = sticker_name
    payload[len(sticker_name) + 2: len(sticker_name) + 6] = len(pickled).to_bytes(4, 'big')

    payload[len(sticker_name) + 6:] = pickled

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.connect(SOCKET_FILE_PATH)
    server.sendall(payload)

    size = int(server.recv(4).hex(), 16)
    read = 0
    buffer_size = 1024
    msg = b''
    while read < size:
        chunk = server.recv(buffer_size)
        read += buffer_size
        msg += chunk
    server.close()
    return pickle.loads(msg)


def dl_sticker(sticker_name, identifier, paths):
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

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.connect(SOCKET_FILE_PATH)
    server.sendall(payload)

    result = int(server.recv(1).hex(), 16)
    server.close()
    return bool(result)


if __name__ == '__main__':
    st_name = sys.argv[1]
    sticker_set = get_sticker(st_name)
    base = sticker_set.set.id
    path = {}
    map_ = {}
    for sticker in sticker_set.documents:
        path[sticker.id] = "{}/{}.webp".format(base, sticker.id)
        map_[sticker.id] = sticker
    failures = dl_sticker_set(st_name, path)
    print(failures)
    for id_ in failures:
        print(dl_sticker(st_name, id_, path.get(id_)))
