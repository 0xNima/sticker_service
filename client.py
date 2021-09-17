import socket
from config import SOCKET_FILE_PATH
import sys

if __name__ == '__main__':
    sticker_name = sys.argv[1]
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.connect(SOCKET_FILE_PATH)
    sticker_name = sticker_name.encode()
    sticker_size = len(sticker_name)
    payload = bytearray(1+sticker_size)
    payload[0] = sticker_size

    for i, char in enumerate(sticker_name):
        payload[i+1] = char

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
        chunk = server.recv(1024)
        read += 1024
        payload += chunk

    server.close()
    print(payload.decode())
