import logging
import random
import socket
import json

class Client:
    def __init__(self, node_ports):
        self.node_ports = node_ports

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(("", 0))
        self.socket.settimeout(.5)

    # Sync operation
    def send_recv(self, data, port=None):
        if not port:
            port = random.choice(self.node_ports)
        self.socket.sendto(json.dumps(data).encode(), ("127.0.0.1", port))
        logging.info(f"Client: send message to node:{port} : {data}")
        data, addr = self.socket.recvfrom(1024)
        logging.info(f"Client: received message: {data} from {addr}")
        return json.loads(data.decode())

    def send_all(self, data):
        for port in self.node_ports:
            self.socket.sendto(json.dumps(data).encode(), ("127.0.0.1", port))

    def write(self, key, value, port=None, blocking=True):
        data = {
            "type": "client_write",
            "key": key,
            "value": value
        }

        if blocking:
            self.send_recv(data, port=port)
        else:
            self.socket.sendto(json.dumps(data).encode(), ("127.0.0.1", port))

    def write_recv(self, key, value, port=None):
        try:
            data, addr = self.socket.recvfrom(1024)
            logging.info(f"Client: received message: {data} from {addr}")
            return json.loads(data.decode())
        except socket.timeout:
            # Follower has been disconnected. Resend request to different node.
            logging.info(f"Client: received timeout from {port}")
            self.write(key, value, port=random.choice([x for x in self.node_ports if x != port]), blocking=False)

    def read(self, key, port=None):
        data = {
            "type": "client_read",
            "key": key,
        }

        return self.send_recv(data, port=port)

    def exit(self):
        data = {
            "type": "exit"
        }

        self.send_all(data)
        self.socket.close()

    def exit_single(self, port):
        data = {
            "type": "exit"
        }

        self.socket.sendto(json.dumps(data).encode(), ("127.0.0.1", port))