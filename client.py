import logging
import random
import socket
import json


class Client:
    def __init__(self, node_hosts):
        self.node_hosts = node_hosts

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(("", 0))
        self.socket.settimeout(5)

        logging.info("Client: constructed with hosts: {}".format(node_hosts))

    # Sync operation
    def send_recv(self, data, host=None):
        if not host:
            host = random.choice(self.node_hosts)

        self.socket.sendto(json.dumps(data).encode(), host)
        logging.info("Client: send message to node:{} : {}".format(host, data))
        data, addr = self.socket.recvfrom(1024)
        logging.info("Client: received message: {} from {}".format(data, addr))

        result = json.loads(data.decode())
        result['host'] = host

        return result

    def send_all(self, data):
        for host in self.node_hosts:
            self.socket.sendto(json.dumps(data).encode(), host)

    def write(self, key, value, host=None, blocking=True):
        data = {
            "type": "client_write",
            "key": key,
            "value": value
        }

        if blocking:
            host = self.send_recv(data, host=host)['host']
        else:
            if not host:
                host = random.choice(self.node_hosts)
            self.socket.sendto(json.dumps(data).encode(), host)

        return host

    def write_recv(self):
        data, addr = self.socket.recvfrom(1024)
        logging.info("Client: received message: {} from {}".format(data, addr))
        return json.loads(data.decode())

    def read(self, key, host=None):
        if not isinstance(key, list):
            key = [key]

        data = {
            "type": "client_read",
            "key": key,
        }

        return self.send_recv(data, host=host)

    def exit(self):
        data = {
            "type": "exit"
        }

        self.send_all(data)
        self.socket.close()

    def exit_single(self, host):
        data = {
            "type": "exit"
        }

        self.socket.sendto(json.dumps(data).encode(), host)
