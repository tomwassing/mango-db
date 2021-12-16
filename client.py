"""
client.py

Description:
    This file contains the definition of the client class.
    It contains functions for performing read and write operations
    on the distributed key-value store system.
"""

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

    # Sends 'data' to specific host and awaits the response
    def send_recv(self, data, host=None):
        # If no host is specified a random one is chosen
        if not host:
            host = random.choice(self.node_hosts)

        # Send data and await response
        self.socket.sendto(json.dumps(data).encode(), host)
        logging.info("Client: send message to node:{} : {}".format(host, data))
        data, addr = self.socket.recvfrom(1024)
        logging.info("Client: received message: {} from {}".format(data, addr))

        result = json.loads(data.decode())
        result['host'] = host

        return result

    # Send data to all known hosts
    def send_all(self, data):
        for host in self.node_hosts:
            self.socket.sendto(json.dumps(data).encode(), host)

    # Performs a write operation
    def write(self, keys, values, host=None, blocking=True):
        # This makes sure that even single inputs get put into a tuple
        keys = keys if type(keys) == list else [keys]
        values = values if type(values) == list else [values]

        ''' Checks whether there is one key for every value and it checks
            wheter there are no duplicate keys '''
        if len(keys) != len(values) or len(keys) != len(set(keys)):
            logging.DEBUG(f"{self} invallid key-value pair(s)")
            return None

        # Constructing client write message containing key-value pair(s)
        data = {
            "type": "client_write",
            "keys": keys,
            "values": values
        }

        # Sending the client write message
        if blocking:
            host = self.send_recv(data, host=host)['host']
        else:
            if not host:
                host = random.choice(self.node_hosts)
            self.socket.sendto(json.dumps(data).encode(), host)

        return host

    # Function for receiving write acknoledgement
    def write_recv(self):
        data, addr = self.socket.recvfrom(1024)
        logging.info("Client: received message: {} from {}".format(data, addr))
        return json.loads(data.decode())

    # Performs a read operation
    def read(self, key, host=None):
        if not isinstance(key, list):
            key = [key]

        # Constructing client read message
        data = {
            "type": "client_read",
            "key": key,
        }

        return self.send_recv(data, host=host)

    # Function to shut down all known hosts
    def exit(self):
        data = {
            "type": "exit"
        }

        self.send_all(data)
        self.socket.close()

    # Function for only shutting down one specific host
    def exit_single(self, host):
        data = {
            "type": "exit"
        }

        self.socket.sendto(json.dumps(data).encode(), host)