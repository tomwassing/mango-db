from collections import defaultdict
from data import PendingElement
from node import Node
import logging
import sys
class Follower(Node):
    def __init__(self, port, node_ports, leader_port):
        super().__init__(port, node_ports, leader_port)
        self.ack_buffer = {}
        self.write_buffer = {}
        self.read_buffer = defaultdict(list)
        self.write_id = 0
        self.data = {}
        self.order_index = 0
        self.order_buffer = []
        self.leader_port = leader_port

    def write(self, key, value, addr):
        '''Add key-value pair to acknowledge buffer and send write message to
        all the other nodes.'''
        msg_id = f"{self.port}:{self.write_id}"
        self.ack_buffer[msg_id] = PendingElement(key, value, msg_id, addr)
        self.write_id += 1

        data = {
            "type": "write",
            "id": msg_id,
            "key": key,
            "value": value,
            "from": self.port,
        }

        self.send_to_all(data)
        return msg_id

    def is_key_pending(self, key):
        for value in self.ack_buffer.values():
            if value.key == key:
                print("ack_buffer", self.ack_buffer)
                return True

        for value in self.write_buffer.values():
            if value[0] == key:
                return True

        return False

    def handle_write_order(self, addr, data):
        self.order_buffer.append(data)

        for write_order in list(sorted(self.order_buffer, key=lambda x: x["index"])):
            if write_order["index"] == self.order_index:
                key, value = self.write_buffer[write_order["id"]]
                del self.write_buffer[write_order["id"]]
                self.order_buffer.remove(write_order)

                logging.debug(f"{self}: saved {key} = {value} of message: {write_order['id']}")
                self.data[key] = (value, self.order_index)
                self.order_index += 1
            else:
                break

        for key, clients in list(self.read_buffer.items()):
            if self.is_key_pending(key):
                continue

            for client in clients:
                data = {
                    "type": "read_result",
                    "key": key,
                    "value": self.data[key][0],
                    "order_index": self.data[key][1]
                }

                self.send(client, data)
            del self.read_buffer[key]

    def handle_client_read(self, addr, data):
        key = data["key"]
        if self.is_key_pending(key):
            self.read_buffer[key].append(addr)
        else:
            data = {
                "type": "read_result",
                "key": key,
                "value": self.data[key][0],
                "order_index": self.data[key][1]
            }

            self.send(addr, data)

    def handle_client_write(self, addr, data):
        self.write(data["key"], data["value"], addr)

    def handle_write(self, addr, data):
        # Handling incoming write message from other nodes. Ack the message and
        # add to own write buffer.
        self.write_buffer[data["id"]] = (data["key"], data["value"])

        data = {
            "type": "acknowledge",
            "id": data["id"],
            "from": self.port,
        }

        self.send(addr, data)

    def send_client_write_ack(self, msg_id):
        data = {
            "type": "client_write_ack",
            "id": msg_id,
        }

        self.send(("127.0.0.1", self.leader_port), data)

    def handle_acknowledge(self, addr, data):
        # Receiving ack message from other nodes, finalize if all ack messages
        # have been received
        msg_id = data["id"]
        self.ack_buffer[msg_id].acknowledge(addr)

        if self.ack_buffer[msg_id].is_complete(len(self.ports)):
            logging.debug(f"{self}: received all acknowledgements for message: {msg_id}")
            pending_element = self.ack_buffer[msg_id]
            self.write_buffer[msg_id] = (pending_element.key, pending_element.value)
            del self.ack_buffer[msg_id]

            receiver = pending_element.client_addr
            data = {
                "type": "write_result",
                "key": pending_element.key,
                "value": pending_element.value
            }

            self.send(receiver, data)
            self.send_client_write_ack(msg_id)

    def on_message(self, addr, data):
        if data["type"] == "exit":
            logging.debug(f"{self}: received exit message from {addr}")
            self.is_connected = False
            self.socket.close()
            # sys.exit()
        elif data["type"] == "write_order":
            self.handle_write_order(addr, data)
        elif data["type"] == "client_read":
            self.handle_client_read(addr, data)
        elif data["type"] == "client_write":
            self.handle_client_write(addr, data)
        elif data["type"] == "write":
            self.handle_write(addr, data)
        elif data["type"] == "acknowledge":
            self.handle_acknowledge(addr, data)

    def __str__(self) -> str:
        return f"Follower:{self.port}"
