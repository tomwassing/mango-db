from data import PendingElement
from node import Node
import logging

class Follower(Node):
    def __init__(self, port, node_ports):
        super().__init__(port, node_ports)
        self.ack_buffer = {}
        self.write_buffer = {}
        self.write_id = 0

    def handle_acknowledgement(self, addr, msg_id):
        data = {
            "type": "acknowledge",
            "id": msg_id,
            "from": self.port,
        }

        self.send(addr, data)

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

    def on_message(self, addr, data):
        if data["type"] == "client_write":
            logging.debug(f"Follower:{self.port}: received client_write message: {data} from client")
            self.write(data["key"], data["value"], addr)

        elif data["type"] == "write":
            # Handling incoming write message from other nodes. Ack the message and
            # add to own write buffer.
            logging.debug(f"Follower:{self.port}: received write message: {data} from node:{addr}")
            self.write_buffer[data["id"]] = (data["key"], data["value"])
            self.handle_acknowledgement(addr, data["id"])
        
        elif data["type"] == "acknowledge":
            # Receiving ack message from other nodes, finalize if all ack messages
            # have been received
            msg_id = data["id"]
            logging.debug(f"Follower:{self.port}: received acknowledge message: {data} from node:{addr}")
            self.ack_buffer[msg_id].acknowledge(addr)

            if self.ack_buffer[msg_id].is_complete(len(self.ports)):
                logging.debug(f"Follower:{self.port}: received all acknowledgements for message: {msg_id}")
                self.write_buffer[msg_id] = self.ack_buffer[msg_id]
                del self.ack_buffer[msg_id]

                receiver = self.write_buffer[msg_id].client_addr
                data = {
                    "type": "write_result",
                    "key": self.write_buffer[msg_id].key,
                    "value": self.write_buffer[msg_id].value
                }

                self.send(receiver, data)
