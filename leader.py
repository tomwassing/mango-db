
import logging
from follower import Follower


class Leader(Follower):

    def __init__(self, port, node_ports):
        super().__init__(port, node_ports)
        self.index = 0

    def handle_acknowledgement(self, addr, data):
        super().handle_acknowledgement(addr, data)
        self.store_data(data["id"], data["key"], data["value"])

    def write(self, key, value, addr):
        msg_id = super().write(key, value, addr)
        self.store_data(msg_id, key, value)

        return msg_id

    def store_data(self, msg_id, key, value):
        self.data[key] = value

        logging.info(f"{self}: saved '{key} = {value}'")

        data = {
            "type": "write_order",
            "id": msg_id,
            "index": self.index
        }

        self.index += 1

        self.send_to_all(data)

    def __str__(self) -> str:
        return f"Leader:{self.port}"

