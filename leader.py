
import logging
from follower import Follower


class Leader(Follower):

    def __init__(self, port, node_ports):
        super().__init__(port, node_ports)
        logging.info(f"Leader: started on port {port}")

    def handle_acknowledgement(self, addr, data):
        super().handle_acknowledgement(addr, data)

        self.store_data(data["key"], data["value"])


    def write(self, key, value, addr):
        super().write(key, value, addr)
        self.store_data(key, value)

    def store_data(self, key, value):
        self.data[key] = value
        logging.info(f"Leader:{self.port}: saved '{key} = {value}'")
