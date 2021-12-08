import logging
from follower import Follower


class Leader(Follower):
    def __init__(self, port, node_ports, leader_port):
        super().__init__(port, node_ports, leader_port)

    def send_client_write_ack(self, msg_id):
        key, value = self.write_buffer[msg_id]
        self.store_data(msg_id, key, value)

    def handle_client_write_ack(self, addr, data):
        key, value = self.write_buffer[data["id"]]
        self.store_data(data["id"], key, value)

    def on_message(self, addr, data):
        super().on_message(addr, data)
        if data["type"] == "client_write_ack":
            self.handle_client_write_ack(addr, data)

    def store_data(self, msg_id, key, value):
        self.data[key] = (value, self.order_index)
        del self.write_buffer[msg_id]

        logging.info(f"{self}: saved '{key} = {value}'")

        data = {
            "type": "write_order",
            "id": msg_id,
            "index": self.order_index
        }

        self.send_to_all(data)
        self.order_index += 1

    def __str__(self) -> str:
        return f"Leader:{self.port}"
