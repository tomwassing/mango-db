import os
import socket
from client import Client
from follower import Follower
import signal
import logging
from threading import Thread
import time

from leader import Leader

'''
TODO:
    - Shutdown correctly using Ctrl+C
    - READ operation
    - Ordering of messages
'''

def main(port=25000):
    logging.basicConfig(format='%(asctime)s.%(msecs)03d - %(levelname)-8s: %(message)s',
                        level=logging.DEBUG,datefmt='%d-%m-%y %H:%M:%S')


    hostnames = os.getenv('HOSTS').split()
    hostname = socket.gethostname()

    logging.info('Starting server on {}'.format(hostname))
    logging.info("Found {} hosts: {}".format(len(hostnames), hostnames))

    host = (hostname, port)

    is_client = hostname == hostnames[0]
    is_leader = hostname == hostnames[-1]
    node_hosts = [(h, port) for h in hostnames[1:-1] if h != hostname]

    if is_client:
        client = Client(node_hosts)
        time.sleep(5)

        client.write("World!", 'Hello1?')
        client.write("World!", 'Hello2?')
        client.write("World!", 'Hello3?')
        client.write("World!", 'Hello4?')
        client.write("World!", 'Hello5?')
        client.read("World!")
        client.exit()

    elif is_leader:
        leader = Leader(host, node_hosts, (hostname, port))
        leader.run()
    else:
        follower = Follower(host, node_hosts, (hostname, port))
        follower.run()



if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    main()
