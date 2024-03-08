import select
import socket
from queue import Queue
import logging
from threading import Thread, Event
from data_types import SocketAddress
import time

TIMEOUT = 0.1
FORMATO_TEXTO = 'utf-8'


# Clase abstracta que representa un Thread que controla una conexión TCP
# Manejada a través de colas de emisión y recepción
class TcpInterruptServer(Thread):
    server_socket: socket.socket
    server_address: SocketAddress
    client_socket: socket.socket
    client_address: SocketAddress

    client_connected = False
    command = ''

    client_interrupt_queue: Queue

    logger: logging.Logger
    kill_thread: Event

    def __init__(self, logger: logging.Logger, server_address: SocketAddress, client_interrupt_queue: Queue,
                 kill_thread: Event):
        super().__init__(daemon=True, name="tcp_interrupt_server")

        self.client_interrupt_queue = client_interrupt_queue
        self.logger = logger
        self.server_address = server_address
        self.kill_thread = kill_thread

    def run(self):
        self.create_server()
        while True:
            self.wait_for_client()
            self.process_socket_data()
            if self.kill_thread.is_set():
                self.logger.debug("Interrupt server CLOSED!")
                return

    def create_server(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.settimeout(TIMEOUT)
        try:
            self.server_socket.bind((self.server_address.ip_address, self.server_address.port))
        except OSError as err:
            self.logger.error(
                "No pudo crearse el servidor de Interrupciones en el socket con IP: " + self.server_address.ip_address +
                ",PUERTO: " + str(self.server_address.port) + "\n OSError: " + str(err))
            raise Exception("SOCKET_CERRADO")

        self.server_socket.listen(1)
        self.logger.info(
            "Servidor de Interrupciones arrancado en IP: " + self.server_address.ip_address + ", PUERTO: " + str(
                self.server_address.port))

    def wait_for_client(self):
        try:
            self.client_socket, client_address = self.server_socket.accept()
            self.client_address = SocketAddress(client_address[0], client_address[1])
            self.client_connected = True
            self.logger.info(
                "IP: " + self.client_address.ip_address + ", PUERTO: " + str(
                    self.client_address.port) + " se ha conectado al servidor de Interrupciuones!")
        except (TimeoutError, socket.error):
            pass

    def process_socket_data(self):
        while self.client_connected:
            rx_sock_ready, tx_sock_ready, err = select.select([self.client_socket], [self.client_socket], [],
                                                              TIMEOUT)

            if tx_sock_ready and not self.client_interrupt_queue.empty():
                self.send_data_to_socket()

            if self.kill_thread.is_set():
                self.client_socket.shutdown(socket.SHUT_RDWR)
                self.client_socket.close()
                self.client_connected = False
                break

            time.sleep(0.1)

    def send_data_to_socket(self):
        server_response = self.client_interrupt_queue.get()
        raw_data = server_response.encode(encoding=FORMATO_TEXTO)
        self.client_socket.sendall(raw_data)
