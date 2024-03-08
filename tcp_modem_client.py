import logging
import select
import socket
import time
from queue import Queue
from threading import Thread, Event

from data_types import ModemMessage, SocketAddress

TCP_BUFFFER_SIZE = 2048
TIMEOUT = 0.1
FORMATO_TEXTO = 'utf-8'
END_OF_COMMAND = '\n'


# Clase abstracta que representa un Thread que controla una conexión TCP
# Manejada a través de colas de emisión y recepción
class ModemClient(Thread):
    server_socket: socket.socket
    server_address: SocketAddress
    client_socket: socket.socket
    client_address: SocketAddress

    client_connected = False
    modem_rebooting = False
    command = ''

    modem_queue_tx: Queue
    modem_queue_rx: Queue

    logger: logging.Logger
    kill_thread: Event

    def __init__(self, logger: logging.Logger, modem_address: SocketAddress, modem_queue_tx: Queue,
                 modem_queue_rx: Queue, kill_thread: Event):
        super().__init__(daemon=True, name="modem_client")

        self.modem_queue_rx = modem_queue_rx
        self.modem_queue_tx = modem_queue_tx
        self.logger = logger
        self.server_address = modem_address
        self.kill_thread = kill_thread

    def run(self):
        while True:
            self.connect_to_modem()
            self.process_socket_data()
            if self.kill_thread.is_set():
                self.logger.debug("TCP Modem client socket CLOSED!")
                return

    def connect_to_modem(self):
        self.logger.debug(
            "tratando de conectarse al Módem, IP: " + self.server_address.ip_address + ", PUERTO: " + str(
                self.server_address.port))
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.client_socket.connect((self.server_address.ip_address, self.server_address.port))
        except TimeoutError as err:
            self.logger.error(
                "No se pudo conectar al módem con IP " + self.server_address.ip_address + ", PUERTO: " + str(
                    self.server_address.port) + ". TimeoutError: " + str(err))
            return
        except InterruptedError as err:
            self.logger.error(
                "No se pudo conectar al módem con IP " + self.server_address.ip_address + ", PUERTO: " + str(
                    self.server_address.port) + ". InterruptedError: " + str(err))
            return
        except Exception as err:
            self.logger.error(
                "No se pudo conectar al módem con IP " + self.server_address.ip_address + ", PUERTO: " + str(
                    self.server_address.port) + ". CAUSA: " + str(err))
            return


        self.client_connected = True
        self.logger.info(
            "Conectado al Módem, IP: " + self.server_address.ip_address + ", PUERTO: " + str(
                self.server_address.port))

    def process_socket_data(self):
        while self.client_connected:
            rx_sock_ready, tx_sock_ready, err = select.select([self.client_socket], [self.client_socket], [],
                                                              TIMEOUT)
            if rx_sock_ready:
                self.read_data_from_socket()

            if tx_sock_ready and not self.modem_queue_tx.empty():
                self.send_data_to_socket()

            if self.kill_thread.is_set():
                self.client_socket.shutdown(socket.SHUT_RDWR)
                self.client_socket.close()
                self.client_connected = False
                break

            time.sleep(0.1)

    def read_data_from_socket(self):
        chunk = self.client_socket.recv(TCP_BUFFFER_SIZE)
        if len(chunk) == 0:
            if self.modem_rebooting:
                self.logger.info("El modem con IP: " + self.server_address.ip_address + ", PUERTO: " + str(
                    self.server_address.port) + " se esta reiniciando!")
                time.sleep(1)
                self.modem_rebooting = False
            else:
                self.logger.debug(
                    "Se ha caido la conexion con el modem con IP: " + self.server_address.ip_address + ", PUERTO: "
                    + str(self.server_address.port))
                # raise Exception("CONEXION_TCP_ROTA")
            self.client_connected = False
        else:
            str_chunk = chunk.decode(encoding=FORMATO_TEXTO)
            self.command += str_chunk
            if self.command.find("\n") != -1:
                self.send_command_to_queue()

    def send_command_to_queue(self):
        separator_pos = self.command.find("\n")
        last_cmd = self.command[:separator_pos]
        self.command = self.command[separator_pos + 1:]
        self.logger.debug(f"RECIBIDO TCP: {last_cmd}")
        modem_message = ModemMessage(last_cmd)
        self.modem_queue_rx.put(modem_message)
        self.command = ''

    def send_data_to_socket(self):
        at_command = self.modem_queue_tx.get()
        self.logger.debug(f"ENVIADO TCP: {at_command}")
        raw_data = at_command.get().encode(encoding=FORMATO_TEXTO)
        self.client_socket.sendall(raw_data)

        if at_command.get().find("ATZ0") > -1:
            self.modem_rebooting = True
