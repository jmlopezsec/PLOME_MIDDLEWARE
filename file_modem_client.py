import select
import socket
from queue import Queue
import logging
from threading import Event
from data_types import SocketAddress
from tcp_modem_client import ModemClient
import time

TCP_BUFFFER_SIZE = 2048
TIMEOUT = 0.1
FORMATO_TEXTO = 'utf-8'
END_OF_COMMAND = '\n'


class FileModemClient(ModemClient):
    modem_online: Event

    def __init__(self, logger: logging.Logger, modem_address: SocketAddress, modem_queue_tx: Queue,
                 modem_queue_rx: Queue, modem_online: Event, kill_thread: Event):
        super().__init__(logger=logger, modem_address=modem_address, modem_queue_tx=modem_queue_tx,
                         modem_queue_rx=modem_queue_rx, kill_thread=kill_thread)
        self.modem_online = modem_online

    def run(self):
        while True:
            self.modem_online.wait(timeout=TIMEOUT)
            if self.modem_online.is_set():
                self.connect_to_modem()
                self.process_socket_data()
            if self.kill_thread.is_set():
                self.logger.debug("TCP Modem file client socket CLOSED!")
                return

    def connect_to_modem(self):
        self.logger.debug(
            "tratando de conectarse al canal de datos del Módem, IP: " + self.server_address.ip_address + ", PUERTO: " +
            str(self.server_address.port))
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.client_socket.connect((self.server_address.ip_address, self.server_address.port))
        except TimeoutError as err:
            self.logger.error(
                "No se pudo conectar al canal de datos del módem con IP " + self.server_address.ip_address +
                ", PUERTO: " + str(self.server_address.port) + ". TimeoutError: " + str(err))
            return
        except InterruptedError as err:
            self.logger.error(
                "No se pudo conectar al canal de datos del módem con IP " + self.server_address.ip_address +
                ", PUERTO: " + str(self.server_address.port) + ". InterruptedError: " + str(err))
            return
        except Exception as err:
            self.logger.error(
                "No se pudo conectar al módem con IP " + self.server_address.ip_address + ", PUERTO: " + str(
                    self.server_address.port) + ". CAUSA: " + str(err))
            return

        self.client_connected = True
        self.logger.info(
            "Conectado al canal de datos del Módem, IP: " + self.server_address.ip_address + ", PUERTO: " + str(
                self.server_address.port))

    def process_socket_data(self):
        while self.client_connected:
            rx_sock_ready, tx_sock_ready, err = select.select([self.client_socket], [self.client_socket], [],
                                                              TIMEOUT)
            if rx_sock_ready:
                self.read_data_from_socket()

            if tx_sock_ready and not self.modem_queue_tx.empty():
                self.send_data_to_socket()

            if not self.modem_online.is_set():
                self.client_socket.shutdown(socket.SHUT_RDWR)
                self.client_socket.close()
                self.client_connected = False
                self.logger.debug("Canal de datos desconectado, modem en bajo consumo")
                break

            if self.kill_thread.is_set():
                self.client_socket.shutdown(socket.SHUT_RDWR)
                self.client_socket.close()
                self.client_connected = False
                break

            time.sleep(0.1)

    def read_data_from_socket(self):
        chunk = self.client_socket.recv(TCP_BUFFFER_SIZE)
        if len(chunk) == 0:
            self.logger.info(
                "Se ha caido la conexion en el canal de datos con el modem con IP: " + self.server_address.ip_address +
                ", PUERTO: " + str(self.server_address.port))
            self.client_connected = False
        else:
            str_chunk = chunk.decode(encoding=FORMATO_TEXTO)
            self.command += str_chunk
            if self.command.find("\n") != -1:
                self.send_command_to_queue()

    def send_data_to_socket(self):
        at_command = self.modem_queue_tx.get()
        raw_data = at_command.get().encode(encoding=FORMATO_TEXTO)
        self.client_socket.sendall(raw_data)
