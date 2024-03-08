from serial import LF, Serial, SerialException
from queue import Queue, Empty
from logging import Logger
from threading import Thread, Event
from data_types import ModemMessage

TIMEOUT = 0.5


# Controlador basico del puerto serie
class SerialController:
    com_port: Serial

    def __init__(self, port: str, baudrate: int):
        try:
            self.com_port = Serial(port=port, baudrate=baudrate, timeout=TIMEOUT)
        except (ValueError, SerialException) as e:
            raise e

    def is_available(self) -> bool:
        return self.com_port.in_waiting > 0

    def send_serial_command(self, command: str):
        formatted_cmd = f"{command}\r"
        self.com_port.write(formatted_cmd.encode())
        self.com_port.flush()

    def read_serial_response(self) -> str:
        raw_response = self.com_port.read_until(expected=LF)
        return raw_response.decode()


class SerialModemClient(Thread):
    modem_queue_tx: Queue
    modem_queue_rx: Queue

    logger: Logger

    serial_modem: SerialController

    queue_timeout: float

    kill_thread: Event

    def __init__(self, logger: Logger, serial_controller: SerialController, modem_queue_tx: Queue,
                 modem_queue_rx: Queue, queue_timeout: float, kill_thread: Event):
        super().__init__(daemon=True, name="serial_modem_client")

        self.modem_queue_rx = modem_queue_rx
        self.modem_queue_tx = modem_queue_tx
        self.logger = logger
        self.serial_modem = serial_controller
        self.queue_timeout = queue_timeout
        self.kill_thread = kill_thread

    def run(self):
        while True:
            self.send_command()

            if self.serial_modem.is_available():
                self.get_response()

            if self.kill_thread.is_set():
                self.logger.debug("Serial modem CLOSED!")
                return

    def send_command(self):
        try:
            at_command = self.modem_queue_tx.get(timeout=self.queue_timeout)
            self.serial_modem.send_serial_command(at_command.get())
        except Empty:
            pass
        return

    def get_response(self):
        modem_message = ModemMessage(self.serial_modem.read_serial_response())
        self.modem_queue_rx.put(modem_message)
        return
