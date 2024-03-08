from logging import Logger
from queue import Queue, Empty
from threading import Thread, Event
from data_types import ModemMessage, ModemConfig, ClientCommand


class MessageHandler(Thread):
    logger: Logger

    at_command_queue_rx: Queue
    at_command_queue_tx: Queue

    tcp_server_queue_rx: Queue

    modem_queue_rx: Queue
    modem_queue_tx: Queue

    modem_interrupt_queue: Queue

    modem_config: ModemConfig

    queue_timeout: float

    kill_thread: Event

    def __init__(self, logger: Logger, modem_config: ModemConfig, modem_queue_rx: Queue, modem_queue_tx: Queue,
                 at_command_queue_rx: Queue, at_command_queue_tx: Queue, tcp_server_queue_rx: Queue,
                 modem_interrupt_queue: Queue, queue_timeout: float, kill_thread: Event):
        super().__init__(daemon=True, name="message_handler")

        self.logger = logger

        self.modem_queue_rx = modem_queue_rx
        self.modem_queue_tx = modem_queue_tx

        self.at_command_queue_rx = at_command_queue_rx
        self.at_command_queue_tx = at_command_queue_tx

        self.tcp_server_queue_rx = tcp_server_queue_rx

        self.modem_interrupt_queue = modem_interrupt_queue

        self.modem_config = modem_config

        self.queue_timeout = queue_timeout

        self.kill_thread = kill_thread

    def run(self):
        while True:
            try:
                at_command = self.at_command_queue_tx.get(timeout=self.queue_timeout)
                self.logger.debug("COMANDO AT HA PASADO POR MESSAGE HANDLER: " + at_command.get())
                self.modem_queue_tx.put(at_command)
            except Empty:
                pass

            try:
                modem_response = self.modem_queue_rx.get(timeout=self.queue_timeout)
                self.handle_modem_response(modem_response)
            except Empty:
                pass

            if self.kill_thread.is_set():
                self.logger.debug("Message handler CLOSED!")
                return

    def handle_modem_response(self, modem_response: ModemMessage):
        if modem_response.is_ping_msg():
            self.logger.debug("RECEIVED PING FROM MODEM " + modem_response.get_message_chunks()[2])
            # Ignora mensajes ping recibidos
            return
        elif modem_response.is_power_ping_msg():
            self.logger.debug("RECEIVED AUTOPOWER PING FROM MODEM " + modem_response.get_message_chunks()[2])
            # Ignora mensajes ping recibidos
            return
        elif modem_response.is_received_im():
            self.send_interrupt(modem_response)
        elif modem_response.is_sleep_request():
            self.handle_sleep()
        elif modem_response.is_wakeup_request():
            self.handle_wakeup()
        elif modem_response.is_position_data():
            self.logger.debug("MESSAGE RECEIVED BY HANDLER IS POSITION MSG: " + modem_response.get_message())
        else:
            self.process_at_response(modem_response)

    def send_interrupt(self, instant_message: ModemMessage):
        self.logger.debug("MENSAJE IM MANDADO A COLA DE INTERRUPCIONES: " + instant_message.get_message())
        self.modem_interrupt_queue.put(instant_message)

    def process_at_response(self, modem_response: ModemMessage):
        self.logger.debug(
            "RESPUESTA ENVIADA DE HANDLER A DISPATCHER: " + modem_response.get_message())
        self.at_command_queue_rx.put(modem_response)

    def handle_sleep(self):
        self.logger.debug("RECIBIDA SOLICITUD DE SLEEP")
        self.tcp_server_queue_rx.put(ClientCommand("MODEM SLEEP\n\0"))
        return

    def handle_wakeup(self):
        self.logger.debug("RECIBIDA SOLICITUD DE WAKEUP")
        self.tcp_server_queue_rx.put(ClientCommand("MODEM WAKEUP\n\0"))
        return
