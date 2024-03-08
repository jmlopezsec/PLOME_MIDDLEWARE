import time
from logging import Logger
from queue import Queue, Empty
from threading import Thread, Timer, Event
from data_types import ClientCommand, ClientCommandResponse, AtCommand, ModemMessage
import zlib
import hashlib
import base64


class FileHandler(Thread):
    logger: Logger

    file_command_queue_rx: Queue
    file_command_queue_tx: Queue

    modem_file_queue_rx: Queue
    modem_file_queue_tx: Queue

    client_interrupt_queue: Queue

    transmitting_file: bool
    receiving_file: bool

    # Common params
    dir_path: str
    block_size: int
    timeout: int
    ack_timeout: int
    n_intentos: int

    # Transmission data
    tx_filename: str
    receiver_dir: str
    tx_file_md5: str
    tx_file_blocks: list
    tx_block_count: int
    tx_next_block: int = 0
    tx_actual_block: int = 0
    tx_accepted: bool = False
    intentos_actuales: int = 0
    tx_timer: Timer

    # Reception data
    transmitter_dir: str
    recv_filename: str
    recv_num_blocks: int
    recv_md5: str
    recv_blocks: list
    recv_actual_block: int = 0
    intentos_actuales_ack: int = 0
    recv_timer: Timer

    queue_timeout: float

    kill_thread: Event

    def __init__(self, logger: Logger, dir_path: str, block_size: int, file_command_queue_rx: Queue,
                 file_command_queue_tx: Queue, modem_file_queue_rx: Queue, modem_file_queue_tx: Queue,
                 client_interrupt_queue: Queue, queue_timeout: float, kill_thread: Event):
        super().__init__(daemon=True, name="file_handler")
        self.logger = logger
        self.dir_path = dir_path
        self.block_size = block_size
        self.queue_timeout = queue_timeout

        self.file_command_queue_tx = file_command_queue_tx
        self.file_command_queue_rx = file_command_queue_rx
        self.modem_file_queue_tx = modem_file_queue_tx
        self.modem_file_queue_rx = modem_file_queue_rx
        self.client_interrupt_queue = client_interrupt_queue

        self.transmitting_file = False
        self.receiving_file = False

        self.recv_num_blocks = 0
        self.recv_filename = ''
        self.recv_md5 = ''
        self.recv_blocks = []

        # DEBUG
        self.timeout = 17
        self.ack_timeout = 13
        self.n_intentos = 5

        self.kill_thread = kill_thread

    def run(self):
        while True:
            try:
                modem_message = self.modem_file_queue_rx.get(timeout=self.queue_timeout)
                self.handle_modem_data(modem_message)
            except Empty:
                pass

            try:
                client_command = self.file_command_queue_rx.get(timeout=self.queue_timeout)
                self.execute_command(client_command)
            except Empty:
                pass

            if self.kill_thread.is_set():
                self.logger.debug("File Handler CLOSED!")
                return

    # PROCESAMIENTO DE LOS DATOS RECIBIDOS DEL CLIENTE
    def execute_command(self, client_command: ClientCommand):
        self.logger.debug(f"Comando recibido en file handler: {client_command.get_command()}")
        if client_command.get_command() == "SENDFILE":
            if self.transmitting_file or self.receiving_file:
                self.send_response_to_client("TRANSMITTER BUSY")
            else:
                self.request_file_transmission(client_command)

    # PROCESAMIENTO DE LOS DATOS RECIBIDOS DEL MODEM
    def handle_modem_data(self, modem_message: ModemMessage):
        if modem_message.is_transmission_request():
            self.process_transmission_request(modem_message)
            return

        if self.transmitting_file:
            if modem_message.is_nack():
                self.reply_nack(modem_message)
                return
            elif modem_message.is_ack():
                self.send_next_block(modem_message)
                return

        if modem_message.is_received_data() and self.receiving_file:
            self.process_next_block(modem_message)
            return

    # FUNCION PARA LA TRANSMISION DE MENSAJES AL CLIENTE
    def send_response_to_client(self, response_type: str, value=''):
        server_response = ClientCommandResponse(response_type, value)
        self.logger.debug("CLIENT RESPONSE SENT BY FILE HANDLER: " + server_response.get_entire_response())
        self.file_command_queue_tx.put(server_response)

    def send_interrupt_to_client(self, msg: str):
        self.logger.debug("CLIENT INTERRUPT SENT BY FILE HANDLER: " + msg)
        self.client_interrupt_queue.put(msg)

    # FUNCION GENERICA PARA LA TRANSMISION DE DATOS CON AT*SEND
    def send_data(self, data: str, receiver_dir: str):
        command_chunks = ("AT*SEND", str(len(data)), receiver_dir, data)
        at_command_str = ",".join(command_chunks)
        at_command = AtCommand(at_command_str, communication_hardware='tcp')
        self.modem_file_queue_tx.put(at_command)
        self.logger.debug("MSG SENT BY FILETHREAD: " + at_command.get())
        return

    # SOLICITUD DE TRANSMISION DE ARCHIVOS
    def request_file_transmission(self, client_command: ClientCommand):
        command_args = client_command.get_arguments()
        self.tx_filename = command_args[0].split('=')[1]
        self.receiver_dir = command_args[1].split('=')[1]
        self.get_file_blocks()
        self.tx_next_block = 0
        self.tx_actual_block = 0

        if self.tx_block_count == 0:
            self.send_response_to_client("SENDFILE FAILED")
            return

        self.tx_file_md5 = self.get_md5(self.tx_file_blocks)
        self.logger.debug(
            f"Requested file transmission -> Name: {self.tx_filename} MD5: {self.tx_file_md5} "
            f"NBlocks: {self.tx_block_count}")

        if not self.send_header_block():
            self.send_response_to_client("SENDFILE FAILED")
            return

        self.transmitting_file = True

        self.tx_timer = Timer(self.timeout, self.retry_block_transmission)
        self.tx_timer.start()
        self.send_response_to_client("SENDFILE REQUESTED")
        return

    def get_file_blocks(self):
        file_path = f"{self.dir_path}/{self.tx_filename}"
        self.tx_file_blocks = []
        try:
            f = open(file_path, 'rb')
        except (OSError, IOError):
            self.logger.error(f"Error al tratar de abrir el archivo: {file_path}")
            self.send_response_to_client("SENDFILE FAILED")
            self.tx_block_count = 0
            return

        with f:
            for byte_block in iter(lambda: f.read(self.block_size), b""):
                self.tx_file_blocks.append(byte_block)
        self.tx_block_count = len(self.tx_file_blocks)
        return

    def send_header_block(self) -> bool:
        block_data = f"H|{self.tx_filename}|{self.tx_block_count}|{self.tx_file_md5}"
        try:
            str_crc = FileHandler.get_crc(block_data.encode('utf-8'))
        except UnicodeDecodeError:
            self.logger.error(f"Error: El nombre de archivo {self.tx_filename} no es soportado por UTF-8")
            return False
        self.send_data(f"{block_data},{str_crc}", self.receiver_dir)
        return True

    # TRANSMISION DEL ARCHIVO POR BLOQUES
    def send_next_block(self, modem_message: ModemMessage):
        self.intentos_actuales = 0

        n_secuencia = int(modem_message.get_message_chunks()[10])
        self.logger.debug(f"Recibido ack, siguiente num secuencia -> {n_secuencia}")

        if n_secuencia == 0 and self.tx_next_block == 0:
            self.send_interrupt_to_client(f"FILE {self.tx_filename} TRANSMISSION ACCEPTED\n")
            self.tx_timer.cancel()
            self.logger.debug(f"Bloque {self.tx_next_block} enviado, esperando ack...")
            self.send_file_block()
            self.tx_timer = Timer(self.timeout, self.retry_block_transmission)
            self.tx_timer.start()
            self.tx_next_block = 1
            return

        if n_secuencia != self.tx_next_block:
            self.logger.debug(
                f"Descartado ack antiguo, n_secuencia:{n_secuencia} siguiente bloque esperado:{self.tx_next_block}")
            return

        if n_secuencia == self.tx_block_count:
            self.tx_timer.cancel()
            self.clean_transmitter()
            # Espera a que el receptor pare y vuelva a estar disponible
            time.sleep(self.n_intentos * (self.ack_timeout + 1))
            self.logger.info(f"Archivo {self.tx_filename} enviado correctamente!")
            self.send_interrupt_to_client(f"FILE {self.tx_filename} TRANSMISSION COMPLETE\n")
            return

        self.tx_timer.cancel()
        self.logger.debug(f"Bloque {self.tx_next_block} enviado, esperando ack...")
        self.tx_actual_block = n_secuencia
        self.tx_next_block = n_secuencia + 1
        self.send_file_block()
        self.tx_timer = Timer(self.timeout, self.retry_block_transmission)
        self.tx_timer.start()
        return

    def reply_nack(self, modem_message: ModemMessage):
        self.tx_timer.cancel()
        self.intentos_actuales = 0
        n_secuencia = int(modem_message.get_message_chunks()[10])
        self.logger.debug(f"Recibido NACK, retransmitir bloque -> {n_secuencia}")
        self.tx_actual_block = n_secuencia - 1
        self.tx_next_block = n_secuencia
        self.send_file_block()
        self.tx_timer = Timer(self.timeout, self.retry_block_transmission)
        self.tx_timer.start()

    def retry_block_transmission(self):
        self.tx_timer.cancel()
        self.intentos_actuales += 1

        if self.intentos_actuales == self.n_intentos:
            if self.tx_next_block == 0:
                self.logger.info(
                    f"Transmision de la cabecera {self.tx_filename} fallida o rechazada, numero de intentos agotado")
                self.send_interrupt_to_client(f"FILE {self.tx_filename} TRANSMISSION REJECTED\n")
            else:
                self.logger.info(f"Transmision del archivo {self.tx_filename} fallida, numero de intentos agotado")
                self.send_interrupt_to_client(f"FILE {self.tx_filename} TRANSMISSION FAILED: TIMEOUT\n")
            self.clean_transmitter()
            return

        if self.tx_next_block == 0:
            self.send_header_block()
            self.logger.debug(f"Reintento numero {self.intentos_actuales} de enviar la cabecera")
        else:
            self.send_file_block()
            self.logger.debug(
                f"Reintento numero {self.intentos_actuales} de enviar el bloque numero {self.tx_next_block}")
        self.tx_timer = Timer(self.timeout, self.retry_block_transmission)
        self.tx_timer.start()

    def clean_transmitter(self):
        # Limpiar y notificar por el canal de interrupciones
        self.tx_next_block = 0
        self.intentos_actuales = 0
        self.tx_file_blocks = []
        self.tx_actual_block = 0
        self.transmitting_file = False
        return

    def send_file_block(self):
        str_file_block = base64.b64encode(self.tx_file_blocks[self.tx_actual_block]).decode('utf-8')
        str_crc = FileHandler.get_crc(self.tx_file_blocks[self.tx_actual_block])
        self.send_data(f"{self.tx_actual_block}|{str_file_block}|{str_crc}", self.receiver_dir)

    # PROCESADO DE PETICIONES DE TRANSMISION DE ARCHIVOS

    def process_transmission_request(self, received_message: ModemMessage):
        message_chunks = received_message.get_message_chunks()
        requester_dir = message_chunks[2]

        if self.receiving_file or self.transmitting_file:
            self.send_ack(False, 0, requester_dir)
            return

        header_data = message_chunks[9]
        checksum = message_chunks[10]
        calculated_checksum = FileHandler.get_crc(header_data.encode('utf-8'))

        if calculated_checksum != checksum:
            self.send_ack(False, 0, requester_dir)
            return

        data_chunks = header_data.split('|')
        self.recv_filename = data_chunks[1]
        self.recv_num_blocks = int(data_chunks[2])
        self.recv_md5 = data_chunks[3]
        self.transmitter_dir = requester_dir

        self.receiving_file = True
        self.recv_actual_block = 0
        self.send_interrupt_to_client(f"FILE {self.recv_filename} RECEPTION ACCEPTED\n")
        self.send_ack(True, self.recv_actual_block, self.transmitter_dir)
        return

    # PROCESADO DE BLOQUES RECIBIDOS
    def process_next_block(self, modem_message: ModemMessage):
        self.logger.debug(
            f"BLOQUE HA LLEGADO AL RECEPTOR: {modem_message.get_message()}")
        self.recv_timer.cancel()
        self.intentos_actuales_ack = 0

        payload: str
        message_chunks = modem_message.get_message_chunks()
        if len(message_chunks) > 10:
            payload = ','.join(message_chunks[9:])
        else:
            payload = message_chunks[9]

        transmitter_dir = message_chunks[2]

        if transmitter_dir != self.transmitter_dir or not self.receiving_file:
            return

        first_separator_pos = payload.find('|')
        last_separator_pos = payload.rfind('|')
        num_secuencia = int(payload[:first_separator_pos])
        received_crc = payload[last_separator_pos + 1:]
        str_data_block = payload[first_separator_pos + 1:last_separator_pos]

        if num_secuencia != self.recv_actual_block:
            self.logger.debug(
                f"Bloque recibido no coincide con esperado. n_secuencia: {num_secuencia}, "
                f"esperado: {self.recv_actual_block}")
            self.send_ack(False, self.recv_actual_block, self.transmitter_dir)
            return

        raw_data_block = base64.b64decode(str_data_block.encode('utf-8'))
        calculated_crc = FileHandler.get_crc(raw_data_block)
        self.logger.debug(
            f"BLOQUE RECIBIDO: NUM SECUENCIA {num_secuencia} CRC RECIBIDO: {received_crc} "
            f"CRC CALCULADO: {calculated_crc}")

        if received_crc == calculated_crc:
            self.recv_blocks.append(raw_data_block)
            self.recv_actual_block += 1

            if self.recv_actual_block == self.recv_num_blocks:
                self.logger.debug(f"Archivo recibido al completo, {self.recv_actual_block} recibidos")
                calculated_md5 = self.get_md5(self.recv_blocks)
                if self.recv_md5 != calculated_md5:
                    self.logger.error(
                        f"FALLO LA RECEPCION DEL ARCHIVO {self.recv_filename}, MD5 CALCULADO NO COINCIDE!")
                    self.logger.debug(f"RECEIVED MD5: {self.recv_md5} CALCULATED MD5: {calculated_md5}")
                    self.send_interrupt_to_client(f"FILE {self.recv_filename} RECEPTION FAILED: WRONG MD5\n")
                    self.clean_receiver()

                self.buid_file()
                self.send_ack(True, self.recv_actual_block, self.transmitter_dir)
                return

            self.logger.debug(
                f"Bloque {num_secuencia} procesado correctamente, {self.recv_actual_block} bloques recibidos")
            self.send_ack(True, self.recv_actual_block, self.transmitter_dir)
        else:
            self.send_ack(False, self.recv_actual_block, self.transmitter_dir)
        return

    def buid_file(self):
        file_path = f"{self.dir_path}/{self.recv_filename}"

        try:
            f = open(file_path, 'wb')
        except (OSError, IOError):
            self.logger.debug(f"Error al tratar de crear el archivo: {file_path}")
            self.send_interrupt_to_client(f"FILE {self.recv_filename} RECEPTION FAILED: FILE ERROR\n")
            return

        with f:
            for byte_block in self.recv_blocks:
                f.write(byte_block)
        self.logger.debug(f"Archivo {self.recv_filename} creado correctamente!")
        return

    def send_ack(self, valid_reception: bool, numero_secuencia: int, transmitter_dir: str):
        ack_str: str
        if valid_reception:
            ack_str = f"ack,{numero_secuencia}"
        else:
            ack_str = f"nack,{numero_secuencia}"

        self.send_data(ack_str, transmitter_dir)
        self.recv_timer = Timer(self.ack_timeout, self.retry_ack_cb,
                                args=[valid_reception, numero_secuencia, transmitter_dir])
        self.recv_timer.start()

    def retry_ack_cb(self, *args, **kwargs):
        self.intentos_actuales_ack += 1
        if self.intentos_actuales_ack == self.n_intentos:
            if self.recv_actual_block == self.recv_num_blocks:
                self.logger.debug("Receptor listo para siguiente transmision!")
                self.send_interrupt_to_client(f"FILE {self.recv_filename} RECEPTION COMPLETE\n")
            else:
                self.logger.info(
                    f"Recepcion del archivo {self.recv_filename} fallida, numero de intentos de "
                    f"retransmitir ACK agotados")
                self.send_interrupt_to_client(f"FILE {self.recv_filename} RECEPTION FAILED: TIMEOUT\n")
            self.clean_receiver()
            return
        self.logger.debug(f"Retransmitiendo ACK, intento {self.intentos_actuales_ack}")
        self.send_ack(args[0], args[1], args[2])
        return

    def clean_receiver(self):
        self.recv_timer.cancel()
        self.receiving_file = False
        self.recv_actual_block = 0
        self.intentos_actuales_ack = 0
        self.recv_blocks = []
        return

    # CALCULO DE MD5 Y CRC

    @staticmethod
    def get_md5(file_blocks: list) -> str:
        md5_hash = hashlib.md5()
        for block in file_blocks:
            md5_hash.update(block)
        return md5_hash.hexdigest()

    @staticmethod
    def get_crc(file_block: bytes) -> str:
        return hex(zlib.crc32(file_block) & 0xffffffff)
