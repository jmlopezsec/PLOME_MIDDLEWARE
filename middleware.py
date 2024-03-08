import configparser
import logging
import sys
from threading import Event, Timer
import threading
from datetime import datetime
from queue import Queue

from data_types import SocketAddress, ModemConfig, ClientCommand
from dispatcher import Dispatcher
from file_handler import FileHandler
from file_modem_client import FileModemClient
from interrupt_dispatcher import InterruptDispatcher
from message_handler import MessageHandler
from serial_modem_client import SerialModemClient, SerialController, SerialException
from tcp_command_server import TcpCommandServer
from tcp_interrupt_server import TcpInterruptServer
from tcp_modem_client import ModemClient

VERSION = "v0.8"
QUEUE_MAX_SIZE = 32
QUEUE_TIMEOUT = 0.1
T_QUIT = 60.0  # Seconds


class Middleware:
    # Modem
    modem_config: ModemConfig

    # Logger y Parser
    logger: logging.Logger
    config_parser: configparser.ConfigParser

    # Sockets
    command_server_address: SocketAddress
    interrupt_server_address: SocketAddress

    modem_address: SocketAddress
    file_modem_address: SocketAddress

    # FilePath
    file_path: str

    # File transmission block size
    block_size: int

    # Serial
    serial_controller: SerialController

    # Colas
    tcp_server_queue_rx: Queue
    tcp_server_queue_tx: Queue

    modem_interrupt_queue: Queue
    client_interrupt_queue: Queue

    at_command_queue_rx: Queue
    at_command_queue_tx: Queue

    modem_queue_rx: Queue
    modem_queue_tx: Queue

    modem_online: Event
    kill_request: Event
    kill_threads: Event

    active_threads: list = []

    def __init__(self, ini_file_path):
        threading.excepthook = self.exception_handler

        self.tcp_server_queue_rx = Queue(maxsize=QUEUE_MAX_SIZE)
        self.tcp_server_queue_tx = Queue(maxsize=QUEUE_MAX_SIZE)

        self.client_interrupt_queue = Queue(maxsize=QUEUE_MAX_SIZE)
        self.modem_interrupt_queue = Queue(maxsize=QUEUE_MAX_SIZE)

        self.at_command_queue_rx = Queue(maxsize=QUEUE_MAX_SIZE)
        self.at_command_queue_tx = Queue(maxsize=QUEUE_MAX_SIZE)

        self.modem_queue_rx = Queue(maxsize=QUEUE_MAX_SIZE)
        self.modem_queue_tx = Queue(maxsize=QUEUE_MAX_SIZE)

        self.file_command_queue_tx = Queue(maxsize=QUEUE_MAX_SIZE)
        self.file_command_queue_rx = Queue(maxsize=QUEUE_MAX_SIZE)

        self.modem_file_queue_tx = Queue(maxsize=QUEUE_MAX_SIZE)
        self.modem_file_queue_rx = Queue(maxsize=QUEUE_MAX_SIZE)

        self.modem_online = Event()
        self.kill_request = Event()
        self.kill_threads = Event()
        self.parse_config(ini_file_path)

    # Parser del archivo de configuración

    def parse_config(self, ini_file_path):
        self.parse_ini_file(ini_file_path)
        self.parse_logger_config()
        self.parse_middleware_config()
        self.parse_modem_config()

    def parse_ini_file(self, ini_file_path):
        self.config_parser = configparser.ConfigParser()
        self.config_parser.read(ini_file_path)

    def parse_logger_config(self):
        fecha = datetime.now()
        logger_config = self.config_parser["LOGGER"]
        log_file_path = f"plome_{fecha.strftime('%d%m%Y_%H%M%S')}.log"
        log_level = logger_config["log_level"]

        # Saltará un error en tiempo de ejecución si el nivel de log no es válido
        numeric_level = getattr(logging, log_level.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError('Invalid log level: %s' % log_level)

        logging.basicConfig(filename=log_file_path, filemode='w', level=numeric_level,
                            format='%(asctime)s.%(msecs)03d %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
        self.logger = logging.getLogger()

    def parse_middleware_config(self):
        middleware_config = self.config_parser['MIDDLEWARE']

        server_ip = middleware_config["server_ip"]
        command_port = int(middleware_config["command_port"])
        interrupt_port = int(middleware_config["interrupt_port"])
        self.file_path = middleware_config["file_path"]
        self.block_size = int(middleware_config["block_size"])
        try:
            self.command_server_address = SocketAddress(server_ip, command_port)
            self.interrupt_server_address = SocketAddress(server_ip, interrupt_port)
        except OSError:
            self.logger.critical(
                "No se pudo resolver el nombre de dominio del servidor local, configuracion IP local invalida!")
            sys.exit(1)

        filetransfer = int(middleware_config["file_transfer"])
        if filetransfer:
            self.logger.debug("FILE TRANSFER ENABLED")
            self.modem_online.set()
        else:
            self.logger.debug("FILE TRANSFER DISABLED")
            self.modem_online.clear()

    def parse_modem_config(self):
        self.modem_config = ModemConfig()
        modem_config_file = self.config_parser["MODEM"]

        self.parse_modem_config_parameters(modem_config_file)
        self.parse_modem_connection_config()
        self.logger.info(f"MIDDLEWARE {VERSION} INICIADO, DIRECCION MODEM -> {self.modem_config.modem_address}")

    def parse_modem_config_parameters(self, modem_config_file):
        # Toma solo los atributos de ModemConfig y genera una lista con estos
        parameter_list = filter(lambda a: not a.startswith('__') and not callable(getattr(self.modem_config, a)),
                                dir(self.modem_config))

        for parameter in parameter_list:
            if isinstance(getattr(self.modem_config, parameter), str):
                setattr(self.modem_config, parameter, modem_config_file[parameter])

            elif isinstance(getattr(self.modem_config, parameter), int):
                setattr(self.modem_config, parameter, int(modem_config_file[parameter]))

            elif isinstance(getattr(self.modem_config, parameter), float):
                setattr(self.modem_config, parameter, float(modem_config_file[parameter]))

    def parse_modem_connection_config(self):
        if self.modem_config.connection_mode == 'tcp':
            self.parse_modem_inet_config()
        elif self.modem_config.connection_mode == 'rs232':
            self.parse_modem_serial_config()
        else:
            self.logger.critical("No se ha escogido un método de conexión al módem válido. OPCIONES: tcp o rs232")
            sys.exit(1)
        self.parse_file_inet_config()

    def parse_modem_inet_config(self):
        try:
            self.modem_address = SocketAddress(self.modem_config.inet_addr, self.modem_config.inet_port)
        except OSError as e:
            self.logger.critical(
                "No se pudo resolver el nombre de dominio del modem a traves de DNS, configuracion invalida!\n ERROR: "
                + str(e))
            sys.exit(1)

    def parse_modem_serial_config(self):
        try:
            self.serial_controller = SerialController(self.modem_config.com_port, self.modem_config.baudrate)
        except (ValueError, SerialException) as e:
            self.logger.critical(
                "No se pudo conectar al puerto serie especificado, configuracion invalida!\n ERROR: "
                + str(e))
            sys.exit(1)

    def parse_file_inet_config(self):
        try:
            self.file_modem_address = SocketAddress(self.modem_config.inet_addr, self.modem_config.file_inet_port)
        except OSError as e:
            self.logger.critical(
                "No se pudo resolver el nombre de dominio del modem a traves de DNS, configuracion invalida!\n ERROR: "
                + str(e))
            sys.exit(1)

    def start(self):
        self.start_dispatcher()
        self.start_interrupt_dispatcher()
        self.start_message_handler()
        self.start_file_handler()
        self.start_modem_client()
        self.start_modem_file_client()
        self.start_interrupt_server()
        self.start_command_server()

        self.boot_modem()

        self.kill_request.wait()
        self.kill_threads.set()
        t = Timer(T_QUIT, self.force_quit)
        t.start()
        for th in self.active_threads:
            th.join()
            self.logger.debug(f"Thread: {th.getName} KILLED!")
        t.cancel()
        self.logger.info("Middleware shutted down correctly.")
        return

    # INICIALIZACION THREADS
    def start_command_server(self):
        tcp_command_server_thread = TcpCommandServer(self.logger, self.command_server_address,
                                                     self.tcp_server_queue_tx,
                                                     self.tcp_server_queue_rx, kill_thread=self.kill_threads)
        tcp_command_server_thread.start()
        # self.logger.info("Started thread COMAND SERVER, PID: " + str(tcp_command_server_thread.native_id))

        self.active_threads.append(tcp_command_server_thread)

    def start_interrupt_server(self):
        tcp_interrupt_server_thread = TcpInterruptServer(self.logger, self.interrupt_server_address,
                                                         self.client_interrupt_queue, kill_thread=self.kill_threads)
        tcp_interrupt_server_thread.start()
        # self.logger.info("Started thread INTERRUPT SERVER, PID: " + str(tcp_interrupt_server_thread.native_id))
        self.active_threads.append(tcp_interrupt_server_thread)

    def start_dispatcher(self):
        dispatcher_thread = Dispatcher(self.logger, self.file_path, VERSION, self.modem_config, self.tcp_server_queue_rx,
                                       self.tcp_server_queue_tx, self.at_command_queue_rx,
                                       self.at_command_queue_tx, self.file_command_queue_rx, self.file_command_queue_tx,
                                       self.modem_online, QUEUE_TIMEOUT, self.kill_request,
                                       kill_thread=self.kill_threads)
        dispatcher_thread.start()
        # self.logger.info("Started thread DISPATCHER, PID: " + str(dispatcher_thread.native_id))
        self.active_threads.append(dispatcher_thread)

    def start_message_handler(self):
        message_handler_thread = MessageHandler(self.logger, self.modem_config, self.modem_queue_rx,
                                                self.modem_queue_tx,
                                                self.at_command_queue_rx, self.at_command_queue_tx,
                                                self.tcp_server_queue_rx, self.modem_interrupt_queue, QUEUE_TIMEOUT,
                                                kill_thread=self.kill_threads)
        message_handler_thread.start()
        # self.logger.info("Started thread MSG HANDLER, PID: " + str(message_handler_thread.native_id))
        self.active_threads.append(message_handler_thread)

    def start_interrupt_dispatcher(self):
        interrupt_dispatcher_thread = InterruptDispatcher(self.logger, self.modem_interrupt_queue,
                                                          self.client_interrupt_queue, QUEUE_TIMEOUT,
                                                          kill_thread=self.kill_threads)
        interrupt_dispatcher_thread.start()
        # self.logger.info("Started thread INTERRUPT DISPATCHER, PID: " + str(interrupt_dispatcher_thread.native_id))
        self.active_threads.append(interrupt_dispatcher_thread)

    def start_modem_client(self):
        if self.modem_config.connection_mode == 'tcp':
            modem_client_thread = ModemClient(self.logger, self.modem_address, self.modem_queue_tx,
                                              self.modem_queue_rx, kill_thread=self.kill_threads)

        elif self.modem_config.connection_mode == 'rs232':
            modem_client_thread = SerialModemClient(self.logger, self.serial_controller, self.modem_queue_tx,
                                                    self.modem_queue_rx, QUEUE_TIMEOUT, kill_thread=self.kill_threads)
        else:
            return

        modem_client_thread.start()
        # self.logger.info("Started thread MODEM CLIENT, PID: " + str(modem_client_thread.native_id))
        self.active_threads.append(modem_client_thread)

    def start_file_handler(self):
        file_handler_thread = FileHandler(self.logger, self.file_path, self.block_size, self.file_command_queue_rx,
                                          self.file_command_queue_tx, self.modem_file_queue_rx,
                                          self.modem_file_queue_tx, self.client_interrupt_queue, QUEUE_TIMEOUT,
                                          kill_thread=self.kill_threads)
        file_handler_thread.start()
        # self.logger.info("Started thread FILE HANDLER, PID: " + str(file_handler_thread.native_id))
        self.active_threads.append(file_handler_thread)

    def start_modem_file_client(self):
        file_modem_client = FileModemClient(self.logger, self.file_modem_address, self.modem_file_queue_tx,
                                            self.modem_file_queue_rx, self.modem_online, kill_thread=self.kill_threads)
        file_modem_client.start()
        # self.logger.info("Started thread MODEM FILE CLIENT, PID: " + str(file_modem_client.native_id))
        self.active_threads.append(file_modem_client)

    # Reinicia el modem y carga la configuracion en el automaticamente
    def boot_modem(self):
        self.tcp_server_queue_rx.put(ClientCommand("LOADCONFIG\n\0"))

    def force_quit(self):
        self.logger.error("El middleware tuvo que cerrarse abrubtamente!")
        sys.exit(1)

    def exception_handler(self, args):
        thread_name = args.thread.getName()

        if thread_name == "tcp_command_server":
            self.logger.error("Error fatal! Cerrando sistema...\n ERROR: " + str(args.exc_value))
            sys.exit(1)
        elif thread_name == "tcp_modem_driver":
            self.logger.warning("Thread modem caido!")
            pass
        else:
            self.logger.error("THREAD CAIDO: " + thread_name)
            self.logger.error(f"EXCEPCION: {args.exc_type} -> {args.exc_value}")
            self.logger.error(f"TRACEBACK: {args.exc_traceback}")
            pass
