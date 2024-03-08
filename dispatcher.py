import os
import time
from logging import Logger
from queue import Queue, Empty
from threading import Thread, Event

from data_types import ClientCommand, ClientCommandResponse, AtCommand, ModemConfig, ModemMessage, Measure


class Dispatcher(Thread):
    logger: Logger

    file_path: str

    tcp_server_queue_rx: Queue
    tcp_server_queue_tx: Queue

    at_command_queue_rx: Queue
    at_command_queue_tx: Queue

    file_command_queue_rx: Queue
    file_command_queue_tx: Queue

    modem_config: ModemConfig
    middleware_version: str

    client_command: ClientCommand

    modem_online: Event
    kill_request: Event
    kill_thread: Event

    queue_timeout: float

    def __init__(self, logger: Logger, file_path: str, middleware_version: str, modem_config: ModemConfig,
                 tcp_server_queue_rx: Queue,
                 tcp_server_queue_tx: Queue,
                 at_command_queue_rx: Queue, at_command_queue_tx: Queue, file_command_queue_rx: Queue,
                 file_command_queue_tx: Queue, modem_online: Event, queue_timeout: float, kill_request: Event,
                 kill_thread: Event):
        super().__init__(daemon=True, name="dispatcher")

        self.logger = logger
        self.file_path = file_path

        self.tcp_server_queue_rx = tcp_server_queue_rx
        self.tcp_server_queue_tx = tcp_server_queue_tx

        self.at_command_queue_rx = at_command_queue_rx
        self.at_command_queue_tx = at_command_queue_tx

        self.file_command_queue_tx = file_command_queue_tx
        self.file_command_queue_rx = file_command_queue_rx

        self.modem_config = modem_config
        self.middleware_version = middleware_version

        self.modem_online = modem_online
        self.kill_request = kill_request
        self.kill_thread = kill_thread

        self.queue_timeout = queue_timeout

        #   Diccionario con todos los comandos posibles
        self.command_dict = {
            "REBOOT": self.restart_modem,
            "LOADCONFIG": self.load_config,
            "KILL": self.request_kill,
            "MODEM": self.get_modem_state,
            "PING": self.get_ping_parameter,
            "GETMEAS": self.get_meas,
            "GETFILE": self.get_file,
            "SENDMEAS": self.send_meas,
            "SENDRAW": self.send_raw,
            "SENDFILE": self.send_file,
            "FILETRANSFER": self.set_file_transfer,
            "GETDIR": self.get_dir,
            "SENDDIR": self.send_dir
        }

        # Parser del archivo de configuración

    def run(self):
        while True:
            try:
                client_command = self.tcp_server_queue_rx.get(timeout=self.queue_timeout)
                self.execute_command(client_command)
            except Empty:
                pass
            if self.kill_thread.is_set():
                self.logger.debug("Dispatcher CLOSED!")
                return

    # Procesa el comando, e invoca la función correspondiente según el diccionario

    def execute_command(self, client_command: ClientCommand):
        self.logger.debug("CLIENT COMMAND RECEIVED BY DISPATCHER: " + client_command.get_command())
        self.client_command = client_command
        self.command_dict.get(self.client_command.get_command(), self.cmd_format_error)()

    # Espera a la respuesta de un comando AT
    # Si recibe otro tipo de datos, los procesa y continúa esperando la respuesta

    def process_at_command(self, at_command: str, value='') -> ModemMessage:
        at_command_str = at_command + str(value)
        self.send_at_command(at_command_str)
        at_response = self.wait_for_at_response()
        return at_response

    def send_at_command(self, at_command_str: str):
        at_command = AtCommand(at_command_str, self.modem_config.connection_mode)
        self.logger.debug("AT CMD SENT BY DISPATCHER: " + at_command.get())
        self.at_command_queue_tx.put(at_command)

    def wait_for_at_response(self) -> ModemMessage:
        while True:
            try:
                modem_response = self.at_command_queue_rx.get(timeout=self.queue_timeout)
                return modem_response
            except Empty:
                pass

    def send_response_to_client(self, response_type: str, value=''):
        server_response = ClientCommandResponse(response_type, value)
        self.logger.debug("CLIENT RESPONSE SENT BY DISPATCHER: " + server_response.get_entire_response())
        self.tcp_server_queue_tx.put(server_response)

    # Función de error que será llamda en caso de que se introduzca un comando incorrecto

    def cmd_format_error(self):
        self.logger.debug("COMANDO CORRECTO INTRODUCIDO EN DISPATCHER")
        self.send_response_to_client('Comando incorrecto')
        return

    # Funcion de error que será llamada en caso de que el modem responda a un comando AT con un error

    def modem_error_response(self, modem_response: ModemMessage, at_command: str, value=''):
        self.logger.debug("MODEM RECHAZO COMANDO AT -> " + at_command + value + ": " + modem_response.get_message())
        self.send_response_to_client("CMD ERROR")
        return

    # CONFIGURACION DEL MODEM (LOADCONFIG)

    def load_config(self):
        self.set_modem_config_parameter('AT@CTRL')

        parameter_list = filter(lambda a: not a.startswith('__') and not callable(
            getattr(self.modem_config, a)) and not (type(getattr(self.modem_config, a)) is dict),
                                dir(self.modem_config))
        for parameter in parameter_list:
            try:
                self.set_modem_config_parameter(self.modem_config.at_config_dict[parameter],
                                                getattr(self.modem_config, parameter))
            except KeyError:
                self.logger.info("Key " + parameter + " not found in at_config_dict")
                pass
        # SAVE IN FLASH MEM
        self.set_modem_config_parameter("AT&W")
        self.send_response_to_client('config', 'ok')
        return

    def set_modem_config_parameter(self, at_command: str, value=''):
        at_response = self.process_at_command(at_command, value)
        if at_response.is_error():
            self.modem_error_response(at_response, at_command)
            return

        self.logger.debug(at_command + str(value) + ": " + at_response.get_message())

    # REINICIO DEL MODEM (REBOOT)

    def restart_modem(self):
        at_command = 'ATZ0'
        at_response = self.process_at_command(at_command)
        if at_response.is_error():
            self.modem_error_response(at_response, at_command)
            return

        self.send_response_to_client('reboot', at_response.get_message())

    # SOLICITUD DE APAGADO DEL MIDDLEWARE

    def request_kill(self):
        self.send_response_to_client('ok')
        self.kill_request.set()
        return

    # ESTADO DEL MODEM (MODEM TIME, MODEM BATTERY y MODEM INFO)

    def get_modem_state(self):
        args = self.client_command.get_arguments()

        if args[0] == "SETPOWER":
            self.set_power(args)
            return

        if len(args) != 1:
            self.cmd_format_error()
            return

        if args[0] == "TIME":
            self.get_modem_time()
        elif args[0] == "BATTERY":
            self.get_modem_battery()
        elif args[0] == "INFO":
            self.get_modem_info()
        # elif args[0] == "SLEEP":
        #    self.sleep_modem()
        # elif args[0] == "WAKEUP":
        #    self.wakeup_modem()
        elif args[0] == "GETPOWER":
            self.get_power()
        else:
            self.cmd_format_error()

    def get_modem_time(self):
        at_command = 'AT?UT'
        at_response = self.process_at_command(at_command)
        if at_response.is_error():
            self.modem_error_response(at_response, at_command)
            return

        timeon = float(at_response.get_message())
        self.send_response_to_client('time', str(timeon))

    def get_modem_battery(self):
        at_command = 'AT?BV'
        at_response = self.process_at_command(at_command)
        if at_response.is_error():
            self.modem_error_response(at_response, at_command)
            return

        battery_voltage = at_response.get_message()
        self.send_response_to_client('battery', battery_voltage)

    def get_modem_info(self):
        # version middleware
        self.send_response_to_client('middleware', self.middleware_version)
        # informacion modem
        self.get_modem_firmware_version()
        self.get_modem_serial()
        self.get_modem_address()

    def get_modem_firmware_version(self):
        at_command = 'ATI0'
        at_response = self.process_at_command(at_command)
        if at_response.is_error():
            self.modem_error_response(at_response, at_command)
            return

        firmware_version = "v" + at_response.get_message()
        self.send_response_to_client('firmware', firmware_version)

    def get_modem_serial(self):
        at_command = 'ATI2'
        at_response = self.process_at_command(at_command)
        if at_response.is_error():
            self.modem_error_response(at_response, at_command)
            return

        modem_serial = at_response.get_message()
        self.send_response_to_client('serial', modem_serial)

    def get_modem_address(self):
        at_command = 'AT?AL'
        at_response = self.process_at_command(at_command)
        if at_response.is_error():
            self.modem_error_response(at_response, at_command)
            return

        modem_address = at_response.get_message()
        self.send_response_to_client('address', modem_address)

    def get_power(self):
        at_command = 'AT?L'
        at_response = self.process_at_command(at_command)
        if at_response.is_error():
            self.modem_error_response(at_response, at_command)
            return

        str_msg = at_response.get_message()
        str_msg = str_msg.replace("[*]", "")

        self.send_response_to_client('power', str_msg)

    def set_power(self, args: list):
        power_level: int

        if len(args) != 2:
            self.cmd_format_error()
            return

        if not args[1].isnumeric():
            self.cmd_format_error()
            return

        power_level = int(args[1])
        if power_level < 0 or power_level > 3:
            self.cmd_format_error()
            return

        at_command = 'AT!L' + str(power_level)
        at_response = self.process_at_command(at_command)
        if at_response.is_error():
            self.modem_error_response(at_response, at_command)
            return

        self.send_response_to_client("OK")

    def set_file_transfer(self):
        args = self.client_command.get_arguments()

        if len(args) != 1:
            self.cmd_format_error()
            return

        if args[0] == "ENABLE":
            self.logger.debug("FILETRANSFER ENABLED")
            self.modem_online.set()
            self.send_response_to_client("OK")
        elif args[0] == "DISABLE":
            self.logger.debug("FILETRANSFER DISABLED")
            self.modem_online.clear()
            self.send_response_to_client("OK")
        else:
            self.cmd_format_error()
        return

    # PING Y FUNCIONES CORRESPONDIENTES (DELAY, RSSI y INTEGRITY)

    def get_ping_parameter(self):
        args = self.client_command.get_arguments()
        if len(args) != 2:
            self.cmd_format_error()
            return

        if not args[1].isnumeric():
            self.cmd_format_error()
            return

        # Special ping for autopower setting
        if args[0] == "POWER":
            if self.send_raw_msg(args[1], "pow"):
                self.send_response_to_client("PING OK")
                return
            else:
                self.send_response_to_client("PING FAILED")
                return
        # Normal ping
        if not self.send_im(args[1], "mwp", True):
            self.send_response_to_client("PING FAILED")
            return

        if args[0] == "DELAY":
            self.send_response_to_client("delay", self.get_propagation_time())
        elif args[0] == "RSSI":
            self.send_response_to_client("rssi", self.get_rssi())
        elif args[0] == "INTEGRITY":
            self.send_response_to_client("integrity", self.get_integrity())
        else:
            self.cmd_format_error()

    def get_propagation_time(self):
        at_command = 'AT?T'
        at_response = self.process_at_command(at_command)
        if at_response.is_error():
            self.modem_error_response(at_response, at_command)
            return

        return at_response.get_message()

    def get_rssi(self):
        at_command = 'AT?E'
        at_response = self.process_at_command(at_command)
        if at_response.is_error():
            self.modem_error_response(at_response, at_command)
            return

        return at_response.get_message()

    def get_integrity(self):
        at_command = 'AT?I'
        at_response = self.process_at_command(at_command)
        if at_response.is_error():
            self.modem_error_response(at_response, at_command)
            return

        return at_response.get_message()

    # FUNCIONES DE TRANSMISION DE INSTANT MESSAGES (GETMEAS y SENDMEAS)

    def get_meas(self):
        args = self.client_command.get_arguments()
        if len(args) != 2:
            self.cmd_format_error()
            return
        if not args[1].startswith("DESTINO="):
            self.cmd_format_error()
            return

        try:
            im_payload = Measure.getmeas_im_encode(args[0])
        except KeyError:
            self.cmd_format_error()
            return

        dest = args[1].replace("DESTINO=", '')
        if not self.send_im(dest, im_payload, ack=True):
            self.send_response_to_client("GETMEAS FAILED")
            return
        self.send_response_to_client("GETMEAS OK")
        return

    def get_file(self):
        args = self.client_command.get_arguments()
        if len(args) != 2:
            self.cmd_format_error()
            return
        if not args[1].startswith("DESTINO="):
            self.cmd_format_error()
            return

        try:
            im_payload = Measure.getfile_im_encode(args[0])
        except KeyError:
            self.cmd_format_error()
            return

        dest = args[1].replace("DESTINO=", '')
        if not self.send_im(dest, im_payload, ack=True):
            self.send_response_to_client("GETFILE FAILED")
            return
        self.send_response_to_client("GETFILE OK")
        return

    def send_meas(self):
        args = self.client_command.get_arguments()
        if len(args) != 2:
            self.cmd_format_error()
            return
        if not args[1].startswith("DESTINO="):
            self.cmd_format_error()
            return

        try:
            im_payload = Measure.setmeas_im_encode(args[0])
        except KeyError:
            self.cmd_format_error()
            return

        dest = args[1].replace("DESTINO=", '')
        if not self.send_im(dest, im_payload, ack=True):
            self.send_response_to_client("SENDMEAS FAILED")
            return
        self.send_response_to_client("SENDMEAS OK")
        return

    def send_raw(self):
        args = self.client_command.get_arguments()
        if len(args) < 2:
            self.cmd_format_error()
            return
        if not args[0].startswith("DESTINO="):
            self.cmd_format_error()
            return

        if not args[1].startswith("DATA="):
            self.cmd_format_error()
            return

        try:
            im_payload = Measure.sendraw_im_encode(self.client_command.get_raw_message())
        except KeyError:
            self.cmd_format_error()
            return

        dest = args[0].replace("DESTINO=", '')
        if not self.send_im(dest, im_payload, ack=True):
            self.send_response_to_client("SENDRAW FAILED")
            return
        self.send_response_to_client("SENDRAW OK")
        return

    # LISTADO DEL DIRECTORIO
    def get_dir(self):
        args = self.client_command.get_arguments()
        if len(args) < 1:
            self.cmd_format_error()
            return

        if not args[len(args) - 1].startswith("DESTINO="):
            self.cmd_format_error()
            return

        if len(args) == 1:
            im_payload = "ls"
        elif len(args) == 2 and args[0] == "FULL":
            im_payload = "lsf"
        else:
            self.cmd_format_error()
            return

        dest = args[len(args) - 1].replace("DESTINO=", '')
        if not self.send_im(dest, im_payload, ack=True):
            self.send_response_to_client("GETDIR FAILED")
            return
        self.send_response_to_client("GETDIR OK")
        return

    def send_dir(self):
        args = self.client_command.get_arguments()
        if len(args) < 1:
            self.cmd_format_error()
            return

        if not args[len(args) - 1].startswith("DESTINO="):
            self.cmd_format_error()
            return

        if len(args) == 1:
            os.system(f"ls {self.file_path} > {self.file_path}/dir.txt")
        elif len(args) == 2 and args[0] == "FULL":
            os.system(f"ls -g -o {self.file_path} > {self.file_path}/dir.txt")
        else:
            self.cmd_format_error()
            return
        self.logger.debug("dir.txt file GENERATED")

        dest = args[len(args) - 1].replace("DESTINO=", '')
        send_dir_cmd = ClientCommand(f"SENDFILE NOMBRE=dir.txt DESTINO={dest}")
        self.file_command_queue_rx.put(send_dir_cmd)

        file_handler_response: ClientCommandResponse = self.file_command_queue_tx.get()
        res: str = file_handler_response.get_entire_response()
        client_res = res.replace("SENDFILE", "SENDDIR")
        self.send_response_to_client(client_res)
        return

    # FUNCION DE ENVIO DE IM

    # Devuelve False si noack o ha fallado el ack, True si ack y el mensaje se recibio y confirmo
    def send_im(self, receiver_dir: str, data: str, ack: bool) -> bool:
        ack_str = "ack" if ack else "noack"
        command_chunks = ("AT*SENDIM", str(len(data)), receiver_dir, ack_str, data)
        at_command = ",".join(command_chunks)

        at_response = self.process_at_command(at_command)
        if at_response.is_error():
            self.modem_error_response(at_response, at_command)
            return False
        self.logger.debug("MSG SENT BY MODEM: " + at_command)

        if not ack:
            return False

        msg_deliver_status = self.wait_for_at_response()
        if msg_deliver_status.get_message().startswith("DELIVEREDIM"):
            self.logger.debug("MSG RECEIVED: " + msg_deliver_status.get_message())
            return True

        self.logger.debug("MSG FAILED: " + msg_deliver_status.get_message())
        return False

    # Devuelve False si noack o ha fallado el ack, True si ack y el mensaje se recibio y confirmo
    def send_raw_msg(self, receiver_dir: str, data: str) -> bool:
        command_chunks = ("AT*SEND", str(len(data)), receiver_dir, data)
        at_command = ",".join(command_chunks)

        at_response = self.process_at_command(at_command)
        if at_response.is_error():
            self.modem_error_response(at_response, at_command)
            return False
        self.logger.debug("MSG SENT BY MODEM: " + at_command)

        msg_deliver_status = self.wait_for_at_response()
        if msg_deliver_status.get_message().startswith("DELIVERED"):
            self.logger.debug("MSG RECEIVED: " + msg_deliver_status.get_message())
            return True

        self.logger.debug("MSG FAILED: " + msg_deliver_status.get_message())
        return False

    # ENVIO DE ARCHIVOS

    def send_file(self):
        file_handler_response: ClientCommandResponse

        args = self.client_command.get_arguments()
        if len(args) != 2:
            self.cmd_format_error()
            return
        if not args[0].startswith("NOMBRE=") or not args[1].startswith("DESTINO="):
            self.cmd_format_error()
            return
        if not args[1].split('=')[1].isnumeric():
            self.cmd_format_error()
            return

        self.file_command_queue_rx.put(self.client_command)
        file_handler_response = self.file_command_queue_tx.get()
        self.tcp_server_queue_tx.put(file_handler_response)

    # FUNCIONES DE BAJO CONSUMO REMOTAS
    def set_sleep(self):
        res: ModemMessage
        args = self.client_command.get_arguments()
        if len(args) != 1:
            self.cmd_format_error()
            return
        if not args[0].startswith("DESTINO="):
            self.cmd_format_error()
            return

        dest = args[0].replace("DESTINO=", '')
        if not self.send_raw_msg(dest, "slp"):
            self.logger.debug(f"Fallo el envío mensaje de solicitud de bajo consumo al modem {dest}")
            self.send_response_to_client("SETSLEEP FAILED")
            return
        self.send_response_to_client("SETSLEEP OK")
        return

    def set_wakeup(self):
        res: ModemMessage
        args = self.client_command.get_arguments()
        if len(args) != 1:
            self.cmd_format_error()
            return
        if not args[0].startswith("DESTINO="):
            self.cmd_format_error()
            return

        dest = args[0].replace("DESTINO=", '')
        if not self.send_raw_msg(dest, "wup"):
            self.logger.debug(f"Fallo el envío mensaje de solicitud de despertado al modem {dest}")
            self.send_response_to_client("SETWAKEUP FAILED")
            return
        self.send_response_to_client("SETWAKEUP OK")
        return

    def send_raw_msg(self, receiver_dir: str, data: str) -> bool:
        command_chunks = ("AT*SEND", str(len(data)), receiver_dir, data)
        at_command = ",".join(command_chunks)

        at_response = self.process_at_command(at_command)
        if at_response.is_error():
            return False
        self.logger.debug("MSG SENT BY MODEM: " + at_command)

        msg_deliver_status = self.wait_for_at_response()
        if msg_deliver_status.get_message().startswith("DELIVERED"):
            self.logger.debug("MSG RECEIVED: " + msg_deliver_status.get_message())
            return True

        self.logger.debug("MSG FAILED: " + msg_deliver_status.get_message())
        return False
