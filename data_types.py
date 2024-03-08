import socket


class ModemConfig:
    connection_mode: str = ''

    inet_addr: str = ''
    inet_port: int = 0
    file_inet_port: int = 0

    com_port: str = ''
    baudrate: int = 0
    # Parametros de configuracion del Modem

    extended_protocol_mode: int = 0
    extended_notifications: int = 0
    pool_size: int = 0
    enable_usbl: int = 0

    hold_timeout: int = 0
    enable_awake_remote_mode: int = 0
    remote_active_time: int = 0

    # Parametros de configuracion de la transmision
    tx_power: int = 0
    tx_power_autoset: int = 0
    rx_gain: int = 0

    carrier_waveform_id: int = 0
    modem_address: int = 0
    max_address: int = 0

    cluster_size: int = 0
    packet_time: int = 0
    retry_count: int = 0
    retry_timeout: int = 0
    keep_online_count: int = 0
    idle_timeout: int = 0

    sound_speed: int = 0

    IM_retry_count: int = 0
    promiscuous_mode: int = 0

    at_config_dict = {
        "extended_protocol_mode": "AT@ZF",
        "extended_notifications": "AT@ZX",
        "pool_size": "AT@ZL",
        "enable_usbl": "AT@ZU",
        "hold_timeout": "AT!ZH",
        "enable_awake_remote_mode": "AT!DW",
        "remote_active_time": "AT!DR",
        "tx_power": "AT!L",
        "tx_power_autoset": "AT!LC",
        "rx_gain": "AT!G",
        "carrier_waveform_id": "AT!C",
        "modem_address": "AT!AL",
        "max_address": "AT!AM",
        "cluster_size": "AT!ZC",
        "packet_time": "AT!ZP",
        "retry_count": "AT!RC",
        "retry_timeout": "AT!RT",
        "keep_online_count": "AT!KO",
        "idle_timeout": "AT!ZI",
        "sound_speed": "AT!CA",
        "IM_retry_count": "AT!RI",
        "promiscuous_mode": "AT!RP"
    }


# FORMATO DE MEDIDAS
class Measure:
    #   Diccionario con los distintos tipos de medidas posibles
    meas_dict = {
        "TEMPERATURA": "temp",
        "PH": "ph",
        "SALINIDAD": "sal",
        "PRESION": "pres",
    }

    @staticmethod
    def is_im_a_meas_msg(instant_message: str) -> bool:
        return instant_message.startswith("g_") or instant_message.startswith("s_")

    @staticmethod
    def is_im_a_raw_msg(instant_message: str) -> bool:
        return instant_message.startswith("sr")

    @staticmethod
    def is_im_a_file_request(instant_message: str) -> bool:
        return instant_message.startswith("gf")

    @staticmethod
    def is_im_a_list_dir_req(instant_message: str) -> bool:
        return instant_message.startswith("ls")

    @staticmethod
    def getmeas_im_encode(measure: str) -> str:
        try:
            raw_response = f"g_{Measure.meas_dict[measure]}"
            return raw_response
        except KeyError as err:
            raise err

    @staticmethod
    def setmeas_im_encode(measure: str) -> str:
        meas_tokens = measure.split('=')
        if len(meas_tokens) != 2:
            raise KeyError
        if not meas_tokens[1]:
            raise KeyError

        try:
            raw_response = f"s_{Measure.meas_dict[meas_tokens[0]]} {meas_tokens[1]}"
            return raw_response
        except KeyError as err:
            raise err

    @staticmethod
    def getfile_im_encode(file: str) -> str:
        meas_tokens = file.split('=')
        if len(meas_tokens) != 2:
            raise KeyError
        if not meas_tokens[1]:
            raise KeyError
        if meas_tokens[0] != "NOMBRE":
            raise KeyError
        try:
            raw_response = f"gf {meas_tokens[1]}"
            return raw_response
        except KeyError as err:
            raise err

    @staticmethod
    def sendraw_im_encode(raw_data: str) -> str:
        msg_start = raw_data.find("DATA=") + 5
        msg_payload = raw_data[msg_start:]
        try:
            raw_response = f"sr {msg_payload}"
            return raw_response
        except KeyError as err:
            raise err

    @staticmethod
    def meas_im_decode(im_payload: str):
        if im_payload.startswith("g_"):
            response = Measure._get_key(im_payload[2:])
            if response is None:
                return None
            return f"GETMEAS {response}"
        elif im_payload.startswith("s_"):
            response_tokens = im_payload[2:].split(' ')
            response = Measure._get_key(response_tokens[0])
            if response is None:
                return None
            return f"SENDMEAS {response}={response_tokens[1]}"
        else:
            return None

    @staticmethod
    def rawmsg_im_decode(im_payload: str):
        raw_msg = im_payload[3:]  # Los primeros tres bytes son "sr "
        return f"SENDRAW DATA={raw_msg}"

    @staticmethod
    def getfile_im_decode(im_payload: str):
        return f"GETFILE NOMBRE={im_payload.split(' ')[1]}"

    @staticmethod
    def listdir_im_decode(im_payload: str):
        if im_payload == "ls":
            return "GETDIR"
        else:
            return "GETDIR FULL"

    @staticmethod
    def _get_key(val):
        keys = [k for k, v in Measure.meas_dict.items() if v == val]
        if keys:
            return keys[0]
        return None


# COMANDOS Y RESPUESTAS AT
class AtCommand:
    ETHERNET_EOL = '\n'

    def __init__(self, raw_at_command: str = '', communication_hardware: str = 'tcp'):
        if communication_hardware == 'tcp':
            self.at_command = raw_at_command + self.ETHERNET_EOL
        elif communication_hardware == 'rs232':
            self.at_command = raw_at_command

    def get(self):
        return self.at_command


class ModemMessage:
    raw_message: str

    def __init__(self, raw_message: str = ''):
        self.raw_message = raw_message.rstrip('\r\n')

    # INSTANT MESSAGES
    def is_ping_msg(self) -> bool:
        return self.raw_message.startswith('RECVIM') and self.raw_message.endswith(',mwp')

    def is_power_ping_msg(self) -> bool:
        return self.raw_message.startswith('RECV') and self.raw_message.endswith(',pow')

    def is_received_im(self) -> bool:
        return self.raw_message.startswith('RECVIM')

    # FILE TRANSMISSION
    def is_received_data(self) -> bool:
        return self.raw_message.startswith('RECV,')

    def is_transmission_request(self) -> bool:
        if not self.is_received_data():
            return False
        payload = self.get_message_chunks()[9]
        return payload.startswith('H')

    def is_ack(self) -> bool:
        if not self.is_received_data():
            return False
        payload = self.get_message_chunks()[9]
        return payload.startswith('ack')

    def is_nack(self) -> bool:
        if not self.is_received_data():
            return False
        payload = self.get_message_chunks()[9]
        return payload.startswith('nack')

    # REMOTE SLEEP CONTROL
    def is_sleep_request(self):
        if not self.is_received_data():
            return False
        payload = self.get_message_chunks()[9]
        return payload.startswith('slp')

    def is_wakeup_request(self):
        if not self.is_received_data():
            return False
        payload = self.get_message_chunks()[9]
        return payload.startswith('wup')

    # USBL DATA
    def is_position_data(self) -> bool:
        return self.raw_message.startswith('USBL')

    def is_error(self) -> bool:
        return self.raw_message.startswith('ERROR')

    def get_message(self) -> str:
        return self.raw_message

    def get_message_chunks(self):
        return self.raw_message.split(',')


# Del cliente se reciben ClientCommand y se le mandan ClientCommandResponse
class ClientCommand:

    def __init__(self, raw_message: str):
        self.formatted_message = raw_message[:len(raw_message) - 2]
        self.message_chunks = self.formatted_message.split(' ', -1)

    def get_command(self) -> str:
        return self.message_chunks[0]

    def get_arguments(self):
        return self.message_chunks[1:]

    def get_raw_message(self):
        return self.formatted_message


class ClientCommandResponse:
    EOL_RESPONSE = '\n\r'

    def __init__(self, type_id: str = '', value=''):
        self.command_response = ''
        self.add_response_line(type_id, value)

    def add_response_line(self, type_id: str = '', value=''):
        self.command_response = type_id.upper()

        string_value = str(value).upper()
        if len(string_value) > 0:
            self.command_response += '=' + string_value

        self.command_response += self.EOL_RESPONSE

    def get_entire_response(self):
        return self.command_response


# Clase que representa el par IP/Puerto de un soket
class SocketAddress:
    port: int = 0
    ip_address: str = ''

    def __init__(self, ip_address: str = '', port: int = 0):
        self.port = port

        if not is_ip(ip_address):
            try:
                self.ip_address = socket.gethostbyname(ip_address)
            except OSError:
                raise OSError("No se pudo resolver el nombre de dominio a ni ninguna IP a traves de DNS")
        else:
            self.ip_address = ip_address


def is_ip(ip_address):
    return ip_address.replace('.', '').isnumeric()


def is_port(port):
    return port.isnumeric()
