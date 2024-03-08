from logging import Logger
from queue import Queue, Empty
from threading import Thread, Event

from data_types import ModemMessage, Measure


class InterruptDispatcher(Thread):
    logger: Logger

    modem_interrupt_queue: Queue
    client_interrupt_queue: Queue

    queue_timeout: float

    kill_thread: Event

    def __init__(self, logger: Logger, modem_interrupt_queue: Queue, client_interrupt_queue: Queue,
                 queue_timeout: float, kill_thread: Event):
        super().__init__(daemon=True, name="interrupt_dispatcher")
        self.logger = logger
        self.modem_interrupt_queue = modem_interrupt_queue
        self.client_interrupt_queue = client_interrupt_queue
        self.queue_timeout = queue_timeout
        self.kill_thread = kill_thread

    def run(self) -> None:
        while True:
            try:
                raw_interrupt = self.modem_interrupt_queue.get(timeout=self.queue_timeout)
                self.process_interrupt(raw_interrupt)
            except Empty:
                pass
            if self.kill_thread.is_set():
                self.logger.debug("Interrupt dispatcher CLOSED!")
                return

    def process_interrupt(self, raw_interrupt: ModemMessage):
        raw_im_chunks = raw_interrupt.get_message_chunks()
        source_address = raw_im_chunks[2]
        im_payload = raw_im_chunks[9]

        if Measure.is_im_a_meas_msg(im_payload):
            formatted_msg = f"{Measure.meas_im_decode(im_payload)} ORIGEN={source_address}\r\n"
        elif Measure.is_im_a_file_request(im_payload):
            formatted_msg = f"{Measure.getfile_im_decode(im_payload)} ORIGEN={source_address}\r\n"
        elif Measure.is_im_a_raw_msg(im_payload):
            formatted_msg = f"{Measure.rawmsg_im_decode(im_payload)} ORIGEN={source_address}\r\n"
        elif Measure.is_im_a_list_dir_req(im_payload):
            formatted_msg = f"{Measure.listdir_im_decode(im_payload)} ORIGEN={source_address}\r\n"
        else:
            return

        self.client_interrupt_queue.put(formatted_msg)
        return
