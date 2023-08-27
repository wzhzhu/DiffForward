#!/usr/bin/python2
#coding:utf-8
from gevent import monkey;monkey.patch_all()
import sys
import gevent
import socket
from gevent.event import Event
from ..Properties.config import *
import getopt
from ..Utils.DataTrans import *
import struct
import psutil, os
import logging

p = psutil.Process(os.getpid())

class MergeSpeedController:
    '''**********************************************************************************************
        init
    **********************************************************************************************'''
    def __init__(self):
        logging.basicConfig(level=logging.INFO,
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )
        self.server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        serverAddr = '/home/k8s/wzhzhu/merge_ctrl_uds'
        if os.path.exists(serverAddr):
            os.unlink(serverAddr)
        self.server_socket.bind(serverAddr)
        #禁用nagle算法
        # self.server_socket.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,True)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.listen(1000000)

        # 队列阻塞状态,每个segment单独维护,以支持动态更改
        self.direct_in = dict()
        self.direct_out = dict()
        self.buffer_in = dict()
        self.buffer_out = dict()
        self.storage = dict()
        # 队列的长度,即所有segment队列中元素的总和
        self.direct_in_sum = 0
        self.direct_out_sum = 0
        self.buffer_in_sum = 0
        self.buffer_out_sum = 0
        self.storage_sum = 0
        # 队列长度cross阈值时触发的事件,事件触发后会向所有订阅连接发送信息
        self.in_state_change_events = set()
        self.out_state_change_events = set()
        # 预设队列阈值
        self.in_threshold = 0
        self.out_threshold = 0
        # 队列状态,用于判断是否越过阈值
        self.in_idle = False
        self.out_idle = False

    def run(self):
        self.serve()

    def serve(self):
        while True:
            client_socket, client_addr = self.server_socket.accept()     # 等待客户端连接
            gevent.spawn(self.recv_application, client_socket, client_addr)

    def recv_application(self, client_socket, client_addr):
        try:
            while True:
                request = recv(client_socket)
                if not request:
                    print("{} offline".format(client_addr))
                    client_socket.close()
                    break
                op = struct.unpack('>B', request[0:1])[0]
                if op == REPORT_DIRECT_IN:
                    self.report_direct_in(request[1:], client_socket)
                elif op == REPORT_DIRECT_OUT:
                    self.report_direct_out(request[1:], client_socket)
                elif op == REPORT_BUFFER_IN:
                    self.report_buffer_in(request[1:], client_socket)
                elif op == REPORT_BUFFER_OUT:
                    self.report_buffer_out(request[1:], client_socket)
                elif op == REPORT_STORAGE:
                    self.report_storage(request[1:], client_socket)
                elif op == GET_IN:
                    self.get_in(request[1:], client_socket)
                elif op == GET_OUT:
                    self.get_out(request[1:], client_socket)
        except Exception as err:
            logging.info("recv_data error:{}".format(err))
        finally:
            logging.info('socket close!, addr = {0}, op = {1}'.format(client_addr, op))
            client_socket.close()

    def report_direct_in(self, request, client_socket):
        try:
            volume_id, segment_id, queue_len = struct.unpack('>IIQ', request[0:4+4+8])
            if (volume_id, segment_id) in self.direct_in:
                self.direct_in_sum -= self.direct_in[(volume_id, segment_id)]
            self.direct_in[(volume_id, segment_id)] = queue_len
            self.direct_in_sum += queue_len
            self.judge_in_state()
        except Exception as err:
            logging.info(err)

    def report_direct_out(self, request, client_socket):
        try:
            volume_id, segment_id, queue_len = struct.unpack('>IIQ', request[0:4+4+8])
            if (volume_id, segment_id) in self.direct_out:
                self.direct_out_sum -= self.direct_out[(volume_id, segment_id)]
            self.direct_out[(volume_id, segment_id)] = queue_len
            self.direct_out_sum += queue_len
            self.judge_out_state()
        except Exception as err:
            logging.info(err)

    def report_buffer_in(self, request, client_socket):
        try:
            volume_id, queue_len = struct.unpack('>IQ', request[0:4+8])
            if volume_id in self.buffer_in:
                self.buffer_in_sum -= self.buffer_in[volume_id]
            self.buffer_in[volume_id] = queue_len
            self.buffer_in_sum += queue_len
            self.judge_in_state()
        except Exception as err:
            logging.info(err)

    def report_buffer_out(self, request, client_socket):
        try:
            volume_id, queue_len = struct.unpack('>IQ', request[0:4+8])
            if volume_id in self.buffer_out:
                self.buffer_out_sum -= self.buffer_out[volume_id]
            self.buffer_out[volume_id] = queue_len
            self.buffer_out_sum += queue_len
            self.judge_out_state()
        except Exception as err:
            logging.info(err)

    def report_storage(self, request, client_socket):
        try:
            storage_server_id, queue_len = struct.unpack('>IQ', request[0:4+8])
            if storage_server_id in self.storage:
                self.storage_sum -= self.storage[storage_server_id]
            self.storage[storage_server_id] = queue_len
            self.storage_sum += queue_len
            self.judge_in_state()
            self.judge_out_state()
        except Exception as err:
            logging.info(err)

    def judge_in_state(self):
        if self.in_idle and self.direct_in_sum + self.buffer_in_sum + self.storage_sum > self.in_threshold:
            self.in_idle = False
            self.notify_all_in()
        elif not self.in_idle and self.direct_in_sum + self.buffer_in_sum + self.storage_sum <= self.in_threshold:
            self.in_idle = True
            self.notify_all_in()

    def judge_out_state(self):
        if self.out_idle and self.direct_out_sum + self.buffer_out_sum + self.storage_sum > self.out_threshold:
            self.out_idle = False
            self.notify_all_out()
        elif not self.out_idle and self.direct_out_sum + self.buffer_out_sum + self.storage_sum  <= self.out_threshold:
            self.out_idle = True
            self.notify_all_out()

    def notify_all_in(self):
        for e in self.in_state_change_events:
            e.set()

    def notify_all_out(self):
        for e in self.out_state_change_events:
            e.set()


    def get_in(self, request, client_socket):
        in_state_change_event = Event()
        self.in_state_change_events.add(in_state_change_event)
        in_state_change_event.set()
        try:
            while True:
                in_state_change_event.wait()
                if self.in_idle:
                    request = struct.pack('>B',SUCCEED)
                else:
                    request = struct.pack('>B',ERROR)
                in_state_change_event.clear()
                send(request, client_socket)
        except Exception as err:
            logging.info('断开过期get in socket')
            self.in_state_change_events.remove(in_state_change_event)

    def get_out(self, request, client_socket):
        out_state_change_event = Event()
        self.out_state_change_events.add(out_state_change_event)
        out_state_change_event.set()
        try:
            while True:
                out_state_change_event.wait()
                if self.out_idle:
                    request = struct.pack('>B',SUCCEED)
                else:
                    request = struct.pack('>B',ERROR)
                out_state_change_event.clear()
                send(request, client_socket)
        except Exception as err:
            logging.info('断开过期get out socket')
            self.out_state_change_events.remove(out_state_change_event)

def main(argv):
    # p.nice(-20)
    opts, args = getopt.getopt(argv,"h:",["help="])
    for opt, arg in opts:
        if opt == '-h':
            print('MergeSpeedController.py -p <port>')
            sys.exit()
    my_merge_speed_controller = MergeSpeedController()
    my_merge_speed_controller.run()

if __name__ == "__main__":
    main(sys.argv[1:])



