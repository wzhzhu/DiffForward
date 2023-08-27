#!/usr/bin/python2
#coding:utf-8
import socket
from ..Utils.DataTrans import *
import struct
from ..Properties.config import *
import time
import gevent
from gevent.event import Event
import collections
import math
import logging
import os
import sys

class ForwardingClient:
    def __init__(self, volume_id, segment_id, ip, port, manager_ip, manager_port, busy_flag):
        logging.basicConfig(level=logging.INFO,
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )
        self.busy_flag = busy_flag
        self.busy = 0
        self.manager_ip = manager_ip
        self.manager_port = manager_port
        self.ip = ip 
        self.port = port
        # self.bandwidth_applier = bandwidth_applier
        self.volume_id = volume_id
        self.segment_id = segment_id
        self.state = ALIVE
        self.write_pending_queue = collections.deque()
        self.read_pending_queue = collections.deque()
        self.write_waiting_queue = dict()
        self.read_waiting_queue = dict()
        self.request_log = collections.deque()
        self.request_log_path = '/home/k8s/wzhzhu/my_balance/log/direct_latency'+str(volume_id)+'.csv'
        self.pending_traffic_log_path = '/home/k8s/wzhzhu/my_balance/log/pending_traffic'+str(volume_id)+'.csv'
        self.init_socket(ip, port)
        self.aaa = 0
        self.pending_write_traffic = 0
        self.pending_read_traffic = 0
        # lock
        self.waiting_socket = False
        self.socket_use = 0
        gevent.spawn(self.flush_write_loop)
        gevent.spawn(self.flush_read_loop)
        self.flush_write_event = Event()
        self.flush_read_event = Event()
        gevent.spawn(self.probe_loop)
        # gevent.spawn(self.count_pending_traffic)
        gevent.spawn(self.get_finished_write_loop)
        gevent.spawn(self.get_finished_read_loop)
        self.free_socket_pool_flag = time.time()
        self.pending_traffic_limit = 1024*1024*4

    '''创建连接req_server的socket保存在pool里'''
    def init_socket(self, ip, port):
        self.socket_num = 6
        self.free_socket_pool = collections.deque()
        self.socket_pool = collections.deque()
        for i in range(0, self.socket_num):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,True)
            s.connect((ip, port))
            self.free_socket_pool.append(s)
            self.socket_pool.append(s)

    def get_socket_helper(self):
        if len(self.free_socket_pool)!=0:
            s = self.free_socket_pool.popleft()
            return s
        # if self.socket_num < 10:
        #     self.socket_num += 1
        #     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #     s.connect((self.ip, self.port))
        #     self.socket_pool.append(s)
        #     return s
        return None

    def get_socket(self):
        s = self.get_socket_helper()
        while s is None:
            time.sleep(0.001)
            s = self.get_socket_helper()
        return s

    def set_req_event_queue(self, write_event_queue, read_event_queue):
        self.write_event_queue = write_event_queue
        self.read_event_queue = read_event_queue

    def count_pending_traffic(self):
        self.pending_traffic_log = collections.deque()
        while True:
            self.pending_traffic_log.append((self.pending_write_traffic+self.pending_read_traffic, time.time()*1000000))
            time.sleep(1)

    def flush_write_loop(self):
        send_list = list()
        while True:
            if self.state == ZOMBIE:
                time.sleep(0.001)
                continue
            if self.state == ZOMBIE:
                sys.exit(0)
            self.flush_write_event.wait()
            # self.busy_flag.add()
            s = self.get_socket()
            free_socket_pool_flag=self.free_socket_pool_flag
            report_flag = 0
            report_len = len(self.write_pending_queue)
            while len(self.write_pending_queue) != 0:
                send_list.clear()
                request = bytearray()
                request.extend(struct.pack('>BII', WRITE, self.volume_id, self.segment_id))
                req_id, sub_req_id, offset, length, data = self.write_pending_queue.popleft()
                req_head = struct.pack(">QIIIQI", req_id, sub_req_id, self.volume_id, self.segment_id, offset, length)
                request.extend(req_head)
                request.extend(data)
                try:
                    if report_flag == 0:
                        report_flag = 1
                        report_request = struct.pack('>BIIQ',REPORT_IN_QUEUE_WRITE, self.volume_id, self.segment_id, report_len)
                        # send(report_request, s)
                        send_list.append(report_request)

                    # while True:
                    #     event = self.write_event_queue[0]
                    #     if self.segment_id != event[0]:
                    #         event[1].wait()
                    #     else:
                    #         break
                    # send(request, s)
                    send_list.append(request)
                    self.write_waiting_queue[(req_id, sub_req_id, offset)] = (req_id, sub_req_id, offset, length, data)
                    self.pending_write_traffic += length
                    # self.request_log.append((self.volume_id, self.segment_id, req_id, sub_req_id, length, time.time()*1000000, 'None'))
                    # event[1].set()
                    # self.write_event_queue.popleft()
                    if len(self.write_pending_queue) == 0:
                        report_request = struct.pack('>BIIQ',REPORT_IN_QUEUE_WRITE, self.volume_id, self.segment_id, 0)
                        # send(report_request, s)
                        send_list.append(report_request)
                    
                    batch_send(send_list, s)

                except Exception as err:
                    self.write_pending_queue.appendleft((req_id, sub_req_id, offset, length, data))
                    if free_socket_pool_flag == self.free_socket_pool_flag:
                        self.free_socket_pool.append(s)
                    logging.info('original socket unvalid, reconnecting！{0}'.format(err))
                    time.sleep(0.001)
                    s = self.get_socket()
                    free_socket_pool_flag=self.free_socket_pool_flag
            if len(self.write_pending_queue)==0:
                self.flush_write_event.clear()
            if free_socket_pool_flag == self.free_socket_pool_flag:
                self.free_socket_pool.append(s)
            # self.busy_flag.sub()
            if len(self.write_pending_queue) + len(self.read_pending_queue) == 0 and self.busy == 1:
                self.busy_flag.sub()
                self.busy = 0

    def flush_read_loop(self):
        send_list = list()
        while True:
            if self.state == ZOMBIE:
                time.sleep(0.001)
                continue
            if self.state == ZOMBIE:
                sys.exit(0)
            self.flush_read_event.wait()
            # self.busy_flag.add()
            s = self.get_socket()
            free_socket_pool_flag = self.free_socket_pool_flag
            report_flag = 0
            report_len = len(self.read_pending_queue)
            while len(self.read_pending_queue) != 0:
                # if self.pending_read_traffic >= self.pending_traffic_limit:
                #     # logging.info('TOO MUCH PENDING TRAFFIC!!!')
                #     time.sleep(0.001)
                #     break
                send_list.clear()
                request = bytearray()
                request.extend(struct.pack('>BII',READ, self.volume_id, self.segment_id))
                req_id, sub_req_id, offset, length = self.read_pending_queue.popleft()
                req_head = struct.pack(">QIIIQI", req_id, sub_req_id, self.volume_id, self.segment_id, offset, length)
                request.extend(req_head)
                try:
                    if report_flag == 0:
                        report_flag = 1
                        report_request = struct.pack('>BIIQ',REPORT_IN_QUEUE_READ, self.volume_id, self.segment_id, report_len)
                        # send(report_request, s)
                        send_list.append(report_request)
                    # while True:
                    #     event = self.read_event_queue[0]
                    #     if self.segment_id != event[0]:
                    #         event[1].wait()
                    #     else:
                    #         break
                    # send(request, s)
                    send_list.append(request)
                    self.read_waiting_queue[(req_id, sub_req_id, offset)] = (req_id, sub_req_id, offset, length)
                    self.pending_read_traffic += length
                    # self.request_log.append((self.volume_id, self.segment_id, req_id, sub_req_id, length, time.time()*1000000, 'None'))
                    # event[1].set()
                    # self.read_event_queue.popleft()
                    if len(self.read_pending_queue) == 0:
                        report_request = struct.pack('>BIIQ',REPORT_IN_QUEUE_READ, self.volume_id, self.segment_id, 0)
                        # send(report_request, s)
                        send_list.append(report_request)

                    batch_send(send_list, s)

                except Exception as err:
                    self.read_pending_queue.appendleft((req_id, sub_req_id, offset, length))
                    if free_socket_pool_flag == self.free_socket_pool_flag:
                        self.free_socket_pool.append(s)
                    logging.info('original socket invalid, reconnecting！{0}'.format(err))
                    time.sleep(0.001)
                    s = self.get_socket()
                    free_socket_pool_flag=self.free_socket_pool_flag
            if len(self.read_pending_queue)==0:
                self.flush_read_event.clear()
            if free_socket_pool_flag == self.free_socket_pool_flag:
                self.free_socket_pool.append(s)
            # self.busy_flag.sub()
            if len(self.write_pending_queue) + len(self.read_pending_queue) == 0 and self.busy == 1:
                self.busy_flag.sub()
                self.busy = 0

    def get_finished_write_loop(self):
        self.get_finished_write_loop_socket_state = 0
        while True:
            if self.waiting_socket == True:
                time.sleep(0.001)
                continue
            if self.get_finished_write_loop_socket_state == 0:
                self.get_finished_write_socket = self.get_socket()
                free_socket_pool_flag = self.free_socket_pool_flag
                request = struct.pack('>BII',GET_FINISHED_WRITE, self.volume_id, self.segment_id)
                try:
                    send(request, self.get_finished_write_socket)
                    self.get_finished_write_loop_socket_state = 1
                except Exception as err:
                    logging.info("send_data error:{0}".format(err))
                    time.sleep(0.001)
            # self.socket_use += 1
            try:
                response = recv(self.get_finished_write_socket,self.busy_flag)
                # self.socket_use -= 1
                rsp_len = len(response)
                start = 0
                finish_time = time.time()*1000000
                while start < rsp_len:
                    req_id, sub_req_id, offset, state, log_head = struct.unpack('>QIQBQ', response[start:start+8+4+8+1+8])
                    start += 8+4+8+1+8
                    if state == SUCCEED:
                        req_id, sub_req_id, offset, length, data = self.write_waiting_queue[(req_id, sub_req_id, offset)]
                        del self.write_waiting_queue[(req_id, sub_req_id, offset)]
                        self.pending_write_traffic -= length
                        self.request_log.append((self.volume_id, self.segment_id, req_id, sub_req_id, length, finish_time, 'None'))
                    # self.index.rmLog(log_head)
                    # self.rmLog_flag = 0
            except Exception as err:
                logging.info("recv_data error:{0}".format(err))
                if free_socket_pool_flag == self.free_socket_pool_flag:
                    self.free_socket_pool.append(self.get_finished_write_socket)
                self.get_finished_write_loop_socket_state = 0
                time.sleep(0.001)

    def get_finished_read_loop(self):
        self.get_finished_read_loop_socket_state = 0
        while True:
            if self.waiting_socket == True:
                time.sleep(0.001)
                continue
            if self.get_finished_read_loop_socket_state == 0:
                self.get_finished_read_socket = self.get_socket()
                free_socket_pool_flag = self.free_socket_pool_flag
                request = struct.pack('>BII',GET_FINISHED_READ, self.volume_id, self.segment_id)
                try:
                    send(request, self.get_finished_read_socket)
                    self.get_finished_read_loop_socket_state = 1
                except Exception as err:
                    logging.info("send_data error:{0}".format(err))
                    time.sleep(0.001)
            # self.socket_use += 1
            try:
                response = recv(self.get_finished_read_socket,self.busy_flag)
                # self.socket_use -= 1
                rsp_len = len(response)
                start = 0
                finish_time = time.time()*1000000
                while start < rsp_len:
                    req_id, sub_req_id, offset, state, log_head = struct.unpack('>QIQBQ', response[start:start+8+4+8+1+8])
                    start += 8+4+8+1+8
                    if state == SUCCEED:
                        req_id, sub_req_id, offset, length = self.read_waiting_queue[(req_id, sub_req_id, offset)]
                        data = response[start:start+length]
                        start += length
                        del self.read_waiting_queue[(req_id, sub_req_id, offset)]
                        self.pending_read_traffic -= length
                        self.request_log.append((self.volume_id, self.segment_id, req_id, sub_req_id, length, finish_time, 'None'))
                    # self.index.rmLog(log_head)
                    # self.rmLog_flag = 0
            except Exception as err:
                logging.info("recv_data error:{0}".format(err))
                if free_socket_pool_flag == self.free_socket_pool_flag:
                    self.free_socket_pool.append(self.get_finished_read_socket)
                self.get_finished_read_loop_socket_state = 0
                time.sleep(0.001)

    def probe_loop(self):
        while True:
            if self.state == ZOMBIE:
                time.sleep(0.1)
                continue
            request = struct.pack('>BII',PROBE_STATE, self.volume_id, self.segment_id)
            s=self.get_socket()
            free_socket_pool_flag = self.free_socket_pool_flag
            try:
                send(request, s)
                response = recv(s)
                state = struct.unpack('>B',response[0:1])[0]
                if state == ERROR and self.state != ZOMBIE:
                    logging.info('state: zombie')
                    self.state = ZOMBIE
                    self.process_zombie()
                else:
                    pass
            except Exception as err:
                logging.info('unknown error！{0}'.format(err))
            finally:
                if free_socket_pool_flag == self.free_socket_pool_flag:
                    self.free_socket_pool.append(s)

    def read(self, req_id, sub_req_id, offset, length, timestamp):
        # request = struct.pack(">IIIQI", req_id, self.volume_id, self.segment_id, offset, length)
        self.read_pending_queue.append((req_id, sub_req_id, offset, length))
        self.request_log.append((self.volume_id, self.segment_id, req_id, sub_req_id, length, timestamp, self.ip))
        self.flush_read_event.set()
        if self.busy == 0:
            self.busy_flag.add()
            self.busy = 1
        return True

    def write(self, req_id, sub_req_id, offset, length, data, timestamp):
        # request_head = struct.pack(">IIIQI", req_id, self.volume_id, self.segment_id, offset, length)
        self.write_pending_queue.append((req_id, sub_req_id, offset, length, data))
        self.request_log.append((self.volume_id, self.segment_id, req_id, sub_req_id, length, timestamp, self.ip))
        self.flush_write_event.set()
        if self.busy == 0:
            self.busy_flag.add()
            self.busy = 1
        return True

    def get_log_head(self):
        s = self.get_socket()
        free_socket_pool_flag = self.free_socket_pool_flag
        request = struct.pack('>BIII', GET_LOG_HEAD, self.volume_id, self.segment_id, 0)
        # self.socket_use += 1
        try:
            send(request, s)
            response = recv(s)
            # self.socket_use -= 1
            log_head = struct.unpack('>Q',response[0:8])[0]
            if free_socket_pool_flag == self.free_socket_pool_flag:
                self.free_socket_pool.append(s)
            # if free_socket_pool_flag != self.free_socket_pool_flag:
            #     logging.info('error here')
            return log_head
        except Exception as err:
            logging.info('original socket invalid！')
            if free_socket_pool_flag == self.free_socket_pool_flag:
                self.free_socket_pool.append(s)
            # time.sleep(0.01)
            return -1

    def output_log(self):
        output_fd = open(self.request_log_path, 'a')
        if os.path.getsize(self.request_log_path) == 0:
            output_fd.write('disk,segment,req id,sub req id,length,time,node ip\n')
        while len(self.request_log) != 0:
            req = self.request_log.popleft()
            for i in range(0, 7):
                output_fd.write(str(req[i]))
                if i != 6:
                    output_fd.write(',')
            output_fd.write('\n')
        output_fd.flush()
        output_fd.close()

    def process_zombie(self):
        success_flag = 0
        while success_flag == 0:
            try:
                request = struct.pack('>BII',GET_FORWARDING_PROXY_INFO,self.volume_id,self.segment_id)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,True)
                s.connect((self.manager_ip,self.manager_port))
                send(request, s)
                response = recv(s)
                ip_len = struct.unpack('>I',response[0:4])[0]
                ip = response[4:4+ip_len].decode(encoding='utf-8')
                port = struct.unpack('>I', response[4+ip_len:4+ip_len+4])[0]
                self.ip = ip
                self.port = port
                logging.info('new ip {0}, port {1}'.format(ip, port))
                # self.waiting_socket = True
                # while self.socket_use != 0:
                #     time.sleep(0.001)
                while len(self.socket_pool)!=0:
                    s = self.socket_pool.popleft()
                    s.close()
                self.free_socket_pool = collections.deque()
                self.free_socket_pool_flag = time.time()
                logging.info('close all obselete sockets')
                socket_pool = collections.deque()
                free_socket_pool = collections.deque()
                for i in range(0, self.socket_num):
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,True)
                    s.connect((ip,port))
                    socket_pool.append(s)
                    free_socket_pool.append(s)
                self.socket_pool = socket_pool
                self.free_socket_pool = free_socket_pool
                self.waiting_socket = False
                logging.info('renew sockets successfully')
                self.get_finished_read_loop_socket_state = 0
                self.get_finished_write_loop_socket_state = 0
                for req in self.write_waiting_queue.values():
                    req_id, sub_req_id, offset, length, data = req
                    # logging.info('resubmit write {0} {1} {2} {3}'.format(req_id, volume_id, offset, length))
                    self.pending_write_traffic -= length
                for req in self.read_waiting_queue.values():
                    req_id, sub_req_id, offset, length = req
                    # logging.info('resubmit read {0} {1} {2} {3}'.format(req_id, volume_id, offset, length))
                    self.pending_read_traffic -= length
                self.write_pending_queue.extendleft(self.write_waiting_queue.values())
                self.read_pending_queue.extendleft(self.read_waiting_queue.values())
                if len(self.write_waiting_queue)!=0:
                    self.flush_write_event.set()
                if len(self.read_waiting_queue)!=0:
                    self.flush_read_event.set()
                self.write_waiting_queue = dict()
                self.read_waiting_queue = dict()
                logging.info('insert all unacknowledged requests to pending queue')
                self.state = ALIVE
                logging.info('all done！！')
                success_flag = 1
            except Exception as err:
                logging.info('process_zombie fail!! {0}'.format(err))
