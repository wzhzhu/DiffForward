#!/usr/bin/python2
#coding:utf-8
from ..Utils.DataTrans import *
import struct
from ..Properties.config import *
import collections
import gevent
import socket
import os
import time
import math
import logging
from gevent.event import Event
from memory_profiler import profile
import random


class BufferClient:
    '''**********************************************************************************************
        init
    **********************************************************************************************'''
    def __init__(self, volume_id, manager_ip, manager_port, buffer_server_list, buffer_writing_manner, busy_flag):
        logging.basicConfig(level=logging.INFO,
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )
        self.busy_flag = busy_flag
        self.buffer_server_id = random.randint(0,4)
        #segment info
        self.volume_id = int(volume_id)
        #connect manager and save the socket
        self.manager_ip = manager_ip
        self.manager_port = int(manager_port)
        self.set_manager()
        #connect buffer server，and save the socket
        self.buffer_server_list = buffer_server_list
        self.init_buffer_server_socket()
        # segment log tail
        self.init_buffer_status()
        #cache log history for later index recovering
        self.segment_log_meta = collections.deque()
        #all sorts of used queues
        self.write_pending_queue = dict()
        for i in range(0, len(self.buffer_server_list)):
            self.write_pending_queue[i] = collections.deque()
        self.write_waiting_queue = dict()
        self.read_pending_queue = dict()
        for i in range(0, len(self.buffer_server_list)):
            self.read_pending_queue[i] = collections.deque()
        self.read_waiting_queue = dict()
        self.write_log_path = '/home/k8s/wzhzhu/DiffForward/log/buffer_latency'+str(self.volume_id)+'.csv'
        self.write_log = collections.deque()
        self.buffer_writing_manner = buffer_writing_manner
        #Events
        self.flush_write_event = dict()
        self.flush_read_event = dict()
        for i in range(0, len(self.buffer_server_list)):
            self.flush_write_event[i] = Event()
            self.flush_read_event[i] = Event() 
        #Threads
        for i in range(0, len(self.buffer_server_list)):
            gevent.spawn(self.flush_loop, i)
            gevent.spawn(self.flush_read_loop, i)
            gevent.spawn(self.get_finished_read_loop, i)
            gevent.spawn(self.get_finished_write_loop, i)
        # gevent.spawn(self.sync_server_free_band)
        self.busy = 0

    def set_manager(self):
        self.manager_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.manager_socket.connect((self.manager_ip,self.manager_port))

    def init_buffer_server_socket(self):
        logging.info('buffer server info')
        logging.info(self.buffer_server_list)
        self.socket_num = dict()
        self.buffer_server_socket_pool = dict()
        self.buffer_server_socket_num = 1
        for i in range(0, len(self.buffer_server_list)):
            self.socket_num[i] = self.buffer_server_socket_num
            self.buffer_server_socket_pool[i] = collections.deque()
            for j in range(0, self.buffer_server_socket_num):
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # s.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,True)
                s.connect((self.buffer_server_list[i][0],self.buffer_server_list[i][1]))
                self.buffer_server_socket_pool[i].append(s)

    def get_socket(self, buffer_server_id):
        if len(self.buffer_server_socket_pool[buffer_server_id])!=0:
            s = self.buffer_server_socket_pool[buffer_server_id].popleft()
            return s
        if self.socket_num[buffer_server_id] < 5:
            self.socket_num[buffer_server_id] += 1
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # s.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,True)
            s.connect((self.buffer_server_list[buffer_server_id][0],self.buffer_server_list[buffer_server_id][1]))
            return s
        return None

    def init_buffer_status(self):
        buffer_server_num = len(self.buffer_server_list)
        self.file_id = dict()
        self.file_offset = dict()
        #used file_id and file_offset in buffer servers
        for i in range(0, buffer_server_num):
            self.file_id[i] = dict()
            self.file_offset[i] = dict()
        #next log id to be allocated
        self.segment_log_id = dict()
        #servers' estimated free bandwidth
        self.buffer_server_free_band = dict()
        for i in range(0, buffer_server_num):
            self.buffer_server_free_band[i] = 1
        self.segment_traffic_size = dict()

    def sync_server_free_band(self):
        while True:
            request = struct.pack('>B',GET_FREE_BANDWIDTH)
            send(request,self.manager_socket)
            response = recv(self.manager_socket)
            start = 0
            server_id = 0
            while start < len(response):
                self.buffer_server_free_band[server_id] = struct.unpack('>I',response[start:start+4])[0]
                start += 4
                server_id += 1
            time.sleep(10)
            

    def buffer_server_write(self, req_id, sub_req_id, volume_id, segment_id, log_id, offset, length, buffer_server_id, file_id, file_offset, data, index):
        # request_head = struct.pack('>QIIIQQIII', req_id, sub_req_id, volume_id, segment_id, log_id, offset, length, file_id, file_offset)
        request_head = (req_id, sub_req_id, volume_id, segment_id, log_id, offset, length, file_id, file_offset)
        self.write_pending_queue[buffer_server_id].append((request_head, data, buffer_server_id, index))
        self.write_waiting_queue[(req_id, sub_req_id)] = (segment_id, log_id, offset, length, file_id, file_offset, index)
        if self.busy == 0:
            self.busy_flag.add()
            self.busy = 1
        

    def buffer_server_read(self, req_id, sub_req_id, volume_id, segment_id, log_id, offset, length, buffer_server_id, file_id, file_offset, on_read_obsolete, paras):
        # request = struct.pack('>QIIIQQIII', req_id, sub_req_id, volume_id, segment_id, log_id, offset, length, file_id, file_offset)
        request = (req_id, sub_req_id, volume_id, segment_id, log_id, offset, length, file_id, file_offset)
        self.read_pending_queue[buffer_server_id].append((request, on_read_obsolete, paras, buffer_server_id))
        self.read_waiting_queue[(req_id, sub_req_id, log_id, offset)] = (segment_id, on_read_obsolete, paras)
        if self.busy == 0:
            self.busy_flag.add()
            self.busy = 1

    def flush_loop(self, buffer_server_id):
        try:
            s = self.get_socket(buffer_server_id)
            send_list = list()
            while True:
                self.flush_write_event[buffer_server_id].wait()
                # if busy_flag == 0:
                #     self.busy_flag.add()
                #     busy_flag = 1
                # report queue length
                send_list.clear()
                report_len = len(self.write_pending_queue[buffer_server_id])
                report_request = struct.pack('>BIQ',REPORT_BUFFER_IN_QUEUE_WRITE, self.volume_id, report_len)
                # send(report_request, s)
                send_list.append(report_request)
                while len(self.write_pending_queue[buffer_server_id]) != 0:
                    request_head, data, buffer_server_id, index = self.write_pending_queue[buffer_server_id].popleft()
                    req_id, sub_req_id, volume_id, segment_id, log_id, offset, length, file_id, file_offset = request_head
                    request = bytearray()
                    request.extend(struct.pack('>BI',BUFFER_WRITE, self.volume_id))
                    request.extend(struct.pack('>QIIIQQIII', req_id, sub_req_id, volume_id, segment_id, log_id, offset, length, file_id, file_offset))
                    request.extend(data)
                    # send(request, s)
                    send_list.append(request)
                # report queue length
                report_len = 0
                report_request = struct.pack('>BIQ',REPORT_BUFFER_IN_QUEUE_WRITE, self.volume_id, report_len)
                # send(report_request, s)
                send_list.append(report_request)
                batch_send(send_list, s)
                if len(self.write_pending_queue[buffer_server_id]) == 0:
                    self.flush_write_event[buffer_server_id].clear()
                    # self.busy_flag.sub()
                    # busy_flag = 0
        except Exception as err:
            logging.info(err)

    def flush_read_loop(self, buffer_server_id):
        try:
            # busy_flag = 0
            s = self.get_socket(buffer_server_id)
            send_list = list()
            while True:
                self.flush_read_event[buffer_server_id].wait()
                # if busy_flag == 0:
                #     self.busy_flag.add()
                #     busy_flag = 1
                # report queue length
                send_list.clear()
                report_len = len(self.read_pending_queue[buffer_server_id])
                report_request = struct.pack('>BIQ',REPORT_BUFFER_IN_QUEUE_READ,self.volume_id,report_len)
                # send(report_request, s)
                send_list.append(report_request)
                while len(self.read_pending_queue[buffer_server_id]) != 0:
                    request_head, on_read_obsolete, paras, buffer_server_id = self.read_pending_queue[buffer_server_id].popleft()
                    req_id, sub_req_id, volume_id, segment_id, log_id, offset, length, file_id, file_offset = request_head
                    request = bytearray()
                    request.extend(struct.pack('>BI',BUFFER_READ, self.volume_id))
                    request.extend(struct.pack('>QIIIQQIII', req_id, sub_req_id, volume_id, segment_id, log_id, offset, length, file_id, file_offset))
                    # send(request, s)
                    send_list.append(request)
                # report queue length
                report_len = 0
                report_request = struct.pack('>BIQ',REPORT_BUFFER_IN_QUEUE_READ, self.volume_id, report_len)
                # send(report_request, s)
                send_list.append(report_request)
                batch_send(send_list, s)
                if len(self.read_pending_queue[buffer_server_id]) == 0:
                    self.flush_read_event[buffer_server_id].clear()
                    # self.busy_flag.sub()
                    # busy_flag = 0
        except Exception as err:
            logging.info(err)

    def get_finished_write_loop(self, buffer_server_id):
        s = self.get_socket(buffer_server_id)
        request = struct.pack('>BI',GET_BUFFER_FINISHED_WRITE, self.volume_id)
        send(request, s)
        try:
            while True:
                response = recv(s)
                rsp_len = len(response)
                start = 0
                # finish_time = time.time()*1000000
                while start < rsp_len:
                    req_id, sub_req_id = struct.unpack('>QI', response[start:start+8+4])
                    start += 8+4
                    segment_id, log_id, offset, length, file_id, file_offset, index = self.write_waiting_queue[(req_id, sub_req_id)]
                    # index.insert(log_id, offset, length, file_id, file_offset, buffer_server_id)
                    del self.write_waiting_queue[(req_id, sub_req_id)]
                    finish_time = time.time()*1000000
                    self.write_log.append((self.volume_id, segment_id, req_id, sub_req_id, length, finish_time, 'None'))
                if len(self.write_waiting_queue) + len(self.read_waiting_queue) == 0 and self.busy == 1:
                    self.busy_flag.sub()
                    self.busy = 0
        except Exception as err:
            logging.info(err)
            with open('aaa.txt','w') as out_file:
                out_file.write(str(self.volume_id)+str(err))

    def get_finished_read_loop(self, buffer_server_id):
        #连接socket
        s = self.get_socket(buffer_server_id)
        request = struct.pack('>BI',GET_BUFFER_FINISHED_READ, self.volume_id)
        send(request, s)
        try:
            while True:
                response = recv(s)
                rsp_len = len(response)
                start = 0
                # finish_time = time.time()*1000000
                while start < rsp_len:
                    req_id, sub_req_id, log_id, offset, length = struct.unpack('>QIQQI', response[start:start+8+4+8+8+4])
                    start += 8+4+8+8+4
                    if length == 0:
                        flag = struct.unpack('>I',response[start:start+4])[0]
                        start+=4
                        segment_id, on_read_obsolete, paras = self.read_waiting_queue[(req_id, sub_req_id, log_id, offset)]
                        del self.read_waiting_queue[(req_id, sub_req_id, log_id, offset)]
                        on_read_obsolete(paras, flag)
                    else:
                        data = response[start:start+length]
                        start += length
                        segment_id, on_read_obsolete, paras = self.read_waiting_queue[(req_id, sub_req_id, log_id, offset)]
                        del self.read_waiting_queue[(req_id, sub_req_id, log_id, offset)]
                        finish_time = time.time()*1000000
                        self.write_log.append((self.volume_id, segment_id, req_id, sub_req_id, length, finish_time, 'None'))
                if len(self.write_waiting_queue) + len(self.read_waiting_queue) == 0 and self.busy == 1:
                    self.busy_flag.sub()
                    self.busy = 0
        except Exception as err:
            logging.info(err)

    def buffer_write(self, segment_id, req_id, sub_req_id, offset, length, data, timestamp, index):
        if self.buffer_writing_manner == 'rb1':
            return self.buffer_write_rb1(segment_id, req_id, sub_req_id, offset, length, data, timestamp, index)
        elif self.buffer_writing_manner == 'rb2':
            return self.buffer_write_rb2(segment_id, req_id, sub_req_id, offset, length, data, timestamp, index)
        elif self.buffer_writing_manner == 'rb3':
            return self.buffer_write_rb3(segment_id, req_id, sub_req_id, offset, length, data, timestamp, index)

    def buffer_write_rb1(self, segment_id, req_id, sub_req_id, offset, length, data, timestamp, index):
        if segment_id not in self.segment_log_id:
            self.segment_log_id[segment_id] = 0
        buffer_server_id = self.segment_log_id[segment_id]%len(self.buffer_server_list)
        if segment_id not in self.file_id[buffer_server_id]:
            self.file_id[buffer_server_id][segment_id] = 0
        if segment_id not in self.file_offset[buffer_server_id]:
            self.file_offset[buffer_server_id][segment_id] = 0
        file_id = self.file_id[buffer_server_id][segment_id]
        file_offset = self.file_offset[buffer_server_id][segment_id]
        self.write_log.append((self.volume_id, segment_id, req_id, sub_req_id, length, timestamp, self.buffer_server_list[buffer_server_id][0]))
        self.buffer_server_write(req_id, sub_req_id, self.volume_id, segment_id, self.segment_log_id[segment_id], offset, length, buffer_server_id, file_id, file_offset, data, index)
        return_value =(self.segment_log_id[segment_id], offset, length, file_id, file_offset, buffer_server_id)
        self.segment_log_id[segment_id] = self.segment_log_id[segment_id] + 1
        self.file_offset[buffer_server_id][segment_id] = self.file_offset[buffer_server_id][segment_id] + int(math.ceil(length/float(4096)))*4096
        if self.file_offset[buffer_server_id][segment_id] >= 1024*1024:
            self.file_id[buffer_server_id][segment_id] =  self.file_id[buffer_server_id][segment_id] + 1
            self.file_offset[buffer_server_id][segment_id] = 0
        self.flush_write_event[buffer_server_id].set()
        return return_value
    
    def buffer_write_rb2(self, segment_id, req_id, sub_req_id, offset, length, data, timestamp, index):
        buffer_server_num = len(self.buffer_server_list)
        if segment_id not in self.segment_log_id:
            self.segment_log_id[segment_id] = 0
            self.segment_traffic_size[segment_id] = list()
            for i in range(0, buffer_server_num):
                self.segment_traffic_size[segment_id].append(0)
        min_server = 0
        min_norm_traffic = self.segment_traffic_size[segment_id][0]/self.buffer_server_free_band[0]
        for i in range(1, buffer_server_num):
            if self.segment_traffic_size[segment_id][i]/self.buffer_server_free_band[i] < min_norm_traffic:
                min_norm_traffic = self.segment_traffic_size[segment_id][i]/self.buffer_server_free_band[i]
                min_server = i
        self.segment_traffic_size[segment_id][min_server] += length
        buffer_server_id = min_server
        if segment_id not in self.file_id[buffer_server_id]:
            self.file_id[buffer_server_id][segment_id] = 0
        if segment_id not in self.file_offset[buffer_server_id]:
            self.file_offset[buffer_server_id][segment_id] = 0
        file_id = self.file_id[buffer_server_id][segment_id]
        file_offset = self.file_offset[buffer_server_id][segment_id]
        self.write_log.append((self.volume_id, segment_id, req_id, sub_req_id, length, timestamp, self.buffer_server_list[buffer_server_id][0]))
        self.buffer_server_write(req_id, sub_req_id, self.volume_id, segment_id, self.segment_log_id[segment_id], offset, length, buffer_server_id, file_id, file_offset, data, index)
        return_value =(self.segment_log_id[segment_id], offset, length, file_id, file_offset, buffer_server_id)
        self.segment_log_id[segment_id] = self.segment_log_id[segment_id] + 1
        self.file_offset[buffer_server_id][segment_id] = self.file_offset[buffer_server_id][segment_id] + int(math.ceil(length/float(4096)))*4096
        if self.file_offset[buffer_server_id][segment_id] >= 1024*1024:
            self.file_id[buffer_server_id][segment_id] =  self.file_id[buffer_server_id][segment_id] + 1
            self.file_offset[buffer_server_id][segment_id] = 0
        self.flush_write_event[buffer_server_id].set()
        return return_value

    def buffer_write_rb3(self, segment_id, req_id, sub_req_id, offset, length, data, timestamp, index):
        if segment_id not in self.segment_log_id:
            self.segment_log_id[segment_id] = 0
        buffer_server_id = self.buffer_server_id
        if segment_id not in self.file_id[buffer_server_id]:
            self.file_id[buffer_server_id][segment_id] = 0
        if segment_id not in self.file_offset[buffer_server_id]:
            self.file_offset[buffer_server_id][segment_id] = 0
        file_id = self.file_id[buffer_server_id][segment_id]
        file_offset = self.file_offset[buffer_server_id][segment_id]
        self.write_log.append((self.volume_id, segment_id, req_id, sub_req_id, length, timestamp, self.buffer_server_list[buffer_server_id][0]))
        self.buffer_server_write(req_id, sub_req_id, self.volume_id, segment_id, self.segment_log_id[segment_id], offset, length, buffer_server_id, file_id, file_offset, data, index)
        return_value =(self.segment_log_id[segment_id], offset, length, file_id, file_offset, buffer_server_id)
        self.segment_log_id[segment_id] = self.segment_log_id[segment_id] + 1
        self.file_offset[buffer_server_id][segment_id] = self.file_offset[buffer_server_id][segment_id] + int(math.ceil(length/float(4096)))*4096
        if self.file_offset[buffer_server_id][segment_id] >= 1024*1024:
            self.file_id[buffer_server_id][segment_id] =  self.file_id[buffer_server_id][segment_id] + 1
            self.file_offset[buffer_server_id][segment_id] = 0
        self.flush_write_event[buffer_server_id].set()
        return return_value

    def buffer_read(self, segment_id, req_id, sub_req_id, log_id, offset, length, file_id, file_offset, buffer_server_id, timestamp, on_read_obsolete, paras):
        self.write_log.append((self.volume_id, segment_id, req_id, sub_req_id, length, timestamp, self.buffer_server_list[buffer_server_id][0]))
        self.buffer_server_read(req_id, sub_req_id, self.volume_id, segment_id, log_id, offset, length, buffer_server_id, file_id, file_offset, on_read_obsolete, paras)
        self.flush_read_event[buffer_server_id].set()

    def output_log(self):
        latency_dict = dict()
        output_fd = open(self.write_log_path, 'a')
        if os.path.getsize(self.write_log_path) == 0:
            output_fd.write('disk,segment,req id,sub req id,length,time,node ip\n')
        while len(self.write_log) != 0:
            req = self.write_log.popleft()
            for i in range(0, 7):
                output_fd.write(str(req[i]))
                if i != 6:
                    output_fd.write(',')
            output_fd.write('\n')
        output_fd.flush()
        output_fd.close()