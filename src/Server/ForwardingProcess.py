#!/usr/bin/python2
#coding:utf-8
from gevent import monkey;monkey.patch_all()
import sys
import socket
import gevent
from gevent.event import Event
from ..Properties.config import *
import time
import getopt
from ..Utils.DataTrans import *
import struct
import psutil, os
from heapq import *
import collections
import math
import logging
import signal
import random
import gc
gc.disable()

p = psutil.Process(os.getpid())
proxy_dict = dict()
server_socket = forwarding_server_id = manager_ip = manager_port = with_ceph = storage_server_port = storage_server_id = port = process_id = None

class ForwardingProxy:
    '''**********************************************************************************************
        init
    **********************************************************************************************'''
    def __init__(self, volume_id, segment_id, manager_ip, manager_port, with_ceph, storage_server_port, storage_server_id, port):
        logging.basicConfig(level=logging.INFO,
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )
        #本req server状态，alive or zombie
        self.state = ALIVE
        # 网络参数
        self.port = port
        #本req server负责的虚拟盘id
        self.volume_id = int(volume_id)
        self.segment_id = int(segment_id)
        #保存异步拉取来的日志，按log id排序
        self.log_queue = list()
        #下一个应该被合并到存储层的log_id
        self.log_head = 0
        self.next_merge_log_head = 0
        self.report_log_head_event = Event()
        #初始化
        self.manager_ip = manager_ip
        self.manager_port = int(manager_port)
        self.set_manager(manager_ip, manager_port)
        self.set_buffer_server()
        self.init_buffer_server_socket()
        self.init_merge_speed_controller_socket()
        #正在执行的req_num
        self.pending_req_num = 0
        #正在合并的req_num
        self.pending_merge_num = 0
        #读写请求待发送到存储层的队列
        self.write_temp_queue = collections.deque()
        self.read_temp_queue = collections.deque()
        #读写请求完成队列
        self.write_finish_queue = collections.deque()
        self.read_finish_queue = collections.deque()

        self.with_ceph = True if with_ceph == 1 else False
        self.data_seg = bytearray('8'*1024*1024, encoding='utf-8')
        self.queue_state = Event()
        self.report_msc_event = Event()
        self.in_queue_num_read = self.in_queue_num_write = self.out_queue_num_read = self.out_queue_num_write = 0
        #第一个要拉取的log_id，用于跳过server
        self.start_get_log_entry = 0
        #控速开关，开启则不用等待前台空闲
        self.pass_time = False
        # self.pass_time = True

        #连接storage server
        self.storage_server_id = int(storage_server_id)
        self.storage_server_port = int(storage_server_port)
        self.set_storage_server(self.storage_server_id, self.storage_server_port)
        
        '''Events'''
        self.pull_log_event = Event()
        self.flush_data_event = Event()
        self.start_flush_event = Event()
        #标志暂停还是继续返回执行完毕的请求信息给client
        self.get_finished_write_event = Event()
        self.get_finished_read_event = Event()
        #标志是否有请求需要发送到存储层
        self.flush_write_event = Event()
        self.flush_read_event = Event()
        # zombie标志
        self.zombie_event = Event()
        # 是否发送remove_log通知
        self.remove_log_event = Event()
        # 解除msc
        self.unlock_msc_occupy = False
        self.unlock_msc_occupy_complete_event = Event()
        
        '''Threads'''
        self.thread_list = list()
        # 发送请求到存储层
        self.thread_list.append(gevent.spawn(self.flush_write_loop))
        self.thread_list.append(gevent.spawn(self.flush_read_loop))
        # 处理存储层反馈的请求结果
        self.thread_list.append(gevent.spawn(self.process_write))
        self.thread_list.append(gevent.spawn(self.process_read))

        # # 发送拉取日志数据请求线程
        self.thread_list.append(gevent.spawn(self.pull_log_batch))
        # 已刷入存储层通知buffer server标记删除线程
        self.thread_list.append(gevent.spawn(self.remove_log))
        # 拉取来的日志合并线程
        self.thread_list.append(gevent.spawn(self.flush_data))
        # 拉取存储层log_head
        self.thread_list.append(gevent.spawn(self.get_merge_log_head))
        # 向merge speed controller实时汇报队列状态
        self.thread_list.append(gevent.spawn(self.report_msc))

        #回传req server的port
        self.send_port()

    '''设定本manager的ip和port'''
    def set_manager(self, manager_ip, manager_port):
        #连接manager
        self.manager_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.manager_socket.connect((manager_ip, manager_port))

    '''连接storage server'''
    def set_storage_server(self, storage_server_id, storage_server_port):
        logging.info('set storage server, id {0}, port {1}'.format(storage_server_id, storage_server_port))
        self.storage_server_socket = collections.deque()
        domain_socket = True
        for i in range(0, 10):
            if domain_socket == True:
                # domain socket
                s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                s.connect('/home/k8s/wzhzhu/uds'+str(self.storage_server_id))
            else:
                # network socket
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # s.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,True)
                s.connect(('127.0.0.1', storage_server_port))
            self.storage_server_socket.append(s)
        try:
            s = self.storage_server_socket.popleft()
            request = struct.pack('>BII', REGISTER_SEGMENT, self.volume_id, self.segment_id)
            send(request, s)
            recv(s)
            self.storage_server_socket.append(s)
        except Exception as err:
            logging.info(err)

    '''找manager确认buffer_server数量，他们的ip和port.--------->对应Manager的get_buffer_server_info'''
    def set_buffer_server(self):
        logging.info('set buffer server')
        request = struct.pack('>BI',GET_BUFFER_PROXY_INFO, self.volume_id)
        send(request, self.manager_socket)
        response = recv(self.manager_socket)
        rsp_len = len(response)
        start = 0
        self.buffer_server_list = list()
        while start < rsp_len:
            ip_len = struct.unpack('>I',response[start:start+4])[0]
            start += 4
            ip = response[start:start+ip_len].decode(encoding='utf-8')
            start += ip_len
            port = struct.unpack('>I', response[start:start+4])[0]
            start += 4
            self.buffer_server_list.append((ip, port))
        self.log_pending_queue = dict()
        #存log拉取请求的队列
        for i in range(0, len(self.buffer_server_list)):
            self.log_pending_queue[i] = collections.deque()
        logging.info('buffer server list')
        logging.info(self.buffer_server_list)

    def init_buffer_server_socket(self):
        self.socket_num = dict()
        self.buffer_server_socket_pool = dict()
        buffer_server_socket_num = 5
        for i in range(0, len(self.buffer_server_list)):
            self.socket_num[i]= buffer_server_socket_num
            self.buffer_server_socket_pool[i] = collections.deque()
            for j in range(0, buffer_server_socket_num):
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,True)
                s.connect((self.buffer_server_list[i][0],self.buffer_server_list[i][1]))
                self.buffer_server_socket_pool[i].append(s)

    def init_merge_speed_controller_socket(self):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect('/home/k8s/wzhzhu/merge_ctrl_uds')
        self.mscs_send  = s

    def send_port(self):
        s = self.manager_socket
        request = struct.pack('>B',NOTIFY_FORWARDING_PROXY_PORT)
        request += struct.pack('>I', self.volume_id)
        request += struct.pack('>I', self.segment_id)
        request += struct.pack('>I', self.port)
        send(request, s)
        s.close()

    def set_log_head(self, request, client_socket):
        log_head = struct.unpack('>Q',request[0:8])[0]
        self.log_head = log_head
        self.next_merge_log_head = log_head
        self.start_flush_event.set()
    
    def get_log_head(self, request, client_socket):
        # logging.info(self.log_head)
        get_type = struct.unpack('>I',request)[0]
        if get_type == 0:
            response = struct.pack('>Q', self.log_head)
            send(response, client_socket)
        else:
            while True:
                self.report_log_head_event.wait()
                response = struct.pack('>Q', self.log_head)
                send(response, client_socket)
                self.report_log_head_event.clear()

    def report_in_queue_write(self, request, client_socket):
        try:
            write_queue_len = struct.unpack('>Q',request[0:8])[0]
            self.in_queue_num_write = write_queue_len
            self.report_msc_event.set()
        except Exception as err:
            logging.info(err)

    def report_in_queue_read(self, request, client_socket):
        try:
            read_queue_len = struct.unpack('>Q',request[0:8])[0]
            self.in_queue_num_read = read_queue_len
            self.report_msc_event.set()
        except Exception as err:
            logging.info(err)

    def switch_zombie(self, request, client_socket):
        logging.info('volume_id {0} segment_id {1} switch to zombie!!!...'.format(self.volume_id, self.segment_id))
        self.state = STOP_WORK
        # 等待正在处理的请求完成
        while self.pending_req_num != 0:
            time.sleep(0.001)
        # logging.info('step1 complete')

        # while self.next_merge_log_head != self.log_head:
        #     time.sleep(0.001)
        # # logging.info('step2 complete')

        # # 解除对msc的网络占用，停止report_msc
        # self.report_msc_event.set()
        # self.unlock_msc_occupy = True
        # self.unlock_msc_occupy_complete_event.wait()
        # # logging.info('step3 complete')

        # 关闭线程
        for thread in self.thread_list:
            thread.kill()
        # logging.info('step4 complete')
        
        # 注销segment到storage server
        s = self.storage_server_socket.popleft()
        request = struct.pack('>BII', UNREGISTER_SEGMENT, self.volume_id, self.segment_id)
        send(request, s)
        recv(s)
        self.storage_server_socket.append(s)
        # logging.info('step5 complete')

        # #关闭拉取日志entry线程
        # if self.start_get_log_entry == 1:
        #     request = struct.pack('>BIII', STOP_GET_LOG_ENTRY, self.volume_id, self.volume_id, self.segment_id)
        #     socket_list = list()
        #     for i in range(0, len(self.buffer_server_list)):
        #         s = self.buffer_server_socket_pool[i].popleft()
        #         send(request, s)
        #         socket_list.append(s)
        #     for s in socket_list:
        #         recv(s)
        #     for i in range(0, len(self.buffer_server_list)):
        #         self.buffer_server_socket_pool[i].append(socket_list[i])
        #     self.get_log_entry_thread.kill()
        # # logging.info('step6 complete')

        response = struct.pack('>Q', self.log_head)
        send(response, client_socket)
        self.state = ZOMBIE
        self.zombie_event.set()
        logging.info('volume_id {0} segment_id {1} switch to zombie end!!'.format(self.volume_id, self.segment_id))

    def probe_state(self, request, client_socket):
        self.zombie_event.wait()
        logging.info('volume_id {0} segment_id {1} inform client zombie state and suicide'.format(self.volume_id, self.segment_id))
        response = struct.pack('>B',ERROR)
        send(response, client_socket)
        del proxy_dict[(self.volume_id, self.segment_id)]
        logging.info('volume_id {0} segment_id {1} memory released'.format(self.volume_id, self.segment_id))

    def write(self, request, client_socket):
        start = 0
        request_len = len(request)
        while start < request_len:
            sub_request = request[start: start + 8 + 4 + 4 + 4 + 8 + 4]
            req_id, sub_req_id, volume_id, segment_id, offset, length = struct.unpack('>QIIIQI', request[start: start + 8 + 4 + 4 + 4 + 8 + 4])
            start += 8 + 4 + 4 + 4 + 8 + 4
            data = request[start: start + length]
            start += length
            if self.state == ALIVE:
                if self.with_ceph:
                    try:
                        storage_request = bytearray()
                        storage_request.extend(struct.pack('>B', STORAGE_WRITE))
                        storage_request.extend(sub_request)
                        storage_request.extend(data)
                        self.write_temp_queue.append(storage_request)
                        self.pending_req_num += 1
                        self.flush_write_event.set()
                    except Exception as err:
                        logging.info(err)
                else:
                    state = SUCCEED
                    self.write_finish_queue.append((req_id, sub_req_id, offset, state))
                    self.get_finished_write_event.set()

    def read(self, request, client_socket):
        start = 0
        request_len = len(request)
        while start < request_len:
            sub_request = request[start: start + 8 + 4 + 4 + 4 + 8 + 4]
            req_id, sub_req_id, volume_id, segment_id, offset, length = struct.unpack('>QIIIQI', request[start: start + 8 + 4 + 4 + 4 + 8 + 4])
            start += 8 + 4 + 4 + 4 + 8 + 4
            if self.state == ALIVE:
                if self.with_ceph:
                    try:
                        storage_request = bytearray()
                        storage_request.extend(struct.pack('>B', STORAGE_READ))
                        storage_request.extend(sub_request)
                        self.read_temp_queue.append(storage_request)
                        self.pending_req_num += 1
                        self.flush_read_event.set()
                    except Exception as err:
                        logging.info(err)
                else:
                    state = SUCCEED
                    self.read_finish_queue.append((req_id, sub_req_id, offset, SUCCEED, self.data_seg[0:length]))
                    self.get_finished_read_event.set()
    
    def flush_write_loop(self):
        while True:
            self.flush_write_event.wait()
            s = self.storage_server_socket.popleft()
            while len(self.write_temp_queue)!=0:
                request = self.write_temp_queue.popleft()
                send(request,s)
            self.storage_server_socket.append(s)
            if len(self.write_temp_queue) == 0:
                self.flush_write_event.clear()

    def flush_read_loop(self):
        while True:
            self.flush_read_event.wait()
            s = self.storage_server_socket.popleft()
            while len(self.read_temp_queue)!=0:
                request = self.read_temp_queue.popleft()
                send(request,s)
            self.storage_server_socket.append(s)
            if len(self.read_temp_queue) == 0:
                self.flush_read_event.clear()

    def process_write(self):
        storage_server_socket = self.storage_server_socket.popleft()
        request = struct.pack('>BII', STORAGE_GET_FINISHED_WRITE, self.volume_id, self.segment_id)
        send(request, storage_server_socket)
        try:
            while True:
                response = recv(storage_server_socket)
                rsp_len = len(response)
                start = 0
                while start < rsp_len:
                    req_id, sub_req_id, offset, length, state = struct.unpack('>QIQIB', response[start:start+8+4+8+4+1])
                    start += 8+4+8+4+1
                    self.write_finish_queue.append((req_id, sub_req_id, offset, state))
                    self.pending_req_num -= 1
                    self.get_finished_write_event.set()
        except Exception as err:
            logging.info("recv_data error:{0}".format(err))
            time.sleep(0.001)

    def process_read(self):
        storage_server_socket = self.storage_server_socket.popleft()
        request = struct.pack('>BII', STORAGE_GET_FINISHED_READ, self.volume_id, self.segment_id)
        send(request, storage_server_socket)
        try:
            while True:
                response = recv(storage_server_socket)
                rsp_len = len(response)
                start = 0
                while start < rsp_len:
                    req_id, sub_req_id, offset, length, state = struct.unpack('>QIQIB', response[start:start+8+4+8+4+1])
                    start += 8+4+8+4+1
                    data = response[start:start+length]
                    start += length
                    self.read_finish_queue.append((req_id, sub_req_id, offset, state, data))
                    self.pending_req_num -= 1
                    self.get_finished_read_event.set()
        except Exception as err:
            logging.info("recv_data error:{0}".format(err))
            time.sleep(0.001)

    def get_finished_write(self, request, client_socket):
        try:
            while True:
                self.get_finished_write_event.wait()
                self.out_queue_num_write = len(self.write_finish_queue)
                self.report_msc_event.set()
                time.sleep(0)
                while len(self.write_finish_queue)!=0:
                    response = bytearray()
                    req_id, sub_req_id, offset, state = self.write_finish_queue.popleft()
                    response.extend(struct.pack('>QIQBQ', req_id, sub_req_id, offset, state, self.log_head))
                    send(response, client_socket)
                if len(self.write_finish_queue) == 0:
                    self.get_finished_write_event.clear()
                self.out_queue_num_write = 0
                self.report_msc_event.set()
        except Exception as err:
            logging.info(err)

    def get_finished_read(self, request, client_socket):
        try:
            while True:
                self.get_finished_read_event.wait()
                self.out_queue_num_read = len(self.read_finish_queue)
                self.report_msc_event.set()
                time.sleep(0)
                while len(self.read_finish_queue)!=0:
                    response = bytearray()
                    req_id, sub_req_id, offset, state, data = self.read_finish_queue.popleft()
                    # logging.info((req_id, offset, length, state))
                    response.extend(struct.pack('>QIQBQ', req_id, sub_req_id, offset, state, self.log_head))
                    if state == SUCCEED:
                        response.extend(data)
                    send(response, client_socket)
                if len(self.read_finish_queue)==0:
                    self.get_finished_read_event.clear()
                self.out_queue_num_read = 0
                self.report_msc_event.set()
        except Exception as err:
            logging.info(err)
    
    def pull_log_meta(self, request, client_socket):
        if self.start_get_log_entry == 0:
            self.start_get_log_entry_event = Event()
            self.start_log_id = -1
            self.get_log_entry_server = 0
            # for i in range(0, len(self.buffer_server_list)):
            self.get_log_entry_thread = gevent.spawn(self.get_log_entry)
            while self.get_log_entry_server != len(self.buffer_server_list):
                time.sleep(0.001)
            self.start_get_log_entry = 1

        start = 0
        total_log_size, segment_log_size = struct.unpack('>II',request[start:start+4+4])
        self.total_log_size = total_log_size
        # logging.info('总日志规模为{0}MB'.format(total_log_size))
        max_pull_gap_per_byte = 10/(500*1024*1024)
        self.pull_gap_per_byte = 10/(self.total_log_size*1024*1024)
        # self.pull_gap_per_byte = 1/(85980*4096*0.3)
        # logging.info('每byte间隔时间为{0}s'.format(self.pull_gap_per_byte))
        predict_time = int((segment_log_size/total_log_size)*10*1000)
        # logging.info('最大预期合并时间为{0} ms'.format(predict_time))
        start += 4 + 4
        rsp_len = len(request)
        log_num = 0
        while start < rsp_len:
            volume_id, segment_id, log_id, offset, length, file_id, file_offset = struct.unpack('>IIQQIII', request[start:start+36])
            if self.start_log_id == -1:
                self.start_log_id = log_id
                self.start_get_log_entry_event.set()
            start += 36
            buffer_server_num = len(self.buffer_server_list)
            buffer_server_id = log_id % buffer_server_num
            # logging.info((volume_id, log_id, offset, length, buffer_server_id, file_id, file_offset))
            self.log_pending_queue[buffer_server_id].append((volume_id, segment_id, log_id, offset, length, file_id, file_offset))
            log_num += 1
        self.log_gap = 0.001*predict_time/log_num
        # logging.info('每请求间隔时间为{0}s, target_id为{1}'.format(self.log_gap, log_id))
        self.target_log_id = log_id

        self.pull_log_event.set()
        # 开始订阅msc空闲in
        self.mscs_recv = None
        self.subscribe_thread = gevent.spawn(self.subscribe_msc_in)
        # if self.total_log_size <= 3000:
        #     # self.timer_thread = gevent.spawn(self.timer, 3*0.001*predict_time)
        #     self.pass_time = False
        # else:
        #     self.pass_time = True
        # 通知storage server订阅msc空闲out
        request = struct.pack('>BIII', START_SUBSCRIBE_MSC, self.total_log_size, predict_time, log_num)
        s = self.storage_server_socket.popleft()
        send(request, s)
        self.storage_server_socket.append(s)

        # time.sleep(0.001*predict_time)
        # self.log_head = log_id + 1
        # self.next_merge_log_head = self.log_head
        # self.report_log_head_event.set()
        # self.remove_log_event.set()
        # logging.info('本次合并结束')

    def get_log_entry(self):
        #发送请求，汇报自己的volume_id, segment_id
        request = struct.pack('>BIII', GET_LOG_ENTRY, self.volume_id, self.volume_id, self.segment_id)
        buffer_server_socket = list()
        for i in range(0, len(self.buffer_server_list)):
            s = self.buffer_server_socket_pool[i].popleft()
            send(request, s)
            buffer_server_socket.append(s)
        for s in buffer_server_socket:
            recv(s)
            self.get_log_entry_server += 1
        try:
            self.start_get_log_entry_event.wait()
            # 计算要跳过几次
            self.start_log_id = self.start_log_id%len(self.buffer_server_list)
            while True:
                for s in buffer_server_socket:
                    if self.start_log_id != 0:
                        self.start_log_id -= 1
                        continue
                    if not self.pass_time:
                        self.queue_state.wait()
                    response = recv(s)
                    start = 0
                    log_id, offset, length = struct.unpack('>QQI',response[start:start+8+8+4])
                    # logging.info('拉到日志log_id {0}'.format(log_id))
                    start += 8 + 8 + 4
                    data = response[start:start+length]
                    start += length
                    heappush(self.log_queue,(log_id, (log_id, offset, length, data)))
                    self.flush_data_event.set()
                    # logging.info('拉到日志log_id {0}'.format(log_id))
                    # 控制日志拉取速度
                    gap = self.pull_gap_per_byte*length
                    # time.sleep(self.log_gap)
                    time.sleep(gap)
                    # time.sleep(0)
                    gevent.idle()
        except Exception as err:
            logging.info(err)

    def pull_log_batch(self):
        try:
            buffer_server_socket = list()
            for i in range(0, len(self.buffer_server_list)):
                s = self.buffer_server_socket_pool[i].popleft()
                buffer_server_socket.append(s)
            while True:
                if self.state == ZOMBIE or self.state == STOP_WORK:
                    break
                self.pull_log_event.wait()
                while True:
                    flag =  0
                    for i in self.log_pending_queue:
                        if len(self.log_pending_queue[i]) != 0:
                            flag = 1
                    if flag == 0:
                        self.pull_log_event.clear()
                        break
                    for i in self.log_pending_queue:
                        if len(self.log_pending_queue[i]) == 0:
                            continue
                        volume_id, segment_id, log_id, offset, length, file_id, file_offset = self.log_pending_queue[i].popleft()
                        request = struct.pack('>BIIIQQIII', PULL_LOG_ENTRY, self.volume_id, self.volume_id, segment_id, log_id, offset, length, file_id, file_offset)
                        send(request, buffer_server_socket[i])
                        # logging.info('拉取日志log_id {0}'.format(log_id))
                        # # 控制日志拉取速度
                        # gap = self.pull_gap_per_byte*length
                        # time.sleep(self.log_gap)
                        # time.sleep(0)
                        gevent.idle()
        except Exception as err:
            logging.info(err)

    '''拉单buffer server的日志'''
    def pull_log_batch_single(self, waiting_queue, s, buffer_server_id):
        meta_list = list()
        merged_request_list = list()
        merged_request_list.append(struct.pack('>BI', BUFFER_PULL, self.volume_id))
        while len(waiting_queue) != 0:
            volume_id, segment_id, log_id, offset, length, file_id, file_offset = waiting_queue.popleft()
            request = struct.pack('>IIQQIII', self.volume_id, segment_id, log_id, offset, length, file_id, file_offset)
            merged_request_list.append(request)
            meta_list.append((log_id, offset, length))
        merged_request = bytearray().join(merged_request_list)
        send(merged_request, s)
        meta_idx = 0
        try:
            while True:
                response = recv(s)
                # logging.info(meta_list)
                #将返回数据拼接元数据加入日志队列
                rsp_len = len(response)
                start = 0
                # logging.info('response长度{0}'.format(rsp_len))
                while start < rsp_len:
                    state, length = struct.unpack('>BI',response[start:start+1+4])
                    start += 1 + 4
                    if length == 0:
                        break
                    data = response[start:start+length]
                    start += length
                    heappush(self.log_queue,(meta_list[meta_idx][0], [meta_list[meta_idx][0], meta_list[meta_idx][1], meta_list[meta_idx][2], data]))
                    meta_idx += 1
                    # 告知有日志要合并
                    self.flush_data_event.set()
                if length == 0:
                    break
        except Exception as err:
            logging.info(err)
        #放回socket
        # logging.info(self.log_queue)
        self.buffer_server_socket_pool[buffer_server_id].append(s)

    '''将日志数据定期转储到存储层，需要保证日志按序写入'''
    def flush_data(self):
        try:
            self.start_flush_event.wait()
            count = 0
            storage_server_socket = self.storage_server_socket.popleft()
            while True:
                #阻塞等待有日志数据需要合并
                self.flush_data_event.wait()
                self.flush_data_event.clear()
                # logging.info('开始合并日志到存储层！')
                if self.state == ZOMBIE or self.state == STOP_WORK:
                    break
                log_list = list()
                #统计本地可以合并的日志数量
                while len(self.log_queue)!=0:
                    log = heappop(self.log_queue)
                    if log[0] != self.next_merge_log_head:
                        heappush(self.log_queue,log)
                        break
                    self.next_merge_log_head += 1
                    log_list.append(log)
                #将日志合并到存储层
                for log in log_list:
                    log_id = log[1][0]
                    offset = log[1][1]
                    length = log[1][2]

                    request = bytearray()
                    request.extend(struct.pack('>B',MERGE_LOG))
                    request.extend(struct.pack('>IIQIQI', self.volume_id, self.segment_id, offset, length, log_id, self.total_log_size))
                    request.extend(log[1][3])
                    send(request,storage_server_socket)
                    # logging.info('flush日志log_id {0}'.format(log_id))
                    # 控制日志合并速度
                    gap = self.pull_gap_per_byte*length
                    # time.sleep(self.log_gap)
                    # time.sleep(gap)
                    # time.sleep(0)
                    gevent.idle()
        except Exception as err:
            logging.info(err)
    
    def get_merge_log_head(self):
        try:
            # 发送get_merge_log_head请求给storage server
            # while 循环获得最新的merge_log_head
            request = struct.pack('>BII', GET_MERGE_LOG_HEAD, self.volume_id, self.segment_id)
            storage_server_socket = self.storage_server_socket.popleft()
            send(request, storage_server_socket)
            while True:
                response = recv(storage_server_socket)
                merge_log_head = struct.unpack('>Q', response[0:8])[0]
                # logging.info('收到响应日志log_id {0}'.format(merge_log_head))
                self.log_head = merge_log_head + 1
                if self.log_head%1000 == 0:
                    self.report_log_head_event.set()
                if merge_log_head == self.target_log_id:
                    self.subscribe_thread.kill()
                    if self.mscs_recv is not None:
                        self.mscs_recv.close()
                    self.queue_state.clear()
                    self.report_log_head_event.set()
                    self.remove_log_event.set()
                    # if self.total_log_size <= 3000:
                    #     self.timer_thread.kill()
                    # 通知storage server取消订阅msc空闲out
                    request = struct.pack('>B', END_SUBSCRIBE_MSC)
                    s = self.storage_server_socket.popleft()
                    send(request, s)
                    recv(s)
                    self.storage_server_socket.append(s)
                    logging.info('本次合并结束')
        except Exception as err:
            logging.info(err)
    
    '''定期通知所有buffer server log head以清除过期日志'''
    def remove_log(self):
        buffer_server_num = len(self.buffer_server_list)
        socket_list = list()
        for buffer_server_id in range(0, buffer_server_num):
            s = self.buffer_server_socket_pool[buffer_server_id].popleft()
            socket_list.append(s)
        try:
            while True:
                if self.state == ZOMBIE:
                    break
                # logging.info('通知buffer server可以清除虚拟盘{0} segment {1} {2}之前的log'.format(self.volume_id, self.segment_id, self.log_head))
                # print(self.log_head)
                self.remove_log_event.wait()
                request = struct.pack('>BIIIQ',BUFFER_REMOVE, self.volume_id, self.volume_id, self.segment_id, self.log_head)
                for buffer_server_id in range(0, buffer_server_num):
                    s = socket_list[buffer_server_id]
                    send(request, s)
                self.remove_log_event.clear()
        except Exception as err:
            logging.info(err)

    '''**********************************************************************************************
        订阅merge speed controller队列状态,以及汇报队列信息
    **********************************************************************************************'''
    def subscribe_msc_in(self):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect('/home/k8s/wzhzhu/merge_ctrl_uds')
        self.mscs_recv  = s
        request = struct.pack('>B',GET_IN)
        send(request, self.mscs_recv)
        while True:
            response = recv(self.mscs_recv)
            queue_state = struct.unpack('>B',response[0:1])[0]
            if queue_state == SUCCEED:
                self.queue_state.set()
                # logging.info('msc switch on')
            else:
                self.queue_state.clear()
                # logging.info('msc switch off')
    
    def timer(self, target_time):
        time.sleep(target_time)
        self.pass_time = True

    def report_msc(self):
        try:
            last_in_queue_num  = last_out_queue_num = 0
            while True:
                self.report_msc_event.wait()
                self.report_msc_event.clear()
                if self.unlock_msc_occupy:
                    in_queue_num = 0
                    out_queue_num = 0
                else:
                    in_queue_num = self.in_queue_num_write + self.in_queue_num_read
                    out_queue_num = self.out_queue_num_write + self.out_queue_num_read
                    # in_queue_num = self.in_queue_num_write
                    # out_queue_num = self.out_queue_num_read
                if (last_in_queue_num == 0 and in_queue_num !=0) or (last_in_queue_num != 0 and in_queue_num ==0):
                    last_in_queue_num = in_queue_num
                    request = struct.pack('>BIIQ',REPORT_DIRECT_IN, self.volume_id, self.segment_id, in_queue_num)
                    send(request, self.mscs_send)
                    # logging.info('汇报in queue num {0}'.format(in_queue_num))
                if (last_out_queue_num == 0 and out_queue_num !=0) or (last_out_queue_num != 0 and out_queue_num ==0):
                    last_out_queue_num = out_queue_num
                    request = struct.pack('>BIIQ',REPORT_DIRECT_OUT, self.volume_id, self.segment_id, out_queue_num)
                    send(request, self.mscs_send)
                    # logging.info('汇报out queue num {0}'.format(out_queue_num))
                if self.unlock_msc_occupy:
                    self.unlock_msc_occupy_complete_event.set()
                    return
        except Exception as err:
            logging.info(err)

def recv_request(client_socket, client_addr):
    """处理申请"""
    global proxy_dict, forwarding_server_id, server_socket, manager_ip, manager_port, with_ceph, storage_server_port, storage_server_id, port, process_id
    try:
        while True:
            # logging.info(client_addr)
            op = -1
            request = recv(client_socket)  # 接收数据
            if not request:  # 客户端断开
                logging.info("{0} offline".format(client_addr))
                client_socket.close()
                break
            op, volume_id, segment_id = struct.unpack('>BII', request[0:1+4+4])
            start = 1+4+4
            if op == CREATE_PROXY:
                logging.info('create proxy')
                logging.info((volume_id, segment_id, manager_ip, manager_port, with_ceph, storage_server_port, storage_server_id, port))
                proxy_dict[(volume_id, segment_id)] = ForwardingProxy(volume_id, segment_id, manager_ip, manager_port, with_ceph, storage_server_port, storage_server_id, port)
                logging.info('create proxy complete')
            else:
                proxy = proxy_dict[(volume_id, segment_id)]
                if op == READ:
                    proxy.read(request[start:], client_socket)
                elif op == WRITE:
                    proxy.write(request[start:], client_socket)
                elif op == SWITCH_ZOMBIE:
                    proxy.switch_zombie(request[start:], client_socket)
                elif op == PULL_LOG:
                    proxy.pull_log_meta(request[start:], client_socket)
                elif op == SET_LOG_HEAD:
                    proxy.set_log_head(request[start:], client_socket)
                elif op == PROBE_STATE:
                    proxy.probe_state(request[start:], client_socket)
                elif op == GET_FINISHED_WRITE:
                    proxy.get_finished_write(request[start:], client_socket)
                elif op == GET_FINISHED_READ:
                    proxy.get_finished_read(request[start:], client_socket)
                elif op == GET_LOG_HEAD:
                    proxy.get_log_head(request[start:], client_socket)
                elif op == REPORT_IN_QUEUE_READ:
                    proxy.report_in_queue_read(request[start:], client_socket)
                elif op == REPORT_IN_QUEUE_WRITE:
                    proxy.report_in_queue_write(request[start:], client_socket)
                else:
                    logging.info('Something wrong!')
                    f = open('/home/k8s/wzhzhu/my_balance/output/error_log', 'w')
                    f.write('Something wrong!, {0}'.format(client_addr))
                    f.flush()
                    f.close()
                    sys.exit(0)
    except Exception as err:
        logging.info("recv_data error1:{0}".format(err))
    finally:
        logging.info('socket close!, addr = {0}, op = {1}'.format(client_addr, op))
        client_socket.close()
        return 1

def run():
    global proxy_dict, forwarding_server_id, server_socket, manager_ip, manager_port, with_ceph, storage_server_port, storage_server_id, port, process_id
    logging.info('启动')
    while True:
        client_socket, client_addr = server_socket.accept()     #等待客户端连接
        logging.info(('连接', client_addr))
        gevent.spawn(recv_request, client_socket, client_addr)

def main(argv):
    # p.nice(-20)
    global proxy_dict, forwarding_server_id, server_socket, manager_ip, manager_port, with_ceph, storage_server_port, storage_server_id, port, process_id
    opts, args = getopt.getopt(argv,"hf:r:i:p:w:t:d:",["forwarding_server_id=", "process_id=", "manager_ip=","manager_port=","with_ceph=","storage_server_port=","storage_server_id="])
    for opt, arg in opts:
        if opt == '-h':
            print('ForwardingProcess.py -f <forwarding_server_id> -r <process_id> -i <manager_ip> -p <manager_port> -w <with_ceph> -t <storage_server_port> -d <storage_server_id>')
            sys.exit(0)
        elif opt in ("-f", "--forwarding_server_id"):
            forwarding_server_id = int(arg)
        elif opt in ("-r", "--process_id"):
            process_id = int(arg)
        elif opt in ("-i", "--manager_ip"):
            manager_ip = arg
        elif opt in ("-p", "--manager_port"):
            manager_port = int(arg)
        elif opt in ("-w", "--with_ceph"):
            with_ceph = int(arg)
        elif opt in ("-t", "--storage_server_port"):
            storage_server_port = int(arg)
        elif opt in ("-d", "--storage_server_id"):
            storage_server_id = int(arg)
    
    bind_cpu_core = False
    if bind_cpu_core:
        p = psutil.Process(os.getpid())
        count = psutil.cpu_count()
        cpu_list = p.cpu_affinity()
        target_cpu_core = cpu_list[process_id%len(cpu_list)]
        p.cpu_affinity([target_cpu_core])

    # 网络参数
    # self.code_mode = code_mode     #收发数据编码/解码格式
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)     # 创建socket
    server_socket.bind(('0.0.0.0', 0))      # 绑定端口
    #禁用nagle算法
    server_socket.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,True)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.listen(100000000)           # 设置为被动socket
    port = server_socket.getsockname()[1]

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((manager_ip, manager_port))
    request = struct.pack('>B',NOTIFY_FORWARDING_PROCESS_PORT)
    request += struct.pack('>I',forwarding_server_id)
    request += struct.pack('>I', process_id)
    request += struct.pack('>I', port)
    send(request, s)
    s.close()

    run()

if __name__ == "__main__":
    main(sys.argv[1:])



