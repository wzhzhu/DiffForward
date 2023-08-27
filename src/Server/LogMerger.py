#!/usr/bin/python2
#coding:utf-8
from gevent import monkey;monkey.patch_all()
import sys
import gevent
import socket
from gevent.event import Event
from ..Properties.config import *
import time
import getopt
from ..Utils.DataTrans import *
import struct
import psutil, os
import math
import collections
import logging
import random

p = psutil.Process(os.getpid())

class LogMerger:
    '''**********************************************************************************************
        init
    **********************************************************************************************'''
    def __init__(self, log_merger_id, port, manager_ip, manager_port, inflight_merging_traffic_max, merge_log):
        logging.basicConfig(level=logging.INFO,
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )
        self.manager_ip = manager_ip
        self.manager_port = int(manager_port)
        self.log_merger_id = int(log_merger_id)
        self.inflight_merging_traffic_max = int(inflight_merging_traffic_max)
        self.merge_log = int(merge_log)
        logging.info('inflight_merging_traffic_max {0}'.format(self.inflight_merging_traffic_max))
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        # self.server_socket.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,True)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        self.server_socket.bind(('0.0.0.0', int(port)))
        self.server_socket.listen(100000000)
        '''异步拉数据部分状态'''
        # 本地有哪些segment
        self.segments = list()
        # 每个segment元数据当前已经被拉到哪个位置
        self.segment_pull_meta_pos = dict()
        # 本地同步segment合并到的log_head
        self.segment_log_recv = dict()
        self.log_size = dict()
        self.log_merge_succeed_event = Event()
        self.log_merge_succeed_event.set()
        self.segment_log_send = dict()
        # segment还没有被写入存储层的缓存队列（只保留元数据，实际执行的时候通知req server让req server去拉）
        self.segment_log_meta = dict()
        self.segment_log_size = dict()
        # segment信息
        self.segment_2_req_port = dict()
        self.buffer_server_list = list()
        # 每个segment存在状态有变动时更新时间戳，便于清空资源
        self.segment_change_time = dict()
        # 其余初始化
        self.set_manager()
        self.set_segment()
        self.set_buffer_server()
        self.init_req_server_socket()
        self.init_buffer_server_socket()
        self.init_get_log_head()
        self.merging_segment = (-1,-1)

        self.inflight_merging_traffic = 0
        '''Events'''
        self.log_merge_end_event = False
        '''Threads'''
        if self.merge_log == 1:
            # 拉取日志元数据线程
            gevent.spawn(self.pull_log_meta)
            # 通知req server拉取日志线程
            gevent.spawn(self.log_read)

        #废弃！！
        # 从req server同步各个VDisk的log_head的线程
        # gevent.spawn(self.update_log_head_loop)

    '''设定本replayer manager的ip和port'''
    def set_manager(self):
        #连接manager
        self.manager_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print(self.manager_ip)
        print(self.manager_port)
        self.manager_socket.connect((self.manager_ip,self.manager_port))

    '''找manager确认本机器上有哪些segment，以及这些segment对应req_server的port，后续拉取日志要用到.--------->对应Manager'''
    def set_segment(self):
        request = struct.pack('>B',GET_SERVER_SEGMENT_INFO)
        request += struct.pack('>I', self.log_merger_id)
        send(request, self.manager_socket)
        response = recv(self.manager_socket)
        req_len = len(response)
        start = 0
        while start < req_len:
            volume_id, segment_id = struct.unpack('>II',response[start: start+8])
            start += 8
            self.segments.append((volume_id, segment_id))
            self.segment_log_meta[(volume_id, segment_id)] = collections.deque()
            self.segment_log_size[(volume_id, segment_id)] = 0
            # 初始所有volume下一个要拉取的日志id都设置为0
            self.segment_pull_meta_pos[(volume_id, segment_id)] = 0
            # 初始所有segment本地记录的log_head为0
            self.segment_log_send[(volume_id, segment_id)] = -1
            self.segment_log_recv[(volume_id, segment_id)] = 0
            self.log_size[(volume_id, segment_id)] = collections.deque()
            port = struct.unpack('>I',response[start:start+4])[0]
            start += 4
            self.segment_2_req_port[(volume_id, segment_id)] = port
            self.segment_change_time[(volume_id, segment_id)] = time.time()
        logging.info('req server list')
        logging.info(self.segment_2_req_port)

    '''找manager确认buffer_server数量，他们的ip和port.--------->对应Manager的get_buffer_server_info'''
    def set_buffer_server(self):
        self.buffer_server_dict = dict()
        for volume_id,segment_id in self.segments:
            request = struct.pack('>BI',GET_BUFFER_PROXY_INFO, volume_id)
            send(request, self.manager_socket)
            response = recv(self.manager_socket)
            rsp_len = len(response)
            start = 0
            buffer_server_list = list()
            while start < rsp_len:
                ip_len = struct.unpack('>I',response[start:start+4])[0]
                start += 4
                ip = response[start:start+ip_len].decode(encoding='utf-8')
                start += ip_len
                port = struct.unpack('>I', response[start:start+4])[0]
                start += 4
                buffer_server_list.append((ip, port))
            self.buffer_server_dict[(volume_id, segment_id)] = buffer_server_list
        logging.info('buffer server dict')
        logging.info(self.buffer_server_dict)

    '''连接本机上segment的req server，每个一个socket'''
    def init_req_server_socket(self):
        self.req_server_socket = dict()
        self.req_server_socket_state = dict()
        self.req_server_socket_for_head = dict()
        for volume_id,segment_id in self.segments:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # s.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,True)
            s.connect(('127.0.0.1',self.segment_2_req_port[(volume_id,segment_id)]))
            self.req_server_socket[(volume_id,segment_id)] = s
            self.req_server_socket_state[(volume_id, segment_id)] = FREE
            #激活req server的异步写入进程
            self.set_req_server_log_head(volume_id,segment_id)
            #拉取log_head的socket
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # s.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,True)
            s.connect(('127.0.0.1',self.segment_2_req_port[(volume_id,segment_id)]))
            self.req_server_socket_for_head[(volume_id, segment_id)] = s

    '''连接所有buffer server，每个一个socket'''
    def init_buffer_server_socket(self):
        self.buffer_server_socket = dict()
        self.buffer_server_socket_state = dict()
        for volume_id, segment_id in self.segments:
            self.buffer_server_socket[(volume_id,segment_id)] = dict()
            self.buffer_server_socket_state[(volume_id,segment_id)] = dict()
            for i in range(0, len(self.buffer_server_dict[(volume_id,segment_id)])):
                ip = self.buffer_server_dict[(volume_id,segment_id)][i][0]
                port = self.buffer_server_dict[(volume_id,segment_id)][i][1]
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # s.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,True)
                s.connect((ip,port))
                self.buffer_server_socket[(volume_id,segment_id)][i] = s
                self.buffer_server_socket_state[(volume_id,segment_id)][i] = FREE
    
    '''为每个segment创建一个线程，更新下一个要执行合并的log_id:log_head'''
    def init_get_log_head(self):
        for volume_id,segment_id in self.segments:
            gevent.spawn(self.get_log_head, volume_id, segment_id)
    '''**********************************************************************************************
        helper
    **********************************************************************************************'''
    def run(self):
        self.serve()

    # check done
    def serve(self):
        """启动，等待客户端到来"""
        while True:
            client_socket, client_addr = self.server_socket.accept()     # 等待客户端连接
            gevent.spawn(self.recv_application, client_socket, client_addr)

    def recv_application(self, client_socket, client_addr):
        """处理申请"""
        try:
            while True:
                request = recv(client_socket)  # 接收数据
                if not request:  # 客户端断开
                    print("{} offline".format(client_addr))
                    client_socket.close()
                    break
                op = struct.unpack('>B', request[0:1])[0]
                if op == ADD_SEGMENT:
                    # print('add segment...')
                    self.add_segment(request[1:], client_socket)
                elif op == REMOVE_SEGMENT:
                    # print('remove segment...')
                    self.remove_segment(request[1:], client_socket)
                elif op == SHUTDOWN:
                    # print('shutdown...')
                    response = bytearray()
                    send(response, client_socket)
                    sys.exit()
        except Exception as err:
            logging.info("recv_data error:{0}, op = {1}".format(err,op))
        finally:
            client_socket.close()
    '''**********************************************************************************************
        异步拉元数据部分代码
    **********************************************************************************************'''
    '''周期性异步拉取日志元数据到本地'''
    def pull_log_meta(self):
        try:
            while True:
                #如果日志数量小于阈值，则开始，否则跳过
                segments = list(self.segments)
                for volume_id,segment_id in segments:
                    if (volume_id,segment_id) not in self.segments:
                        continue
                    #拉取日志元数据
                    # self.segment_log_meta_read(volume_id,segment_id)
                    pull_meta_threads = list()
                    pull_meta_threads.append(gevent.spawn(self.segment_log_meta_read, volume_id, segment_id))
                    # time.sleep(0.01)
                    gevent.wait(pull_meta_threads)
                    # time.sleep(10/len(segments))
                    time.sleep(0.01+0.01*random.random())
                # time.sleep(0)
        except Exception as err:
            logging.info(err)

    '''拉取指定segment元数据'''
    def segment_log_meta_read(self, volume_id, segment_id):
        try:
            # time.sleep(5*random.random())
            # logging.info('虚拟盘{0} segment{1}开始拉取元数据'.format(volume_id, segment_id))
            segment_version = self.segment_change_time[(volume_id,segment_id)]
            if (volume_id,segment_id) not in self.segment_pull_meta_pos:
                return
            log_start = self.segment_pull_meta_pos[(volume_id,segment_id)]
            #检验是否所有socket都可用
            for i in self.buffer_server_socket_state[(volume_id,segment_id)]:
                if self.buffer_server_socket_state[(volume_id,segment_id)][i]==USED:
                    return
            #加锁
            for i in self.buffer_server_socket_state[(volume_id,segment_id)]:
                self.buffer_server_socket_state[(volume_id,segment_id)][i]=USED
            request = struct.pack('>BIIIQ', SEGMENT_LOG_META_READ, volume_id, volume_id, segment_id, log_start)
            # logging.info('拉取虚拟盘{0} segment{1} 从log id {2}开始的日志元数据'.format(volume_id, segment_id, log_start))
            #取所有buffer server返回元数据，按log_id排序
            meta_list = list()
            for s in self.buffer_server_socket[(volume_id,segment_id)].values():
                # logging.info('拉取虚拟盘{0} segment{1} 从log id {2}开始的日志元数据'.format(volume_id, segment_id, log_start))
                send(request, s)
                response = recv(s)
                rsp_len = len(response)
                start = 0
                while start < rsp_len:
                    state, volume_id_, log_id, offset, length, file_id, file_offset = struct.unpack('>IIQQIII', response[start: start+36])
                    #这里由于buffer写入时经过拆分，分到多个线程，所以log_id小的不一定先落盘，导致元数据文件中间被填充为0000000....这里要防止这种情况！！！
                    if log_id !=0 or length != 0 or offset != 0:
                        meta_list.append((volume_id_, log_id, offset, length, file_id, file_offset))
                    start += 36
            #给meta_list按log_id排序
            meta_list.sort(key = lambda x:x[1])
            #抛弃尾部因为各个buffer server响应时间节点不同导致的元数据间断部分
            meta_list_len = len(meta_list)
            # logging.info('待拉取的log start为{0}'.format(log_start))
            if meta_list_len != 0:
                continuous_len = meta_list_len
                for i in range(0, meta_list_len):
                    if log_start + i != meta_list[i][1]:
                        continuous_len = i
                        break
                meta_list = meta_list[0:continuous_len]
                #统计新增的待拉取日志规模
                add_log_size = 0
                for meta in meta_list:
                    add_log_size += meta[3]
                #如果虚拟盘在，且状态未变动，则追加到本地元数据队列，更改self.volume_pull_meta_pos
                if (volume_id,segment_id) in self.segment_pull_meta_pos and segment_version == self.segment_change_time[(volume_id,segment_id)]:
                    self.segment_pull_meta_pos[(volume_id,segment_id)] += continuous_len
                    # logging.info('虚拟盘 {0} segment{1} 拉取到log {2}成功！！'.format(volume_id, segment_id, self.segment_pull_meta_pos[(volume_id,segment_id)]))
                if (volume_id,segment_id) in self.segment_log_meta and segment_version == self.segment_change_time[(volume_id,segment_id)]:
                    self.segment_log_meta[(volume_id, segment_id)].extend(meta_list)
                    self.segment_log_size[(volume_id, segment_id)] = self.segment_log_size[(volume_id, segment_id)] + add_log_size
            #解锁，这里不用对比version，因为buffer_server_socket加了锁，不会在线程执行过程中被修改状态
            for i in self.buffer_server_socket_state[(volume_id,segment_id)]:
                self.buffer_server_socket_state[(volume_id,segment_id)][i]=FREE
        except Exception as err:
            logging.info(err)

    def log_read(self):
        try:
            logging.info('log read start')
            self.merging_segment = (-1,-1)
            segment_index = 0
            while True:
                # step1.选中本次要合并日志的segment
                max_len = 0
                segment = None
                for volume_id,segment_id in self.segment_log_meta:
                    if len(self.segment_log_meta[(volume_id,segment_id)]) > max_len:
                        max_len = len(self.segment_log_meta[(volume_id,segment_id)])
                        segment = (volume_id, segment_id)
                # for volume_id,segment_id in self.segment_log_size:
                #     if self.segment_log_size[(volume_id,segment_id)] > max_len:
                #         max_len = self.segment_log_size[(volume_id,segment_id)]
                #         segment = (volume_id, segment_id)
                # segment = list(self.segment_log_meta.keys())[segment_index]
                # segment_index = (segment_index + 1)%len(self.segment_log_meta)
                # max_len = len(self.segment_log_meta[(segment[0], segment[1])])
                if segment is None or max_len == 0:
                    time.sleep(0.1)
                    continue
                self.log_merge_end_event = True
                self.merging_segment = segment
                total_log_size = 0
                for volume_id, segment_id in self.segments:
                    total_log_size += self.segment_log_size[(volume_id, segment_id)]
                total_log_size = int(math.ceil(total_log_size/1024/1024))
                volume_id,segment_id = segment
                segment_log_size = int(math.ceil(self.segment_log_size[(volume_id, segment_id)]/1024/1024))
                logging.info('剩余待拉取合并日志规模为 {0} MB'.format(total_log_size))
                logging.info('选中volume_id {0}, segment_id {1}, 共 {2} 条日志项，{3} MB数据'.format(volume_id, segment_id, max_len, segment_log_size))
                # step2.执行合并，并指定日志合并速度(传输total_log_size给req server，由其计算每KB数据间隔拉取时间)
                accumulated_log_size = 0
                request_list = list()
                request_list.append(struct.pack('>BIIII', PULL_LOG, volume_id, segment_id, total_log_size, segment_log_size))
                while len(self.segment_log_meta[(volume_id,segment_id)]) != 0:
                    meta = self.segment_log_meta[(volume_id,segment_id)].popleft()
                    volume_id, log_id, offset, length, file_id, file_offset = meta
                    request_list.append(struct.pack('>IIQQIII', volume_id, segment_id, log_id, offset, length, file_id, file_offset))
                    # accumulated_log_size += length
                    self.segment_log_send[(volume_id, segment_id)] = log_id
                    self.log_size[(volume_id, segment_id)].append(int(math.ceil(length/float(4096))))
                    self.inflight_merging_traffic += int(math.ceil(length/float(4096)))
                    self.segment_log_size[(volume_id, segment_id)] = self.segment_log_size[(volume_id, segment_id)] - length
                    # if accumulated_log_size >= 0.01*total_log_size*1024*1024:
                    #     break
                # logging.info('本次截取 {0} MB数据'.format(accumulated_log_size/(1024*1024)))
                request = bytearray().join(request_list)
                if len(request_list) > 1:
                    self.segment_log_read(volume_id, segment_id, request)
                # step3.等待segment日志合并完毕
                while self.inflight_merging_traffic != 0:
                    time.sleep(0.001)
                self.log_merge_end_event = False
                logging.info('合并结束')
                time.sleep(0.001)
        except Exception as err:
            logging.info('合并日志错误{0}'.format(err))

    '''调用req server进行日志拉取'''
    def segment_log_read(self, volume_id, segment_id, request):
        s = self.req_server_socket[(volume_id,segment_id)]
        try:
            send(request, s)
        except Exception as err:
            logging.info(err)

    '''周期性检查各个虚拟盘当前日志头，以此计算还有多少流量已经开始异步合并但还未合并成功。例如该值为40。假设要保证异步合并不超过100，那么本次就该继续执行100-50合并操作'''
    def update_log_head_loop(self):
        while True:
            thread_list = list()
            #遍历所有本机segment，拉取最新的log_head
            for volume_id, segment_id in self.segments:
                thread_list.append(gevent.spawn(self.update_log_head, volume_id, segment_id))
            #等待这次拉取结束后执行下次拉取
            gevent.wait(thread_list)
            time.sleep(0.001)
            
    '''拉取单个虚拟盘的日志头'''
    def update_log_head(self, volume_id, segment_id):
        if (volume_id, segment_id)  not in self.req_server_socket_for_head:
            return
        s = self.req_server_socket_for_head[(volume_id, segment_id)]
        request = struct.pack('>BII', GET_LOG_HEAD, volume_id, segment_id)
        send(request, s)
        try:
            response = recv(s)
            log_head = struct.unpack('>Q', response[0:8])[0]
            if log_head > self.segment_log_recv[(volume_id, segment_id)]:
                # 删除log_size
                for i in range(0, log_head - self.segment_log_recv[(volume_id, segment_id)]):
                    length = self.log_size[(volume_id, segment_id)].popleft()
                    self.inflight_merging_traffic -= length
                self.segment_log_recv[(volume_id, segment_id)] = log_head
                self.log_merge_succeed_event.set()
            # logging.info('虚拟盘 {0} segment{1} log_head_recv 更新为 {2}！！'.format(volume_id, segment_id, log_head))
        except Exception as err:
            logging.info('原有socket废弃！')
            logging.info("recv_data error:{}".format(err))

    def get_log_head(self, volume_id, segment_id):
        if (volume_id, segment_id) not in self.req_server_socket_for_head:
            return
        s = self.req_server_socket_for_head[(volume_id, segment_id)]
        request = struct.pack('>BIII', GET_LOG_HEAD, volume_id, segment_id, 1)
        send(request, s)
        try:
            while True:
                response = recv(s)
                log_head = struct.unpack('>Q', response[0:8])[0]
                # logging.info('虚拟盘 {0} segment{1} log_head_recv 更新为 {2}！！'.format(volume_id, segment_id, log_head))
                if log_head > self.segment_log_recv[(volume_id, segment_id)]:
                    for i in range(0, log_head - self.segment_log_recv[(volume_id, segment_id)]):
                        length = self.log_size[(volume_id, segment_id)].popleft()
                        self.inflight_merging_traffic -= length
                    self.segment_log_recv[(volume_id, segment_id)] = log_head
                    self.log_merge_succeed_event.set()
                # logging.info('虚拟盘 {0} segment{1} log_head_recv 更新为 {2}！！'.format(volume_id, segment_id, log_head))
        except Exception as err:
            logging.info('segment删除，get log head socket废弃！')
            logging.info("recv_data error:{}".format(err))

    '''向本异步写模块中添加指定segment'''
    def add_segment(self, request, client_socket):
        volume_id, segment_id, port, log_head = struct.unpack('>IIIQ', request[0:4+4+4+8])
        logging.info('add volume id {0} segment_id {1} req port {2} log head {3}'.format(volume_id, segment_id, port, log_head))
        self.segment_change_time[(volume_id,segment_id)] = time.time()
        self.segment_log_meta[(volume_id,segment_id)] = collections.deque()
        self.segment_log_size[(volume_id, segment_id)] = 0
        self.segment_2_req_port[(volume_id,segment_id)] = port
        # req_server_socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # s.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,True)
        s.connect(('127.0.0.1',port))
        self.req_server_socket[(volume_id,segment_id)] = s
        self.req_server_socket_state[(volume_id,segment_id)] = FREE
        # req_server_socket_for_head
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # s.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,True)
        s.connect(('127.0.0.1',port))
        self.req_server_socket_for_head[(volume_id,segment_id)] = s
        # 拉取volume对应buffer server信息，连接socket
        self.add_buffer_server(volume_id,segment_id)
        self.add_buffer_server_socket(volume_id,segment_id)
        # 设置下一个要异步拉取的日志id，即发送来的log_head
        self.segment_pull_meta_pos[(volume_id,segment_id)] = log_head
        # 初始segment本地记录的log_head为
        self.segment_log_send[(volume_id, segment_id)] = log_head - 1
        self.segment_log_recv[(volume_id, segment_id)] = log_head
        self.log_size[(volume_id, segment_id)] = collections.deque()
        self.segments.append((volume_id,segment_id))
        self.set_req_server_log_head(volume_id,segment_id)
        # 创建更新log_head线程
        gevent.spawn(self.get_log_head, volume_id, segment_id)
        response = bytearray()
        send(response, client_socket)
        logging.info('add volume id {0} segment_id {1} req port {2} log head {3} Succeed!'.format(volume_id, segment_id, port, log_head))

    '''找manager确认指定segment buffer_server的ip和port.'''
    def add_buffer_server(self, volume_id, segment_id):
        request = struct.pack('>BI',GET_BUFFER_PROXY_INFO, volume_id)
        manager_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        manager_socket.connect((self.manager_ip,self.manager_port))
        send(request, manager_socket)
        response = recv(manager_socket)
        manager_socket.close()
        rsp_len = len(response)
        start = 0
        buffer_server_list = list()
        while start < rsp_len:
            ip_len = struct.unpack('>I',response[start:start+4])[0]
            start += 4
            ip = response[start:start+ip_len].decode(encoding='utf-8')
            start += ip_len
            port = struct.unpack('>I', response[start:start+4])[0]
            start += 4
            buffer_server_list.append((ip, port))
        self.buffer_server_dict[(volume_id,segment_id)] = buffer_server_list

    '''连接指定segment的buffer server，每个一个socket'''
    def add_buffer_server_socket(self, volume_id, segment_id):
        self.buffer_server_socket[(volume_id,segment_id)] = dict()
        self.buffer_server_socket_state[(volume_id,segment_id)] = dict()
        for i in range(0, len(self.buffer_server_dict[(volume_id,segment_id)])):
            ip = self.buffer_server_dict[(volume_id,segment_id)][i][0]
            port = self.buffer_server_dict[(volume_id,segment_id)][i][1]
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # s.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,True)
            s.connect((ip,port))
            self.buffer_server_socket[(volume_id,segment_id)][i] = s
            self.buffer_server_socket_state[(volume_id,segment_id)][i] = FREE

    '''设置req server下一个应该异步写入的log id，初始化req server异步刷盘'''
    def set_req_server_log_head(self, volume_id, segment_id):
        if (volume_id,segment_id) not in self.segments:
            return
        if (volume_id,segment_id) in self.segment_pull_meta_pos:
            log_head = self.segment_pull_meta_pos[(volume_id,segment_id)]
            request = struct.pack('>BIIQ', SET_LOG_HEAD, volume_id, segment_id, log_head)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # s.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,True)
            s.connect(('127.0.0.1',self.segment_2_req_port[(volume_id,segment_id)]))
            send(request, s)
            s.close()
            # logging.info('设置虚拟盘{0} segment{1} req server log head {2}'.format(volume_id, segment_id, log_head))

    '''从本异步写模块中删除指定虚拟盘'''
    def remove_segment(self, request, client_socket):
        volume_id, segment_id = struct.unpack('>II', request[0:4+4])
        if (volume_id, segment_id) == self.merging_segment:
            while self.log_merge_end_event:
                time.sleep(0.001)
        # while self.log_merge_end_event:
        #     time.sleep(0.001)
        logging.warning('remove volume id {0} segment_id {1}'.format(volume_id, segment_id))
        self.segment_change_time[(volume_id,segment_id)] = time.time()
        # logging.info(self.segments)
        # print(self.segment_log_meta)
        if (volume_id,segment_id) in self.segments:
            self.segments.remove((volume_id,segment_id))
        else:
            logging.warning('removing a non-exist disk!!!!')
        if (volume_id,segment_id) in self.segment_pull_meta_pos:
            del(self.segment_pull_meta_pos[(volume_id,segment_id)])
        if (volume_id,segment_id) in self.segment_log_meta:
            del(self.segment_log_meta[(volume_id,segment_id)])
        if (volume_id,segment_id) in self.segment_log_size:
            del(self.segment_log_size[(volume_id, segment_id)])
        if (volume_id,segment_id) in self.req_server_socket_state:
            del(self.req_server_socket_state[(volume_id,segment_id)])
        if (volume_id,segment_id) in self.req_server_socket:
            self.req_server_socket[(volume_id,segment_id)].close()
            del(self.req_server_socket[(volume_id,segment_id)])
        if (volume_id,segment_id) in self.req_server_socket_for_head:
            self.req_server_socket_for_head[(volume_id,segment_id)].close()
            del(self.req_server_socket_for_head[(volume_id,segment_id)])
        if len(self.log_size[(volume_id, segment_id)]) != 0:
            self.inflight_merging_traffic -= sum(self.log_size[(volume_id, segment_id)])
            logging.info(self.log_size[(volume_id, segment_id)])
        del self.log_size[(volume_id, segment_id)]
        self.log_merge_succeed_event.set()
        logging.warning('remove volume id {0} segment_id {1} step 1'.format(volume_id, segment_id))
        # 断开segment对应buffer server socket，删除buffer server信息
        if (volume_id, segment_id) in self.buffer_server_socket:
            self.remove_buffer_server_socket(volume_id,segment_id)
        if (volume_id, segment_id) in self.buffer_server_dict:
            self.remove_buffer_server(volume_id,segment_id)
        logging.warning('remove volume id {0} segment_id {1} step 2'.format(volume_id, segment_id))
        response = bytearray()
        send(response, client_socket)

    '''断开socket'''
    def remove_buffer_server_socket(self, volume_id, segment_id):
        #由于前面segment已经从self.segmnets删除，不会有新的任务试图竞争socket，只要发现FREE就可以删除; 由于log_pull_pos被删了，旧的任务也不会在socket更新后按旧的log_pull_pos拉日志元数据
        for i in self.buffer_server_socket[(volume_id,segment_id)]:
            while self.buffer_server_socket_state[(volume_id,segment_id)][i] != FREE:
                time.sleep(0.001)
            self.buffer_server_socket[(volume_id,segment_id)][i].close()
        del(self.buffer_server_socket[(volume_id,segment_id)])
        del(self.buffer_server_socket_state[(volume_id,segment_id)])
    
    def remove_buffer_server(self, volume_id, segment_id):
        del(self.buffer_server_dict[(volume_id,segment_id)])

def main(argv):
    # p.nice(-20)
    opts, args = getopt.getopt(argv,"i:p:m:o:t:l:",["log_merger_id=","port=",'manager_ip=','manager_port=','inflight_merging_traffic_max=','merge_log='])
    for opt, arg in opts:
        if opt == '-h':
            print('LogMerger.py -i <ip> -p <port>')
            sys.exit()
        elif opt in ("-i", "--log_merger_id"):
            log_merger_id = arg
        elif opt in ("-p", "--port"):
            port = arg
        elif opt in ("-m", "--manager_ip"):
            manager_ip = arg
        elif opt in ("-o", "--manager_port"):
            manager_port = arg
        elif opt in ("-t", "--inflight_merging_traffic_max"):
            inflight_merging_traffic_max = arg
        elif opt in ("-l", "--merge_log"):
            merge_log = arg

    # 创建一个后台扫描进程
    my_log_merger = LogMerger(log_merger_id=log_merger_id, port=port, manager_ip=manager_ip, manager_port=manager_port, inflight_merging_traffic_max = inflight_merging_traffic_max, merge_log = merge_log)
    my_log_merger.run()

if __name__ == "__main__":
    main(sys.argv[1:])



