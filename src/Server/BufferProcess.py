#!/usr/bin/python3
#coding:utf-8
from gevent import monkey;monkey.patch_all()
import sys
import struct
import gevent
import socket
from gevent.event import Event
import math
from ..Properties.config import *
import time
import getopt
import collections
from ..Utils.DataTrans import *
import struct
import math
import logging
import psutil, os
import asyncio
import aiofiles
from aiofile import async_open
import gevent.selectors
from threading import Thread

p = psutil.Process(os.getpid())

engine_dict = dict()
server_socket = process_id = buffer_path = buffer_server_num = buffer_server_id = manager_ip = manager_port = port = None

def start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

class EventLoop(asyncio.SelectorEventLoop):
    def __init__(self, selector=None):
        super().__init__(selector or gevent.selectors.DefaultSelector())

    def run_forever(self):
        greenlet = gevent.spawn(super(EventLoop, self).run_forever)
        greenlet.join()

class BufferEngine:
    '''**********************************************************************************************
        init
    **********************************************************************************************'''
    def __init__(self, buffer_path, buffer_server_id, buffer_server_num, manager_ip, manager_port, volume_id, port):
        logging.basicConfig(level=logging.INFO,
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )
        self.buffer_server_id = int(buffer_server_id)
        self.buffer_server_num = int(buffer_server_num)
        self.port = port
        self.volume_id = int(volume_id)
        self.busy_flag = 0
        self.dir_path = buffer_path
        self.fd = dict()
        self.meta_fd = dict()
        self.log_head = dict()
        #strip文件的使用，删除状态
        self.strip_used_capacity = dict()
        self.strip_gc_capacity = dict()
        # 连接manager,汇报自己的监听端口
        self.set_manager(manager_ip, int(manager_port))
        # 未完成的请求队列
        self.write_pending_queue = collections.deque()
        self.read_pending_queue = collections.deque()
        # 完成的请求队列
        self.write_finish_queue = collections.deque()
        self.read_finish_queue = collections.deque()
        # 拉取到的日志队列
        self.log_entry_queue = dict()
        self.log_entry_event = dict()
        # segment迁移需要关闭get_log_entry线程，关闭标志
        self.stop_get_log_entry_flag = dict()
        # Event
        self.process_write_event = Event()
        self.process_read_event = Event()
        self.get_finished_read_event = Event()
        self.get_finished_write_event = Event()
        self.data_seg = bytearray('8'*1024*1024, encoding='utf-8')
        # msc reporter
        self.init_merge_speed_controller_socket()
        self.queue_state = 0
        self.report_msc_event = Event()
        self.in_queue_num_read = self.in_queue_num_write = 0
        self.out_queue_num_read = self.out_queue_num_write = 0
        # 后台GC线程
        gevent.spawn(self.garbage_collection)
        # 向merge speed controller实时汇报队列状态
        gevent.spawn(self.report_msc)
        self.send_port()

    def init_merge_speed_controller_socket(self):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect('/home/k8s/wzhzhu/merge_ctrl_uds')
        self.mscs_send  = s
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect('/home/k8s/wzhzhu/merge_ctrl_uds')
        self.mscs_recv  = s

    '''根据所给volume_id(self变量),segment_id和file_id创建存日志数据的data文件, 并记录其使用情况和gc情况'''
    def create_data_file(self, segment_id, file_id):
        logging.info('create data file segment{0} file{1}'.format(segment_id, file_id))
        file_name = 'data' + str(self.volume_id) + '_' + str(segment_id) + '_' + str(file_id)
        fd = open(self.dir_path+ file_name,'wb+')
        # fd.close()
        # fd = None
        self.fd[(segment_id, file_id)] = fd
        self.strip_used_capacity[(segment_id, file_id)] = 0
        self.strip_gc_capacity[(segment_id, file_id)] = 0
        # logging.info('create succeed')

    '''根据所给volume_id(self变量),segment_id和file_id删除存日志数据的data文件'''
    def remove_data_file(self, segment_id, file_id):
        file_name = 'data' + str(self.volume_id) + '_' + str(segment_id) + '_' + str(file_id)
        self.fd[(segment_id, file_id)].close()
        del(self.fd[(segment_id, file_id)])
        os.remove(self.dir_path+file_name)
        logging.info('remove data file segment{0} file{1}'.format(segment_id, file_id))

    '''根据所给volume_id(self变量),segment_id创建存日志元数据的meta文件'''
    def create_meta_file(self, segment_id):
        logging.info('create meta file segment{0}'.format(segment_id))
        file_name = 'meta' + str(self.volume_id) + '_' + str(segment_id)  
        fd = open(self.dir_path+ file_name,'wb+')
        # fd.close()
        # fd = None
        self.meta_fd[segment_id] = fd
        self.log_head[segment_id] = 0
    
    '''连接manager'''
    def set_manager(self, manager_ip, manager_port):
        #连接manager
        self.manager_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.manager_socket.connect((manager_ip, manager_port))

    '''准备工作做好之后通知manager自己负责的volume_id, buffer server id 以及port'''
    def send_port(self):
        s = self.manager_socket
        request = struct.pack('>B',NOTIFY_BUFFER_PROXY_PORT)
        request += struct.pack('>I', self.volume_id)
        request += struct.pack('>I', self.buffer_server_id)
        request += struct.pack('>I', self.port)
        send(request, s)
        s.close()

    def report_buffer_in_queue_write(self, request, client_socket):
        try:
            write_queue_len = struct.unpack('>Q',request[0:8])[0]
            self.in_queue_num_write = write_queue_len
            self.report_msc_event.set()
        except Exception as err:
            logging.info(err)

    def report_buffer_in_queue_read(self, request, client_socket):
        try:
            read_queue_len = struct.unpack('>Q',request[0:8])[0]
            self.in_queue_num_read = read_queue_len
            self.report_msc_event.set()
        except Exception as err:
            logging.info(err)

    def buffer_write(self,request,client_socket):
        fd_list = list()
        meta_fd_list = list()
        start = 0
        request_len = len(request)
        while start < request_len:
            req_id, sub_req_id, volume_id, segment_id, log_id, offset, length, file_id, file_offset = struct.unpack('>QIIIQQIII',request[start: start + 8+4+4+4+8+8+4+4+4])
            # logging.info('收到写请求 req_id {0}, volume_id {1}, segment_id {2}, log_id {3}, offset {4}, length {5}, file_id {6}, file_offset {7}'.format(req_id, volume_id, segment_id, log_id, offset, length, file_id, file_offset))
            start += 8+4+4+4+8+8+4+4+4
            data = request[start: start + length]
            start += length
            # asyncio.run_coroutine_threadsafe(self.process_write(req_id, volume_id, segment_id, log_id, offset, length, file_id, file_offset, data),self.loop)
            self.process_write(req_id, sub_req_id, volume_id, segment_id, log_id, offset, length, file_id, file_offset, data)

            # self.write_pending_queue.append((req_id, volume_id, segment_id, log_id, offset, length, file_id, file_offset, data))

    def process_write(self, req_id, sub_req_id, volume_id, segment_id, log_id, offset, length, file_id, file_offset, data):
        # 写入数据
        if (segment_id, file_id) not in self.fd:
            self.create_data_file(segment_id, file_id)
        data = bytes(data)
        fd = self.fd[(segment_id, file_id)]
        fd.seek(file_offset)
        fd.write(data)
        fd.flush()
        self.strip_used_capacity[(segment_id, file_id)] += int(math.ceil(length/float(4096)))*4096
        #写入元数据
        if segment_id not in self.meta_fd:
            self.create_meta_file(segment_id)
        meta_offset = int(log_id / self.buffer_server_num)*36
        meta_data = struct.pack('>IIQQIII', VALID, volume_id, log_id, offset, length, file_id, file_offset)
        fd = self.meta_fd[segment_id]
        fd.seek(meta_offset)
        fd.write(meta_data)
        fd.flush()
        self.write_finish_queue.append((req_id, sub_req_id))
        self.get_finished_write_event.set()

    async def async_process_write(self, req_id, volume_id, segment_id, log_id, offset, length, file_id, file_offset, data):
        # 写入数据
        if (segment_id, file_id) not in self.fd:
            self.create_data_file(segment_id, file_id)
        # f = self.fd[(segment_id, file_id)]
        file_name = 'data' + str(self.volume_id) + '_' + str(segment_id) + '_' + str(file_id)
        file_name = self.dir_path + file_name
        data = bytes(data)
        fd = self.fd[(segment_id, file_id)]
        fd.seek(file_offset)
        fd.write(data)
        fd.flush()
        # async with async_open(file_name, mode='rb+') as f:
        #     f.seek(file_offset)
            # await f.write(data)
            # await afp.fsync()
        self.strip_used_capacity[(segment_id, file_id)] += int(math.ceil(length/float(4096)))*4096
        #写入元数据
        if segment_id not in self.meta_fd:
            self.create_meta_file(segment_id)
        # f = self.meta_fd[segment_id]
        file_name = 'meta' + str(self.volume_id) + '_' + str(segment_id)
        file_name = self.dir_path + file_name
        meta_offset = int(log_id / self.buffer_server_num)*36
        meta_data = struct.pack('>IIQQIII', VALID, volume_id, log_id, offset, length, file_id, file_offset)
        fd = self.meta_fd[segment_id]
        fd.seek(meta_offset)
        fd.write(meta_data)
        fd.flush()
        # async with async_open(file_name, mode='rb+') as f:
        #     f.seek(meta_offset)
        #     await f.write(meta_data)
            # await afp.fsync()
        # for i in range(0,10):
        #     await asyncio.sleep(0.001)
        self.write_finish_queue.append((req_id, volume_id, segment_id, log_id))
        self.get_finished_write_event.set()

    '''从指定文件指定偏移读取数据,客户端做了batch'''
    def buffer_read(self,request,client_socket):
        start = 0
        response = bytearray()
        req_len = len(request)
        while start < req_len:
            req_id, sub_req_id, volume_id, segment_id, log_id, offset, length, file_id, file_offset = struct.unpack('>QIIIQQIII',request[start: start + 8+4+4+4+8+8+4+4+4])
            # logging.info('收到读请求 req_id {0}, volume_id {1}, segment_id {2}, log_id {3}, offset {4}, length {5}, file_id {6}, file_offset {7}'.format(req_id, volume_id, segment_id, log_id, offset, length, file_id, file_offset))
            start += 8+4+4+4+8+8+4+4+4
            self.process_read(req_id, sub_req_id, volume_id, segment_id, log_id, offset, length, file_id, file_offset)

    def process_read(self, req_id, sub_req_id, volume_id, segment_id, log_id, offset, length, file_id, file_offset):
        try:
            #读取元数据验证与请求的元数据是否匹配
            flag = 0
            if segment_id in self.meta_fd:
                fd = self.meta_fd[segment_id]
                meta_offset = int(log_id /self.buffer_server_num)*36
                fd.seek(meta_offset)
                meta_data = fd.read(36)
                if len(meta_data) == 36:
                    state, volume_id2, log_id2, offset2, length2, file_id2, file_offset2 = struct.unpack('>IIQQIII', meta_data)
                    if state == VALID:
                        flag = 1
                    else:
                        flag = 2
                else:
                    flag = 3
            else:
                flag = 3

            if flag == 1:
                fd = self.fd[(segment_id, file_id)]
                fd.seek(file_offset)
                data = fd.read(length)
                self.read_finish_queue.append((req_id, sub_req_id, log_id, offset, data))
                self.get_finished_read_event.set()
            elif flag == 2:
                logging.info((volume_id, sub_req_id, log_id, offset, length))
                logging.info('reading obsolete data!!')
                # sys.exit()
                self.read_finish_queue.append((req_id, sub_req_id, log_id, offset, 2))
                self.get_finished_read_event.set()
            else:
                logging.info((volume_id, sub_req_id, log_id, offset, length))
                logging.info('reading yet uncompleted written data!!')
                # sys.exit()
                self.read_finish_queue.append((req_id, sub_req_id, log_id, offset, 3))
                self.get_finished_read_event.set()
        except Exception as err:
            logging.info(err)

    '''**********************************************************************************************
        获取已经完成的读写请求信息
    **********************************************************************************************'''
    def get_finished_write(self, request, client_socket):
        while True:
            self.get_finished_write_event.wait()
            self.out_queue_num_write = len(self.write_finish_queue)
            self.report_msc_event.set()
            time.sleep(0)
            while len(self.write_finish_queue)!=0:
                req_id, sub_req_id = self.write_finish_queue.popleft()
                response = struct.pack('>QI', req_id, sub_req_id)
                # logging.info('send log id {0}'.format(log_id))
                self.busy_flag += 1
                send(response, client_socket)
                self.busy_flag -= 1
            if len(self.write_finish_queue)==0:
                self.get_finished_write_event.clear()
            self.out_queue_num_write = 0
            self.report_msc_event.set()

    def get_finished_read(self, request, client_socket):
        while True:
            self.get_finished_read_event.wait()
            self.out_queue_num_read = len(self.read_finish_queue)
            self.report_msc_event.set()
            time.sleep(0)
            while len(self.read_finish_queue)!=0:
                req_id, sub_req_id, log_id, offset, data = self.read_finish_queue.popleft()
                if data == 2:
                    response = struct.pack('>QIQQII', req_id, sub_req_id, log_id, offset, 0, 2)
                elif data == 3:
                    response = struct.pack('>QIQQII', req_id, sub_req_id, log_id, offset, 0, 3)
                else:
                    response = bytearray()
                    response.extend(struct.pack('>QIQQI', req_id, sub_req_id, log_id, offset, len(data)))
                    response.extend(bytearray(data))
                self.busy_flag += 1
                send(response, client_socket)
                self.busy_flag -= 1
            if len(self.read_finish_queue)==0:
                self.get_finished_read_event.clear()
            self.out_queue_num_read = 0
            self.report_msc_event.set()

    def get_segment_log_head(self, request, client_socket):
        #遍历meta文件，找到第一个valid的log id, 即可能需要拉元数据异步写入的log
        volume_id, segment_id = struct.unpack('>II',request[0:4+4])
        if segment_id not in self.meta_fd:
            log_head = 0
        else:
            log_head = self.log_head[segment_id]
        response = struct.pack('>Q', log_head)
        send(response, client_socket)

    '''获得指定segment 日志id在[start:]区间的所有log元数据'''
    def segment_log_meta_read(self, request, client_socket):
        volume_id, segment_id, log_start = struct.unpack('>IIQ',request[0:4+4+8])
        # logging.info('读取虚拟盘 {0}segment {1}从{2}开始的日志元数据'.format(volume_id, segment_id, log_start))
        if segment_id in self.meta_fd:
            fd = self.meta_fd[segment_id]
            #找到第一个大于等于log_start的元数据位置
            fd.seek(int(math.ceil((log_start - self.buffer_server_id)/float(self.buffer_server_num)))*36)
            response_list = list()
            while True:
                meta = fd.read(36)
                if len(meta) == 0:
                    break
                response_list.append(meta)
            response = bytearray().join(response_list)
        else:
            response = bytearray()
        send(response, client_socket)

    '''同步读，拉取日志的时候用得到'''
    def buffer_pull(self,request,client_socket):
        start = 0
        req_len = len(request)
        response_list = list()
        pull_list = list()
        while start < req_len:
            volume_id, segment_id, log_id, offset, length, file_id, file_offset = struct.unpack('>IIQQIII',request[start: start+4+4+8+8+4+4+4])
            pull_list.append((volume_id, segment_id, log_id, offset, length, file_id, file_offset))
            start += 4+4+8+8+4+4+4
        for req in pull_list:
            volume_id, segment_id, log_id, offset, length, file_id, file_offset = req
            f = self.fd[(segment_id, file_id)]
            f.seek(file_offset)
            data = f.read(length)
            response = bytearray()
            response.extend(struct.pack('>BI', SUCCEED, length))
            response.extend(data)
            send(response, client_socket)
        response = bytearray()
        response.extend(struct.pack('>BI', SUCCEED, 0))
        send(response, client_socket)
        #     response_list.append(struct.pack('>BI', SUCCEED, length))
        #     response_list.append(data)
        # response = bytes().join(response_list)
        # send(response, client_socket)

    def pull_log_entry(self,request,client_socket):
        start = 0
        volume_id, segment_id, log_id, offset, length, file_id, file_offset = struct.unpack('>IIQQIII',request[start: start+4+4+8+8+4+4+4])
        f = self.fd[(segment_id, file_id)]
        f.seek(file_offset)
        data = f.read(length)
        self.log_entry_queue[segment_id].append((log_id, offset, length, data))
        self.log_entry_event[segment_id].set()
        # logging.info((log_id, offset, length, segment_id))
    
    def get_log_entry(self,request,client_socket):
        try:
            logging.info('start get log entry')
            volume_id, segment_id = struct.unpack('>II',request[0:4+4])
            self.stop_get_log_entry_flag[segment_id] = 0
            self.log_entry_queue[segment_id] = collections.deque()
            self.log_entry_event[segment_id] = Event()
            log_entry_queue = self.log_entry_queue[segment_id]
            response = bytearray()
            send(response, client_socket)
            while True:
                self.log_entry_event[segment_id].wait()
                if self.stop_get_log_entry_flag[segment_id] == 1:
                    logging.info('get_log_entry结束')
                    self.stop_get_log_entry_flag[segment_id] = 0
                    return
                while len(log_entry_queue) != 0:
                    log_id, offset, length, data = log_entry_queue.popleft()
                    response = bytearray()
                    response.extend(struct.pack('>QQI', log_id, offset, length))
                    response.extend(data)
                    # while self.busy_flag != 0:
                    #     # 等待idle时刻
                    #     time.sleep(0)
                    # logging.info((log_id, offset, length, segment_id))
                    send(response, client_socket)
                self.log_entry_event[segment_id].clear()
        except Exception as err:
            logging.info(err)

    def stop_get_log_entry(self,request,client_socket):
        volume_id, segment_id = struct.unpack('>II',request[0:4+4])
        self.log_entry_event[segment_id].set()
        self.stop_get_log_entry_flag[segment_id] = 1
        while self.stop_get_log_entry_flag[segment_id] == 1:
            time.sleep(0.001)
        response = bytearray()
        send(response, client_socket)

    def buffer_remove(self,request,client_socket):
        req_len = len(request)
        start = 0
        meta_fd_list = list()
        volume_id, segment_id, log_tail= struct.unpack('>IIQ',request[start:start+4+4+8])
        # logging.info('virtual disk{0} segment{1} logs before {2} are taged obselete'.format(volume_id, segment_id, log_tail))
        if segment_id not in self.meta_fd:
            return
        log_head = self.log_head[segment_id]
        logging.info('virtual disk{0} segment{1} logs before {2} are taged obselete, current log_head is {3}'.format(volume_id, segment_id, log_tail, log_head))
        if log_tail > log_head:
            #暂时元数据大小为36 bytef
            f = self.meta_fd[segment_id]
            meta_offset = int(math.ceil((log_head - self.buffer_server_id)/float(self.buffer_server_num)))*36
            i = 0
            while True:
                # logging.info(meta_offset+ i*36)
                f.seek(meta_offset+ i*36)
                meta_data = f.read(36)
                if len(meta_data) == 0:
                    break
                state, volume_id, log_id, offset, length, file_id, file_offset = struct.unpack('>IIQQIII', meta_data)
                if log_id >= log_tail:
                    break
                elif log_id == 0 and volume_id ==0 and length ==0 :
                    break
                f.seek(meta_offset+ i*36)
                update = struct.pack('>I', OBSELETE)
                f.write(update)
                # 更新gc capacity
                self.strip_gc_capacity[(segment_id, file_id)] += int(math.ceil(length/float(4096)))*4096
                i += 1
            #批量落盘
            f.flush()
            #更新log_head
            self.log_head[segment_id] = log_tail

    '''拉取哪些条带已经写满且已经回收完成，可以回收；被回收的条带会被清空这些信息，不会再次发送'''
    def garbage_collection(self):
        while True:
            file_info = list(self.fd.keys())
            for segment_id, file_id in file_info:
                if self.strip_used_capacity[(segment_id, file_id)] >= 1024*1024 and self.strip_gc_capacity[(segment_id, file_id)] == self.strip_used_capacity[(segment_id, file_id)]:
                    self.remove_data_file(segment_id, file_id)
                    time.sleep(0.001)
                # if self.strip_used_capacity[(segment_id, file_id)] >= 1024*1024:
                #     self.remove_data_file(segment_id, file_id)
                #     time.sleep(0.001)
            time.sleep(5)
    '''**********************************************************************************************
        merge speed controller
    **********************************************************************************************'''
    def report_msc(self):
        try:
            last_in_queue_num  = last_out_queue_num = 0
            while True:
                self.report_msc_event.wait()
                self.report_msc_event.clear()
                in_queue_num = self.in_queue_num_write + self.in_queue_num_read
                out_queue_num = self.out_queue_num_write + self.out_queue_num_read
                # in_queue_num = self.in_queue_num_write
                # out_queue_num = self.out_queue_num_read
                if (last_in_queue_num == 0 and in_queue_num !=0) or (last_in_queue_num != 0 and in_queue_num ==0):
                    last_in_queue_num = in_queue_num
                    request = struct.pack('>BIQ',REPORT_BUFFER_IN, self.volume_id, in_queue_num)
                    send(request, self.mscs_send)
                    # logging.info('汇报in queue num {0}'.format(in_queue_num))
                if (last_out_queue_num == 0 and out_queue_num !=0) or (last_out_queue_num != 0 and out_queue_num ==0):
                    last_out_queue_num = out_queue_num
                    request = struct.pack('>BIQ',REPORT_BUFFER_OUT, self.volume_id, out_queue_num)
                    send(request, self.mscs_send)
                    # logging.info('汇报out queue num {0}'.format(out_queue_num))
        except Exception as err:
            logging.info(err)

'''**********************************************************************************************
    主循环
**********************************************************************************************'''
def run():
    # loop = EventLoop()
    # self.loop = loop
    # t=Thread(target=start_loop, args=(self.loop,))
    # t.start()
    """启动，等待客户端到来"""
    logging.info('启动')
    global server_socket
    while True:
        client_socket, client_addr = server_socket.accept()     #等待客户端连接
        gevent.spawn(recv_data, client_socket, client_addr)

def recv_data(client_socket, client_addr):
    """处理数据"""
    try:
        global engine_dict, server_socket, process_id, buffer_path, buffer_server_num, buffer_server_id, manager_ip, manager_port, port
        while True:
            op = -1
            request = recv(client_socket)  # 接收数据
            if not request:    #客户端断开
                logging.info("{} offline".format(client_addr))
                client_socket.close()
                break
            op, volume_id =struct.unpack('>BI',request[0:1+4])
            start = 1+4
            if op == CREATE_ENGINE:
                logging.info('create engine')
                logging.info((buffer_path, buffer_server_id, buffer_server_num, manager_ip, manager_port, volume_id, port))
                engine_dict[volume_id] = BufferEngine(buffer_path, buffer_server_id, buffer_server_num, manager_ip, manager_port, volume_id, port)
                logging.info('create engine complete')
            else:
                engine = engine_dict[volume_id]
                if op==BUFFER_WRITE:
                    engine.buffer_write(request[start:],client_socket)
                elif op==BUFFER_READ:
                    engine.buffer_read(request[start:],client_socket)
                elif op==GET_BUFFER_FINISHED_READ:
                    engine.get_finished_read(request[start:],client_socket)
                elif op==GET_BUFFER_FINISHED_WRITE:
                    engine.get_finished_write(request[start:],client_socket)
                elif op==GET_SEGMENT_LOG_HEAD:
                    engine.get_segment_log_head(request[start:],client_socket)
                elif op==SEGMENT_LOG_META_READ:
                    engine.segment_log_meta_read(request[start:],client_socket)
                elif op==BUFFER_REMOVE:
                    engine.buffer_remove(request[start:],client_socket)
                elif op==BUFFER_PULL:
                    engine.buffer_pull(request[start:],client_socket)
                elif op==GET_LOG_ENTRY:
                    engine.get_log_entry(request[start:],client_socket)
                elif op==STOP_GET_LOG_ENTRY:
                    engine.stop_get_log_entry(request[start:],client_socket)
                elif op==PULL_LOG_ENTRY:
                    engine.pull_log_entry(request[start:],client_socket)
                elif op==REPORT_BUFFER_IN_QUEUE_READ:
                    engine.report_buffer_in_queue_read(request[start:],client_socket)
                elif op==REPORT_BUFFER_IN_QUEUE_WRITE:
                    engine.report_buffer_in_queue_write(request[start:],client_socket)
                elif op==SHUTDOWN:
                    response = bytearray()
                    send(response, client_socket)
                    sys.exit()
    except Exception as err:
        logging.info("recv_data error:{}".format(err))
    finally:
        logging.info('socket close!, addr = {0}, op = {1}'.format(client_addr, op))
        client_socket.close()

def main(argv):
    global engine_dict, server_socket, process_id, buffer_path, buffer_server_num, buffer_server_id, manager_ip, manager_port, port
    opts, args = getopt.getopt(argv,"hd:i:b:n:m:p:",['buffer_server_id=', 'process_id=' 'buffer_path=', 'buffer_server_num=','manager_ip=','manager_port='])
    for opt, arg in opts:
        if opt == '-h':
            logging.info('BufferProcess.py -d <buffer_server_id> -b <buffer_path> -n <buffer_server_num> -m <manager_ip> -p <manager_port>')
            sys.exit()
        elif opt in ("-b", "--buffer_path"):
            buffer_path = arg
        elif opt in ("-d", "--buffer_server_id"):
            buffer_server_id = int(arg)
        elif opt in ("-i", "--process_id"):
            process_id = int(arg)
        elif opt in ("-n", "--buffer_server_num"):
            buffer_server_num = int(arg)
        elif opt in ("-m", "--manager_ip"):
            manager_ip = arg
        elif opt in ("-p", "--manager_port"):
            manager_port = int(arg)

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
    # server_socket.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,True)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.listen(100000000)           # 设置为被动socket
    port = server_socket.getsockname()[1]

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((manager_ip, manager_port))
    request = struct.pack('>B',NOTIFY_BUFFER_PROCESS_PORT)
    request += struct.pack('>I', buffer_server_id)
    request += struct.pack('>I', process_id)
    request += struct.pack('>I', port)
    send(request, s)
    s.close()

    run()

if __name__ == "__main__":
    main(sys.argv[1:])



